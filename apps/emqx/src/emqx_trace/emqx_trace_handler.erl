%%--------------------------------------------------------------------
%% Copyright (c) 2018-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_trace_handler).

-include("logger.hrl").
-include("emqx_trace.hrl").
-include_lib("emqx/include/emqx_config.hrl").

-logger_header("[Tracer]").

%% APIs
-export([
    running/0,
    install/4,
    install/6,
    uninstall/1,
    filesync/1
]).

-export([
    mk_log_handler/1,
    log/2
]).

%% For logger handler filters callbacks
-export([
    filter_ruleid/2,
    filter_clientid/2,
    filter_topic/2,
    filter_ip_address/2
]).

-export([fallback_handler_id/2]).
-export([payload_encode/0]).

-export_type([log_handler/0]).

%% Tracer, prototype for a log handler:
-type tracer() :: #{
    name := binary(),
    filter := emqx_trace:filter(),
    namespace => maybe_namespace(),
    payload_encode := text | hidden | hex,
    payload_limit := non_neg_integer(),
    formatter => json | text
}.

%% Active log handler information:
-type handler_info() :: #{
    id => logger:handler_id(),
    name => binary(),
    filter => emqx_trace:filter(),
    namespace => maybe_namespace(),
    level => logger:level(),
    dst => file:filename() | console | unknown
}.

-record(filterctx, {
    name :: binary(),
    match :: string() | binary(),
    namespace :: maybe_namespace()
}).

%% Minimal represenation of a log handler, suitable for `log/2`.
-type filter_fun() :: {function(), #filterctx{}}.
-opaque log_handler() :: {logger:handler_id(), filter_fun()}.

-define(LOG_HANDLER_OLP_KILL_MEM_SIZE, 50 * 1024 * 1024).
-define(LOG_HANDLER_OLP_KILL_QLEN, 20000).

-type namespace() :: binary().
-type maybe_namespace() :: ?global_ns | namespace().

-define(LOG_HANDLER_FILESYNC_INTERVAL, 5_000).

-ifdef(TEST).
-undef(LOG_HANDLER_FILESYNC_INTERVAL).
-define(LOG_HANDLER_FILESYNC_INTERVAL, 100).
-endif.

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

-spec install(
    HandlerId :: logger:handler_id(),
    Name :: binary() | list(),
    Filter :: emqx_trace:filter(),
    Level :: logger:level() | all,
    LogFilePath :: string(),
    Formatter :: text | json
) -> ok | {error, term()}.
install(HandlerId, Name, Filter, Level, LogFile, Formatter) ->
    Who = #{
        name => ensure_bin(Name),
        filter => Filter,
        namespace => ?global_ns,
        payload_encode => payload_encode(),
        payload_limit => ?MAX_PAYLOAD_FORMAT_SIZE,
        formatter => Formatter
    },
    install(HandlerId, Who, Level, LogFile).

-spec install(logger:handler_id(), tracer(), logger:level() | all, string()) ->
    ok | {error, term()}.
install(HandlerId, Who, all, LogFile) ->
    install(HandlerId, Who, debug, LogFile);
install(HandlerId, Who, Level, LogFile) ->
    Config = #{
        level => Level,
        formatter => logger_formatter(Who),
        filter_default => stop,
        filters => logger_filters(Who),
        config => logger_config(LogFile)
    },
    Res = logger:add_handler(HandlerId, logger_disk_log_h, Config),
    show_prompts(Res, Who, "start_trace"),
    Res.

logger_config(LogFile) ->
    MaxBytes = emqx_config:get([trace, max_file_size]),
    #{
        type => halt,
        file => LogFile,
        max_no_bytes => MaxBytes,
        filesync_repeat_interval => ?LOG_HANDLER_FILESYNC_INTERVAL,
        overload_kill_enable => true,
        overload_kill_mem_size => ?LOG_HANDLER_OLP_KILL_MEM_SIZE,
        overload_kill_qlen => ?LOG_HANDLER_OLP_KILL_QLEN,
        %% disable restart
        overload_kill_restart_after => infinity
    }.

-spec uninstall(HandlerId :: atom()) -> ok | {error, term()}.
uninstall(HandlerId) ->
    Res = logger:remove_handler(HandlerId),
    show_prompts(Res, HandlerId, "stop_trace"),
    Res.

%% @doc Return all running trace handlers information.
-spec running() -> list(handler_info()).
running() ->
    lists:foldl(fun filter_handler_info/2, [], emqx_logger:get_log_handlers(started)).

-spec filesync(logger:handler_id()) -> ok | {error, any()}.
filesync(HandlerId) ->
    case logger:get_handler_config(HandlerId) of
        {ok, #{module := Mod}} ->
            try
                Mod:filesync(HandlerId)
            catch
                error:undef ->
                    {error, unsupported}
            end;
        Error ->
            Error
    end.

%%

-spec mk_log_handler(handler_info()) -> log_handler().
mk_log_handler(Info = #{id := HandlerId}) ->
    {HandlerId, mk_filter_fun(Info)}.

-spec log(logger:log_event(), [log_handler()]) -> ok.
log(Log, [{Id, {FilterFun, Ctx}} | Rest]) ->
    case FilterFun(Log, Ctx) of
        stop ->
            stop;
        ignore ->
            ignore;
        NLog ->
            case logger_config:get(logger, Id) of
                {ok, #{module := Module} = HandlerConfig0} ->
                    HandlerConfig = maps:without(?OWN_KEYS, HandlerConfig0),
                    try
                        Module:log(NLog, HandlerConfig)
                    catch
                        C:R:S ->
                            case logger:remove_handler(Id) of
                                ok ->
                                    logger:internal_log(
                                        error, {removed_failing_handler, Id, C, R, S}
                                    );
                                {error, {not_found, _}} ->
                                    %% Probably already removed by other client
                                    %% Don't report again
                                    ok;
                                {error, Reason} ->
                                    logger:internal_log(
                                        error,
                                        {removed_handler_failed, Id, Reason, C, R, S}
                                    )
                            end
                    end;
                {error, {not_found, _}} ->
                    ok
            end
    end,
    log(Log, Rest);
log(_, []) ->
    ok.

%%

-spec mk_filter_fun(tracer() | handler_info()) -> filter_fun().
mk_filter_fun(Who = #{filter := {Type, Filter}, name := Name}) ->
    Ctx = #filterctx{
        name = Name,
        namespace = maps:get(namespace, Who, ?global_ns),
        match = Filter
    },
    {filter_fun(Type), filter_ctx(Type, Ctx)}.

filter_fun(clientid) -> fun ?MODULE:filter_clientid/2;
filter_fun(topic) -> fun ?MODULE:filter_topic/2;
filter_fun(ip_address) -> fun ?MODULE:filter_ip_address/2;
filter_fun(ruleid) -> fun ?MODULE:filter_ruleid/2.

filter_ctx(ip_address, Ctx = #filterctx{match = IP}) ->
    Ctx#filterctx{match = ensure_list(IP)};
filter_ctx(_Type, Ctx = #filterctx{match = Match}) ->
    Ctx#filterctx{match = ensure_bin(Match)}.

-spec filter_ruleid(logger:log_event(), #filterctx{}) -> logger:log_event() | stop.
filter_ruleid(
    #{meta := Meta = #{rule_id := RuleId}} = Log,
    #filterctx{namespace = Namespace, match = MatchId}
) ->
    LogNamespace = maps:get(namespace, Meta, ?global_ns),
    RuleIDs = maps:get(rule_ids, Meta, #{}),
    IsMatch =
        (LogNamespace =:= Namespace) andalso
            ((RuleId =:= MatchId) orelse maps:get(MatchId, RuleIDs, false)),
    filter_ret(IsMatch andalso is_trace(Meta), Log);
filter_ruleid(
    #{meta := Meta = #{rule_ids := RuleIDs}} = Log,
    #filterctx{namespace = Namespace, match = MatchId}
) ->
    LogNamespace = maps:get(namespace, Meta, ?global_ns),
    IsMatch =
        (LogNamespace =:= Namespace) andalso
            (maps:get(MatchId, RuleIDs, false) andalso is_trace(Meta)),
    filter_ret(IsMatch, Log);
filter_ruleid(_Log, _FilterCtx) ->
    stop.

-spec filter_clientid(logger:log_event(), #filterctx{}) -> logger:log_event() | stop.
filter_clientid(
    #{meta := Meta = #{clientid := ClientId}} = Log,
    #filterctx{match = MatchId}
) ->
    ClientIDs = maps:get(client_ids, Meta, #{}),
    IsMatch = (ClientId =:= MatchId) orelse maps:get(MatchId, ClientIDs, false),
    filter_ret(IsMatch andalso is_trace(Meta), Log);
filter_clientid(
    #{meta := Meta = #{client_ids := ClientIDs}} = Log,
    #filterctx{match = MatchId}
) ->
    filter_ret(maps:get(MatchId, ClientIDs, false) andalso is_trace(Meta), Log);
filter_clientid(_Log, _FilterCtx) ->
    stop.

-spec filter_topic(logger:log_event(), #filterctx{}) -> logger:log_event() | stop.
filter_topic(#{meta := Meta = #{topic := Topic}} = Log, #filterctx{match = TopicFilter}) ->
    filter_ret(is_trace(Meta) andalso emqx_topic:match(Topic, TopicFilter), Log);
filter_topic(_Log, _FilterCtx) ->
    stop.

-spec filter_ip_address(logger:log_event(), #filterctx{}) -> logger:log_event() | stop.
filter_ip_address(#{meta := Meta = #{peername := Peername}} = Log, #filterctx{match = IP}) ->
    filter_ret(is_trace(Meta) andalso lists:prefix(IP, Peername), Log);
filter_ip_address(_Log, _FilterCtx) ->
    stop.

-compile({inline, [is_trace/1, filter_ret/2]}).
%% TRUE when is_trace is missing.
is_trace(#{is_trace := false}) -> false;
is_trace(_) -> true.

filter_ret(true, Log) -> Log;
filter_ret(false, _Log) -> stop.

logger_filters(Who = #{filter := {Type, _Filter}}) ->
    [{Type, mk_filter_fun(Who)}].

logger_formatter(#{
    formatter := json,
    payload_encode := PayloadEncode,
    payload_limit := PayloadLimit
}) ->
    PayloadFmtOpts = #{
        payload_encode => PayloadEncode,
        truncate_above => PayloadLimit,
        truncate_to => PayloadLimit
    },
    {emqx_trace_json_formatter, #{payload_fmt_opts => PayloadFmtOpts}};
logger_formatter(#{
    formatter := _Text,
    payload_encode := PayloadEncode,
    payload_limit := PayloadLimit
}) ->
    {emqx_trace_formatter, #{
        %% template is for ?SLOG message not ?TRACE.
        %% XXX: Don't need to print the time field in logger_formatter due to we manually concat it
        %% in emqx_logger_textfmt:fmt/2
        template => ["[", level, "] ", msg, "\n"],
        single_line => true,
        max_size => unlimited,
        depth => unlimited,
        payload_fmt_opts => #{
            payload_encode => PayloadEncode,
            truncate_above => PayloadLimit,
            truncate_to => PayloadLimit
        }
    }}.

filter_handler_info(#{id := Id, level := Level, dst := Dst, filters := Filters}, Acc) ->
    case Filters of
        [{Type, {_Fun, #filterctx{name = Name, namespace = Namespace, match = Filter}}}] ->
            Info = #{
                id => Id,
                level => Level,
                dst => Dst,
                name => Name,
                filter => {Type, Filter},
                namespace => Namespace
            },
            [Info | Acc];
        _ ->
            Acc
    end.

-spec fallback_handler_id(string(), string() | binary()) -> atom().
fallback_handler_id(Prefix, Name) when is_binary(Name) ->
    NameString = [_ | _] = unicode:characters_to_list(Name),
    fallback_handler_id(Prefix, NameString);
fallback_handler_id(Prefix, Name) when is_list(Name), length(Prefix) < 50 ->
    case length(Name) of
        L when L < 200 ->
            list_to_atom(Prefix ++ ":" ++ Name);
        _ ->
            Hash = erlang:md5(term_to_binary(Name)),
            HashString = binary_to_list(binary:encode_hex(Hash, lowercase)),
            list_to_atom(Prefix ++ ":" ++ lists:sublist(Name, 160) ++ "." ++ HashString)
    end.

payload_encode() -> emqx_config:get([trace, payload_encode], text).

ensure_bin(List) when is_list(List) ->
    %% NOTE: Asserting the result is UTF-8 binary, not expected to fail.
    <<_/binary>> = unicode:characters_to_binary(List);
ensure_bin(Bin) when is_binary(Bin) ->
    Bin.

ensure_list(Bin) when is_binary(Bin) -> unicode:characters_to_list(Bin, utf8);
ensure_list(List) when is_list(List) -> List.

show_prompts(ok, Who, Msg) ->
    ?SLOG(info, #{msg => "trace_action_succeeded", action => Msg, traced => Who});
show_prompts({error, Reason}, Who, Msg) ->
    ?SLOG(info, #{msg => "trace_action_failed", action => Msg, traced => Who, reason => Reason}).
