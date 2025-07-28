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
    install/7,
    uninstall/1
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

-type tracer() :: #{
    name := binary(),
    namespace => maybe_namespace(),
    type := clientid | topic | ip_address,
    filter := filter(),
    payload_encode := text | hidden | hex,
    payload_limit := non_neg_integer(),
    formatter => json | text
}.

-define(LOG_HANDLER_OLP_KILL_MEM_SIZE, 50 * 1024 * 1024).
-define(LOG_HANDLER_OLP_KILL_QLEN, 20000).

-type filter() ::
    emqx_types:clientid()
    | emqx_types:topic()
    | {?global_ns | binary(), emqx_trace:ruleid()}
    | string().

-type namespace() :: binary().
-type maybe_namespace() :: ?global_ns | namespace().

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

-spec install(
    HandlerId :: logger:handler_id(),
    Name :: binary() | list(),
    Type :: clientid | topic | ip_address,
    Filter :: filter(),
    Level :: logger:level() | all,
    LogFilePath :: string(),
    Formatter :: text | json
) -> ok | {error, term()}.
install(HandlerId, Name, Type, Filter, Level, LogFile, Formatter) ->
    Who = #{
        type => Type,
        filter => ensure_bin(Filter),
        name => ensure_bin(Name),
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
        formatter => formatter(Who),
        filter_default => stop,
        filters => filters(Who),
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
-spec running() ->
    [
        #{
            name => binary(),
            type => topic | clientid | ip_address,
            id => atom(),
            filter => emqx_types:topic() | emqx_types:clientid() | emqx_trace:ip_address(),
            level => logger:level(),
            dst => file:filename() | console | unknown
        }
    ].
running() ->
    lists:foldl(fun filter_traces/2, [], emqx_logger:get_log_handlers(started)).

-spec filter_ruleid(logger:log_event(), {binary(), atom()}) -> logger:log_event() | stop.
filter_ruleid(#{meta := Meta = #{rule_id := RuleId}} = Log, {{Namespace, MatchId}, _Name}) ->
    LogNamespace = maps:get(namespace, Meta, ?global_ns),
    RuleIDs = maps:get(rule_ids, Meta, #{}),
    IsMatch =
        (LogNamespace =:= Namespace) andalso
            ((RuleId =:= MatchId) orelse maps:get(MatchId, RuleIDs, false)),
    filter_ret(IsMatch andalso is_trace(Meta), Log);
filter_ruleid(#{meta := Meta = #{rule_ids := RuleIDs}} = Log, {{Namespace, MatchId}, _Name}) ->
    LogNamespace = maps:get(namespace, Meta, ?global_ns),
    IsMatch =
        (LogNamespace =:= Namespace) andalso
            (maps:get(MatchId, RuleIDs, false) andalso is_trace(Meta)),
    filter_ret(IsMatch, Log);
filter_ruleid(_Log, _ExpectId) ->
    stop.

-spec filter_clientid(logger:log_event(), {binary(), atom()}) -> logger:log_event() | stop.
filter_clientid(#{meta := Meta = #{clientid := ClientId}} = Log, {MatchId, _Name}) ->
    ClientIDs = maps:get(client_ids, Meta, #{}),
    IsMatch = (ClientId =:= MatchId) orelse maps:get(MatchId, ClientIDs, false),
    filter_ret(IsMatch andalso is_trace(Meta), Log);
filter_clientid(#{meta := Meta = #{client_ids := ClientIDs}} = Log, {MatchId, _Name}) ->
    filter_ret(maps:get(MatchId, ClientIDs, false) andalso is_trace(Meta), Log);
filter_clientid(_Log, _ExpectId) ->
    stop.

-spec filter_topic(logger:log_event(), {binary(), atom()}) -> logger:log_event() | stop.
filter_topic(#{meta := Meta = #{topic := Topic}} = Log, {TopicFilter, _Name}) ->
    filter_ret(is_trace(Meta) andalso emqx_topic:match(Topic, TopicFilter), Log);
filter_topic(_Log, _ExpectId) ->
    stop.

-spec filter_ip_address(logger:log_event(), {string(), atom()}) -> logger:log_event() | stop.
filter_ip_address(#{meta := Meta = #{peername := Peername}} = Log, {IP, _Name}) ->
    filter_ret(is_trace(Meta) andalso lists:prefix(IP, Peername), Log);
filter_ip_address(_Log, _ExpectId) ->
    stop.

-compile({inline, [is_trace/1, filter_ret/2]}).
%% TRUE when is_trace is missing.
is_trace(#{is_trace := false}) -> false;
is_trace(_) -> true.

filter_ret(true, Log) -> Log;
filter_ret(false, _Log) -> stop.

filters(#{type := clientid, filter := Filter, name := Name}) ->
    [{clientid, {fun ?MODULE:filter_clientid/2, {Filter, Name}}}];
filters(#{type := topic, filter := Filter, name := Name}) ->
    [{topic, {fun ?MODULE:filter_topic/2, {ensure_bin(Filter), Name}}}];
filters(#{type := ip_address, filter := Filter, name := Name}) ->
    [{ip_address, {fun ?MODULE:filter_ip_address/2, {ensure_list(Filter), Name}}}];
filters(#{type := ruleid, filter := Filter, name := Name} = Who) ->
    Namespace = maps:get(namespace, Who, ?global_ns),
    [{ruleid, {fun ?MODULE:filter_ruleid/2, {{Namespace, ensure_bin(Filter)}, Name}}}].

formatter(#{
    type := _Type, payload_encode := PayloadEncode, formatter := json, payload_limit := PayloadLimit
}) ->
    PayloadFmtOpts = #{
        payload_encode => PayloadEncode,
        truncate_above => PayloadLimit,
        truncate_to => PayloadLimit
    },
    {emqx_trace_json_formatter, #{payload_fmt_opts => PayloadFmtOpts}};
formatter(#{type := _Type, payload_encode := PayloadEncode, payload_limit := PayloadLimit}) ->
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

filter_traces(#{id := Id, level := Level, dst := Dst, filters := Filters}, Acc) ->
    Init = #{id => Id, level => Level, dst => Dst},
    case Filters of
        [{Type, {FilterFun, {Filter, Name}}}] when
            Type =:= topic orelse
                Type =:= clientid orelse
                Type =:= ip_address
        ->
            [Init#{type => Type, filter => Filter, name => Name, filter_fun => FilterFun} | Acc];
        [{Type, {FilterFun, {{Namespace, Filter}, Name}}}] when
            Type =:= ruleid
        ->
            [
                Init#{
                    type => Type,
                    filter => {Namespace, Filter},
                    name => Name,
                    filter_fun => FilterFun
                }
                | Acc
            ];
        _ ->
            Acc
    end.

-spec fallback_handler_id(string(), string() | binary()) -> atom().
fallback_handler_id(Prefix, Name) when is_binary(Name) ->
    NameString = [_ | _] = unicode:characters_to_list(Name),
    fallback_handler_id(Prefix, NameString);
fallback_handler_id(Prefix, Name) when is_list(Name) ->
    list_to_atom(Prefix ++ ":" ++ Name).

payload_encode() -> emqx_config:get([trace, payload_encode], text).

ensure_bin({Namespace, Match}) ->
    MatchBin = ensure_bin(Match),
    {Namespace, MatchBin};
ensure_bin(List) when is_list(List) ->
    iolist_to_binary(List);
ensure_bin(Bin) when is_binary(Bin) ->
    Bin.

ensure_list(Bin) when is_binary(Bin) -> unicode:characters_to_list(Bin, utf8);
ensure_list(List) when is_list(List) -> List.

show_prompts(ok, Who, Msg) ->
    ?SLOG(info, #{msg => "trace_action_succeeded", action => Msg, traced => Who});
show_prompts({error, Reason}, Who, Msg) ->
    ?SLOG(info, #{msg => "trace_action_failed", action => Msg, traced => Who, reason => Reason}).
