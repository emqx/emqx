%%--------------------------------------------------------------------
%% Copyright (c) 2018-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_trace_handler).

-include("logger.hrl").
-include("emqx_trace.hrl").
-include_lib("emqx/include/emqx_config.hrl").

-include_lib("kernel/include/file.hrl").

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

-export([
    find_log_fragment/2,
    read_log_fragment_at/4
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

-ifdef(TEST).
-export([read_file_info_impl/1]).
-endif.

-export_type([log_handler/0]).
-export_type([log_fragment/0]).

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

-define(LOG_HANDLER_ROTATION_N_FILES, 10).
-define(LOG_HANDLER_FILESYNC_INTERVAL, 5_000).
-define(LOG_HANDLER_FILECHECK_INTERVAL, 60_000).
-define(LOG_HANDLER_OLP_KILL_MEM_SIZE, 50 * 1024 * 1024).
-define(LOG_HANDLER_OLP_KILL_QLEN, 20000).

-type namespace() :: binary().
-type maybe_namespace() :: ?global_ns | namespace().

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
    Res = logger:add_handler(HandlerId, logger_std_h, Config),
    show_prompts(Res, Who, "start_trace"),
    Res.

logger_config(LogFile) ->
    MaxBytes = emqx_config:get([trace, max_file_size]),
    NFiles = ?LOG_HANDLER_ROTATION_N_FILES,
    #{
        type => file,
        file => LogFile,
        %% Split `MaxBytes` across allowed number of files:
        max_no_bytes => MaxBytes div NFiles,
        %% Number of "archives" is one less than allowed number of files:
        max_no_files => NFiles - 1,
        filesync_repeat_interval => ?LOG_HANDLER_FILESYNC_INTERVAL,
        file_check => ?LOG_HANDLER_FILECHECK_INTERVAL,
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
                exit:{noproc, _} -> ok;
                exit:{{shutdown, _}, _} -> ok;
                error:undef -> {error, unsupported}
            end;
        Error ->
            Error
    end.

%%

-opaque log_fragment() :: #{
    %% Log fragment index:
    i := non_neg_integer(),
    %% Inode number from the filesystem (always 0 if unsupported):
    in := integer(),
    %% Modification time:
    mt := integer(),
    %% Last seen file size:
    ls := non_neg_integer()
}.

-doc """
Find either the first log fragment to read data from, or the next to the one previously
obtained through a call to this function.

* `none`
  There's no more fragments, either the last (most recent) fragment was reached,
  or there is actually zero fragments.

* `{error, stale}`
  It's now impossible to find the next fragment because too much log rotation happened
  in the meantime.

* `{error, enoent}`
  No such file, can actually mean concurrent log rotation is in progress, and the
  caller may choose to retry.

""".
-spec find_log_fragment(first | {next, _After :: log_fragment()}, file:filename()) ->
    {ok, file:filename(), file:file_info(), log_fragment()}
    | none
    | {error, stale | file:posix()}.
find_log_fragment(first, Basename) ->
    find_oldest_log_fragment(Basename);
find_log_fragment({next, #{i := I0, mt := MTime, in := Inode, ls := LastSz}}, Basename) ->
    case find_log_fragment(I0, MTime, Inode, LastSz, Basename) of
        #{i := I} ->
            next_log_fragment(I, Basename);
        Error ->
            Error
    end.

%% Find the oldest existing log fragment file.
find_oldest_log_fragment(Basename) ->
    SearchFun = fun(I) ->
        Filename = mk_log_fragment_filename(Basename, I),
        case read_file_info(Filename) of
            {ok, Info} ->
                {true, {ok, Filename, Info, mk_log_fragment(I, Info)}};
            {error, enoent} ->
                false;
            {error, Reason} ->
                {error, Reason}
        end
    end,
    find_boundary(SearchFun, 1, ?LOG_HANDLER_ROTATION_N_FILES, none).

find_log_fragment(I, MTime, Inode, LastSz, Basename) ->
    find_log_fragment(I, MTime, Inode, LastSz, I =:= 1, Basename).

find_log_fragment(I, MTime, Inode, LastSz, Topmost, Basename) ->
    %% First we need to make sure the given fragment still resides under
    %% the same index / filename.
    Filename = mk_log_fragment_filename(Basename, I),
    InfoRet = read_file_info(Filename),
    case match_log_fragment(InfoRet, I, MTime, Inode, LastSz, Topmost, Basename) of
        Fragment = #{} ->
            Fragment;
        nomatch ->
            %% Let's try the next older fragment.
            find_log_fragment(I + 1, MTime, Inode, LastSz, Topmost, Basename);
        Error ->
            Error
    end.

match_log_fragment(InfoRet, I, MTime, Inode, LastSz, Topmost, Basename) ->
    %% Basically:
    %% * Topmost fragments should have the same inode + same or newer mtime.
    %% * Non-topmost fragments should have the same inode + same mtime.
    case InfoRet of
        {ok, #file_info{mtime = MTime1, inode = Inode1, size = Size} = Info} ->
            case has_match(Topmost, MTime, Inode, LastSz, MTime1, Inode1, Size) of
                true ->
                    mk_log_fragment(I, Info);
                false ->
                    nomatch;
                ambiguous ->
                    InfoRetNext = read_file_info(mk_log_fragment_filename(Basename, I + 1)),
                    case resolve_ambiguity(InfoRetNext, MTime) of
                        true -> mk_log_fragment(I, Info);
                        false -> nomatch;
                        Error -> Error
                    end;
                Error ->
                    Error
            end;
        {error, enoent} when I < ?LOG_HANDLER_ROTATION_N_FILES ->
            %% Fragment is likely being rotated right now, try to find it.
            nomatch;
        {error, enoent} ->
            %% Fragment was rotated _and_ deleted, there's a discontinuity.
            {error, stale};
        Error ->
            Error
    end.

%% Must be the same file:
has_match(_, MTime, Inode, LastSz, MTime, Inode, LastSz) -> true;
%% If the size is smaller than what was seen last time, definitely look further:
has_match(_, _, _, LastSz, _, _, Size) when Size < LastSz -> false;
%% This is unexpected, fragment should have either the same or newer mtime:
has_match(_, MTime, _, _, Older, _, _) when Older < MTime -> {error, stale};
%% This is ambiguous, topmost fragment may have been updated and then rotated N times:
has_match(true, MTime, Inode, _, Newer, Inode, _) when Newer >= MTime -> ambiguous;
%% Otherwise, look further:
has_match(_, _, _, _, _, _, _) -> false.

%% The current fragment is same or newer, and has same or larger size.
%% If the next one is same or older, the current one is or was the topmost fragment:
resolve_ambiguity({ok, #file_info{mtime = MTimeNext}}, MTime) -> MTimeNext =< MTime;
%% The current fragment supposedly was the topmost fragment, now it's the last one:
resolve_ambiguity({error, enoent}, _) -> true;
resolve_ambiguity(Error, _) -> Error.

next_log_fragment(1, _Basename) ->
    none;
next_log_fragment(I, Basename) ->
    %% NOTE
    %% Subject to races with `logger_std_h:rotate_files/3`, highly unlikely though.
    %% 1. We can accidentally skip one fragment.
    %% 2. We can get `{error, enoent}` for a fragment in the process of rotation.
    %% Currently, upper layer blindly considers `{error, enoent}` as retry-worthy,
    %% i.e. by returning `{retry, Cursor}` as continuation to the user code.
    Filename = mk_log_fragment_filename(Basename, I - 1),
    case read_file_info(Filename) of
        {ok, Info} ->
            {ok, Filename, Info, mk_log_fragment(I - 1, Info)};
        Error ->
            Error
    end.

%% Binary search a boundary where SF turns from `{true, _}` to `false`.
find_boundary(SF, I1, I2, Found0) ->
    IMid = (I1 + I2) div 2,
    case SF(IMid) of
        {true, Found} when I2 > I1 -> find_boundary(SF, IMid + 1, I2, Found);
        {true, Found} -> Found;
        false when I2 > I1 -> find_boundary(SF, I1, IMid - 1, Found0);
        false -> Found0;
        Error -> Error
    end.

-doc """
Read a portion of log fragment, starting from the given position.
Fragment is previously obtained through a call to `find_log_fragment/2`.

Read consistency under concurrent log rotation is preserved as realistically possible,
however there's no strong guarantee. Avoid having configurations that allow for high
log rotation rate.

* `{error, stale}`
  It's now impossible to have consistent read because too much log rotation happened
  in the meantime.

* `{error, enoent}`
  No such file, again can actually mean concurrent log rotation is in progress, and
  the caller may choose to retry.

""".
-spec read_log_fragment_at(
    log_fragment(),
    file:filename(),
    _Pos :: non_neg_integer(),
    _NBytes :: pos_integer()
) ->
    {ok, binary(), log_fragment()} | {error, stale | file:posix()}.
read_log_fragment_at(#{i := I, mt := MTime, in := Inode, ls := LastSz}, Basename, Pos, NBytes) ->
    read_log_fragment_at(I, MTime, Inode, LastSz, I =:= 1, Basename, Pos, NBytes).

read_log_fragment_at(I, MTime, Inode, LastSz, Topmost, Basename, Pos, NBytes) ->
    Filename = mk_log_fragment_filename(Basename, I),
    case file:open(Filename, [read, raw, binary]) of
        {ok, FD} ->
            Result =
                try
                    InfoRet = read_file_info(FD),
                    case match_log_fragment(InfoRet, I, MTime, Inode, LastSz, Topmost, Basename) of
                        Fragment = #{} ->
                            chunk(file:pread(FD, Pos, NBytes), Fragment);
                        Otherwise ->
                            Otherwise
                    end
                after
                    file:close(FD)
                end,
            case Result of
                Ok when element(1, Ok) == ok ->
                    Ok;
                nomatch ->
                    %% MTime mismatch, let's look inside the older fragment.
                    read_log_fragment_at(
                        I + 1, MTime, Inode, LastSz, Topmost, Basename, Pos, NBytes
                    );
                Error ->
                    Error
            end;
        {error, enoent} when I >= ?LOG_HANDLER_ROTATION_N_FILES ->
            %% Fragment was rotated _and_ deleted, there's a discontinuity.
            {error, stale};
        {error, Reason} ->
            {error, Reason}
    end.

chunk({ok, Chunk}, Fragment) ->
    {ok, Chunk, Fragment};
chunk(eof, Fragment) ->
    {ok, <<>>, Fragment};
chunk(Error, _Fragment) ->
    Error.

mk_log_fragment(I, #file_info{mtime = MTime, inode = Inode, size = Size}) ->
    #{i => I, mt => MTime, in => Inode, ls => Size}.

mk_log_fragment_filename(Basename, 1) ->
    Basename;
mk_log_fragment_filename(Basename, I) ->
    Basename ++ "." ++ integer_to_list(I - 2).

-ifndef(TEST).
read_file_info(Filename) ->
    read_file_info_impl(Filename).
-else.
read_file_info(Filename) ->
    ?MODULE:read_file_info_impl(Filename).
-endif.

%% NOTE: Mocked in `emqx_trace_SUITE`.
read_file_info_impl(Filename) ->
    case file:read_file_info(Filename, [raw, {time, posix}]) of
        {ok, #file_info{type = regular} = Info} ->
            {ok, Info};
        {ok, #file_info{type = Type}} ->
            {error, Type};
        Error ->
            Error
    end.

%%

-spec mk_log_handler(handler_info()) -> log_handler().
mk_log_handler(Info = #{id := HandlerId}) ->
    {HandlerId, mk_filter_fun(Info)}.

-spec log(_LoggerEvent, [log_handler()]) -> ok.
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
                        Kind:Error:Stacktrace ->
                            case logger:remove_handler(Id) of
                                ok ->
                                    ?SLOG(error, #{
                                        msg => "trace_log_handler_failing_removed",
                                        handler_id => Id,
                                        reason => {Kind, Error},
                                        stacktrace => Stacktrace
                                    });
                                {error, {not_found, _}} ->
                                    %% Probably already removed by other client
                                    %% Don't report again
                                    ok;
                                {error, Reason} ->
                                    ?SLOG(error, #{
                                        msg => "trace_log_handler_failing_removal_error",
                                        removal_error => Reason,
                                        handler_id => Id,
                                        reason => {Kind, Error},
                                        stacktrace => Stacktrace
                                    })
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
