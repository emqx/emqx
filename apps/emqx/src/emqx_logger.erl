%%--------------------------------------------------------------------
%% Copyright (c) 2018-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_logger).

-compile({no_auto_import, [error/1]}).

-elvis([{elvis_style, god_modules, disable}]).

%% Logs
-export([
    debug/1,
    debug/2,
    debug/3,
    info/1,
    info/2,
    info/3,
    warning/1,
    warning/2,
    warning/3,
    error/1,
    error/2,
    error/3,
    critical/1,
    critical/2,
    critical/3,
    %% Metadata:
    attach_label/1
]).

%% Configs
-export([
    set_metadata_peername/1,
    set_metadata_clientid/1,
    set_metadata_username/1,
    set_proc_metadata/1,
    set_primary_log_level/1,
    set_log_handler_level/2,
    set_log_level/1,
    set_level/1,
    set_all_log_handlers_level/1
]).

-export([
    get_primary_log_level/0,
    tune_primary_log_level/0,
    get_log_handlers/0,
    get_log_handlers/1,
    get_log_handler/1
]).

-export([
    start_log_handler/1,
    stop_log_handler/1
]).

-type peername_str() :: list().
-type logger_dst() :: file:filename() | console | unknown.
-type logger_handler_info() :: #{
    id := logger:handler_id(),
    level := logger:level(),
    dst := logger_dst(),
    filters := [{logger:filter_id(), logger:filter()}],
    status := started | stopped
}.

-define(STOPPED_HANDLERS, {?MODULE, stopped_handlers}).

-define(MAX_PROCMETA_STRING_LEN, 64).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------
-spec debug(unicode:chardata()) -> ok.
debug(Msg) ->
    logger:debug(Msg).

-spec debug(io:format(), [term()]) -> ok.
debug(Format, Args) ->
    logger:debug(Format, Args).

-spec debug(logger:metadata(), io:format(), [term()]) -> ok.
debug(Metadata, Format, Args) when is_map(Metadata) ->
    logger:debug(Format, Args, Metadata).

-spec info(unicode:chardata()) -> ok.
info(Msg) ->
    logger:info(Msg).

-spec info(io:format(), [term()]) -> ok.
info(Format, Args) ->
    logger:info(Format, Args).

-spec info(logger:metadata(), io:format(), [term()]) -> ok.
info(Metadata, Format, Args) when is_map(Metadata) ->
    logger:info(Format, Args, Metadata).

-spec warning(unicode:chardata()) -> ok.
warning(Msg) ->
    logger:warning(Msg).

-spec warning(io:format(), [term()]) -> ok.
warning(Format, Args) ->
    logger:warning(Format, Args).

-spec warning(logger:metadata(), io:format(), [term()]) -> ok.
warning(Metadata, Format, Args) when is_map(Metadata) ->
    logger:warning(Format, Args, Metadata).

-spec error(unicode:chardata()) -> ok.
error(Msg) ->
    logger:error(Msg).
-spec error(io:format(), [term()]) -> ok.
error(Format, Args) ->
    logger:error(Format, Args).
-spec error(logger:metadata(), io:format(), [term()]) -> ok.
error(Metadata, Format, Args) when is_map(Metadata) ->
    logger:error(Format, Args, Metadata).

-spec critical(unicode:chardata()) -> ok.
critical(Msg) ->
    logger:critical(Msg).

-spec critical(io:format(), [term()]) -> ok.
critical(Format, Args) ->
    logger:critical(Format, Args).

-spec critical(logger:metadata(), io:format(), [term()]) -> ok.
critical(Metadata, Format, Args) when is_map(Metadata) ->
    logger:critical(Format, Args, Metadata).

-spec set_metadata_clientid(emqx_types:clientid()) -> ok.
set_metadata_clientid(ClientId) when ClientId =:= undefined orelse ClientId =:= <<>> ->
    ok;
set_metadata_clientid(ClientId) ->
    proc_lib:set_label({clientid, truncate_procmeta_string(ClientId)}).

-spec set_metadata_username(emqx_types:username()) -> ok.
set_metadata_username(Username) when Username =:= undefined orelse Username =:= <<>> ->
    ok;
set_metadata_username(Username) ->
    set_proc_metadata(#{username => truncate_procmeta_string(Username)}).

-spec set_metadata_peername(peername_str()) -> ok.
set_metadata_peername(Peername) ->
    set_proc_metadata(#{peername => Peername}).

-spec set_proc_metadata(list({atom(), _}) | logger:metadata()) -> ok.
set_proc_metadata(Meta) when is_list(Meta) ->
    logger:update_process_metadata(
        maps:from_list([KV || KV = {_, V} <- Meta, V =/= undefined andalso V =/= <<>>])
    );
set_proc_metadata(Meta) ->
    logger:update_process_metadata(
        maps:filter(fun(_, V) -> V =/= undefined andalso V =/= <<>> end, Meta)
    ).

-spec attach_label(logger:metadata()) -> logger:metadata().
attach_label(Meta) ->
    case proc_lib:get_label(self()) of
        {K, V} when is_atom(K) -> Meta#{K => V};
        _Otherwise -> Meta
    end.

-spec get_primary_log_level() -> logger:level().
get_primary_log_level() ->
    #{level := Level} = logger:get_primary_config(),
    Level.

-spec tune_primary_log_level() -> ok.
tune_primary_log_level() ->
    LowestLevel = lists:foldl(
        fun(#{level := Level}, OldLevel) ->
            case logger:compare_levels(Level, OldLevel) of
                lt -> Level;
                _ -> OldLevel
            end
        end,
        get_primary_log_level(),
        get_log_handlers()
    ),
    set_primary_log_level(LowestLevel).

-spec set_primary_log_level(logger:level()) -> ok | {error, term()}.
set_primary_log_level(Level) ->
    logger:set_primary_config(level, Level).

-spec get_log_handlers() -> [logger_handler_info()].
get_log_handlers() ->
    get_log_handlers(started) ++ get_log_handlers(stopped).

-spec get_log_handlers(started | stopped) -> [logger_handler_info()].
get_log_handlers(started) ->
    [log_handler_info(Conf, started) || Conf <- logger:get_handler_config()];
get_log_handlers(stopped) ->
    [log_handler_info(Conf, stopped) || Conf <- list_stopped_handler_config()].

-spec get_log_handler(logger:handler_id()) -> logger_handler_info().
get_log_handler(HandlerId) ->
    case logger:get_handler_config(HandlerId) of
        {ok, Conf} ->
            log_handler_info(Conf, started);
        {error, _} ->
            case read_stopped_handler_config(HandlerId) of
                error -> {error, {not_found, HandlerId}};
                {ok, Conf} -> log_handler_info(Conf, stopped)
            end
    end.

-spec start_log_handler(logger:handler_id()) -> ok | {error, term()}.
start_log_handler(HandlerId) ->
    case lists:member(HandlerId, logger:get_handler_ids()) of
        true ->
            ok;
        false ->
            case read_stopped_handler_config(HandlerId) of
                error ->
                    {error, {not_found, HandlerId}};
                {ok, Conf = #{module := Mod}} ->
                    case logger:add_handler(HandlerId, Mod, Conf) of
                        ok -> remove_stopped_handler_config(HandlerId);
                        {error, _} = Error -> Error
                    end
            end
    end.

-spec stop_log_handler(logger:handler_id()) -> ok | {error, term()}.
stop_log_handler(HandlerId) ->
    case logger:get_handler_config(HandlerId) of
        {ok, Conf} ->
            case logger:remove_handler(HandlerId) of
                ok -> save_stopped_handler_config(HandlerId, Conf);
                Error -> Error
            end;
        {error, _} ->
            {error, {not_started, HandlerId}}
    end.

-spec set_log_handler_level(logger:handler_id(), logger:level()) -> ok | {error, term()}.
set_log_handler_level(HandlerId, Level) ->
    case logger:set_handler_config(HandlerId, level, Level) of
        ok ->
            ok;
        {error, _} ->
            case read_stopped_handler_config(HandlerId) of
                error -> {error, {not_found, HandlerId}};
                {ok, Conf} -> save_stopped_handler_config(HandlerId, Conf#{level => Level})
            end
    end.

%% @doc Set both the primary and all handlers level in one command
-spec set_level(logger:level()) -> ok | {error, term()}.
set_level(Level) ->
    case set_primary_log_level(Level) of
        ok -> set_all_log_handlers_level(Level);
        {error, Error} -> {error, {primary_logger_level, Error}}
    end.

set_log_level(Level) ->
    set_level(Level).

%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------

%% Truncate process label / metadata value to a small-ish binary.
%% Namely, MQTT Client ID can be potentially very long, and we:
%% * do not want to store a copy of it,
%% * do not want to reporting / logging facilities to handle it.
truncate_procmeta_string(S) when not is_binary(S) ->
    S;
truncate_procmeta_string(S) when byte_size(S) < ?MAX_PROCMETA_STRING_LEN ->
    S;
truncate_procmeta_string(S) ->
    binary:copy(string:slice(S, 0, ?MAX_PROCMETA_STRING_LEN)).

log_handler_info(
    #{
        id := Id,
        level := Level,
        module := logger_std_h,
        filters := Filters,
        config := #{type := Type}
    },
    Status
) when
    Type =:= standard_io;
    Type =:= standard_error
->
    #{id => Id, level => Level, dst => console, status => Status, filters => Filters};
log_handler_info(
    #{
        id := Id,
        level := Level,
        module := logger_std_h,
        filters := Filters,
        config := Config = #{type := file}
    },
    Status
) ->
    #{
        id => Id,
        level => Level,
        status => Status,
        filters => Filters,
        dst => maps:get(file, Config, atom_to_list(Id))
    };
log_handler_info(
    #{
        id := Id,
        level := Level,
        module := logger_disk_log_h,
        filters := Filters,
        config := #{file := Filename}
    },
    Status
) ->
    #{id => Id, level => Level, dst => Filename, status => Status, filters => Filters};
log_handler_info(#{id := Id, level := Level, filters := Filters}, Status) ->
    #{id => Id, level => Level, dst => unknown, status => Status, filters => Filters}.

%% set level for all log handlers in one command
set_all_log_handlers_level(Level) ->
    set_all_log_handlers_level(get_log_handlers(), Level, []).

set_all_log_handlers_level([#{id := Id, level := Level} | List], NewLevel, ChangeHistory) ->
    case set_log_handler_level(Id, NewLevel) of
        ok ->
            set_all_log_handlers_level(List, NewLevel, [{Id, Level} | ChangeHistory]);
        {error, Error} ->
            rollback(ChangeHistory),
            {error, {handlers_logger_level, {Id, Error}}}
    end;
set_all_log_handlers_level([], _NewLevel, _NewHanlder) ->
    ok.

rollback([{Id, Level} | List]) ->
    _ = set_log_handler_level(Id, Level),
    rollback(List);
rollback([]) ->
    ok.

save_stopped_handler_config(HandlerId, Config) ->
    case persistent_term:get(?STOPPED_HANDLERS, undefined) of
        undefined ->
            persistent_term:put(?STOPPED_HANDLERS, #{HandlerId => Config});
        ConfList ->
            persistent_term:put(?STOPPED_HANDLERS, ConfList#{HandlerId => Config})
    end.
read_stopped_handler_config(HandlerId) ->
    case persistent_term:get(?STOPPED_HANDLERS, undefined) of
        undefined -> error;
        ConfList -> maps:find(HandlerId, ConfList)
    end.
remove_stopped_handler_config(HandlerId) ->
    case persistent_term:get(?STOPPED_HANDLERS, undefined) of
        undefined ->
            ok;
        ConfList ->
            case maps:find(HandlerId, ConfList) of
                error -> ok;
                {ok, _} -> persistent_term:put(?STOPPED_HANDLERS, maps:remove(HandlerId, ConfList))
            end
    end.
list_stopped_handler_config() ->
    case persistent_term:get(?STOPPED_HANDLERS, undefined) of
        undefined -> [];
        ConfList -> maps:values(ConfList)
    end.
