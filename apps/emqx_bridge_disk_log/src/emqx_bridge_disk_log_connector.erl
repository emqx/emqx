%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_disk_log_connector).

-feature(maybe_expr, enable).

-behaviour(emqx_resource).

-include_lib("kernel/include/file.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx/include/emqx_trace.hrl").
-include("emqx_bridge_disk_log.hrl").

%% `emqx_resource' API
-export([
    resource_type/0,
    callback_mode/0,

    on_start/2,
    on_stop/2,
    on_get_status/2,

    on_get_channels/1,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channel_status/3,

    on_query/3,
    on_batch_query/3
]).

-ifdef(TEST).
-export([flush/1]).
-endif.

-elvis([{elvis_style, export_used_types, disable}]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

%% Allocatable resources
-define(disk_log, disk_log).

-define(installed_actions, installed_actions).

-define(template, template).
-define(write_mode, write_mode).

-type connector_config() :: #{}.
-type connector_state() :: #{
    ?installed_actions := #{action_resource_id() => action_state()}
}.

-type action_config() :: #{
    parameters := #{
        template := binary(),
        write_mode := write_mode()
    }
}.
-type action_state() :: #{
    ?template := emqx_template:t(),
    ?write_mode := write_mode()
}.

-type query() :: {_Tag :: channel_id(), _Data :: map()}.

-type log_name() :: connector_resource_id().
-type write_mode() :: sync | async.

%%------------------------------------------------------------------------------
%% `emqx_resource' API
%%------------------------------------------------------------------------------

-spec resource_type() -> atom().
resource_type() ->
    disk_log.

-spec callback_mode() -> callback_mode().
callback_mode() ->
    always_sync.

-spec on_start(connector_resource_id(), connector_config()) ->
    {ok, connector_state()} | {error, _Reason}.
on_start(ConnResId, ConnConfig) ->
    maybe
        ok ?= do_open_log(ConnResId, ConnConfig),
        ConnState = #{?installed_actions => #{}},
        {ok, ConnState}
    else
        {error, Reason} ->
            {error, emqx_utils:explain_posix(Reason)}
    end.

-spec on_stop(connector_resource_id(), connector_state()) -> ok.
on_stop(ConnResId, _ConnState) ->
    release_allocated_resources(ConnResId),
    ?tp("disk_log_connector_stop", #{instance_id => ConnResId}),
    ok.

-spec on_get_status(connector_resource_id(), connector_state()) ->
    ?status_connected | ?status_disconnected.
on_get_status(ConnResId, _State) ->
    case disk_log:info(ConnResId) of
        {error, no_such_log} ->
            ?status_disconnected;
        Info when is_list(Info) ->
            {file, Filepath} = lists:keyfind(file, 1, Info),
            check_file_status(Filepath, ConnResId)
    end.

-spec on_get_channels(connector_resource_id()) ->
    [{action_resource_id(), action_config()}].
on_get_channels(ConnResId) ->
    emqx_bridge_v2:get_channels_for_connector(ConnResId).

-spec on_add_channel(
    connector_resource_id(),
    connector_state(),
    action_resource_id(),
    action_config()
) ->
    {ok, connector_state()}.
on_add_channel(_ConnResId, ConnState0, ActionResId, ActionConfig) ->
    ActionState = create_action(ActionConfig),
    ConnState = emqx_utils_maps:deep_put(
        [?installed_actions, ActionResId], ConnState0, ActionState
    ),
    {ok, ConnState}.

-spec on_remove_channel(
    connector_resource_id(),
    connector_state(),
    action_resource_id()
) ->
    {ok, connector_state()}.
on_remove_channel(
    _ConnResId, ConnState0 = #{?installed_actions := InstalledActions0}, ActionResId
) when
    is_map_key(ActionResId, InstalledActions0)
->
    {_ActionState, InstalledActions} = maps:take(ActionResId, InstalledActions0),
    ConnState = ConnState0#{?installed_actions := InstalledActions},
    {ok, ConnState};
on_remove_channel(_ConnResId, ConnState, _ActionResId) ->
    {ok, ConnState}.

-spec on_get_channel_status(
    connector_resource_id(),
    action_resource_id(),
    connector_state()
) ->
    ?status_connected | ?status_disconnected.
on_get_channel_status(
    _ConnResId,
    ActionResId,
    _ConnState = #{?installed_actions := InstalledActions}
) when is_map_key(ActionResId, InstalledActions) ->
    ?status_connected;
on_get_channel_status(_ConnResId, _ActionResId, _ConnState) ->
    ?status_disconnected.

-spec on_query(connector_resource_id(), query(), connector_state()) ->
    {ok, _Result} | {error, _Reason}.
on_query(
    ConnResId, {ActionResId, #{} = Data}, #{?installed_actions := InstalledActions} = ConnState
) when
    is_map_key(ActionResId, InstalledActions)
->
    ActionState = maps:get(ActionResId, InstalledActions),
    do_write_log(ConnResId, ActionResId, ConnState, ActionState, [Data]);
on_query(_ConnResId, Query, _ConnState) ->
    {error, {unrecoverable_error, {invalid_query, Query}}}.

-spec on_batch_query(connector_resource_id(), [query()], connector_state()) ->
    ok | {error, term()}.
on_batch_query(
    ConnResId,
    [{ActionResId, _Data} | _] = Queries,
    #{?installed_actions := InstalledActions} = ConnState
) when
    is_map_key(ActionResId, InstalledActions)
->
    ActionState = maps:get(ActionResId, InstalledActions),
    Data = lists:map(fun({_ActionResId, Data}) -> Data end, Queries),
    do_write_log(ConnResId, ActionResId, ConnState, ActionState, Data);
on_batch_query(_ConnResId, Queries, _ConnectorState) ->
    {error, {unrecoverable_error, {invalid_batch, Queries}}}.

%%------------------------------------------------------------------------------
%% Test/debug only
%%------------------------------------------------------------------------------

-ifdef(TEST).
%% Only for speeding up tests, due to `disk_log' asynchronous flush nature.
flush(ConnResId) ->
    disk_log:sync(ConnResId).
-endif.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

do_open_log(ConnResId, ConnConfig) ->
    #{
        filepath := FilepathBin,
        max_file_size := MaxFileSize,
        max_file_number := MaxFileNumber
    } = ConnConfig,
    ok = emqx_resource:allocate_resource(ConnResId, ?disk_log, ConnResId),
    ArgL = [
        {name, ConnResId},
        {file, binary_to_list(FilepathBin)},
        {type, rotate},
        {format, external},
        {size, {MaxFileSize, MaxFileNumber}},
        {repair, false}
    ],
    maybe
        {ok, _} ?= disk_log:open(ArgL),
        maybe_rotate(ConnResId, ConnConfig),
        ok
    end.

%% When re-opening an existing log, `disk_log' does not check the current file size and
%% assumes it's starting from 0 size, although it does correctly append to the end of file
%% instead of clobbering it.  To avoid making one file too big, we force rotation if
%% needed.
maybe_rotate(ConnResId, ConnConfig) ->
    #{
        filepath := FilepathBin,
        max_file_size := MaxFileSize
    } = ConnConfig,
    FileSize = filelib:file_size(FilepathBin),
    case FileSize >= MaxFileSize of
        true ->
            _ = disk_log:next_file(ConnResId),
            ok;
        false when FileSize > 0 ->
            %% File has not exceeded maximum, but there is some data in it.  Since we
            %% can't know if it's corrupt, we write a newline to it to avoid confusing the
            %% next record with the previous data.
            _ = disk_log:blog(ConnResId, <<"\n">>),
            ok;
        false ->
            ok
    end.

-spec create_action(action_config()) -> action_state().
create_action(#{parameters := Parameters} = _ActionConfig) ->
    #{
        template := Template,
        write_mode := WriteMode
    } = Parameters,
    CompiledTemplate = emqx_template:parse_deep(Template),
    #{
        ?template => CompiledTemplate,
        ?write_mode => WriteMode
    }.

-spec do_write_log(
    connector_resource_id(),
    action_resource_id(),
    connector_state(),
    action_state(),
    [map()]
) ->
    ok | {error, term()}.
do_write_log(ConnResId, ActionResId, _ConnState, ActionState, Data) when is_list(Data) ->
    #{
        ?template := Template,
        ?write_mode := WriteMode
    } = ActionState,
    Terms = lists:map(
        fun(Map) -> [render_log(Template, Map), $\n] end,
        Data
    ),
    emqx_trace:rendered_action_template(ActionResId, #{terms => Terms}),
    Res =
        case WriteMode of
            sync ->
                disk_log:blog_terms(ConnResId, Terms);
            async ->
                disk_log:balog_terms(ConnResId, Terms)
        end,
    map_error(Res).

render_log(Template, Data) ->
    %% NOTE: ignoring errors here, missing variables will be rendered as `null'.
    {Result, _Errors} = emqx_template:render(
        Template,
        {emqx_jsonish, Data},
        #{var_trans => fun render_var/2}
    ),
    Result.

render_var(_Name, undefined) ->
    %% Encoding missing variables as `null'.
    %% Note: cannot actually distinguish existing `undefined' atom from missing
    %% variables...
    <<"null">>;
render_var(_Name, Value) ->
    emqx_utils_json:encode(Value).

check_file_status(Filepath, ConnResId) ->
    maybe
        {ok, #file_info{access = read_write}} ?=
            file:read_file_info(Filepath),
        ?status_connected
    else
        {error, Reason} ->
            ?SLOG(warning, #{
                msg => "failed_to_get_disk_log_file_info",
                reason => Reason,
                connector_resource_id => ConnResId
            }),
            Msg = iolist_to_binary(
                io_lib:format("Failed to get disk log file info: ~0p", [Reason])
            ),
            {?status_disconnected, emqx_utils:explain_posix(Msg)};
        {ok, #file_info{access = Access, mode = Mode0}} ->
            Mode = 8#777 band Mode0,
            ?SLOG(warning, #{
                msg => "bad_disk_log_file_permissions",
                expected_access => read_write,
                access => Access,
                mode => Mode,
                connector_resource_id => ConnResId
            }),
            Msg = iolist_to_binary(
                io_lib:format(
                    "Bad disk log file permissions; access: ~0p, mode: ~3.8.0B",
                    [Access, Mode]
                )
            ),
            {?status_disconnected, Msg}
    end.

release_allocated_resources(ConnResId) ->
    maps:foreach(
        fun(?disk_log, LogName) ->
            log_when_error(
                fun() -> stop_disk_log(LogName) end,
                #{msg => "failed_to_stop_disk_log", log_name => LogName}
            )
        end,
        emqx_resource:get_allocated_resources(ConnResId)
    ).

-spec stop_disk_log(log_name()) -> ok | {error, term()}.
stop_disk_log(LogName) ->
    case disk_log:close(LogName) of
        ok -> ok;
        {error, no_such_log} -> ok;
        Err -> Err
    end.

log_when_error(Fun, Log) ->
    try
        Fun()
    catch
        C:E ->
            ?SLOG(error, Log#{
                exception => C,
                reason => E
            })
    end.

map_error(ok) -> ok;
map_error({error, Reason}) -> {error, {unrecoverable_error, Reason}}.
