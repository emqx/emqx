%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-export([timezone_offset_seconds/1]).

-ifdef(TEST).
-export([flush/1, get_wrap_logs/2]).
-endif.

-elvis([{elvis_style, export_used_types, disable}]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

%% Allocatable resources
-define(disk_log, disk_log).

-define(filepath, filepath).
-define(conn_config, conn_config).
-define(installed_actions, installed_actions).

-define(template, template).
-define(write_mode, write_mode).

-define(SECONDS_PER_DAY, 86400).

-type period() :: none | hour | day.
-type rotation() :: #{
    period := period(),
    retain_period_for_days => pos_integer(),
    timezone := binary()
}.
-type connector_config() :: #{
    filepath := binary(),
    max_file_size := pos_integer(),
    max_file_number := pos_integer(),
    rotation => rotation()
}.
-type connector_state() :: #{
    ?filepath := binary(),
    ?conn_config := connector_config(),
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
    #{filepath := FilepathBin} = ConnConfig,
    NowS = now_s(),
    ActiveFilepath = active_filepath(ConnConfig, NowS),
    maybe
        ok ?= do_open_log(ConnResId, ConnConfig#{filepath := ActiveFilepath}),
        ok = sweep_expired_period_files(ConnConfig, NowS),
        ConnState = #{
            ?filepath => FilepathBin,
            ?conn_config => ConnConfig,
            ?installed_actions => #{}
        },
        {ok, ConnState}
    else
        Error -> map_start_error(Error)
    end.

-spec on_stop(connector_resource_id(), connector_state()) -> ok.
on_stop(ConnResId, _ConnState) ->
    release_allocated_resources(ConnResId),
    ?tp("disk_log_connector_stop", #{instance_id => ConnResId}),
    ok.

-spec on_get_status(connector_resource_id(), connector_state()) ->
    ?status_connected | ?status_disconnected | {?status_disconnected, binary()}.
on_get_status(ConnResId, #{?conn_config := ConnConfig} = _ConnState) ->
    case disk_log:info(ConnResId) of
        {error, no_such_log} ->
            ?status_disconnected;
        LogInfo when is_list(LogInfo) ->
            check_period_and_file_status(ConnResId, ConnConfig, LogInfo)
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
    ok = emqx_resource:allocate_resource(ConnResId, ?MODULE, ?disk_log, ConnResId),
    Filepath = unicode:characters_to_list(FilepathBin, utf8),
    ArgL = [
        {name, ConnResId},
        {file, Filepath},
        {type, wrap},
        {format, external},
        {size, {MaxFileSize, MaxFileNumber}},
        {repair, false}
    ],
    maybe
        true ?= is_list(Filepath) orelse {error, <<"Bad filepath">>},
        ok ?= filelib:ensure_dir(FilepathBin),
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

%% Checks whether the log is still writing to the current period's file, rotating to a
%% new date-stamped file if the period (day / hour) has changed, before performing the
%% usual file status checks.
check_period_and_file_status(ConnResId, ConnConfig, LogInfo0) ->
    case maybe_rotate_period(ConnResId, ConnConfig, LogInfo0) of
        not_rotated ->
            check_file_status(open_filepath(LogInfo0), LogInfo0, ConnResId);
        rotated ->
            case disk_log:info(ConnResId) of
                LogInfo when is_list(LogInfo) ->
                    check_file_status(open_filepath(LogInfo), LogInfo, ConnResId);
                {error, no_such_log} ->
                    ?status_disconnected
            end;
        {error, Reason} ->
            ?SLOG(warning, #{
                msg => "failed_to_rotate_disk_log_period",
                reason => Reason,
                connector_resource_id => ConnResId
            }),
            Msg = iolist_to_binary(
                io_lib:format("Failed to rotate log file: ~0p", [Reason])
            ),
            {?status_disconnected, Msg}
    end.

maybe_rotate_period(ConnResId, ConnConfig, LogInfo) ->
    NowS = now_s(),
    ExpectedFilepath = active_filepath(ConnConfig, NowS),
    case open_filepath(LogInfo) =:= ExpectedFilepath of
        true ->
            not_rotated;
        false ->
            rotate_period(ConnResId, ConnConfig, ExpectedFilepath, NowS)
    end.

rotate_period(ConnResId, ConnConfig, NewFilepath, NowS) ->
    maybe
        ok ?= stop_disk_log(ConnResId),
        ok ?= do_open_log(ConnResId, ConnConfig#{filepath := NewFilepath}),
        ok = sweep_expired_period_files(ConnConfig, NowS),
        ?SLOG(info, #{
            msg => "disk_log_connector_period_rotation",
            new_filepath => NewFilepath,
            connector_resource_id => ConnResId
        }),
        ?tp("disk_log_connector_period_rotated", #{new_filepath => NewFilepath}),
        rotated
    end.

%% The file the underlying `disk_log' is currently open with.
open_filepath(LogInfo) ->
    {file, File} = lists:keyfind(file, 1, LogInfo),
    unicode:characters_to_binary(File).

%% The filepath the log should currently be writing to: the configured filepath, or,
%% when time-based rotation is enabled, the configured filepath with the current
%% period's date stamp inserted before the file extension (if any).
active_filepath(ConnConfig, TimeS) ->
    #{filepath := FilepathBin} = ConnConfig,
    Rotation = maps:get(rotation, ConnConfig, #{}),
    case maps:get(period, Rotation, none) of
        none ->
            FilepathBin;
        Period ->
            Timezone = maps:get(timezone, Rotation, <<"UTC">>),
            period_filepath(FilepathBin, period_key(Period, Timezone, TimeS))
    end.

period_filepath(BaseFilepath, PeriodKey) ->
    Root = filename:rootname(BaseFilepath),
    Ext = filename:extension(BaseFilepath),
    <<Root/binary, "-", PeriodKey/binary, Ext/binary>>.

%% Date stamp identifying the rotation period `TimeS' falls in, in the configured
%% timezone: e.g. `20260624' for `day', `2026062413' for `hour'.
period_key(day, Timezone, TimeS) ->
    format_date(TimeS, Timezone, <<"%Y%m%d">>);
period_key(hour, Timezone, TimeS) ->
    format_date(TimeS, Timezone, <<"%Y%m%d%H">>).

format_date(TimeS, Timezone, Format) ->
    Offset = timezone_offset_seconds(Timezone),
    unicode:characters_to_binary(emqx_utils_calendar:format(TimeS, second, Offset, Format)).

-spec timezone_offset_seconds(binary()) -> integer().
timezone_offset_seconds(<<"UTC">>) ->
    0;
timezone_offset_seconds(Timezone) ->
    emqx_utils_calendar:offset_second(Timezone).

%% Deletes files from periods older than the configured retention window.  Called after
%% each period rotation (and on start); no-op unless both `period' and
%% `retain_period_for_days' are configured.
sweep_expired_period_files(ConnConfig, NowS) ->
    #{filepath := FilepathBin} = ConnConfig,
    Rotation = maps:get(rotation, ConnConfig, #{}),
    RetainDays = maps:get(retain_period_for_days, Rotation, undefined),
    case maps:get(period, Rotation, none) of
        Period when Period =/= none, is_integer(RetainDays) ->
            Timezone = maps:get(timezone, Rotation, <<"UTC">>),
            CutoffKey = period_key(Period, Timezone, NowS - RetainDays * ?SECONDS_PER_DAY),
            delete_expired_period_files(FilepathBin, CutoffKey);
        _ ->
            ok
    end.

delete_expired_period_files(BaseFilepath, CutoffKey) ->
    Root = filename:rootname(BaseFilepath),
    Ext = filename:extension(BaseFilepath),
    Wildcard = unicode:characters_to_list(<<Root/binary, "-*", Ext/binary, ".*">>),
    Files = filelib:wildcard(Wildcard),
    lists:foreach(
        fun(File) ->
            case is_expired_period_file(File, Root, Ext, CutoffKey) of
                true ->
                    _ = file:delete(File),
                    ?SLOG(info, #{
                        msg => "disk_log_connector_expired_log_file_deleted",
                        file => File
                    });
                false ->
                    ok
            end
        end,
        Files
    ).

%% Matches `<Root>-<Stamp><Ext>.<Suffix>' where `Stamp' is a `period_key/3'-shaped date
%% stamp older than `CutoffKey', and `Suffix' is a wrap log suffix (`N', `idx' or
%% `siz').  Anything else is left alone.
is_expired_period_file(File, Root, Ext, CutoffKey) ->
    FileBin = unicode:characters_to_binary(File),
    Prefix = <<Root/binary, "-">>,
    PrefixSize = byte_size(Prefix),
    ExtSize = byte_size(Ext),
    maybe
        <<Prefix:PrefixSize/binary, Rest/binary>> ?= FileBin,
        {Stamp, <<Ext:ExtSize/binary, $., Suffix/binary>>} ?=
            string:take(Rest, lists:seq($0, $9)),
        is_period_key(Stamp) andalso Stamp < CutoffKey andalso is_wrap_log_suffix(Suffix)
    else
        _ -> false
    end.

is_period_key(Stamp) ->
    byte_size(Stamp) =:= 8 orelse byte_size(Stamp) =:= 10.

is_wrap_log_suffix(<<"idx">>) ->
    true;
is_wrap_log_suffix(<<"siz">>) ->
    true;
is_wrap_log_suffix(Suffix) ->
    Suffix =/= <<>> andalso {Suffix, <<>>} =:= string:take(Suffix, lists:seq($0, $9)).

-ifdef(TEST).
%% Clock seam for tests: allows fixing "now" to exercise period boundaries.
now_s() ->
    persistent_term:get({?MODULE, now_s}, erlang:system_time(second)).
-else.
now_s() ->
    erlang:system_time(second).
-endif.

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
    ?tp("disk_log_connector_wrote_terms", #{}),
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

check_file_status(Filepath, LogInfo, ConnResId) ->
    maybe
        [Current | _] ?= get_wrap_logs(Filepath, LogInfo),
        {ok, #file_info{access = read_write}} ?=
            file:read_file_info(Current),
        ?status_connected
    else
        [] ->
            Msg = <<"Current log file not found">>,
            ?SLOG(warning, #{
                msg => "failed_to_get_disk_log_file_info",
                reason => Msg,
                connector_resource_id => ConnResId
            }),
            {?status_disconnected, Msg};
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

%% Gets all logs (including current file) in reverse chronological order.
get_wrap_logs(Filepath, LogInfo) ->
    Wildcard = unicode:characters_to_list(iolist_to_binary([Filepath, ".*"]), utf8),
    Files0 = filelib:wildcard(Wildcard),
    Files1 = lists:filter(
        fun(File) ->
            (not lists:suffix(".siz", File)) andalso (not lists:suffix(".idx", File))
        end,
        Files0
    ),
    Files2 = lists:map(
        fun(File) ->
            {ok, #file_info{mtime = MAt}} = file:read_file_info(File, [{time, posix}]),
            %% If a rotation has just happened, two or more files may have the same
            %% modified timestamp.  We choose the current file based on the log info, if
            %% available
            case is_current_file(File, LogInfo) of
                true ->
                    %% In Erlang term order, atoms will be greater than any integer.
                    {current, File};
                false ->
                    {MAt, File}
            end
        end,
        Files1
    ),
    Files = lists:keysort(1, Files2),
    lists:reverse(lists:map(fun({_MAt, File}) -> File end, Files)).

is_current_file(File, LogInfo) ->
    case lists:keyfind(current_file, 1, LogInfo) of
        {current_file, N} ->
            lists:suffix("." ++ integer_to_list(N), File);
        _ ->
            false
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

map_error(ok) ->
    ok;
map_error({error, no_such_log}) ->
    %% May happen transiently while the log is closed and reopened with a new
    %% date-stamped filepath by a concurrent period rotation; retried by the buffer
    %% worker.
    {error, {recoverable_error, no_such_log}};
map_error({error, Reason}) ->
    {error, {unrecoverable_error, Reason}}.

map_start_error({error, {file_error, _Filepath, Reason}}) ->
    {error, emqx_utils:explain_posix(Reason)};
map_start_error({error, Reason}) ->
    {error, emqx_utils:explain_posix(Reason)};
map_start_error(Error) ->
    Error.
