%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_telemetry_config).

%% Public API
-export([
    set_telemetry_status/1,
    is_enabled/0,
    set_default_status/1
]).

%% emqx_config_handler callback
-export([
    pre_config_update/3,
    post_config_update/5
]).

%% internal use
-export([
    on_server_start/0,
    on_server_stop/0,
    is_official_version/1,
    is_official_version/0
]).

%% @doc License control.
set_default_status(IsEnable) ->
    _ = application:set_env(emqx_telemetry, default_status, IsEnable),
    case is_enabled() of
        true ->
            emqx_telemetry:start_reporting();
        false ->
            emqx_telemetry:stop_reporting()
    end,
    ok.

%% @doc Check if telemetry is enabled.
is_enabled() ->
    case emqx_conf:get([telemetry, enable], no_value) of
        Bool when is_boolean(Bool) ->
            %% Configured from config file to enable/disable telemetry
            Bool;
        no_value ->
            case ?MODULE:is_official_version() of
                true ->
                    %% Control by license
                    application:get_env(emqx_telemetry, default_status, false);
                false ->
                    %% For non-official version, telemetry is disabled
                    false
            end
    end.

on_server_start() ->
    emqx_conf:add_handler([telemetry], ?MODULE).

on_server_stop() ->
    emqx_conf:remove_handler([telemetry]).

-spec set_telemetry_status(boolean()) -> ok | {error, term()}.
set_telemetry_status(Status) ->
    case cfg_update([telemetry], set_telemetry_status, Status) of
        {ok, _} -> ok;
        {error, _} = Error -> Error
    end.

pre_config_update(_, {set_telemetry_status, Status}, RawConf) ->
    {ok, RawConf#{<<"enable">> => Status}};
pre_config_update(_, NewConf, _OldConf) ->
    {ok, NewConf}.

post_config_update(
    _,
    {set_telemetry_status, Status},
    _NewConfig,
    _OldConfig,
    _AppEnvs
) ->
    case Status of
        true -> emqx_telemetry:start_reporting();
        false -> emqx_telemetry:stop_reporting()
    end;
post_config_update(_, _UpdateReq, NewConf, _OldConf, _AppEnvs) ->
    case maps:get(enable, NewConf, ?MODULE:is_official_version()) of
        true -> emqx_telemetry:start_reporting();
        false -> emqx_telemetry:stop_reporting()
    end.

cfg_update(Path, Action, Params) ->
    emqx_conf:update(
        Path,
        {Action, Params},
        #{override_to => cluster}
    ).

is_official_version() ->
    is_official_version(emqx_release:version()).

is_official_version(Version) ->
    Pt = "^\\d+\\.\\d+(?:\\.\\d+)?(?:(-(?:alpha|beta|rc)\\.[1-9][0-9]*))?$",
    match =:= re:run(Version, Pt, [{capture, none}]).
