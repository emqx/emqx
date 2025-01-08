%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_telemetry_config).

%% Public API
-export([
    set_telemetry_status/1,
    is_enabled/0
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

is_enabled() ->
    IsOfficial = ?MODULE:is_official_version(),
    emqx_conf:get([telemetry, enable], IsOfficial).

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
