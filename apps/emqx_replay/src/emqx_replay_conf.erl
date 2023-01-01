%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_replay_conf).

%% TODO: make a proper HOCON schema and all...

%% API:
-export([zone_config/1, db_options/0]).

%%================================================================================
%% API funcions
%%================================================================================

-define(APP, emqx_replay).

-spec zone_config(emqx_types:zone()) ->
    {module(), term()}.
zone_config(Zone) ->
    DefaultConf =
        #{
            timestamp_bits => 64,
            topic_bits_per_level => [8, 8, 8, 32, 16],
            max_tau => 5
        },
    DefaultZoneConfig = application:get_env(
        ?APP, default_zone_config, {emqx_replay_message_storage, DefaultConf}
    ),
    Zones = application:get_env(?APP, zone_config, #{}),
    maps:get(Zone, Zones, DefaultZoneConfig).

-spec db_options() -> emqx_replay_local_store:db_options().
db_options() ->
    application:get_env(?APP, db_options, []).
