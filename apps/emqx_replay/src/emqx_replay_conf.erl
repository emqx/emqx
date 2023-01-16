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

-export([zone_iteration_options/1]).
-export([default_iteration_options/0]).

%%================================================================================
%% API funcions
%%================================================================================

-define(APP, emqx_replay).

-type zone() :: emqx_types:zone().
-type config() ::
    {emqx_replay_message_storage, emqx_replay_message_storage:options()}
    | {module(), _Options}.

-spec zone_config(zone()) -> config().
zone_config(Zone) ->
    DefaultZoneConfig = application:get_env(?APP, default_zone_config, default_zone_config()),
    Zones = application:get_env(?APP, zone_config, #{}),
    maps:get(Zone, Zones, DefaultZoneConfig).

-spec zone_iteration_options(zone()) -> emqx_replay_message_storage:iteration_options().
zone_iteration_options(Zone) ->
    case zone_config(Zone) of
        {emqx_replay_message_storage, Config} ->
            maps:get(iteration, Config, default_iteration_options());
        {_Module, _} ->
            default_iteration_options()
    end.

-spec default_iteration_options() -> emqx_replay_message_storage:iteration_options().
default_iteration_options() ->
    {emqx_replay_message_storage, Config} = default_zone_config(),
    maps:get(iteration, Config).

-spec default_zone_config() -> config().
default_zone_config() ->
    {emqx_replay_message_storage, #{
        timestamp_bits => 64,
        topic_bits_per_level => [8, 8, 8, 32, 16],
        epoch => 5,
        iteration => #{
            iterator_refresh => {every, 100}
        }
    }}.

-spec db_options() -> emqx_replay_local_store:db_options().
db_options() ->
    application:get_env(?APP, db_options, []).
