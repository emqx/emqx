%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_connector_jwt_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include("emqx_connector_tables.hrl").

-compile([export_all, nowarn_export_all]).

%%-----------------------------------------------------------------------------
%% CT boilerplate
%%-----------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_common_test_helpers:start_apps([emqx_connector]),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:stop_apps([emqx_connector]),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ets:delete_all_objects(?JWT_TABLE),
    ok.

%%-----------------------------------------------------------------------------
%% Helper fns
%%-----------------------------------------------------------------------------

insert_jwt(TId, ResourceId, JWT) ->
    ets:insert(TId, {{ResourceId, jwt}, JWT}).

%%-----------------------------------------------------------------------------
%% Test cases
%%-----------------------------------------------------------------------------

t_lookup_jwt_ok(_Config) ->
    TId = ?JWT_TABLE,
    JWT = <<"some jwt">>,
    ResourceId = <<"resource id">>,
    true = insert_jwt(TId, ResourceId, JWT),
    ?assertEqual({ok, JWT}, emqx_connector_jwt:lookup_jwt(ResourceId)),
    ok.

t_lookup_jwt_missing(_Config) ->
    ResourceId = <<"resource id">>,
    ?assertEqual({error, not_found}, emqx_connector_jwt:lookup_jwt(ResourceId)),
    ok.

t_delete_jwt(_Config) ->
    TId = ?JWT_TABLE,
    JWT = <<"some jwt">>,
    ResourceId = <<"resource id">>,
    true = insert_jwt(TId, ResourceId, JWT),
    {ok, _} = emqx_connector_jwt:lookup_jwt(ResourceId),
    ?assertEqual(ok, emqx_connector_jwt:delete_jwt(TId, ResourceId)),
    ?assertEqual({error, not_found}, emqx_connector_jwt:lookup_jwt(TId, ResourceId)),
    ok.
