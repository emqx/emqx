% %%--------------------------------------------------------------------
% %% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
% %%
% %% Licensed under the Apache License, Version 2.0 (the "License");
% %% you may not use this file except in compliance with the License.
% %% You may obtain a copy of the License at
% %% http://www.apache.org/licenses/LICENSE-2.0
% %%
% %% Unless required by applicable law or agreed to in writing, software
% %% distributed under the License is distributed on an "AS IS" BASIS,
% %% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
% %% See the License for the specific language governing permissions and
% %% limitations under the License.
% %%--------------------------------------------------------------------

-module(emqx_connector_mongo_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("emqx_connector.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(MONGO_HOST, "mongo").
-define(MONGO_CLIENT, 'emqx_connector_mongo_SUITE_client').

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    case emqx_common_test_helpers:is_tcp_server_available(?MONGO_HOST, ?MONGO_DEFAULT_PORT) of
        true ->
            ok = emqx_connector_test_helpers:start_apps([ecpool, mongodb]),
            Config;
        false ->
            {skip, no_mongo}
    end.

end_per_suite(_Config) ->
    ok = emqx_connector_test_helpers:stop_apps([ecpool, mongodb]).

init_per_testcase(_, Config) ->
    ?assertEqual(
        {ok, #{poolname => emqx_connector_mongo, type => single}},
        emqx_connector_mongo:on_start(<<"emqx_connector_mongo">>, mongo_config())
    ),
    Config.

end_per_testcase(_, _Config) ->
    ?assertEqual(
        ok,
        emqx_connector_mongo:on_stop(<<"emqx_connector_mongo">>, #{poolname => emqx_connector_mongo})
    ).

% %%------------------------------------------------------------------------------
% %% Testcases
% %%------------------------------------------------------------------------------

% Simple test to make sure the proper reference to the module is returned.
t_roots(_Config) ->
    ExpectedRoots = [
        {config, #{
            type =>
                {union, [
                    {ref, emqx_connector_mongo, single},
                    {ref, emqx_connector_mongo, rs},
                    {ref, emqx_connector_mongo, sharded}
                ]}
        }}
    ],
    ActualRoots = emqx_connector_mongo:roots(),
    ?assertEqual(ExpectedRoots, ActualRoots).

% Not sure if this level of testing is appropriate for this function.
% Checking the actual values/types of the returned term starts getting
% into checking the emqx_connector_schema_lib.erl returns and the shape
% of expected data elsewhere.
t_fields(_Config) ->
    AllFieldTypes = [single, rs, sharded, topology],
    lists:foreach(
        fun(FieldType) ->
            Fields = emqx_connector_mongo:fields(FieldType),
            lists:foreach(fun emqx_connector_test_helpers:check_fields/1, Fields)
        end,
        AllFieldTypes
    ).

% Execute a minimal query to validate connection.
t_basic_query(_Config) ->
    ?assertMatch(
        [],
        emqx_connector_mongo:on_query(
            <<"emqx_connector_mongo">>, {find, <<"connector">>, #{}, #{}}, undefined, #{
                poolname => emqx_connector_mongo
            }
        )
    ).

% Perform health check.
t_do_healthcheck(_Config) ->
    ?assertEqual(
        {ok, #{poolname => emqx_connector_mongo}},
        emqx_connector_mongo:on_health_check(<<"emqx_connector_mongo">>, #{
            poolname => emqx_connector_mongo
        })
    ).

% Perform healthcheck on a connector that does not exist.
t_healthceck_when_connector_does_not_exist(_Config) ->
    ?assertEqual(
        {error, health_check_failed, #{poolname => emqx_connector_mongo_does_not_exist}},
        emqx_connector_mongo:on_health_check(<<"emqx_connector_mongo_does_not_exist">>, #{
            poolname => emqx_connector_mongo_does_not_exist
        })
    ).

% %%------------------------------------------------------------------------------
% %% Helpers
% %%------------------------------------------------------------------------------

mongo_config() ->
    #{
        mongo_type => single,
        pool_size => 8,
        ssl => #{enable => false},
        srv_record => false,
        server => {<<?MONGO_HOST>>, ?MONGO_DEFAULT_PORT}
    }.
