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

-module(emqx_connector_pgsql_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("emqx_connector.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(PGSQL_HOST, "pgsql").

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    case emqx_common_test_helpers:is_tcp_server_available(?PGSQL_HOST, ?PGSQL_DEFAULT_PORT) of
        true ->
            Config;
        false ->
            {skip, no_pgsql}
    end.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_, Config) ->
    ?assertEqual(
        {ok, #{poolname => emqx_connector_pgsql}},
        emqx_connector_pgsql:on_start(<<"emqx_connector_pgsql">>, pgsql_config())
    ),
    Config.

end_per_testcase(_, _Config) ->
    ?assertEqual(
        ok,
        emqx_connector_pgsql:on_stop(<<"emqx_connector_pgsql">>, #{poolname => emqx_connector_pgsql})
    ).

% %%------------------------------------------------------------------------------
% %% Testcases
% %%------------------------------------------------------------------------------

% Simple test to make sure the proper reference to the module is returned.
t_roots(_Config) ->
    ExpectedRoots = [{config, #{type => {ref, emqx_connector_pgsql, config}}}],
    ActualRoots = emqx_connector_pgsql:roots(),
    ?assertEqual(ExpectedRoots, ActualRoots).

% Not sure if this level of testing is appropriate for this function.
% Checking the actual values/types of the returned term starts getting
% into checking the emqx_connector_schema_lib.erl returns and the shape
% of expected data elsewhere.
t_fields(_Config) ->
    Fields = emqx_connector_pgsql:fields(config),
    lists:foreach(
        fun({FieldName, FieldValue}) ->
            ?assert(is_atom(FieldName)),
            if
                is_map(FieldValue) ->
                    ?assert(maps:is_key(type, FieldValue) and maps:is_key(default, FieldValue));
                true ->
                    ?assert(is_function(FieldValue))
            end
        end,
        Fields
    ).

% Execute a minimal query to validate connection.
t_basic_query(_Config) ->
    ?assertMatch(
        {ok, _, [{1}]},
        emqx_connector_pgsql:on_query(
            <<"emqx_connector_pgsql">>, {query, test_query()}, undefined, #{
                poolname => emqx_connector_pgsql
            }
        )
    ).

% Perform health check.
t_do_healthcheck(_Config) ->
    ?assertEqual(
        {ok, #{poolname => emqx_connector_pgsql}},
        emqx_connector_pgsql:on_health_check(<<"emqx_connector_pgsql">>, #{
            poolname => emqx_connector_pgsql
        })
    ).

% Perform healthcheck on a connector that does not exist.
t_healthceck_when_connector_does_not_exist(_Config) ->
    ?assertEqual(
        {error, health_check_failed, #{poolname => emqx_connector_pgsql_does_not_exist}},
        emqx_connector_pgsql:on_health_check(<<"emqx_connector_pgsql_does_not_exist">>, #{
            poolname => emqx_connector_pgsql_does_not_exist
        })
    ).

% %%------------------------------------------------------------------------------
% %% Helpers
% %%------------------------------------------------------------------------------

pgsql_config() ->
    #{
        auto_reconnect => true,
        database => <<"mqtt">>,
        username => <<"root">>,
        password => <<"public">>,
        pool_size => 8,
        server => {?PGSQL_HOST, ?PGSQL_DEFAULT_PORT},
        ssl => #{enable => false}
    }.

pgsql_bad_config() ->
    #{
        auto_reconnect => true,
        database => <<"bad_mqtt">>,
        username => <<"bad_root">>,
        password => <<"bad_public">>,
        pool_size => 8,
        server => {?PGSQL_HOST, ?PGSQL_DEFAULT_PORT},
        ssl => #{enable => false}
    }.

test_query() ->
    <<"SELECT 1">>.
