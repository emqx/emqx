%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_bridge_http_v2_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-import(emqx_mgmt_api_test_util, [request/3]).
-import(emqx_common_test_helpers, [on_exit/1]).
-import(emqx_bridge_http_SUITE, [start_http_server/1, stop_http_server/1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").

-define(BRIDGE_TYPE, <<"http">>).
-define(BRIDGE_NAME, atom_to_binary(?MODULE)).
-define(CONNECTOR_NAME, atom_to_binary(?MODULE)).

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_suite(Config0) ->
    Config =
        case os:getenv("DEBUG_CASE") of
            [_ | _] = DebugCase ->
                CaseName = list_to_atom(DebugCase),
                [{debug_case, CaseName} | Config0];
            _ ->
                Config0
        end,
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_connector,
            emqx_bridge_http,
            emqx_bridge,
            emqx_rule_engine
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    emqx_mgmt_api_test_util:init_suite(),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_mgmt_api_test_util:end_suite(),
    ok = emqx_cth_suite:stop(Apps),
    ok.

suite() ->
    [{timetrap, {seconds, 60}}].

init_per_testcase(_TestCase, Config) ->
    Server = start_http_server(#{response_delay_ms => 0}),
    [{http_server, Server} | Config].

end_per_testcase(_TestCase, Config) ->
    case ?config(http_server, Config) of
        undefined -> ok;
        Server -> stop_http_server(Server)
    end,
    emqx_bridge_v2_testlib:delete_all_bridges(),
    emqx_bridge_v2_testlib:delete_all_connectors(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%--------------------------------------------------------------------
%% tests
%%--------------------------------------------------------------------

t_compose_connector_url_and_action_path(Config) ->
    Path = <<"/foo/bar">>,
    ConnectorCfg = make_connector_config(Config),
    ActionCfg = make_action_config([{path, Path} | Config]),
    CreateConfig = [
        {bridge_type, ?BRIDGE_TYPE},
        {bridge_name, ?BRIDGE_NAME},
        {bridge_config, ActionCfg},
        {connector_type, ?BRIDGE_TYPE},
        {connector_name, ?CONNECTOR_NAME},
        {connector_config, ConnectorCfg}
    ],
    {ok, _} = emqx_bridge_v2_testlib:create_bridge(CreateConfig),

    %% assert: the url returned v1 api is composed by the url of the connector and the
    %% path of the action
    #{port := Port} = ?config(http_server, Config),
    ExpectedUrl = iolist_to_binary(io_lib:format("http://localhost:~p/foo/bar", [Port])),
    {ok, {_, _, [Bridge]}} = emqx_bridge_testlib:list_bridges_api(),
    ?assertMatch(
        #{<<"url">> := ExpectedUrl},
        Bridge
    ),
    ok.

%%--------------------------------------------------------------------
%% helpers
%%--------------------------------------------------------------------

make_connector_config(Config) ->
    #{port := Port} = ?config(http_server, Config),
    #{
        <<"enable">> => true,
        <<"url">> => iolist_to_binary(io_lib:format("http://localhost:~p", [Port])),
        <<"headers">> => #{},
        <<"pool_type">> => <<"hash">>,
        <<"pool_size">> => 1
    }.

make_action_config(Config) ->
    Path = ?config(path, Config),
    #{
        <<"enable">> => true,
        <<"connector">> => ?CONNECTOR_NAME,
        <<"parameters">> => #{
            <<"path">> => Path,
            <<"method">> => <<"post">>,
            <<"headers">> => #{},
            <<"body">> => <<"${.}">>
        }
    }.
