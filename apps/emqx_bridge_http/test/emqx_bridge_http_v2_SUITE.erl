%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

init_per_testcase(t_update_with_sensitive_data, Config) ->
    HTTPPath = <<"/foo/bar">>,
    ServerSSLOpts = false,
    {ok, {HTTPPort, _Pid}} = emqx_bridge_http_connector_test_server:start_link(
        _Port = random, HTTPPath, ServerSSLOpts
    ),
    ok = emqx_bridge_http_connector_test_server:set_handler(success_handler()),
    [{path, HTTPPath}, {http_server, #{port => HTTPPort, path => HTTPPath}} | Config];
init_per_testcase(_TestCase, Config) ->
    Server = start_http_server(#{response_delay_ms => 0}),
    [{http_server, Server} | Config].

end_per_testcase(t_update_with_sensitive_data, Config) ->
    ok = emqx_bridge_http_connector_test_server:stop(),
    end_per_testcase(common, proplists:delete(http_server, Config));
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

%% Checks that we can successfully update a connector containing sensitive headers and
%% they won't be clobbered by the update.
t_update_with_sensitive_data(Config) ->
    ConnectorCfg0 = make_connector_config(Config),
    AuthHeader = <<"Bearer some_token">>,
    ConnectorCfg1 = emqx_utils_maps:deep_merge(
        ConnectorCfg0,
        #{
            <<"headers">> => #{
                <<"authorization">> => AuthHeader,
                <<"x-test-header">> => <<"from-connector">>
            }
        }
    ),
    ActionCfg = make_action_config(Config, #{<<"x-test-header">> => <<"from-action">>}),
    CreateConfig = [
        {bridge_kind, action},
        {action_type, ?BRIDGE_TYPE},
        {action_name, ?BRIDGE_NAME},
        {action_config, ActionCfg},
        {connector_type, ?BRIDGE_TYPE},
        {connector_name, ?CONNECTOR_NAME},
        {connector_config, ConnectorCfg1}
    ],
    {ok, {{_, 201, _}, _, #{<<"headers">> := #{<<"authorization">> := Obfuscated}}}} =
        emqx_bridge_v2_testlib:create_connector_api(CreateConfig),
    {ok, _} =
        emqx_bridge_v2_testlib:create_kind_api(CreateConfig),
    BridgeId = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE, ?BRIDGE_NAME),
    {ok, _} = emqx_bridge_v2_testlib:create_rule_api(
        #{
            sql => <<"select * from \"t/http\" ">>,
            actions => [BridgeId]
        }
    ),
    emqx:publish(emqx_message:make(<<"t/http">>, <<"1">>)),
    ?assertReceive(
        {http,
            #{
                <<"authorization">> := AuthHeader,
                <<"x-test-header">> := <<"from-action">>
            },
            _}
    ),

    %% Now update the connector and see if the header stays deobfuscated.  We send the old
    %% auth header as an obfuscated value to simulate the behavior of the frontend.
    ConnectorCfg2 = emqx_utils_maps:deep_merge(
        ConnectorCfg1,
        #{
            <<"headers">> => #{
                <<"authorization">> => Obfuscated,
                <<"x-test-header">> => <<"from-connector-new">>,
                <<"x-test-header-2">> => <<"from-connector-new">>,
                <<"other_header">> => <<"new">>
            }
        }
    ),
    {ok, _} = emqx_bridge_v2_testlib:update_connector_api(
        ?CONNECTOR_NAME,
        ?BRIDGE_TYPE,
        ConnectorCfg2
    ),

    emqx:publish(emqx_message:make(<<"t/http">>, <<"2">>)),
    %% Should not be obfuscated.
    ?assertReceive(
        {http,
            #{
                <<"authorization">> := AuthHeader,
                <<"x-test-header">> := <<"from-action">>,
                <<"x-test-header-2">> := <<"from-connector-new">>
            },
            _},
        2_000
    ),
    ok.

t_disable_action_counters(Config) ->
    ConnectorCfg = make_connector_config(Config),
    ActionCfg = make_action_config(Config),
    CreateConfig = [
        {bridge_kind, action},
        {action_type, ?BRIDGE_TYPE},
        {action_name, ?BRIDGE_NAME},
        {action_config, ActionCfg},
        {connector_type, ?BRIDGE_TYPE},
        {connector_name, ?CONNECTOR_NAME},
        {connector_config, ConnectorCfg}
    ],
    {ok, {{_, 201, _}, _, _}} =
        emqx_bridge_v2_testlib:create_connector_api(CreateConfig),
    {ok, _} =
        emqx_bridge_v2_testlib:create_kind_api(CreateConfig),
    BridgeId = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE, ?BRIDGE_NAME),
    {ok, Rule} = emqx_bridge_v2_testlib:create_rule_api(
        #{
            sql => <<"select * from \"t/http\" ">>,
            actions => [BridgeId]
        }
    ),
    {{_, 201, _}, _, #{<<"id">> := RuleId}} = Rule,
    emqx:publish(emqx_message:make(<<"t/http">>, <<"1">>)),
    ?assertReceive({http_server, received, _}, 2_000),

    ?retry(
        _Interval = 500,
        _NAttempts = 20,
        ?assertMatch(
            #{
                counters := #{
                    'matched' := 1,
                    'actions.failed' := 0,
                    'actions.failed.unknown' := 0,
                    'actions.success' := 1,
                    'actions.total' := 1,
                    'actions.discarded' := 0
                }
            },
            emqx_metrics_worker:get_metrics(rule_metrics, RuleId)
        )
    ),

    %% disable the action
    {ok, {{_, 200, _}, _, _}} =
        emqx_bridge_v2_testlib:update_bridge_api(CreateConfig, #{<<"enable">> => false}),

    %% this will trigger a discard
    emqx:publish(emqx_message:make(<<"t/http">>, <<"2">>)),
    ?retry(
        _Interval = 500,
        _NAttempts = 20,
        ?assertMatch(
            #{
                counters := #{
                    'matched' := 2,
                    'actions.failed' := 0,
                    'actions.failed.unknown' := 0,
                    'actions.success' := 1,
                    'actions.total' := 2,
                    'actions.discarded' := 1
                }
            },
            emqx_metrics_worker:get_metrics(rule_metrics, RuleId)
        )
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
        <<"pool_size">> => 1,
        <<"resource_opts">> => #{
            <<"health_check_interval">> => <<"100ms">>
        }
    }.

make_action_config(Config) ->
    make_action_config(Config, _Headers = #{}).

make_action_config(Config, Headers) ->
    Path = ?config(path, Config),
    #{
        <<"enable">> => true,
        <<"connector">> => ?CONNECTOR_NAME,
        <<"parameters">> => #{
            <<"path">> => Path,
            <<"method">> => <<"post">>,
            <<"headers">> => Headers,
            <<"body">> => <<"${.}">>
        },
        <<"resource_opts">> => #{
            <<"health_check_interval">> => <<"100ms">>
        }
    }.

success_handler() ->
    TestPid = self(),
    fun(Req0, State) ->
        {ok, Body, Req} = cowboy_req:read_body(Req0),
        TestPid ! {http, cowboy_req:headers(Req), Body},
        Rep = cowboy_req:reply(
            200,
            #{<<"content-type">> => <<"application/json">>},
            <<"{}">>,
            Req
        ),
        {ok, Rep, State}
    end.
