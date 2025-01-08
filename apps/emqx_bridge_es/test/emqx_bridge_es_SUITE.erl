%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_bridge_es_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

-define(TYPE, elasticsearch).
-define(CA, "es.crt").

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_common_test_helpers:clear_screen(),
    ProxyName = "elasticsearch",
    ESHost = os:getenv("ELASTICSEARCH_HOST", "elasticsearch"),
    ESPort = list_to_integer(os:getenv("ELASTICSEARCH_PORT", "9200")),
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_connector,
            emqx_bridge_es,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management,
            {emqx_dashboard, "dashboard.listeners.http { enable = true, bind = 18083 }"}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    {ok, _} = emqx_common_test_http:create_default_app(),
    wait_until_elasticsearch_is_up(ESHost, ESPort),
    [
        {apps, Apps},
        {proxy_name, ProxyName},
        {es_host, ESHost},
        {es_port, ESPort}
        | Config
    ].

es_checks() ->
    case os:getenv("IS_CI") of
        "yes" -> 10;
        _ -> 1
    end.

wait_until_elasticsearch_is_up(Host, Port) ->
    wait_until_elasticsearch_is_up(es_checks(), Host, Port).

wait_until_elasticsearch_is_up(0, Host, Port) ->
    throw({{Host, Port}, not_available});
wait_until_elasticsearch_is_up(Count, Host, Port) ->
    timer:sleep(1000),
    case emqx_common_test_helpers:is_all_tcp_servers_available([{Host, Port}]) of
        true -> ok;
        false -> wait_until_elasticsearch_is_up(Count - 1, Host, Port)
    end.

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_common_test_helpers:call_janitor(60_000),
    ok.

%%-------------------------------------------------------------------------------------
%% Helper fns
%%-------------------------------------------------------------------------------------

check_send_message_with_action(Topic, ActionName, ConnectorName, Expect) ->
    send_message(Topic),
    %% ######################################
    %% Check if message is sent to es
    %% ######################################
    timer:sleep(500),
    check_action_metrics(ActionName, ConnectorName, Expect).

send_message(Topic) ->
    Now = emqx_utils_calendar:now_to_rfc3339(microsecond),
    Doc = #{<<"name">> => <<"emqx">>, <<"release_date">> => Now},
    Index = <<"emqx-test-index">>,
    Payload = emqx_utils_json:encode(#{doc => Doc, index => Index}),

    ClientId = emqx_guid:to_hexstr(emqx_guid:gen()),
    {ok, Client} = emqtt:start_link([{clientid, ClientId}, {port, 1883}]),
    {ok, _} = emqtt:connect(Client),
    ok = emqtt:publish(Client, Topic, Payload, [{qos, 0}]),
    ok.

check_action_metrics(ActionName, ConnectorName, Expect) ->
    ActionId = emqx_bridge_v2:id(?TYPE, ActionName, ConnectorName),
    ?retry(
        300,
        20,
        ?assertEqual(
            Expect,
            #{
                match => emqx_resource_metrics:matched_get(ActionId),
                success => emqx_resource_metrics:success_get(ActionId),
                failed => emqx_resource_metrics:failed_get(ActionId),
                queuing => emqx_resource_metrics:queuing_get(ActionId),
                dropped => emqx_resource_metrics:dropped_get(ActionId)
            },
            {ActionName, ConnectorName, ActionId}
        )
    ).

action_config(ConnectorName) ->
    action_config(ConnectorName, _Overrides = #{}).

action_config(ConnectorName, Overrides) ->
    Cfg0 = action(ConnectorName),
    emqx_utils_maps:deep_merge(Cfg0, Overrides).

action(ConnectorName) ->
    #{
        <<"description">> => <<"My elasticsearch test action">>,
        <<"enable">> => true,
        <<"parameters">> => #{
            <<"index">> => <<"${payload.index}">>,
            <<"action">> => <<"create">>,
            <<"doc">> => <<"${payload.doc}">>,
            <<"overwrite">> => true
        },
        <<"connector">> => ConnectorName,
        <<"resource_opts">> => #{
            <<"health_check_interval">> => <<"30s">>,
            <<"query_mode">> => <<"sync">>,
            <<"metrics_flush_interval">> => <<"300ms">>
        }
    }.

server(Config) ->
    Host = ?config(es_host, Config),
    Port = ?config(es_port, Config),
    iolist_to_binary([
        Host,
        ":",
        integer_to_binary(Port)
    ]).

connector_config(Config) ->
    connector_config(_Overrides = #{}, Config).

connector_config(Overrides, Config) ->
    Defaults =
        #{
            <<"server">> => server(Config),
            <<"enable">> => true,
            <<"authentication">> => #{
                <<"password">> => <<"emqx123">>,
                <<"username">> => <<"elastic">>
            },
            <<"description">> => <<"My elasticsearch test connector">>,
            <<"connect_timeout">> => <<"15s">>,
            <<"pool_size">> => 2,
            <<"pool_type">> => <<"random">>,
            <<"enable_pipelining">> => 100,
            <<"ssl">> => #{
                <<"enable">> => true,
                <<"hibernate_after">> => <<"5s">>,
                <<"cacertfile">> => filename:join(?config(data_dir, Config), ?CA)
            }
        },
    emqx_utils_maps:deep_merge(Defaults, Overrides).

create_connector(Name, Config) ->
    Res = emqx_connector:create(?TYPE, Name, Config),
    on_exit(fun() -> emqx_connector:remove(?TYPE, Name) end),
    Res.

create_action(Name, Config) ->
    Res = emqx_bridge_v2:create(?TYPE, Name, Config),
    on_exit(fun() -> emqx_bridge_v2:remove(?TYPE, Name) end),
    Res.

action_api_spec_props_for_get() ->
    #{
        <<"bridge_elasticsearch.get_bridge_v2">> :=
            #{<<"properties">> := Props}
    } =
        emqx_bridge_v2_testlib:actions_api_spec_schemas(),
    Props.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_create_remove_list(Config) ->
    [] = emqx_bridge_v2:list(),
    ConnectorConfig = connector_config(Config),
    {ok, _} = emqx_connector:create(?TYPE, test_connector, ConnectorConfig),
    ActionConfig = action(<<"test_connector">>),
    {ok, _} = emqx_bridge_v2:create(?TYPE, test_action_1, ActionConfig),
    [ActionInfo] = emqx_bridge_v2:list(),
    #{
        name := <<"test_action_1">>,
        type := <<"elasticsearch">>,
        raw_config := _,
        status := connected
    } = ActionInfo,
    {ok, _} = emqx_bridge_v2:create(?TYPE, test_action_2, ActionConfig),
    2 = length(emqx_bridge_v2:list()),
    ok = emqx_bridge_v2:remove(?TYPE, test_action_1),
    1 = length(emqx_bridge_v2:list()),
    ok = emqx_bridge_v2:remove(?TYPE, test_action_2),
    [] = emqx_bridge_v2:list(),
    emqx_connector:remove(?TYPE, test_connector),
    ok.

%% Test sending a message to a bridge V2
t_create_message(Config) ->
    ConnectorConfig = connector_config(Config),
    {ok, _} = emqx_connector:create(?TYPE, test_connector2, ConnectorConfig),
    ActionConfig = action(<<"test_connector2">>),
    {ok, _} = emqx_bridge_v2:create(?TYPE, test_action_1, ActionConfig),
    Rule = #{
        id => <<"rule:t_es">>,
        sql => <<"SELECT\n  *\nFROM\n  \"es/#\"">>,
        actions => [<<"elasticsearch:test_action_1">>],
        description => <<"sink doc to elasticsearch">>
    },
    {ok, _} = emqx_rule_engine:create_rule(Rule),
    %% Use the action to send a message
    Expect = #{match => 1, success => 1, dropped => 0, failed => 0, queuing => 0},
    check_send_message_with_action(<<"es/1">>, test_action_1, test_connector2, Expect),
    %% Create a few more bridges with the same connector and test them
    ActionNames1 =
        lists:foldl(
            fun(I, Acc) ->
                Seq = integer_to_binary(I),
                ActionNameStr = "test_action_" ++ integer_to_list(I),
                ActionName = list_to_atom(ActionNameStr),
                {ok, _} = emqx_bridge_v2:create(?TYPE, ActionName, ActionConfig),
                Rule1 = #{
                    id => <<"rule:t_es", Seq/binary>>,
                    sql => <<"SELECT\n  *\nFROM\n  \"es/", Seq/binary, "\"">>,
                    actions => [<<"elasticsearch:", (list_to_binary(ActionNameStr))/binary>>],
                    description => <<"sink doc to elasticsearch">>
                },
                {ok, _} = emqx_rule_engine:create_rule(Rule1),
                Topic = <<"es/", Seq/binary>>,
                check_send_message_with_action(Topic, ActionName, test_connector2, Expect),
                [ActionName | Acc]
            end,
            [],
            lists:seq(2, 10)
        ),
    ActionNames = [test_action_1 | ActionNames1],
    %% Remove all the bridges
    lists:foreach(
        fun(BridgeName) ->
            ok = emqx_bridge_v2:remove(?TYPE, BridgeName)
        end,
        ActionNames
    ),
    emqx_connector:remove(?TYPE, test_connector2),
    lists:foreach(
        fun(#{id := Id}) ->
            emqx_rule_engine:delete_rule(Id)
        end,
        emqx_rule_engine:get_rules()
    ),
    ok.

t_update_message(Config) ->
    ConnectorConfig = connector_config(Config),
    {ok, _} = emqx_connector:create(?TYPE, update_connector, ConnectorConfig),
    ActionConfig0 = action(<<"update_connector">>),
    DocId = emqx_guid:to_hexstr(emqx_guid:gen()),
    ActionConfig1 = ActionConfig0#{
        <<"parameters">> => #{
            <<"index">> => <<"${payload.index}">>,
            <<"id">> => DocId,
            <<"max_retries">> => 0,
            <<"action">> => <<"update">>,
            <<"doc">> => <<"${payload.doc}">>
        }
    },
    {ok, _} = emqx_bridge_v2:create(?TYPE, update_action, ActionConfig1),
    Rule = #{
        id => <<"rule:t_es_1">>,
        sql => <<"SELECT\n  *\nFROM\n  \"es/#\"">>,
        actions => [<<"elasticsearch:update_action">>],
        description => <<"sink doc to elasticsearch">>
    },
    {ok, _} = emqx_rule_engine:create_rule(Rule),
    %% failed to update a nonexistent doc
    Expect0 = #{match => 1, success => 0, dropped => 0, failed => 1, queuing => 0},
    check_send_message_with_action(<<"es/1">>, update_action, update_connector, Expect0),
    %% doc_as_upsert to insert a new doc
    ActionConfig2 = ActionConfig1#{
        <<"parameters">> => #{
            <<"index">> => <<"${payload.index}">>,
            <<"id">> => DocId,
            <<"action">> => <<"update">>,
            <<"doc">> => <<"${payload.doc}">>,
            <<"doc_as_upsert">> => true,
            <<"max_retries">> => 0
        }
    },
    {ok, _} = emqx_bridge_v2:create(?TYPE, update_action, ActionConfig2),
    Expect1 = #{match => 1, success => 1, dropped => 0, failed => 0, queuing => 0},
    check_send_message_with_action(<<"es/1">>, update_action, update_connector, Expect1),
    %% update without doc, use msg as default
    ActionConfig3 = ActionConfig1#{
        <<"parameters">> => #{
            <<"index">> => <<"${payload.index}">>,
            <<"id">> => DocId,
            <<"action">> => <<"update">>,
            <<"max_retries">> => 0
        }
    },
    {ok, _} = emqx_bridge_v2:create(?TYPE, update_action, ActionConfig3),
    Expect2 = #{match => 1, success => 1, dropped => 0, failed => 0, queuing => 0},
    check_send_message_with_action(<<"es/1">>, update_action, update_connector, Expect2),
    %% Clean
    ok = emqx_bridge_v2:remove(?TYPE, update_action),
    emqx_connector:remove(?TYPE, update_connector),
    lists:foreach(
        fun(#{id := Id}) ->
            emqx_rule_engine:delete_rule(Id)
        end,
        emqx_rule_engine:get_rules()
    ),
    ok.

%% Test that we can get the status of the bridge V2
t_health_check(Config) ->
    BridgeV2Config = action(<<"test_connector3">>),
    ConnectorConfig = connector_config(Config),
    {ok, _} = emqx_connector:create(?TYPE, test_connector3, ConnectorConfig),
    {ok, _} = emqx_bridge_v2:create(?TYPE, test_bridge_v2, BridgeV2Config),
    #{status := connected} = emqx_bridge_v2:health_check(?TYPE, test_bridge_v2),
    ok = emqx_bridge_v2:remove(?TYPE, test_bridge_v2),
    %% Check behaviour when bridge does not exist
    {error, bridge_not_found} = emqx_bridge_v2:health_check(?TYPE, test_bridge_v2),
    ok = emqx_connector:remove(?TYPE, test_connector3),
    ok.

t_bad_url(Config) ->
    ConnectorName = <<"test_connector">>,
    ActionName = <<"test_action">>,
    ActionConfig = action(<<"test_connector">>),
    ConnectorConfig0 = connector_config(Config),
    ConnectorConfig = ConnectorConfig0#{<<"server">> := <<"bad_host:9092">>},
    ?assertMatch({ok, _}, create_connector(ConnectorName, ConnectorConfig)),
    ?assertMatch({ok, _}, create_action(ActionName, ActionConfig)),
    ?assertMatch(
        {ok, #{
            resource_data :=
                #{
                    status := ?status_disconnected,
                    error := failed_to_start_elasticsearch_bridge
                }
        }},
        emqx_connector:lookup(?TYPE, ConnectorName)
    ),
    ?assertMatch({ok, #{status := ?status_disconnected}}, emqx_bridge_v2:lookup(?TYPE, ActionName)),
    ok.

t_parameters_key_api_spec(_Config) ->
    ActionProps = action_api_spec_props_for_get(),
    ?assertNot(is_map_key(<<"elasticsearch">>, ActionProps), #{action_props => ActionProps}),
    ?assert(is_map_key(<<"parameters">>, ActionProps), #{action_props => ActionProps}),
    ok.

t_http_api_get(Config) ->
    ConnectorName = <<"test_connector">>,
    ActionName = <<"test_action">>,
    ActionConfig = action(ConnectorName),
    ConnectorConfig = connector_config(Config),
    ?assertMatch({ok, _}, create_connector(ConnectorName, ConnectorConfig)),
    ?assertMatch({ok, _}, create_action(ActionName, ActionConfig)),
    ?assertMatch(
        {ok,
            {{_, 200, _}, _, [
                #{
                    <<"connector">> := ConnectorName,
                    <<"description">> := <<"My elasticsearch test action">>,
                    <<"enable">> := true,
                    <<"error">> := <<>>,
                    <<"name">> := ActionName,
                    <<"node_status">> :=
                        [
                            #{
                                <<"node">> := _,
                                <<"status">> := <<"connected">>,
                                <<"status_reason">> := <<>>
                            }
                        ],
                    <<"parameters">> :=
                        #{
                            <<"action">> := <<"create">>,
                            <<"doc">> := <<"${payload.doc}">>,
                            <<"index">> := <<"${payload.index}">>,
                            <<"max_retries">> := 2,
                            <<"overwrite">> := true
                        },
                    <<"resource_opts">> := #{<<"query_mode">> := <<"sync">>},
                    <<"status">> := <<"connected">>,
                    <<"status_reason">> := <<>>,
                    <<"type">> := <<"elasticsearch">>
                }
            ]}},
        emqx_bridge_v2_testlib:list_bridges_api()
    ),
    ok.
