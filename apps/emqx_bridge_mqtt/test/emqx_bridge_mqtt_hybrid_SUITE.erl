%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_mqtt_hybrid_SUITE).

-moduledoc """
This suite holds test cases for `mqtt` connectors having actions and sources simultaneously.
""".

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-import(emqx_common_test_helpers, [on_exit/1]).

-define(CONNECTOR_TYPE, mqtt).
-define(CONNECTOR_TYPE_BIN, <<"mqtt">>).
-define(SOURCE_TYPE, mqtt).
-define(SOURCE_TYPE_BIN, <<"mqtt">>).
-define(ACTION_TYPE, mqtt).
-define(ACTION_TYPE_BIN, <<"mqtt">>).

-define(local, local).
-define(cluster, cluster).

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    [
        {group, ?cluster},
        {group, ?local}
    ].

groups() ->
    AllTCs0 = emqx_common_test_helpers:all_with_matrix(?MODULE),
    AllTCs = lists:filter(
        fun
            ({group, _}) -> false;
            (_) -> true
        end,
        AllTCs0
    ),
    CustomMatrix = emqx_common_test_helpers:groups_with_matrix(?MODULE),
    LocalTCs = merge_custom_groups(?local, AllTCs, CustomMatrix),
    ClusterTCs = merge_custom_groups(?cluster, cluster_testcases(), CustomMatrix),
    [
        {?cluster, ClusterTCs},
        {?local, LocalTCs}
    ].

merge_custom_groups(RootGroup, GroupTCs, CustomMatrix0) ->
    CustomMatrix =
        lists:flatmap(
            fun
                ({G, _, SubGroup}) when G == RootGroup ->
                    SubGroup;
                (_) ->
                    []
            end,
            CustomMatrix0
        ),
    CustomMatrix ++ GroupTCs.

cluster_testcases() ->
    Key = ?cluster,
    lists:filter(
        fun
            ({testcase, TestCase, _Opts}) ->
                emqx_common_test_helpers:get_tc_prop(?MODULE, TestCase, Key, false);
            (TestCase) ->
                emqx_common_test_helpers:get_tc_prop(?MODULE, TestCase, Key, false)
        end,
        emqx_common_test_helpers:all(?MODULE)
    ).

init_per_suite(TCConfig) ->
    TCConfig.

end_per_suite(_TCConfig) ->
    ok.

init_per_group(?cluster = Group, TCConfig) ->
    AppSpecs = [
        emqx,
        emqx_conf,
        emqx_connector,
        emqx_bridge_mqtt,
        emqx_bridge,
        emqx_rule_engine,
        emqx_management
    ],
    Nodes = emqx_cth_cluster:start(
        [
            {bridge_mqtt_pub1, #{
                role => core,
                apps => AppSpecs ++ [emqx_mgmt_api_test_util:emqx_dashboard()]
            }},
            {bridge_mqtt_pub2, #{
                role => core,
                apps => AppSpecs
            }},
            {bridge_mqtt_pub3, #{
                role => core,
                apps => AppSpecs
            }}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Group, TCConfig)}
    ),
    [{nodes, Nodes} | TCConfig];
init_per_group(?local, TCConfig) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_connector,
            emqx_bridge_mqtt,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(TCConfig)}
    ),
    [{apps, Apps} | TCConfig];
init_per_group(_Group, TCConfig) ->
    TCConfig.

end_per_group(?cluster, TCConfig) ->
    Nodes = get_config(nodes, TCConfig),
    ok = emqx_cth_cluster:stop(Nodes),
    ok;
end_per_group(?local, TCConfig) ->
    Apps = get_config(apps, TCConfig),
    emqx_cth_suite:stop(Apps),
    ok;
end_per_group(_Group, _TCConfig) ->
    ok.

init_per_testcase(TestCase, TCConfig) ->
    Path = group_path(TCConfig, no_groups),
    ct:pal(asciiart:visible($%, "~p - ~s", [Path, TestCase])),
    ConnectorName = atom_to_binary(TestCase),
    ConnectorConfig = connector_config(#{}),
    ActionName = ConnectorName,
    ActionConfig = action_config(#{
        <<"connector">> => ConnectorName
    }),
    SourceName = ConnectorName,
    SourceConfig = source_config(#{
        <<"connector">> => ConnectorName
    }),
    setup_auth_header(TCConfig),
    snabbkaffe:start_trace(),
    [
        {bridge_kind, please_choose_one},
        {connector_type, ?CONNECTOR_TYPE},
        {connector_name, ConnectorName},
        {connector_config, ConnectorConfig},
        {action_type, ?ACTION_TYPE},
        {action_name, ActionName},
        {action_config, ActionConfig},
        {source_type, ?SOURCE_TYPE},
        {source_name, SourceName},
        {source_config, SourceConfig}
        | TCConfig
    ].

end_per_testcase(_TestCase, _TCConfig) ->
    snabbkaffe:stop(),
    emqx_bridge_v2_testlib:delete_all_rules(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

connector_config(Overrides) ->
    emqx_bridge_schema_testlib:mqtt_connector_config(Overrides).

source_config(Overrides) ->
    emqx_bridge_schema_testlib:mqtt_source_config(Overrides).

action_config(Overrides) ->
    emqx_bridge_schema_testlib:mqtt_action_config(Overrides).

get_config(K, TCConfig) -> emqx_bridge_v2_testlib:get_value(K, TCConfig).
get_config(K, TCConfig, Default) -> proplists:get_value(K, TCConfig, Default).

group_path(Config, Default) ->
    case emqx_common_test_helpers:group_path(Config) of
        [] -> Default;
        Path -> Path
    end.

get_tc_prop(TestCase, Key, Default) ->
    maybe
        true ?= erlang:function_exported(?MODULE, TestCase, 0),
        {Key, Val} ?= proplists:lookup(Key, ?MODULE:TestCase()),
        Val
    else
        _ -> Default
    end.

create_connector_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(TCConfig, Overrides)
    ).

create_source_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:create_source_api([{bridge_kind, source} | TCConfig], Overrides).

create_action_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_action_api([{bridge_kind, action} | TCConfig], Overrides)
    ).

get_action_metrics_api(TCConfig) ->
    emqx_bridge_v2_testlib:get_action_metrics_api(
        [{bridge_kind, action} | TCConfig]
    ).

get_source_metrics_api(TCConfig) ->
    emqx_bridge_v2_testlib:get_source_metrics_api(
        [{bridge_kind, source} | TCConfig]
    ).

simple_create_rule_action_api(TCConfig) ->
    emqx_bridge_v2_testlib:simple_create_rule_api([{bridge_kind, action} | TCConfig]).

simple_create_rule_source_api(TCConfig) ->
    emqx_bridge_v2_testlib:simple_create_rule_api([{bridge_kind, source} | TCConfig]).

start_client(TCConfigOrNode) ->
    start_client(TCConfigOrNode, _Opts = #{}).

start_client(TCConfig, Opts) when is_list(TCConfig) ->
    case get_config(nodes, TCConfig, undefined) of
        [N | _] ->
            start_client(N, Opts);
        _ ->
            start_client(node(), Opts)
    end;
start_client(Node, Opts) when is_atom(Node) ->
    Port = get_tcp_mqtt_port(Node),
    {ok, C} = emqtt:start_link(Opts#{port => Port, proto_ver => v5}),
    on_exit(fun() -> catch emqtt:stop(C) end),
    {ok, _} = emqtt:connect(C),
    C.

get_tcp_mqtt_port(Node) ->
    {_Host, Port} = ?ON(Node, emqx_config:get([listeners, tcp, default, bind])),
    Port.

setup_auth_header(TCConfig) ->
    case get_config(nodes, TCConfig, undefined) of
        [N1 | _] ->
            Fun = fun() -> ?ON(N1, emqx_mgmt_api_test_util:auth_header_()) end,
            emqx_bridge_v2_testlib:set_auth_header_getter(Fun);
        _ ->
            ok
    end.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_mqtt_conn_bridge_ingress_and_egress(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{
        <<"pool_size">> => 1
    }),
    RemoteTopicIn = <<"remote/t/in">>,
    {201, _} =
        create_source_api(TCConfig, #{
            <<"parameters">> => #{<<"topic">> => RemoteTopicIn}
        }),
    RemoteTopicOut = <<"remote/t/out">>,
    {201, _} =
        create_action_api(TCConfig, #{
            <<"parameters">> => #{<<"topic">> => RemoteTopicOut}
        }),
    #{topic := RuleTopic} = simple_create_rule_action_api(TCConfig),
    #{topic := RepublishTopic} = simple_create_rule_source_api(TCConfig),
    C = start_client(TCConfig),
    {ok, _, _} = emqtt:subscribe(C, RepublishTopic, [{qos, 2}]),
    {ok, _, _} = emqtt:subscribe(C, RemoteTopicOut, [{qos, 2}]),

    emqtt:publish(C, RemoteTopicIn, <<"from source">>),
    {publish, #{payload := PayloadBin1}} = ?assertReceive({publish, _}),
    ?assertMatch(#{<<"payload">> := <<"from source">>}, emqx_utils_json:decode(PayloadBin1)),

    emqtt:publish(C, RuleTopic, <<"from rule">>),
    {publish, #{payload := PayloadBin2}} = ?assertReceive({publish, _}),
    ?assertMatch(#{<<"payload">> := <<"from rule">>}, emqx_utils_json:decode(PayloadBin2)),

    ?retry(
        500,
        20,
        ?assertMatch(
            {200, #{
                <<"metrics">> := #{
                    <<"matched">> := 1,
                    <<"success">> := 1,
                    <<"failed">> := 0,
                    <<"received">> := 0
                }
            }},
            get_action_metrics_api(TCConfig)
        )
    ),
    ?retry(
        500,
        20,
        ?assertMatch(
            {200, #{
                <<"metrics">> := #{
                    <<"received">> := 1,
                    <<"matched">> := 0,
                    <<"success">> := 0,
                    <<"failed">> := 0
                }
            }},
            get_source_metrics_api(TCConfig)
        )
    ),
    ok.

%% Checks that we're able to set the no-local `nl' flag when subscribing.
t_no_local(TCConfig) ->
    %% Only 1 worker to avoid the other workers multiplying the message.
    {201, _} = create_connector_api(TCConfig, #{
        <<"pool_size">> => 1
    }),
    {201, #{<<"parameters">> := #{<<"topic">> := RemoteTopic}}} =
        create_source_api(TCConfig, #{
            <<"parameters">> => #{
                <<"no_local">> => true
            }
        }),
    %% Must be the same topic
    {201, _} =
        create_action_api(TCConfig, #{
            <<"parameters">> => #{<<"topic">> => RemoteTopic}
        }),
    #{topic := RuleTopic} = simple_create_rule_action_api(TCConfig),
    #{topic := RepublishTopic} = simple_create_rule_source_api(TCConfig),
    %% Should not receive own messages echoed back.
    ok = emqx:subscribe(RepublishTopic, #{qos => ?QOS_1, nl => 1}),
    %% Should receive only 1 message copy.
    ok = emqx:subscribe(RemoteTopic),
    emqx:publish(emqx_message:make(<<"external_client">>, ?QOS_1, RuleTopic, <<"hey">>)),
    ?assertReceive({deliver, RemoteTopic, _}),
    ?assertNotReceive({deliver, RemoteTopic, _}),
    ?assertNotReceive({deliver, RuleTopic, _}),
    ok.
