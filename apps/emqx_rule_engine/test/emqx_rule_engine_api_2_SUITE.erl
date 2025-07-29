%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_rule_engine_api_2_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/emqx_config.hrl").

%%------------------------------------------------------------------------------
%% Definitions
%%------------------------------------------------------------------------------

-import(emqx_common_test_helpers, [on_exit/1]).

-define(local, local).
-define(custom_cluster, custom_cluster).
-define(global_namespace, global_namespace).
-define(namespaced, namespaced).

-define(NS, <<"some_namespace">>).

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

-define(assertReceivePublish(TOPIC, EXPR), begin
    ?assertMatch(
        EXPR,
        maps:update_with(
            payload,
            fun emqx_utils_json:decode/1,
            element(2, ?assertReceive({publish, #{topic := TOPIC}}))
        )
    )
end).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    [
        {group, ?local},
        {group, ?custom_cluster}
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
    ClusterTCs = merge_custom_groups(?custom_cluster, [], CustomMatrix),
    [
        {?local, LocalTCs},
        {?custom_cluster, ClusterTCs}
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

custom_cluster_cases() ->
    Key = ?custom_cluster,
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

app_specs_no_dashboard() ->
    [
        {emqx, #{
            before_start =>
                fun(App, AppOpts) ->
                    %% We need this in the tests because `emqx_cth_suite` does not start apps in
                    %% the exact same way as the release works: in the release,
                    %% `emqx_enterprise_schema` is the root schema that knows all root keys.  In
                    %% CTH, we need to manually load the schema below so that when
                    %% `emqx_config:init_load` runs and encounters a namespaced root key, it knows
                    %% the schema module for it.
                    Mod = emqx_rule_engine_schema,
                    emqx_config:init_load(Mod, <<"">>),
                    ok = emqx_schema_hooks:inject_from_modules([?MODULE, Mod]),
                    emqx_cth_suite:inhibit_config_loader(App, AppOpts)
                end
        }},
        emqx_conf,
        emqx_rule_engine,
        emqx_management
    ].

app_specs() ->
    app_specs_no_dashboard() ++ [emqx_mgmt_api_test_util:emqx_dashboard()].

init_per_group(?local, TCConfig) ->
    Apps = emqx_cth_suite:start(
        app_specs(),
        #{work_dir => emqx_cth_suite:work_dir(TCConfig)}
    ),
    [{apps, Apps}, {node, node()} | TCConfig];
init_per_group(?custom_cluster, TCConfig) ->
    TCConfig;
init_per_group(_Group, TCConfig) ->
    TCConfig.

end_per_group(?local, TCConfig) ->
    Apps = get_value(apps, TCConfig),
    ok = emqx_cth_suite:stop(Apps),
    ok;
end_per_group(?custom_cluster, _TCConfig) ->
    ok;
end_per_group(_Group, _TCConfig) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    emqx_bridge_v2_testlib:delete_all_rules(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

maybe_json_decode(X) ->
    case emqx_utils_json:safe_decode(X) of
        {ok, Decoded} -> Decoded;
        {error, _} -> X
    end.

request(Method, Path, Params) ->
    Opts = #{return_all => true},
    request(Method, Path, Params, Opts).

request(Method, Path, Params, Opts) ->
    request(Method, Path, Params, _QueryParams = [], Opts).

request(Method, Path, Params, QueryParams0, Opts) when is_list(QueryParams0) ->
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    QueryParams = uri_string:compose_query(QueryParams0, [{encoding, utf8}]),
    case emqx_mgmt_api_test_util:request_api(Method, Path, QueryParams, AuthHeader, Params, Opts) of
        {ok, {Status, Headers, Body0}} ->
            Body = maybe_json_decode(Body0),
            {ok, {Status, Headers, Body}};
        {error, {Status, Headers, Body0}} ->
            Body =
                case emqx_utils_json:safe_decode(Body0) of
                    {ok, Decoded0 = #{<<"message">> := Msg0}} ->
                        Msg = maybe_json_decode(Msg0),
                        Decoded0#{<<"message">> := Msg};
                    {ok, Decoded0} ->
                        Decoded0;
                    {error, _} ->
                        Body0
                end,
            {error, {Status, Headers, Body}};
        Error ->
            Error
    end.

sql_test_api(Params) ->
    Method = post,
    Path = emqx_mgmt_api_test_util:api_path(["rule_test"]),
    ct:pal("sql test (http):\n  ~p", [Params]),
    Res = request(Method, Path, Params),
    ct:pal("sql test (http) result:\n  ~p", [Res]),
    Res.

list_rules(QueryParams) when is_list(QueryParams) ->
    Method = get,
    Path = emqx_mgmt_api_test_util:api_path(["rules"]),
    Opts = #{return_all => true},
    Res = request(Method, Path, _Body = [], QueryParams, Opts),
    emqx_mgmt_api_test_util:simplify_result(Res).

list_rules_just_ids(QueryParams) when is_list(QueryParams) ->
    case list_rules(QueryParams) of
        {200, #{<<"data">> := Results0}} ->
            Results = lists:sort([Id || #{<<"id">> := Id} <- Results0]),
            {200, Results};
        Res ->
            Res
    end.

rule_config(Overrides) ->
    Default = #{
        <<"enable">> => true,
        <<"sql">> => <<"select true from t">>
    },
    emqx_utils_maps:deep_merge(Default, Overrides).

create_rule() ->
    create_rule(_Overrides = #{}).

create_rule(Overrides) ->
    Params = rule_config(Overrides),
    Method = post,
    Path = emqx_mgmt_api_test_util:api_path(["rules"]),
    Res = request(Method, Path, Params),
    case emqx_mgmt_api_test_util:simplify_result(Res) of
        {201, #{<<"id">> := RuleId}} = SRes ->
            on_exit(fun() ->
                {ok, _} = emqx_conf:remove([rule_engine, rules, RuleId], #{override_to => cluster})
            end),
            SRes;
        SRes ->
            SRes
    end.

update_rule(Id, Params) ->
    Method = put,
    Path = emqx_mgmt_api_test_util:api_path(["rules", Id]),
    Res = request(Method, Path, Params),
    emqx_mgmt_api_test_util:simplify_result(Res).

delete_rule(RuleId) ->
    emqx_mgmt_api_test_util:simple_request(#{
        method => delete,
        url => emqx_mgmt_api_test_util:api_path(["rules", RuleId])
    }).

list_rules() ->
    Method = get,
    Path = emqx_mgmt_api_test_util:api_path(["rules"]),
    Res = request(Method, Path, _Params = ""),
    emqx_mgmt_api_test_util:simplify_result(Res).

get_rule(Id) ->
    Method = get,
    Path = emqx_mgmt_api_test_util:api_path(["rules", Id]),
    Res = request(Method, Path, _Params = ""),
    emqx_mgmt_api_test_util:simplify_result(Res).

simulate_rule(Id, Params) ->
    Method = post,
    Path = emqx_mgmt_api_test_util:api_path(["rules", Id, "test"]),
    Res = request(Method, Path, Params),
    emqx_mgmt_api_test_util:simplify_result(Res).

get_value(Key, TCConfig) ->
    emqx_bridge_v2_testlib:get_value(Key, TCConfig).

ensure_namespaced_api_key(Namespace, TCConfig) ->
    ensure_namespaced_api_key(Namespace, _Opts = #{}, TCConfig).
ensure_namespaced_api_key(Namespace, Opts0, TCConfig) ->
    Node = get_value(node, TCConfig),
    Opts = Opts0#{namespace => Namespace},
    ?ON(Node, emqx_bridge_v2_testlib:ensure_namespaced_api_key(Opts)).

auth_header(TCConfig) ->
    maybe
        undefined ?= ?config(auth_header, TCConfig),
        APIKey = get_value(api_key, TCConfig),
        emqx_common_test_http:auth_header(APIKey)
    end.

list(TCConfig) ->
    Node = get_value(node, TCConfig),
    ?ON(Node, begin
        AuthHeader = auth_header(TCConfig),
        emqx_bridge_v2_testlib:simple_request(#{
            method => get,
            url => emqx_mgmt_api_test_util:api_path(["rules"]),
            auth_header => AuthHeader
        })
    end).

create(Config, TCConfig) ->
    Node = get_value(node, TCConfig),
    ?ON(Node, begin
        AuthHeader = auth_header(TCConfig),
        emqx_bridge_v2_testlib:simple_request(#{
            method => post,
            url => emqx_mgmt_api_test_util:api_path(["rules"]),
            body => Config,
            auth_header => AuthHeader
        })
    end).

get(Id, TCConfig) ->
    Node = get_value(node, TCConfig),
    ?ON(Node, begin
        AuthHeader = auth_header(TCConfig),
        emqx_bridge_v2_testlib:simple_request(#{
            method => get,
            url => emqx_mgmt_api_test_util:api_path(["rules", Id]),
            auth_header => AuthHeader
        })
    end).

update(Id, Config, TCConfig) ->
    Node = get_value(node, TCConfig),
    ?ON(Node, begin
        AuthHeader = auth_header(TCConfig),
        emqx_bridge_v2_testlib:simple_request(#{
            method => put,
            url => emqx_mgmt_api_test_util:api_path(["rules", Id]),
            body => Config,
            auth_header => AuthHeader
        })
    end).

delete(Id, TCConfig) ->
    Node = get_value(node, TCConfig),
    ?ON(Node, begin
        AuthHeader = auth_header(TCConfig),
        emqx_bridge_v2_testlib:simple_request(#{
            method => delete,
            url => emqx_mgmt_api_test_util:api_path(["rules", Id]),
            auth_header => AuthHeader
        })
    end).

get_metrics(Id, TCConfig) ->
    Node = get_value(node, TCConfig),
    ?ON(Node, begin
        AuthHeader = auth_header(TCConfig),
        emqx_bridge_v2_testlib:simple_request(#{
            method => get,
            url => emqx_mgmt_api_test_util:api_path(["rules", Id, "metrics"]),
            auth_header => AuthHeader
        })
    end).

reset_metrics(Id, TCConfig) ->
    Node = get_value(node, TCConfig),
    ?ON(Node, begin
        AuthHeader = auth_header(TCConfig),
        emqx_bridge_v2_testlib:simple_request(#{
            method => put,
            url => emqx_mgmt_api_test_util:api_path(["rules", Id, "metrics", "reset"]),
            auth_header => AuthHeader
        })
    end).

simulate(Id, Params, TCConfig) ->
    Node = get_value(node, TCConfig),
    ?ON(Node, begin
        AuthHeader = auth_header(TCConfig),
        emqx_bridge_v2_testlib:simple_request(#{
            method => post,
            url => emqx_mgmt_api_test_util:api_path(["rules", Id, "test"]),
            body => Params,
            auth_header => AuthHeader
        })
    end).

get_rule_engine_config(TCConfig) ->
    Node = get_value(node, TCConfig),
    ?ON(Node, begin
        AuthHeader = auth_header(TCConfig),
        emqx_bridge_v2_testlib:simple_request(#{
            method => get,
            url => emqx_mgmt_api_test_util:api_path(["rule_engine"]),
            auth_header => AuthHeader
        })
    end).

set_rule_engine_config(Config, TCConfig) ->
    Node = get_value(node, TCConfig),
    ?ON(Node, begin
        AuthHeader = auth_header(TCConfig),
        emqx_bridge_v2_testlib:simple_request(#{
            method => put,
            url => emqx_mgmt_api_test_util:api_path(["rule_engine"]),
            body => Config,
            auth_header => AuthHeader
        })
    end).

sources_sql(Sources) ->
    Froms = iolist_to_binary(lists:join(<<", ">>, lists:map(fun source_from/1, Sources))),
    <<"select * from ", Froms/binary>>.

source_from({v1, Id}) ->
    <<"\"$bridges/", Id/binary, "\" ">>;
source_from({v2, Id}) ->
    <<"\"$sources/", Id/binary, "\" ">>.

spy_action(Selected, Envs, #{pid := TestPidBin}) ->
    TestPid = list_to_pid(binary_to_list(TestPidBin)),
    TestPid ! {rule_called, #{selected => Selected, envs => Envs}},
    ok.

event_type(EventTopic) ->
    EventAtom = emqx_rule_events:event_name(EventTopic),
    emqx_rule_api_schema:event_to_event_type(EventAtom).

fmt(Template, Context) ->
    Parsed = emqx_template:parse(Template),
    iolist_to_binary(emqx_template:render_strict(Parsed, Context)).

do_rule_simulation_simple(Case) ->
    #{
        topic := Topic,
        context := Context,
        expected := Expected
    } = Case,
    SQL = fmt(<<"select * from \"${topic}\" ">>, #{topic => Topic}),
    {201, #{<<"id">> := Id}} = create_rule(#{<<"sql">> => SQL}),
    Params = #{
        <<"context">> => Context,
        <<"stop_action_after_template_rendering">> => false
    },
    {Code, Resp} = simulate_rule(Id, Params),
    maybe
        {ok, ExpectedCode} ?= maps:find(code, Expected),
        true ?= ExpectedCode == Code,
        false
    else
        _ ->
            {true, #{
                expected => Expected,
                hint => maps:get(hint, Case, <<>>),
                input => Context,
                got => {Code, Resp}
            }}
    end.

client_connected_context() ->
    #{
        <<"clientid">> => <<"c_emqx">>,
        <<"event_type">> => <<"client_connected">>,
        <<"peername">> => <<"127.0.0.1:64001">>,
        <<"username">> => <<"u_emqx">>
    }.

get_tcp_mqtt_port(Node) ->
    {_Host, Port} = erpc:call(Node, emqx_config, get, [[listeners, tcp, default, bind]]),
    Port.

setup_namespaced_actions_sources_scenario(TCConfig0) ->
    Node = proplists:get_value(node, TCConfig0, node()),
    MQTTPort = get_tcp_mqtt_port(Node),
    {ok, APIKey} = erpc:call(Node, emqx_common_test_http, create_default_app, []),
    TCConfig = [{node, Node}, {api_key, APIKey} | TCConfig0],

    AuthHeaderGlobal = emqx_common_test_http:auth_header(APIKey),
    TCConfigGlobal = [{auth_header, AuthHeaderGlobal} | TCConfig],
    NS1 = <<"ns1">>,
    AuthHeaderNS1 = ensure_namespaced_api_key(NS1, TCConfig),
    TCConfigNS1 = [{auth_header, AuthHeaderNS1} | TCConfig],
    NS2 = <<"ns2">>,
    AuthHeaderNS2 = ensure_namespaced_api_key(NS2, TCConfig),
    TCConfigNS2 = [{auth_header, AuthHeaderNS2} | TCConfig],

    %% Sanity checks
    ?assertMatch({200, #{<<"data">> := []}}, list(TCConfigGlobal)),
    ?assertMatch({200, #{<<"data">> := []}}, list(TCConfigNS1)),
    ?assertMatch({200, #{<<"data">> := []}}, list(TCConfigNS2)),

    %% setup connectors for each namespace
    on_exit(fun() -> emqx_bridge_v2_testlib:delete_all_bridges_and_connectors() end),
    Server = <<"127.0.0.1:", (integer_to_binary(MQTTPort))/binary>>,
    ConnectorType = <<"mqtt">>,
    ConnectorName = <<"conn">>,
    ConnectorConfigGlobal = emqx_bridge_v2_api_SUITE:source_connector_create_config(#{
        <<"description">> => <<"global">>,
        <<"pool_size">> => 1,
        <<"server">> => Server
    }),
    ConnectorConfigNS1 = emqx_bridge_v2_api_SUITE:source_connector_create_config(#{
        <<"description">> => <<"ns1">>,
        <<"pool_size">> => 1,
        <<"server">> => Server
    }),
    ConnectorConfigNS2 = emqx_bridge_v2_api_SUITE:source_connector_create_config(#{
        <<"description">> => <<"ns2">>,
        <<"pool_size">> => 1,
        <<"server">> => Server
    }),
    {201, #{<<"status">> := <<"connected">>}} =
        emqx_connector_api_SUITE:create(
            ConnectorType, ConnectorName, ConnectorConfigGlobal, TCConfigGlobal
        ),
    {201, #{<<"status">> := <<"connected">>}} =
        emqx_connector_api_SUITE:create(
            ConnectorType, ConnectorName, ConnectorConfigNS1, TCConfigNS1
        ),
    {201, #{<<"status">> := <<"connected">>}} =
        emqx_connector_api_SUITE:create(
            ConnectorType, ConnectorName, ConnectorConfigNS2, TCConfigNS2
        ),

    %% Create actions/sources
    Type = <<"mqtt">>,
    Name = <<"channel1">>,
    RemoteTopic = <<"remote/t">>,
    ConfigGlobal = emqx_bridge_v2_api_SUITE:source_update_config(#{
        <<"description">> => <<"global">>,
        <<"connector">> => ConnectorName,
        <<"parameters">> => #{<<"topic">> => RemoteTopic}
    }),
    ConfigNS1 = emqx_bridge_v2_api_SUITE:source_update_config(#{
        <<"description">> => <<"ns1">>,
        <<"connector">> => ConnectorName,
        <<"parameters">> => #{<<"topic">> => RemoteTopic}
    }),
    ConfigNS2 = emqx_bridge_v2_api_SUITE:source_update_config(#{
        <<"description">> => <<"ns2">>,
        <<"connector">> => ConnectorName,
        <<"parameters">> => #{<<"topic">> => RemoteTopic}
    }),

    ?assertMatch(
        {201, #{<<"description">> := <<"global">>, <<"status">> := <<"connected">>}},
        emqx_bridge_v2_api_SUITE:create(Type, Name, ConfigGlobal, TCConfigGlobal)
    ),
    ?assertMatch(
        {201, #{<<"description">> := <<"ns1">>, <<"status">> := <<"connected">>}},
        emqx_bridge_v2_api_SUITE:create(Type, Name, ConfigNS1, TCConfigNS1)
    ),
    ?assertMatch(
        {201, #{<<"description">> := <<"ns2">>, <<"status">> := <<"connected">>}},
        emqx_bridge_v2_api_SUITE:create(Type, Name, ConfigNS2, TCConfigNS2)
    ),

    BridgeId = emqx_bridge_resource:bridge_id(Type, Name),

    #{
        mqtt_port => MQTTPort,
        remote_topic => RemoteTopic,
        namespaces => [?global_ns, NS1, NS2],
        tc_configs => #{
            ?global_ns => TCConfigGlobal,
            NS1 => TCConfigNS1,
            NS2 => TCConfigNS2
        },
        connector_type => ConnectorType,
        connector_name => ConnectorName,
        kind => emqx_bridge_v2_api_SUITE:kind_of(TCConfig),
        type => Type,
        name => Name,
        bridge_id => BridgeId
    }.

namespace_of(TCConfig) ->
    emqx_common_test_helpers:get_matrix_prop(
        TCConfig, [?global_namespace, ?namespaced], ?global_namespace
    ).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_rule_test_smoke(_Config) ->
    %% Example inputs recorded from frontend on 2023-12-04
    Publish = [
        #{
            expected => #{code => 200},
            input =>
                #{
                    <<"context">> =>
                        #{
                            <<"clientid">> => <<"c_emqx">>,
                            <<"event_type">> => <<"message_publish">>,
                            <<"payload">> => <<"{\"msg\": \"hello\"}">>,
                            <<"qos">> => 1,
                            <<"topic">> => <<"t/a">>,
                            <<"username">> => <<"u_emqx">>
                        },
                    <<"sql">> => <<"SELECT\n  *\nFROM\n  \"t/#\"">>
                }
        },
        #{
            expected => #{code => 412},
            hint => <<"wrong topic">>,
            input =>
                #{
                    <<"context">> =>
                        #{
                            <<"clientid">> => <<"c_emqx">>,
                            <<"event_type">> => <<"message_publish">>,
                            <<"payload">> => <<"{\"msg\": \"hello\"}">>,
                            <<"qos">> => 1,
                            <<"topic">> => <<"a">>,
                            <<"username">> => <<"u_emqx">>
                        },
                    <<"sql">> => <<"SELECT\n  *\nFROM\n  \"t/#\"">>
                }
        },
        #{
            expected => #{code => 412},
            hint => <<
                "Currently, the frontend doesn't try to match against "
                "$events/message_published, but it may start sending "
                "the event topic in the future."
            >>,
            input =>
                #{
                    <<"context">> =>
                        #{
                            <<"clientid">> => <<"c_emqx">>,
                            <<"event_type">> => <<"message_publish">>,
                            <<"payload">> => <<"{\"msg\": \"hello\"}">>,
                            <<"qos">> => 1,
                            <<"topic">> => <<"t/a">>,
                            <<"username">> => <<"u_emqx">>
                        },
                    <<"sql">> => <<"SELECT\n  *\nFROM\n  \"$events/message_published\"">>
                }
        }
    ],
    %% Default input SQL doesn't match any event topic
    DefaultNoMatch = [
        #{
            expected => #{code => 412},
            input =>
                #{
                    <<"context">> =>
                        #{
                            <<"clientid">> => <<"c_emqx_2">>,
                            <<"event_type">> => <<"message_delivered">>,
                            <<"from_clientid">> => <<"c_emqx_1">>,
                            <<"from_username">> => <<"u_emqx_1">>,
                            <<"payload">> => <<"{\"msg\": \"hello\"}">>,
                            <<"qos">> => 1,
                            <<"topic">> => <<"t/a">>,
                            <<"username">> => <<"u_emqx_2">>
                        },
                    <<"sql">> => <<"SELECT\n  *\nFROM\n  \"t/#\"">>
                }
        },
        #{
            expected => #{code => 412},
            input =>
                #{
                    <<"context">> =>
                        #{
                            <<"clientid">> => <<"c_emqx_2">>,
                            <<"event_type">> => <<"message_acked">>,
                            <<"from_clientid">> => <<"c_emqx_1">>,
                            <<"from_username">> => <<"u_emqx_1">>,
                            <<"payload">> => <<"{\"msg\": \"hello\"}">>,
                            <<"qos">> => 1,
                            <<"topic">> => <<"t/a">>,
                            <<"username">> => <<"u_emqx_2">>
                        },
                    <<"sql">> => <<"SELECT\n  *\nFROM\n  \"t/#\"">>
                }
        },
        #{
            expected => #{code => 412},
            input =>
                #{
                    <<"context">> =>
                        #{
                            <<"clientid">> => <<"c_emqx">>,
                            <<"event_type">> => <<"message_dropped">>,
                            <<"payload">> => <<"{\"msg\": \"hello\"}">>,
                            <<"qos">> => 1,
                            <<"reason">> => <<"no_subscribers">>,
                            <<"topic">> => <<"t/a">>,
                            <<"username">> => <<"u_emqx">>
                        },
                    <<"sql">> => <<"SELECT\n  *\nFROM\n  \"t/#\"">>
                }
        },
        #{
            expected => #{code => 412},
            input =>
                #{
                    <<"context">> =>
                        #{
                            <<"clientid">> => <<"c_emqx">>,
                            <<"event_type">> => <<"client_connected">>,
                            <<"peername">> => <<"127.0.0.1:52918">>,
                            <<"username">> => <<"u_emqx">>
                        },
                    <<"sql">> => <<"SELECT\n  *\nFROM\n  \"t/#\"">>
                }
        },
        #{
            expected => #{code => 412},
            input =>
                #{
                    <<"context">> =>
                        #{
                            <<"clientid">> => <<"c_emqx">>,
                            <<"event_type">> => <<"client_disconnected">>,
                            <<"reason">> => <<"normal">>,
                            <<"username">> => <<"u_emqx">>
                        },
                    <<"sql">> => <<"SELECT\n  *\nFROM\n  \"t/#\"">>
                }
        },
        #{
            expected => #{code => 412},
            input =>
                #{
                    <<"context">> =>
                        #{
                            <<"clientid">> => <<"c_emqx">>,
                            <<"event_type">> => <<"client_connack">>,
                            <<"reason_code">> => <<"success">>,
                            <<"username">> => <<"u_emqx">>
                        },
                    <<"sql">> => <<"SELECT\n  *\nFROM\n  \"t/#\"">>
                }
        },
        #{
            expected => #{code => 412},
            input =>
                #{
                    <<"context">> =>
                        #{
                            <<"action">> => <<"publish">>,
                            <<"clientid">> => <<"c_emqx">>,
                            <<"event_type">> => <<"client_check_authz_complete">>,
                            <<"result">> => <<"allow">>,
                            <<"topic">> => <<"t/1">>,
                            <<"username">> => <<"u_emqx">>
                        },
                    <<"sql">> => <<"SELECT\n  *\nFROM\n  \"t/#\"">>
                }
        },
        #{
            expected => #{code => 412},
            input =>
                #{
                    <<"context">> =>
                        #{
                            <<"clientid">> => <<"c_emqx">>,
                            <<"event_type">> => <<"client_check_authn_complete">>,
                            <<"reason_code">> => <<"success">>,
                            <<"is_superuser">> => true,
                            <<"is_anonymous">> => false,
                            <<"username">> => <<"u_emqx">>
                        },
                    <<"sql">> => <<"SELECT\n  *\nFROM\n  \"t/#\"">>
                }
        },
        #{
            expected => #{code => 412},
            input =>
                #{
                    <<"context">> =>
                        #{
                            <<"clientid">> => <<"c_emqx">>,
                            <<"event_type">> => <<"client_check_authn_complete">>,
                            <<"reason_code">> => <<"sucess">>,
                            <<"is_superuser">> => true,
                            <<"is_anonymous">> => false,
                            <<"username">> => <<"u_emqx">>
                        },
                    <<"sql">> => <<"SELECT\n  *\nFROM\n  \"t/#\"">>
                }
        },
        #{
            expected => #{code => 412},
            input =>
                #{
                    <<"context">> =>
                        #{
                            <<"clientid">> => <<"c_emqx">>,
                            <<"event_type">> => <<"session_subscribed">>,
                            <<"qos">> => 1,
                            <<"topic">> => <<"t/a">>,
                            <<"username">> => <<"u_emqx">>
                        },
                    <<"sql">> => <<"SELECT\n  *\nFROM\n  \"t/#\"">>
                }
        },
        #{
            expected => #{code => 412},
            input =>
                #{
                    <<"context">> =>
                        #{
                            <<"clientid">> => <<"c_emqx">>,
                            <<"event_type">> => <<"session_unsubscribed">>,
                            <<"qos">> => 1,
                            <<"topic">> => <<"t/a">>,
                            <<"username">> => <<"u_emqx">>
                        },
                    <<"sql">> => <<"SELECT\n  *\nFROM\n  \"t/#\"">>
                }
        },
        #{
            expected => #{code => 412},
            input =>
                #{
                    <<"context">> =>
                        #{
                            <<"clientid">> => <<"c_emqx_2">>,
                            <<"event_type">> => <<"delivery_dropped">>,
                            <<"from_clientid">> => <<"c_emqx_1">>,
                            <<"from_username">> => <<"u_emqx_1">>,
                            <<"payload">> => <<"{\"msg\": \"hello\"}">>,
                            <<"qos">> => 1,
                            <<"reason">> => <<"queue_full">>,
                            <<"topic">> => <<"t/a">>,
                            <<"username">> => <<"u_emqx_2">>
                        },
                    <<"sql">> => <<"SELECT\n  *\nFROM\n  \"t/#\"">>
                }
        }
    ],
    MultipleFrom = [
        #{
            expected => #{code => 200},
            input =>
                #{
                    <<"context">> =>
                        #{
                            <<"clientid">> => <<"c_emqx">>,
                            <<"event_type">> => <<"message_publish">>,
                            <<"qos">> => 1,
                            <<"topic">> => <<"t/a">>,
                            <<"username">> => <<"u_emqx">>
                        },
                    <<"sql">> =>
                        <<"SELECT\n  *\nFROM\n  \"t/#\", \"$bridges/mqtt:source\" ">>
                }
        },
        #{
            expected => #{code => 200},
            input =>
                #{
                    <<"context">> =>
                        #{
                            <<"clientid">> => <<"c_emqx">>,
                            <<"event_type">> => <<"message_publish">>,
                            <<"qos">> => 1,
                            <<"topic">> => <<"t/a">>,
                            <<"username">> => <<"u_emqx">>
                        },
                    <<"sql">> =>
                        <<"SELECT\n  *\nFROM\n  \"t/#\", \"$sources/mqtt:source\" ">>
                }
        },
        #{
            expected => #{code => 200},
            input =>
                #{
                    <<"context">> =>
                        #{
                            <<"clientid">> => <<"c_emqx">>,
                            <<"event_type">> => <<"session_unsubscribed">>,
                            <<"qos">> => 1,
                            <<"topic">> => <<"t/a">>,
                            <<"username">> => <<"u_emqx">>
                        },
                    <<"sql">> =>
                        <<"SELECT\n  *\nFROM\n  \"t/#\", \"$events/session_unsubscribed\" ">>
                }
        },
        #{
            expected => #{code => 200},
            input =>
                #{
                    <<"context">> =>
                        #{
                            <<"clientid">> => <<"c_emqx">>,
                            <<"event_type">> => <<"session_unsubscribed">>,
                            <<"qos">> => 1,
                            <<"topic">> => <<"t/a">>,
                            <<"username">> => <<"u_emqx">>
                        },
                    <<"sql">> =>
                        <<"SELECT\n  *\nFROM\n  \"$events/message_dropped\", \"$events/session_unsubscribed\" ">>
                }
        },
        #{
            expected => #{code => 412},
            input =>
                #{
                    <<"context">> =>
                        #{
                            <<"clientid">> => <<"c_emqx">>,
                            <<"event_type">> => <<"session_unsubscribed">>,
                            <<"qos">> => 1,
                            <<"topic">> => <<"t/a">>,
                            <<"username">> => <<"u_emqx">>
                        },
                    <<"sql">> =>
                        <<"SELECT\n  *\nFROM\n  \"$events/message_dropped\", \"$events/client_connected\" ">>
                }
        }
    ],
    Cases = Publish ++ DefaultNoMatch ++ MultipleFrom,
    FailedCases = lists:filtermap(fun do_t_rule_test_smoke/1, Cases),
    ?assertEqual([], FailedCases),
    ok.

%% Checks the behavior of MQTT wildcards when used with events (`$events/#`,
%% `$events/sys/+`, etc.).
t_rule_test_wildcards(_Config) ->
    Cases = [
        #{
            expected => #{code => 200},
            input =>
                #{
                    <<"context">> =>
                        #{
                            <<"event_type">> => <<"alarm_activated">>,
                            <<"name">> => <<"alarm_name">>,
                            <<"details">> => #{
                                <<"some_key_that_is_not_a_known_atom">> => <<"yes">>
                            },
                            <<"message">> => <<"boom">>,
                            <<"activated_at">> => 1736512728666
                        },
                    <<"sql">> => <<"SELECT\n  *\nFROM\n  \"$events/sys/+\"">>
                }
        },
        #{
            expected => #{code => 200},
            input =>
                #{
                    <<"context">> =>
                        #{
                            <<"event_type">> => <<"alarm_deactivated">>,
                            <<"name">> => <<"alarm_name">>,
                            <<"details">> => #{
                                <<"some_key_that_is_not_a_known_atom">> => <<"yes">>
                            },
                            <<"message">> => <<"boom">>,
                            <<"activated_at">> => 1736512728666,
                            <<"deactivated_at">> => 1736512728966
                        },
                    <<"sql">> => <<"SELECT\n  *\nFROM\n  \"$events/sys/+\"">>
                }
        }
    ],
    FailedCases = lists:filtermap(fun do_t_rule_test_smoke/1, Cases),
    ?assertEqual([], FailedCases),
    ok.

%% validate check_schema is function with bad content_type
t_rule_test_with_bad_content_type(_Config) ->
    Params =
        #{
            <<"context">> =>
                #{
                    <<"clientid">> => <<"c_emqx">>,
                    <<"event_type">> => <<"message_publish">>,
                    <<"payload">> => <<"{\"msg\": \"hello\"}">>,
                    <<"qos">> => 1,
                    <<"topic">> => <<"t/a">>,
                    <<"username">> => <<"u_emqx">>
                },
            <<"sql">> => <<"SELECT\n  *\nFROM\n  \"t/#\"">>
        },
    Method = post,
    Path = emqx_mgmt_api_test_util:api_path(["rule_test"]),
    Opts = #{return_all => true, 'content-type' => "application/xml"},
    ?assertMatch(
        {error,
            {
                {"HTTP/1.1", 415, "Unsupported Media Type"},
                _Headers,
                #{
                    <<"code">> := <<"UNSUPPORTED_MEDIA_TYPE">>,
                    <<"message">> := <<"content-type:application/json Required">>
                }
            }},
        request(Method, Path, Params, Opts)
    ),
    ok.

do_t_rule_test_smoke(#{input := Input, expected := #{code := ExpectedCode}} = Case) ->
    {_ErrOrOk, {{_, Code, _}, _, Body}} = sql_test_api(Input),
    case Code =:= ExpectedCode of
        true ->
            false;
        false ->
            {true, #{
                expected => ExpectedCode,
                hint => maps:get(hint, Case, <<>>),
                input => Input,
                got => Code,
                resp_body => Body
            }}
    end.

%% Checks that each event is recognized by `/rule_test' and the examples are valid.
t_rule_test_examples(_Config) ->
    AllEventInfos = emqx_rule_events:event_info(),
    Failures = lists:filtermap(
        fun
            (#{event := <<"$bridges/mqtt:*">>}) ->
                %% Currently, our frontend doesn't support simulating source events.
                false;
            (EventInfo) ->
                #{
                    sql_example := SQL,
                    test_columns := TestColumns,
                    event := EventTopic
                } = EventInfo,
                EventType = event_type(EventTopic),
                Context = lists:foldl(
                    fun
                        ({Field, [ExampleValue, _Description]}, Acc) ->
                            Acc#{Field => ExampleValue};
                        ({Field, ExampleValue}, Acc) ->
                            Acc#{Field => ExampleValue}
                    end,
                    #{<<"event_type">> => EventType},
                    TestColumns
                ),
                Case = #{
                    expected => #{code => 200},
                    input => #{<<"context">> => Context, <<"sql">> => SQL}
                },
                do_t_rule_test_smoke(Case)
        end,
        AllEventInfos
    ),
    ?assertEqual([], Failures),
    ok.

%% Tests filtering the rule list by used actions and/or sources.
t_filter_by_source_and_action(_Config) ->
    ?assertMatch(
        {200, #{<<"data">> := []}},
        list_rules([])
    ),

    ActionId1 = <<"mqtt:a1">>,
    ActionId2 = <<"mqtt:a2">>,
    SourceId1 = <<"mqtt:s1">>,
    SourceId2 = <<"mqtt:s2">>,
    {201, #{<<"id">> := Id1}} = create_rule(#{<<"actions">> => [ActionId1]}),
    {201, #{<<"id">> := Id2}} = create_rule(#{<<"actions">> => [ActionId2]}),
    {201, #{<<"id">> := Id3}} = create_rule(#{<<"actions">> => [ActionId2, ActionId1]}),
    {201, #{<<"id">> := Id4}} = create_rule(#{<<"sql">> => sources_sql([{v1, SourceId1}])}),
    {201, #{<<"id">> := Id5}} = create_rule(#{<<"sql">> => sources_sql([{v2, SourceId2}])}),
    {201, #{<<"id">> := Id6}} = create_rule(#{
        <<"sql">> => sources_sql([{v2, SourceId1}, {v2, SourceId1}])
    }),
    {201, #{<<"id">> := Id7}} = create_rule(#{
        <<"sql">> => sources_sql([{v2, SourceId1}]),
        <<"actions">> => [ActionId1]
    }),

    ?assertMatch(
        {200, [_, _, _, _, _, _, _]},
        list_rules_just_ids([])
    ),

    ?assertEqual(
        {200, lists:sort([Id1, Id3, Id7])},
        list_rules_just_ids([{<<"action">>, ActionId1}])
    ),

    ?assertEqual(
        {200, lists:sort([Id1, Id2, Id3, Id7])},
        list_rules_just_ids([{<<"action">>, ActionId1}, {<<"action">>, ActionId2}])
    ),

    ?assertEqual(
        {200, lists:sort([Id4, Id6, Id7])},
        list_rules_just_ids([{<<"source">>, SourceId1}])
    ),

    ?assertEqual(
        {200, lists:sort([Id4, Id5, Id6, Id7])},
        list_rules_just_ids([{<<"source">>, SourceId1}, {<<"source">>, SourceId2}])
    ),

    %% When mixing source and action id filters, we use AND.
    ?assertEqual(
        {200, lists:sort([])},
        list_rules_just_ids([{<<"source">>, SourceId2}, {<<"action">>, ActionId2}])
    ),
    ?assertEqual(
        {200, lists:sort([Id7])},
        list_rules_just_ids([{<<"source">>, SourceId1}, {<<"action">>, ActionId1}])
    ),

    ok.

%% Checks that creating a rule with a `null' JSON value id is forbidden.
t_create_rule_with_null_id(_Config) ->
    ?assertMatch(
        {400, #{<<"message">> := <<"rule id must be a string">>}},
        create_rule(#{<<"id">> => null})
    ),
    %% The string `"null"' should be fine.
    ?assertMatch(
        {201, _},
        create_rule(#{<<"id">> => <<"null">>})
    ),
    ?assertMatch({201, _}, create_rule(#{})),
    ?assertMatch(
        {200, #{<<"data">> := [_, _]}},
        list_rules([])
    ),
    ok.

%% Smoke tests for `$events/sys/alarm_activated' and `$events/sys/alarm_deactivated'.
t_alarm_events(Config) ->
    TestPidBin = list_to_binary(pid_to_list(self())),
    {201, _} = create_rule(#{
        <<"id">> => <<"alarms">>,
        <<"sql">> => iolist_to_binary([
            <<" select * from ">>,
            <<" \"$events/sys/alarm_activated\", ">>,
            <<" \"$events/sys/alarm_deactivated\" ">>
        ]),
        <<"actions">> => [
            #{
                <<"function">> => <<?MODULE_STRING, ":spy_action">>,
                <<"args">> => #{<<"pid">> => TestPidBin}
            }
        ]
    }),
    do_t_alarm_events(Config).

%% Smoke tests for `$events/sys/+'.
t_alarm_events_plus(Config) ->
    TestPidBin = list_to_binary(pid_to_list(self())),
    {201, _} = create_rule(#{
        <<"id">> => <<"alarms">>,
        <<"sql">> => iolist_to_binary([
            <<" select * from ">>,
            <<" \"$events/sys/+\" ">>
        ]),
        <<"actions">> => [
            #{
                <<"function">> => <<?MODULE_STRING, ":spy_action">>,
                <<"args">> => #{<<"pid">> => TestPidBin}
            }
        ]
    }),
    do_t_alarm_events(Config).

%% Smoke tests for `$events/#'.
t_alarm_events_hash(Config) ->
    TestPidBin = list_to_binary(pid_to_list(self())),
    RuleId = <<"alarms_hash">>,
    {201, _} = create_rule(#{
        <<"id">> => RuleId,
        <<"sql">> => iolist_to_binary([
            <<" select * from ">>,
            <<" \"$events/#\" ">>
        ]),
        <<"actions">> => [
            #{
                <<"function">> => <<?MODULE_STRING, ":spy_action">>,
                <<"args">> => #{<<"pid">> => TestPidBin}
            }
        ]
    }),
    do_t_alarm_events(Config),
    %% Message publish shouldn't match `$events/#`, but can match other events such as
    %% `$events/message_dropped`.
    emqx:publish(emqx_message:make(<<"t">>, <<"hey">>)),
    ?assertReceive(
        {rule_called, #{
            selected := #{event := 'message.dropped'},
            envs := #{
                metadata := #{
                    matched := <<"$events/#">>,
                    trigger := <<"$events/message/dropped">>
                }
            }
        }}
    ),
    {ok, _} = emqx_conf:remove([rule_engine, rules, RuleId], #{override_to => cluster}),
    %% Shouldn't match anymore.
    emqx:publish(emqx_message:make(<<"t">>, <<"hey">>)),
    ?assertNotReceive({rule_called, _}),
    ok.

do_t_alarm_events(_Config) ->
    AlarmName = <<"some_alarm">>,
    Details = #{more => details},
    Message = [<<"some">>, $\s | [<<"io">>, "list"]],
    emqx_alarm:activate(AlarmName, Details, Message),
    ?assertReceive(
        {rule_called, #{
            selected :=
                #{
                    message := <<"some iolist">>,
                    details := #{more := details},
                    name := AlarmName,
                    activated_at := _,
                    event := 'alarm.activated'
                }
        }}
    ),

    %% Activating an active alarm shouldn't trigger the event again.
    emqx_alarm:activate(AlarmName, Details, Message),
    emqx_alarm:activate(AlarmName, Details),
    emqx_alarm:activate(AlarmName),
    emqx_alarm:safe_activate(AlarmName, Details, Message),
    ?assertNotReceive({rule_called, _}),

    DeactivateMessage = <<"deactivating">>,
    DeactivateDetails = #{new => details},
    emqx_alarm:deactivate(AlarmName, DeactivateDetails, DeactivateMessage),
    ?assertReceive(
        {rule_called, #{
            selected :=
                #{
                    message := DeactivateMessage,
                    details := DeactivateDetails,
                    name := AlarmName,
                    activated_at := _,
                    deactivated_at := _,
                    event := 'alarm.deactivated'
                }
        }}
    ),

    %% Deactivating an inactive alarm shouldn't trigger the event again.
    emqx_alarm:deactivate(AlarmName),
    emqx_alarm:deactivate(AlarmName, Details),
    emqx_alarm:deactivate(AlarmName, Details, Message),
    emqx_alarm:safe_deactivate(AlarmName),
    ?assertNotReceive({rule_called, _}),

    ok.

%% Checks that, when removing a rule with a wildcard, we remove the hook function for each
%% event for which such rule is the last referencing one.
t_remove_rule_with_wildcard(_Config) ->
    %% This only hooks on `'message.publish'`.
    RuleId1 = <<"simple">>,
    {201, _} = create_rule(#{
        <<"id">> => RuleId1,
        <<"sql">> => iolist_to_binary([
            <<" select * from ">>,
            <<" \"concrete/topic\" ">>
        ]),
        <<"actions">> => []
    }),
    %% This hooks on all `$events/#`
    RuleId2 = <<"all">>,
    {201, _} = create_rule(#{
        <<"id">> => RuleId2,
        <<"sql">> => iolist_to_binary([
            <<" select * from ">>,
            <<" \"$events/#\" ">>
        ]),
        <<"actions">> => []
    }),
    Events = ['message.publish' | emqx_rule_events:match_event_names(<<"$events/#">>)],
    ListRuleHooks = fun() ->
        [
            E
         || E <- Events,
            {callback, {emqx_rule_events, _, _}, _, _} <- emqx_hooks:lookup(E)
        ]
    end,
    ?assertEqual(lists:sort(Events), lists:sort(ListRuleHooks())),
    {204, _} = delete_rule(RuleId2),
    %% Should have cleared up all hooks but `'message.publish'``.
    ?retry(100, 10, ?assertMatch(['message.publish'], ListRuleHooks())),
    {204, _} = delete_rule(RuleId1),
    ?assertMatch([], ListRuleHooks()),
    ok.

%% Smoke tests for `last_modified_at' field when creating/updating a rule.
t_last_modified_at(_Config) ->
    Id = <<"last_mod_at">>,
    CreateParams = #{
        <<"id">> => Id,
        <<"sql">> => iolist_to_binary([
            <<" select * from \"t/a\" ">>
        ]),
        <<"actions">> => [
            #{<<"function">> => <<"console">>}
        ]
    },
    {201, Res} = create_rule(CreateParams),
    ?assertMatch(
        #{
            <<"created_at">> := CreatedAt,
            <<"last_modified_at">> := CreatedAt
        },
        Res
    ),
    ?assertMatch(
        {200, #{
            <<"created_at">> := CreatedAt,
            <<"last_modified_at">> := CreatedAt
        }},
        get_rule(Id)
    ),
    ?assertMatch(
        {200, #{
            <<"data">> := [
                #{
                    <<"created_at">> := CreatedAt,
                    <<"last_modified_at">> := CreatedAt
                }
            ]
        }},
        list_rules()
    ),
    #{
        <<"created_at">> := CreatedAt,
        <<"last_modified_at">> := CreatedAt
    } = Res,
    ct:sleep(10),
    UpdateParams = maps:without([<<"id">>], CreateParams),
    {200, UpdateRes} = update_rule(Id, UpdateParams),
    ?assertMatch(
        #{
            <<"created_at">> := CreatedAt,
            <<"last_modified_at">> := LastModifiedAt
        } when LastModifiedAt =/= CreatedAt,
        UpdateRes,
        #{created_at => CreatedAt}
    ),
    #{<<"last_modified_at">> := LastModifiedAt} = UpdateRes,
    ?assertMatch(
        {200, #{
            <<"created_at">> := CreatedAt,
            <<"last_modified_at">> := LastModifiedAt
        }},
        get_rule(Id)
    ),
    ?assertMatch(
        {200, #{
            <<"data">> := [
                #{
                    <<"created_at">> := CreatedAt,
                    <<"last_modified_at">> := LastModifiedAt
                }
            ]
        }},
        list_rules()
    ),
    ok.

%% This verifies that we don't attempt to transform keys in the `details' value of an
%% alarm activated/deactivated rule test to atoms.
t_alarm_details_with_unknown_atom_key(_Config) ->
    Cases = [
        #{
            expected => #{code => 200},
            hint => <<
                "the original bug was that this failed with 500 when"
                " trying to convert a binary to existing atom"
            >>,
            input =>
                #{
                    <<"context">> =>
                        #{
                            <<"event_type">> => <<"alarm_activated">>,
                            <<"name">> => <<"alarm_name">>,
                            <<"details">> => #{
                                <<"some_key_that_is_not_a_known_atom">> => <<"yes">>
                            },
                            <<"message">> => <<"boom">>,
                            <<"activated_at">> => 1736512728666
                        },
                    <<"sql">> =>
                        <<"SELECT\n  *\nFROM\n  \"$events/sys/alarm_activated\" ">>
                }
        },
        #{
            expected => #{code => 200},
            hint => <<
                "the original bug was that this failed with 500 when"
                " trying to convert a binary to existing atom"
            >>,
            input =>
                #{
                    <<"context">> =>
                        #{
                            <<"event_type">> => <<"alarm_deactivated">>,
                            <<"name">> => <<"alarm_name">>,
                            <<"details">> => #{
                                <<"some_key_that_is_not_a_known_atom">> => <<"yes">>
                            },
                            <<"message">> => <<"boom">>,
                            <<"activated_at">> => 1736512728666,
                            <<"deactivated_at">> => 1736512828666
                        },
                    <<"sql">> =>
                        <<"SELECT\n  *\nFROM\n  \"$events/sys/alarm_deactivated\" ">>
                }
        }
    ],
    Failures = lists:filtermap(fun do_t_rule_test_smoke/1, Cases),
    ?assertEqual([], Failures),
    ok.

%% Verifies that we enrich the list response with status about the Actions in each rule,
%% if available.
t_action_details() ->
    [{matrix, true}].
t_action_details(matrix) ->
    [
        [?local, ?global_namespace],
        [?local, ?namespaced]
    ];
t_action_details(TCConfig0) ->
    ExtraAppSpecs = [
        emqx_bridge_mqtt,
        emqx_bridge
    ],
    ExtraApps = emqx_cth_suite:start_apps(
        ExtraAppSpecs,
        #{work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, TCConfig0)}
    ),
    on_exit(fun() -> emqx_cth_suite:stop_apps(ExtraApps) end),

    {ok, APIKey} = emqx_common_test_http:create_default_app(),
    TCConfig1 = [{api_key, APIKey} | TCConfig0],
    AuthHeader =
        case namespace_of(TCConfig1) of
            ?global_namespace -> auth_header(TCConfig1);
            ?namespaced -> ensure_namespaced_api_key(?NS, TCConfig1)
        end,
    TCConfig = [{auth_header, AuthHeader} | TCConfig1],

    CreateBridge = fun(Name, MQTTPort) ->
        TCConfig2 = [
            {connector_type, <<"mqtt">>},
            {connector_name, Name},
            {connector_config,
                emqx_bridge_mqtt_v2_publisher_SUITE:connector_config(#{
                    <<"server">> => <<"127.0.0.1:", (integer_to_binary(MQTTPort))/binary>>
                })},
            {bridge_kind, action},
            {action_type, <<"mqtt">>},
            {action_name, Name},
            {action_config,
                emqx_bridge_mqtt_v2_publisher_SUITE:action_config(#{
                    <<"connector">> => Name
                })}
            | TCConfig
        ],
        {201, _} = emqx_bridge_v2_testlib:create_connector_api2(TCConfig2, #{}),
        {201, _} = emqx_bridge_v2_testlib:create_action_api2(TCConfig2, #{}),
        ok
    end,
    on_exit(fun emqx_bridge_v2_testlib:delete_all_bridges_and_connectors/0),
    ok = CreateBridge(<<"a1">>, 1883),
    %% Bad port: will be disconnected
    ok = CreateBridge(<<"a2">>, 9999),

    ?assertMatch({200, #{<<"data">> := []}}, list(TCConfig)),

    ActionId1 = <<"mqtt:a1">>,
    ActionId2 = <<"mqtt:a2">>,
    %% This onw does not exist.
    ActionId3 = <<"mqtt:a3">>,
    Base = #{<<"sql">> => <<"select true from t">>},
    {201, _} = create(Base#{<<"id">> => <<"1">>, <<"actions">> => [ActionId1]}, TCConfig),
    {201, _} = create(Base#{<<"id">> => <<"2">>, <<"actions">> => [ActionId2]}, TCConfig),
    {201, _} = create(
        Base#{<<"id">> => <<"3">>, <<"actions">> => [ActionId2, ActionId1]}, TCConfig
    ),
    {201, _} = create(Base#{<<"id">> => <<"4">>, <<"actions">> => [ActionId3]}, TCConfig),

    ?assertMatch(
        {200, #{
            <<"data">> := [
                #{
                    <<"action_details">> := [
                        #{
                            <<"type">> := <<"mqtt">>,
                            <<"name">> := <<"a3">>,
                            <<"status">> := <<"not_found">>
                        }
                    ]
                },
                #{
                    <<"action_details">> := [
                        #{
                            <<"type">> := <<"mqtt">>,
                            <<"name">> := <<"a2">>,
                            <<"status">> := <<"disconnected">>
                        },
                        #{
                            <<"type">> := <<"mqtt">>,
                            <<"name">> := <<"a1">>,
                            <<"status">> := <<"connected">>
                        }
                    ]
                },
                #{
                    <<"action_details">> := [
                        #{
                            <<"type">> := <<"mqtt">>,
                            <<"name">> := <<"a2">>,
                            <<"status">> := <<"disconnected">>
                        }
                    ]
                },
                #{
                    <<"action_details">> := [
                        #{
                            <<"type">> := <<"mqtt">>,
                            <<"name">> := <<"a1">>,
                            <<"status">> := <<"connected">>
                        }
                    ]
                }
            ]
        }},
        list(TCConfig)
    ),
    ok.

%% Checks that `/rules/:id/test` does not reply with 412 (no match) when the rule uses
%% legacy event topics.
t_rule_simulation_legacy_topics(_Config) ->
    Cases = [
        #{
            topic => <<"$events/client_connected">>,
            context => client_connected_context(),
            expected => #{code => 200}
        }
    ],
    Failures = lists:filtermap(fun do_rule_simulation_simple/1, Cases),
    ?assertEqual([], Failures),
    ok.

-doc """
Smoke tests for CRUD operations on namespaced rules.

  - Namespaced users should only see and be able to mutate resources in their namespaces.
""".
t_namespaced_crud(TCConfig0) when is_list(TCConfig0) ->
    %% Worth checking in a clustered setup?
    Node = node(),
    MQTTPort = get_tcp_mqtt_port(Node),
    {ok, APIKey} = erpc:call(Node, emqx_common_test_http, create_default_app, []),
    TCConfig = [{node, Node}, {api_key, APIKey} | TCConfig0],

    AuthHeaderGlobal = emqx_common_test_http:auth_header(APIKey),
    TCConfigGlobal = [{auth_header, AuthHeaderGlobal} | TCConfig],
    NS1 = <<"ns1">>,
    AuthHeaderNS1 = ensure_namespaced_api_key(NS1, TCConfig),
    TCConfigNS1 = [{auth_header, AuthHeaderNS1} | TCConfig],
    NS2 = <<"ns2">>,
    AuthHeaderNS2 = ensure_namespaced_api_key(NS2, TCConfig),
    TCConfigNS2 = [{auth_header, AuthHeaderNS2} | TCConfig],

    AuthHeaderViewerNS1 = ensure_namespaced_api_key(
        NS1, #{name => <<"viewer">>, role => viewer}, TCConfig
    ),
    TCConfigViewerNS1 = [{auth_header, AuthHeaderViewerNS1} | TCConfig],

    ?assertMatch({200, #{<<"data">> := []}}, list(TCConfigGlobal)),
    ?assertMatch({200, #{<<"data">> := []}}, list(TCConfigNS1)),
    ?assertMatch({200, #{<<"data">> := []}}, list(TCConfigNS2)),

    %% Create a few simple rules (no references to actions/sources)
    RuleTopic1 = <<"t">>,
    IdGlobal = <<"id_global">>,
    IdNS1 = <<"id_ns1">>,
    IdNS2 = <<"id_ns2">>,
    RepublishAction = #{
        <<"function">> => <<"republish">>,
        <<"args">> => #{
            <<"topic">> => <<"rep/${.topic}/${.ns}">>,
            <<"qos">> => 2,
            <<"payload">> => <<"${.}">>
        }
    },
    ConfigGlobal = rule_config(#{
        <<"id">> => IdGlobal,
        <<"description">> => <<"global">>,
        <<"sql">> => fmt(<<"select *, 'global' as ns from ${t}">>, #{t => RuleTopic1}),
        <<"actions">> => [RepublishAction]
    }),
    ConfigNS1 = rule_config(#{
        <<"id">> => IdNS1,
        <<"description">> => <<"ns1">>,
        <<"sql">> => fmt(<<"select *, 'ns1' as ns from ${t}">>, #{t => RuleTopic1}),
        <<"actions">> => [RepublishAction]
    }),
    ConfigNS2 = rule_config(#{
        <<"id">> => IdNS2,
        <<"description">> => <<"ns2">>,
        <<"sql">> => fmt(<<"select *, 'ns2' as ns from ${t}">>, #{t => RuleTopic1}),
        <<"actions">> => [RepublishAction]
    }),

    ?assertMatch(
        {201, #{<<"id">> := IdGlobal, <<"description">> := <<"global">>}},
        create(ConfigGlobal, TCConfigGlobal)
    ),
    ?assertMatch(
        {201, #{<<"id">> := IdNS1, <<"description">> := <<"ns1">>}},
        create(ConfigNS1, TCConfigNS1)
    ),
    ?assertMatch(
        {201, #{<<"id">> := IdNS2, <<"description">> := <<"ns2">>}},
        create(ConfigNS2, TCConfigNS2)
    ),

    ?assertMatch(
        {200, #{<<"data">> := [#{<<"id">> := IdGlobal, <<"description">> := <<"global">>}]}},
        list(TCConfigGlobal)
    ),
    ?assertMatch(
        {200, #{<<"data">> := [#{<<"id">> := IdNS1, <<"description">> := <<"ns1">>}]}},
        list(TCConfigNS1)
    ),
    ?assertMatch(
        {200, #{<<"data">> := [#{<<"id">> := IdNS2, <<"description">> := <<"ns2">>}]}},
        list(TCConfigNS2)
    ),

    ?assertMatch({200, #{<<"description">> := <<"global">>}}, get(IdGlobal, TCConfigGlobal)),
    ?assertMatch({200, #{<<"description">> := <<"ns1">>}}, get(IdNS1, TCConfigNS1)),
    ?assertMatch({200, #{<<"description">> := <<"ns2">>}}, get(IdNS2, TCConfigNS2)),

    ?assertMatch({404, _}, get(IdGlobal, TCConfigNS1)),
    ?assertMatch({404, _}, get(IdGlobal, TCConfigNS2)),
    ?assertMatch({404, _}, get(IdNS1, TCConfigGlobal)),
    ?assertMatch({404, _}, get(IdNS1, TCConfigNS2)),

    %% Update
    ?assertMatch(
        {200, #{<<"id">> := IdGlobal, <<"description">> := <<"updated global">>}},
        update(IdGlobal, ConfigGlobal#{<<"description">> => <<"updated global">>}, TCConfigGlobal)
    ),
    ?assertMatch(
        {200, #{<<"id">> := IdNS1, <<"description">> := <<"updated ns1">>}},
        update(IdNS1, ConfigNS1#{<<"description">> => <<"updated ns1">>}, TCConfigNS1)
    ),
    ?assertMatch(
        {200, #{<<"id">> := IdNS2, <<"description">> := <<"updated ns2">>}},
        update(IdNS2, ConfigNS2#{<<"description">> => <<"updated ns2">>}, TCConfigNS2)
    ),

    ?assertMatch(
        {200, #{<<"data">> := [#{<<"id">> := IdGlobal, <<"description">> := <<"updated global">>}]}},
        list(TCConfigGlobal)
    ),
    ?assertMatch(
        {200, #{<<"data">> := [#{<<"id">> := IdNS1, <<"description">> := <<"updated ns1">>}]}},
        list(TCConfigNS1)
    ),
    ?assertMatch(
        {200, #{<<"data">> := [#{<<"id">> := IdNS2, <<"description">> := <<"updated ns2">>}]}},
        list(TCConfigNS2)
    ),

    ?assertMatch(
        {200, #{<<"description">> := <<"updated global">>}}, get(IdGlobal, TCConfigGlobal)
    ),
    ?assertMatch({200, #{<<"description">> := <<"updated ns1">>}}, get(IdNS1, TCConfigNS1)),
    ?assertMatch({200, #{<<"description">> := <<"updated ns2">>}}, get(IdNS2, TCConfigNS2)),

    ?assertMatch({404, _}, get(IdGlobal, TCConfigNS1)),
    ?assertMatch({404, _}, get(IdGlobal, TCConfigNS2)),
    ?assertMatch({404, _}, get(IdNS1, TCConfigGlobal)),
    ?assertMatch({404, _}, get(IdNS1, TCConfigNS2)),

    %% Trigger rule / check topic index
    {ok, C} = emqtt:start_link(#{port => MQTTPort, proto_ver => v5}),
    {ok, _} = emqtt:connect(C),
    {ok, _, _} = emqtt:subscribe(C, <<"rep/#">>, [{qos, 2}]),

    %% Same rule topic is used by all rules here.
    {ok, _} = emqtt:publish(C, RuleTopic1, <<"hello!">>, [{qos, 2}]),
    ?assertReceivePublish(
        <<"rep/t/global">>,
        #{payload := #{<<"metadata">> := #{<<"rule_id">> := IdGlobal}}}
    ),
    ?assertReceivePublish(
        <<"rep/t/ns1">>,
        #{payload := #{<<"metadata">> := #{<<"rule_id">> := IdNS1}}}
    ),
    ?assertReceivePublish(
        <<"rep/t/ns2">>,
        #{payload := #{<<"metadata">> := #{<<"rule_id">> := IdNS2}}}
    ),
    ?assertNotReceive({publish, _}),

    %% Metrics
    ?assertMatch(
        {200, #{
            <<"metrics">> := #{
                <<"actions.discarded">> := 0,
                <<"actions.failed">> := 0,
                <<"actions.failed.out_of_service">> := 0,
                <<"actions.failed.unknown">> := 0,
                <<"actions.success">> := 1,
                <<"actions.total">> := 1,
                <<"failed">> := 0,
                <<"failed.exception">> := 0,
                <<"failed.no_result">> := 0,
                <<"matched">> := 1,
                <<"matched.rate">> := _,
                <<"matched.rate.last5m">> := _,
                <<"matched.rate.max">> := _,
                <<"passed">> := 1
            }
        }},
        get_metrics(IdGlobal, TCConfigGlobal)
    ),
    ?assertMatch(
        {200, #{
            <<"metrics">> := #{
                <<"actions.discarded">> := 0,
                <<"actions.failed">> := 0,
                <<"actions.failed.out_of_service">> := 0,
                <<"actions.failed.unknown">> := 0,
                <<"actions.success">> := 1,
                <<"actions.total">> := 1,
                <<"failed">> := 0,
                <<"failed.exception">> := 0,
                <<"failed.no_result">> := 0,
                <<"matched">> := 1,
                <<"matched.rate">> := _,
                <<"matched.rate.last5m">> := _,
                <<"matched.rate.max">> := _,
                <<"passed">> := 1
            }
        }},
        get_metrics(IdNS1, TCConfigNS1)
    ),
    ?assertMatch(
        {200, #{
            <<"metrics">> := #{
                <<"actions.discarded">> := 0,
                <<"actions.failed">> := 0,
                <<"actions.failed.out_of_service">> := 0,
                <<"actions.failed.unknown">> := 0,
                <<"actions.success">> := 1,
                <<"actions.total">> := 1,
                <<"failed">> := 0,
                <<"failed.exception">> := 0,
                <<"failed.no_result">> := 0,
                <<"matched">> := 1,
                <<"matched.rate">> := _,
                <<"matched.rate.last5m">> := _,
                <<"matched.rate.max">> := _,
                <<"passed">> := 1
            }
        }},
        get_metrics(IdNS2, TCConfigNS2)
    ),

    ?assertMatch({204, _}, reset_metrics(IdNS2, TCConfigNS2)),
    ?assertMatch(
        {200, #{
            <<"metrics">> := #{
                <<"actions.total">> := 1,
                <<"matched">> := 1,
                <<"passed">> := 1
            }
        }},
        get_metrics(IdGlobal, TCConfigGlobal)
    ),
    ?assertMatch(
        {200, #{
            <<"metrics">> := #{
                <<"actions.total">> := 1,
                <<"matched">> := 1,
                <<"passed">> := 1
            }
        }},
        get_metrics(IdNS1, TCConfigNS1)
    ),
    ?assertMatch(
        {200, #{
            <<"metrics">> := #{
                <<"actions.total">> := 0,
                <<"matched">> := 0,
                <<"passed">> := 0
            }
        }},
        get_metrics(IdNS2, TCConfigNS2)
    ),

    ?assertMatch({204, _}, reset_metrics(IdGlobal, TCConfigGlobal)),
    ?assertMatch(
        {200, #{
            <<"metrics">> := #{
                <<"actions.total">> := 0,
                <<"matched">> := 0,
                <<"passed">> := 0
            }
        }},
        get_metrics(IdGlobal, TCConfigGlobal)
    ),
    ?assertMatch(
        {200, #{
            <<"metrics">> := #{
                <<"actions.total">> := 1,
                <<"matched">> := 1,
                <<"passed">> := 1
            }
        }},
        get_metrics(IdNS1, TCConfigNS1)
    ),
    ?assertMatch(
        {200, #{
            <<"metrics">> := #{
                <<"actions.total">> := 0,
                <<"matched">> := 0,
                <<"passed">> := 0
            }
        }},
        get_metrics(IdNS2, TCConfigNS2)
    ),

    %% Simulate
    %% Non-simulation tests don't have anything that depends on namespaces.  Simulate
    %% exercises existing real rules.
    SimulatedEncPayload = emqx_utils_json:encode(#{<<"just">> => <<"testing">>}),
    SimulateParams = #{
        <<"context">> => #{
            <<"clientid">> => <<"c_emqx">>,
            <<"event_type">> => <<"message_publish">>,
            <<"username">> => <<"u_emqx">>,
            <<"payload">> => SimulatedEncPayload,
            <<"qos">> => 1,
            <<"topic">> => RuleTopic1
        },
        <<"stop_action_after_template_rendering">> => false
    },
    ?assertMatch({200, _}, simulate(IdGlobal, SimulateParams, TCConfigGlobal)),
    ?assertMatch({200, _}, simulate(IdNS1, SimulateParams, TCConfigNS1)),
    ?assertMatch({200, _}, simulate(IdNS2, SimulateParams, TCConfigNS2)),

    ?assertReceivePublish(
        <<"rep/t/global">>,
        #{
            payload := #{
                <<"payload">> := SimulatedEncPayload,
                <<"metadata">> := #{<<"rule_id">> := IdGlobal}
            }
        }
    ),
    ?assertReceivePublish(
        <<"rep/t/ns1">>,
        #{
            payload := #{
                <<"payload">> := SimulatedEncPayload,
                <<"metadata">> := #{<<"rule_id">> := IdNS1}
            }
        }
    ),
    ?assertReceivePublish(
        <<"rep/t/ns2">>,
        #{
            payload := #{
                <<"payload">> := SimulatedEncPayload,
                <<"metadata">> := #{<<"rule_id">> := IdNS2}
            }
        }
    ),
    ?assertNotReceive({publish, _}),

    %% Root config
    ?assertMatch({200, _}, get_rule_engine_config(TCConfigGlobal)),
    ?assertMatch({200, _}, get_rule_engine_config(TCConfigNS1)),
    ?assertMatch({200, _}, get_rule_engine_config(TCConfigNS2)),

    {200, RootConfig0} = get_rule_engine_config(TCConfigGlobal),
    ?assertMatch(
        {200, #{<<"jq_function_default_timeout">> := 1_000}},
        set_rule_engine_config(
            RootConfig0#{<<"jq_function_default_timeout">> := 1_000}, TCConfigGlobal
        )
    ),
    ?assertMatch(
        {200, #{<<"jq_function_default_timeout">> := 2_000}},
        set_rule_engine_config(
            RootConfig0#{<<"jq_function_default_timeout">> := 2_000}, TCConfigNS1
        )
    ),
    ?assertMatch(
        {200, #{<<"jq_function_default_timeout">> := 3_000}},
        set_rule_engine_config(
            RootConfig0#{<<"jq_function_default_timeout">> := 3_000}, TCConfigNS2
        )
    ),

    ?assertMatch(
        {200, #{<<"jq_function_default_timeout">> := 1_000}},
        get_rule_engine_config(TCConfigGlobal)
    ),
    ?assertMatch(
        {200, #{<<"jq_function_default_timeout">> := 2_000}},
        get_rule_engine_config(TCConfigNS1)
    ),
    ?assertMatch(
        {200, #{<<"jq_function_default_timeout">> := 3_000}},
        get_rule_engine_config(TCConfigNS2)
    ),

    %% Viewer cannot mutate anything
    ?assertMatch({404, _}, get(IdNS2, TCConfigViewerNS1)),

    ?assertMatch({200, #{<<"data">> := [_]}}, list(TCConfigViewerNS1)),
    ?assertMatch({200, _}, get(IdNS1, TCConfigViewerNS1)),
    ?assertMatch({200, _}, get_metrics(IdNS1, TCConfigViewerNS1)),

    ?assertMatch({403, _}, create(ConfigNS1, TCConfigViewerNS1)),
    ?assertMatch({403, _}, update(IdNS1, ConfigNS1, TCConfigViewerNS1)),
    ?assertMatch({403, _}, delete(IdNS1, TCConfigViewerNS1)),
    ?assertMatch({403, _}, simulate(IdNS1, SimulateParams, TCConfigViewerNS1)),
    ?assertMatch({403, _}, reset_metrics(IdNS1, TCConfigViewerNS1)),

    %% Delete
    ?assertMatch({204, _}, delete(IdNS2, TCConfigNS2)),

    ?assertMatch({200, #{<<"data">> := [_]}}, list(TCConfigGlobal)),
    ?assertMatch({200, #{<<"data">> := [_]}}, list(TCConfigNS1)),
    ?assertMatch({200, #{<<"data">> := []}}, list(TCConfigNS2)),

    ?assertMatch({200, _}, get(IdGlobal, TCConfigGlobal)),
    ?assertMatch({200, _}, get(IdNS1, TCConfigNS1)),
    ?assertMatch({404, _}, get(IdNS2, TCConfigNS2)),

    ?assertMatch({200, _}, get_metrics(IdGlobal, TCConfigGlobal)),
    ?assertMatch({200, _}, get_metrics(IdNS1, TCConfigNS1)),
    ?assertMatch({404, _}, get_metrics(IdNS2, TCConfigNS2)),

    ?assertMatch({204, _}, reset_metrics(IdGlobal, TCConfigGlobal)),
    ?assertMatch({204, _}, reset_metrics(IdNS1, TCConfigNS1)),
    ?assertMatch({404, _}, reset_metrics(IdNS2, TCConfigNS2)),

    ?assertMatch({200, _}, update(IdGlobal, ConfigGlobal, TCConfigGlobal)),
    ?assertMatch({200, _}, update(IdNS1, ConfigNS1, TCConfigNS1)),
    ?assertMatch({404, _}, update(IdNS2, ConfigNS2, TCConfigNS2)),

    ?assertMatch({404, _}, simulate(IdNS2, SimulateParams, TCConfigNS2)),

    ?assertMatch({204, _}, delete(IdGlobal, TCConfigGlobal)),

    ?assertMatch({200, #{<<"data">> := []}}, list(TCConfigGlobal)),
    ?assertMatch({200, #{<<"data">> := [_]}}, list(TCConfigNS1)),
    ?assertMatch({200, #{<<"data">> := []}}, list(TCConfigNS2)),

    ?assertMatch({404, _}, get(IdGlobal, TCConfigGlobal)),
    ?assertMatch({200, _}, get(IdNS1, TCConfigNS1)),
    ?assertMatch({404, _}, get(IdNS2, TCConfigNS2)),

    ?assertMatch({204, _}, delete(IdNS1, TCConfigNS1)),

    ok = emqtt:stop(C),

    ok.

-doc """
Verifies that rules referencing actions are restricted to their namespaces.
""".
t_namespaced_actions() ->
    [{matrix, true}].
t_namespaced_actions(matrix) ->
    [[?local, actions]];
t_namespaced_actions(TCConfig0) when is_list(TCConfig0) ->
    %% Worth checking in a clustered setup?
    Node = node(),
    TCConfig = [{node, Node} | TCConfig0],
    #{
        bridge_id := BridgeId,
        remote_topic := RemoteTopic,
        mqtt_port := MQTTPort,
        namespaces := [?global_ns, NS1, NS2]
    } = Params = setup_namespaced_actions_sources_scenario(TCConfig),
    #{
        tc_configs := #{
            ?global_ns := TCConfigGlobal,
            NS1 := TCConfigNS1,
            NS2 := TCConfigNS2
        }
    } = Params,

    RuleTopicGlobal = <<"global">>,
    RuleTopicNS1 = <<"ns1">>,
    RuleTopicNS2 = <<"ns2">>,
    IdGlobal = <<"id_global">>,
    IdNS1 = <<"id_ns1">>,
    IdNS2 = <<"id_ns2">>,
    ConfigGlobal = rule_config(#{
        <<"id">> => IdGlobal,
        <<"description">> => <<"global">>,
        <<"sql">> => fmt(<<"select *, 'global' as ns from ${t}">>, #{t => RuleTopicGlobal}),
        <<"actions">> => [BridgeId]
    }),
    ConfigNS1 = rule_config(#{
        <<"id">> => IdNS1,
        <<"description">> => <<"ns1">>,
        <<"sql">> => fmt(<<"select *, 'ns1' as ns from ${t}">>, #{t => RuleTopicNS1}),
        <<"actions">> => [BridgeId]
    }),
    ConfigNS2 = rule_config(#{
        <<"id">> => IdNS2,
        <<"description">> => <<"ns2">>,
        <<"sql">> => fmt(<<"select *, 'ns2' as ns from ${t}">>, #{t => RuleTopicNS2}),
        <<"actions">> => [BridgeId]
    }),

    ?assertMatch(
        {201, #{<<"id">> := IdGlobal, <<"description">> := <<"global">>}},
        create(ConfigGlobal, TCConfigGlobal)
    ),
    ?assertMatch(
        {201, #{<<"id">> := IdNS1, <<"description">> := <<"ns1">>}},
        create(ConfigNS1, TCConfigNS1)
    ),
    ?assertMatch(
        {201, #{<<"id">> := IdNS2, <<"description">> := <<"ns2">>}},
        create(ConfigNS2, TCConfigNS2)
    ),

    {ok, C} = emqtt:start_link(#{port => MQTTPort, proto_ver => v5}),
    {ok, _} = emqtt:connect(C),
    {ok, _, _} = emqtt:subscribe(C, RemoteTopic, [{qos, 2}]),

    {ok, _} = emqtt:publish(C, RuleTopicNS1, <<"hello ns1">>, [{qos, 2}]),
    ?assertReceivePublish(
        RemoteTopic,
        #{
            payload := #{
                <<"ns">> := <<"ns1">>,
                <<"metadata">> := #{<<"rule_id">> := IdNS1}
            }
        }
    ),
    ?assertNotReceive({publish, _}),

    {ok, _} = emqtt:publish(C, RuleTopicGlobal, <<"hello global">>, [{qos, 2}]),
    ?assertReceivePublish(
        RemoteTopic,
        #{
            payload := #{
                <<"ns">> := <<"global">>,
                <<"metadata">> := #{<<"rule_id">> := IdGlobal}
            }
        }
    ),
    ?assertNotReceive({publish, _}),

    ok = emqtt:stop(C),

    ok.

-doc """
Verifies that rules referencing sources are restricted to their namespaces.
""".
t_namespaced_sources() ->
    [{matrix, true}].
t_namespaced_sources(matrix) ->
    [[?local, sources]];
t_namespaced_sources(TCConfig0) when is_list(TCConfig0) ->
    %% Worth checking in a clustered setup?
    Node = node(),
    TCConfig = [{node, Node} | TCConfig0],
    #{
        bridge_id := BridgeId,
        remote_topic := RemoteTopic,
        mqtt_port := MQTTPort,
        namespaces := [?global_ns, NS1, NS2]
    } = Params = setup_namespaced_actions_sources_scenario(TCConfig),
    #{
        tc_configs := #{
            ?global_ns := TCConfigGlobal,
            NS1 := TCConfigNS1,
            NS2 := TCConfigNS2
        }
    } = Params,
    HookPoint = emqx_bridge_v2:source_hookpoint(BridgeId),

    IdGlobal = <<"id_global">>,
    IdNS1 = <<"id_ns1">>,
    IdNS2 = <<"id_ns2">>,
    RepublishAction = #{
        <<"function">> => <<"republish">>,
        <<"args">> => #{
            <<"topic">> => <<"rep/${.topic}/${.ns}">>,
            <<"qos">> => 2,
            <<"payload">> => <<"${.}">>
        }
    },
    ConfigGlobal = rule_config(#{
        <<"id">> => IdGlobal,
        <<"description">> => <<"global">>,
        <<"sql">> => fmt(<<"select *, 'global' as ns from \"${t}\"">>, #{t => HookPoint}),
        <<"actions">> => [RepublishAction]
    }),
    ConfigNS1 = rule_config(#{
        <<"id">> => IdNS1,
        <<"description">> => <<"ns1">>,
        <<"sql">> => fmt(<<"select *, 'ns1' as ns from \"${t}\"">>, #{t => HookPoint}),
        <<"actions">> => [RepublishAction]
    }),
    ConfigNS2 = rule_config(#{
        <<"id">> => IdNS2,
        <<"description">> => <<"ns2">>,
        <<"sql">> => fmt(<<"select *, 'ns2' as ns from \"${t}\"">>, #{t => HookPoint}),
        <<"actions">> => [RepublishAction]
    }),

    ?assertMatch(
        {201, #{<<"id">> := IdGlobal, <<"description">> := <<"global">>}},
        create(ConfigGlobal, TCConfigGlobal)
    ),
    ?assertMatch(
        {201, #{<<"id">> := IdNS1, <<"description">> := <<"ns1">>}},
        create(ConfigNS1, TCConfigNS1)
    ),
    ?assertMatch(
        {201, #{<<"id">> := IdNS2, <<"description">> := <<"ns2">>}},
        create(ConfigNS2, TCConfigNS2)
    ),

    {ok, C} = emqtt:start_link(#{port => MQTTPort, proto_ver => v5}),
    {ok, _} = emqtt:connect(C),
    {ok, _, _} = emqtt:subscribe(C, <<"rep/#">>, [{qos, 2}]),

    {ok, _} = emqtt:publish(C, RemoteTopic, <<"hello all sources">>, [{qos, 2}]),
    ?assertReceivePublish(
        <<"rep/remote/t/global">>,
        #{
            payload := #{
                <<"ns">> := <<"global">>,
                <<"metadata">> := #{<<"rule_id">> := IdGlobal}
            }
        }
    ),
    ?assertReceivePublish(
        <<"rep/remote/t/ns1">>,
        #{
            payload := #{
                <<"ns">> := <<"ns1">>,
                <<"metadata">> := #{<<"rule_id">> := IdNS1}
            }
        }
    ),
    ?assertReceivePublish(
        <<"rep/remote/t/ns2">>,
        #{
            payload := #{
                <<"ns">> := <<"ns2">>,
                <<"metadata">> := #{<<"rule_id">> := IdNS2}
            }
        }
    ),
    ?assertNotReceive({publish, _}),

    ok = emqtt:stop(C),

    ok.

-doc """
Asserts that a node restarts fine when there are namespaced rules in its config.
""".
t_namespaced_restart() ->
    [{matrix, true}].
t_namespaced_restart(matrix) ->
    [[?custom_cluster]];
t_namespaced_restart(TCConfig0) when is_list(TCConfig0) ->
    NodeSpecs = emqx_cth_cluster:mk_nodespecs(
        [
            {rule_namespaced_restart1, #{apps => app_specs()}},
            {rule_namespaced_restart2, #{apps => app_specs_no_dashboard()}}
        ],
        #{
            work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, TCConfig0),
            shutdown => 10_000
        }
    ),
    [N1Spec | _] = NodeSpecs,
    Nodes = [Node | _] = emqx_cth_cluster:start(NodeSpecs),
    on_exit(fun() -> ok = emqx_cth_cluster:stop(Nodes) end),

    {ok, APIKey} = erpc:call(Node, emqx_common_test_http, create_default_app, []),
    TCConfig = [{node, Node}, {api_key, APIKey} | TCConfig0],
    NS1 = <<"ns1">>,
    AuthHeaderNS1 = ensure_namespaced_api_key(NS1, TCConfig),
    TCConfigNS1 = [{auth_header, AuthHeaderNS1} | TCConfig],

    RuleTopic1 = <<"t">>,
    IdNS1 = <<"id_ns1">>,
    RepublishAction = #{
        <<"function">> => <<"republish">>,
        <<"args">> => #{
            <<"topic">> => <<"rep/${.topic}/${.ns}">>,
            <<"qos">> => 2,
            <<"payload">> => <<"${.}">>
        }
    },
    ConfigNS1 = rule_config(#{
        <<"id">> => IdNS1,
        <<"description">> => <<"ns1">>,
        <<"sql">> => fmt(<<"select *, 'ns1' as ns from ${t}">>, #{t => RuleTopic1}),
        <<"actions">> => [RepublishAction]
    }),

    ?assertMatch({201, _}, create(ConfigNS1, TCConfigNS1)),

    ?assertMatch(
        {200, #{<<"data">> := [#{<<"description">> := <<"ns1">>}]}},
        list(TCConfigNS1)
    ),

    [Node] = emqx_cth_cluster:restart([N1Spec]),

    ?assertMatch(
        {200, #{<<"data">> := [#{<<"description">> := <<"ns1">>}]}},
        list(TCConfigNS1)
    ),

    MQTTPort = get_tcp_mqtt_port(Node),

    {ok, C} = emqtt:start_link(#{port => MQTTPort, proto_ver => v5}),
    {ok, _} = emqtt:connect(C),
    {ok, _, _} = emqtt:subscribe(C, <<"rep/#">>, [{qos, 2}]),

    %% Same rule topic is used by all rules here.
    {ok, _} = emqtt:publish(C, RuleTopic1, <<"hello!">>, [{qos, 2}]),
    ?assertReceivePublish(
        <<"rep/t/ns1">>,
        #{payload := #{<<"metadata">> := #{<<"rule_id">> := IdNS1}}}
    ),
    ok = emqtt:stop(C),

    ok.

t_direct_dispatch_empty_string(_Config) ->
    ?check_trace(
        begin
            {201, _} = create_rule(#{
                <<"sql">> => <<"select * from t">>,
                <<"actions">> => [
                    #{
                        <<"function">> => <<"republish">>,
                        <<"args">> => #{
                            <<"direct_dispatch">> => <<"">>,
                            <<"topic">> => <<"rep">>
                        }
                    }
                ]
            }),
            emqx:subscribe(<<"rep">>),
            emqx:publish(emqx_message:make(<<"t">>, <<"hey">>)),
            ?assertReceive({deliver, <<"rep">>, _}),
            ok
        end,
        fun(Trace) ->
            ?assertMatch([_], ?of_kind("bad_direct_dispatch_resolved_value", Trace)),
            ok
        end
    ),
    ok.
