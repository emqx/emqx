%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_bridge_v1_compatibility_layer_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/asserts.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        app_specs(),
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    {ok, _} = emqx_common_test_http:create_default_app(),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

app_specs() ->
    [
        emqx,
        emqx_conf,
        emqx_connector,
        emqx_bridge,
        emqx_management,
        emqx_rule_engine,
        {emqx_dashboard, "dashboard.listeners.http { enable = true, bind = 18083 }"}
    ].

init_per_testcase(t_upgrade_raw_conf_with_deprecated_files = TestCase, Config) ->
    NodeSpecs = mk_init_load_cluster_spec(TestCase, Config),
    Nodes = emqx_cth_cluster:start(NodeSpecs),
    erpc:multicall(Nodes, fun() ->
        {ok, _} = emqx_common_test_http:create_default_app(),
        ok
    end),
    [{nodes, Nodes} | Config];
init_per_testcase(_TestCase, Config) ->
    %% Setting up mocks for fake connector and bridge V2
    setup_mocks(),
    ets:new(fun_table_name(), [named_table, public]),
    %% Create a fake connector
    {ok, _} = emqx_connector:create(con_type(), con_name(), con_config()),
    Config.

end_per_testcase(t_upgrade_raw_conf_with_deprecated_files = _TestCase, Config) ->
    Nodes = ?config(nodes, Config),
    emqx_cth_cluster:stop(Nodes),
    emqx_common_test_helpers:call_janitor(),
    ok;
end_per_testcase(_TestCase, _Config) ->
    ets:delete(fun_table_name()),
    delete_all_bridges_and_connectors(),
    meck:unload(),
    emqx_common_test_helpers:call_janitor(),
    ok.

mk_init_load_cluster_spec(Name, Config) ->
    Node1Apps =
        proplists:delete(emqx_dashboard, app_specs()) ++
            [
                {emqx_dashboard, "dashboard.listeners.http { enable = true, bind = 18084 }"}
            ],
    emqx_cth_cluster:mk_nodespecs(
        [
            {emqx_bridge_v1_compat_SUITE_1, #{role => core, apps => Node1Apps}}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Name, Config)}
    ).

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

setup_mocks() ->
    MeckOpts = [passthrough, no_link, no_history],

    catch meck:new(emqx_connector_schema, MeckOpts),
    meck:expect(emqx_connector_schema, fields, 1, con_schema()),
    meck:expect(emqx_connector_schema, connector_type_to_bridge_types, 1, [con_type()]),

    catch meck:new(emqx_connector_resource, MeckOpts),
    meck:expect(emqx_connector_resource, connector_to_resource_type, 1, con_mod()),

    catch meck:new(emqx_bridge_v2_schema, MeckOpts),
    meck:expect(emqx_bridge_v2_schema, fields, 1, bridge_schema()),

    catch meck:new(emqx_bridge_v2, MeckOpts),
    meck:expect(emqx_bridge_v2, bridge_v2_type_to_connector_type, 1, con_type()),
    meck:expect(emqx_bridge_v2, bridge_v1_type_to_bridge_v2_type, 1, bridge_type()),
    IsBridgeV2TypeFun = fun(Type) ->
        BridgeV2Type = bridge_type(),
        BridgeV2TypeBin = bridge_type_bin(),
        case Type of
            BridgeV2Type -> true;
            BridgeV2TypeBin -> true;
            _ -> false
        end
    end,
    meck:expect(emqx_bridge_v2, is_bridge_v2_type, 1, IsBridgeV2TypeFun),

    catch meck:new(emqx_bridge_v2_schema, MeckOpts),
    meck:expect(
        emqx_bridge_v2_schema,
        registered_actions_api_schemas,
        1,
        fun(Method) ->
            [{bridge_type_bin(), hoconsc:ref(?MODULE, "api_v2_" ++ Method)}]
        end
    ),

    catch meck:new(emqx_bridge_schema, MeckOpts),
    meck:expect(
        emqx_bridge_schema,
        enterprise_api_schemas,
        1,
        fun(Method) ->
            [{bridge_type_bin(), hoconsc:ref(?MODULE, "api_v1_" ++ Method)}]
        end
    ),
    meck:expect(
        emqx_bridge_schema,
        enterprise_fields_bridges,
        0,
        fun() ->
            [
                {
                    bridge_type_bin(),
                    hoconsc:mk(
                        hoconsc:map(name, hoconsc:ref(?MODULE, v1_bridge)), #{}
                    )
                }
            ]
        end
    ),

    catch meck:new(emqx_action_info, MeckOpts),
    meck:expect(emqx_action_info, bridge_v1_type_name, 1, {ok, bridge_type()}),

    ok.

con_mod() ->
    emqx_bridge_v2_test_connector.

con_type() ->
    bridge_type().

con_name() ->
    my_connector.

bridge_type() ->
    test_bridge_type.

bridge_type_bin() ->
    atom_to_binary(bridge_type(), utf8).

con_schema() ->
    [
        {
            con_type(),
            hoconsc:mk(
                hoconsc:map(name, hoconsc:ref(?MODULE, "connector")),
                #{
                    desc => <<"Test Connector Config">>,
                    required => false
                }
            )
        }
    ].

fields("connector") ->
    [
        {enable, hoconsc:mk(any(), #{})},
        {password, emqx_schema_secret:mk(#{required => false})},
        {resource_opts, hoconsc:mk(map(), #{})},
        {on_start_fun, hoconsc:mk(binary(), #{})},
        {ssl, hoconsc:ref(ssl)}
    ];
fields("api_v2_post") ->
    [
        {connector, hoconsc:mk(binary(), #{})},
        {name, hoconsc:mk(binary(), #{})},
        {type, hoconsc:mk(bridge_type(), #{})},
        {send_to, hoconsc:mk(atom(), #{})}
        | fields("connector")
    ];
fields("api_v1_post") ->
    ConnectorFields = proplists:delete(resource_opts, fields("connector")),
    [
        {connector, hoconsc:mk(binary(), #{})},
        {name, hoconsc:mk(binary(), #{})},
        {type, hoconsc:mk(bridge_type(), #{})},
        {send_to, hoconsc:mk(atom(), #{})},
        {resource_opts, hoconsc:mk(hoconsc:ref(?MODULE, v1_resource_opts), #{})}
        | ConnectorFields
    ];
fields(v1_bridge) ->
    lists:foldl(fun proplists:delete/2, fields("api_v1_post"), [name, type]);
fields(v1_resource_opts) ->
    emqx_resource_schema:create_opts(_Overrides = []);
fields(ssl) ->
    emqx_schema:client_ssl_opts_schema(#{required => false}).

con_config() ->
    #{
        <<"enable">> => true,
        <<"resource_opts">> => #{
            %% Set this to a low value to make the test run faster
            <<"health_check_interval">> => 100
        }
    }.

bridge_schema() ->
    bridge_schema(_Opts = #{}).

bridge_schema(Opts) ->
    Type = maps:get(bridge_type, Opts, bridge_type()),
    [
        {
            Type,
            hoconsc:mk(
                hoconsc:map(name, typerefl:map()),
                #{
                    desc => <<"Test Bridge Config">>,
                    required => false
                }
            )
        }
    ].

bridge_config() ->
    #{
        <<"connector">> => atom_to_binary(con_name()),
        <<"enable">> => true,
        <<"send_to">> => registered_process_name(),
        <<"resource_opts">> => #{
            <<"resume_interval">> => 100
        }
    }.

fun_table_name() ->
    emqx_bridge_v1_compatibility_layer_SUITE_fun_table.

registered_process_name() ->
    my_registered_process.

delete_all_bridges_and_connectors() ->
    lists:foreach(
        fun(#{name := Name, type := Type}) ->
            ct:pal("removing bridge ~p", [{Type, Name}]),
            emqx_bridge_v2:remove(Type, Name)
        end,
        emqx_bridge_v2:list()
    ),
    lists:foreach(
        fun(#{name := Name, type := Type}) ->
            ct:pal("removing connector ~p", [{Type, Name}]),
            emqx_connector:remove(Type, Name)
        end,
        emqx_connector:list()
    ),
    update_root_config(#{}),
    ok.

%% Hocon does not support placing a fun in a config map so we replace it with a string
wrap_fun(Fun) ->
    UniqRef = make_ref(),
    UniqRefBin = term_to_binary(UniqRef),
    UniqRefStr = iolist_to_binary(base64:encode(UniqRefBin)),
    ets:insert(fun_table_name(), {UniqRefStr, Fun}),
    UniqRefStr.

unwrap_fun(UniqRefStr) ->
    ets:lookup_element(fun_table_name(), UniqRefStr, 2).

update_root_config(RootConf) ->
    emqx_conf:update([actions], RootConf, #{override_to => cluster}).

delete_all_bridges() ->
    lists:foreach(
        fun(#{name := Name, type := Type}) ->
            ok = emqx_bridge:remove(Type, Name)
        end,
        emqx_bridge:list()
    ),
    %% at some point during the tests, sometimes `emqx_bridge:list()'
    %% returns an empty list, but `emqx:get_config([bridges])' returns
    %% a bunch of orphan test bridges...
    lists:foreach(fun emqx_resource:remove_local/1, emqx_resource:list_instances()),
    emqx_config:put([bridges], #{}),
    ok.

maybe_json_decode(X) ->
    case emqx_utils_json:safe_decode(X, [return_maps]) of
        {ok, Decoded} -> Decoded;
        {error, _} -> X
    end.

request(Method, Path, Params) ->
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    Opts = #{return_all => true},
    case emqx_mgmt_api_test_util:request_api(Method, Path, "", AuthHeader, Params, Opts) of
        {ok, {Status, Headers, Body0}} ->
            Body = maybe_json_decode(Body0),
            {ok, {Status, Headers, Body}};
        {error, {Status, Headers, Body0}} ->
            Body =
                case emqx_utils_json:safe_decode(Body0, [return_maps]) of
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

list_bridges_http_api_v1() ->
    list_bridges_http_api_v1(_Host = "http://127.0.0.1:18083").

list_bridges_http_api_v1(Host) ->
    Path = emqx_mgmt_api_test_util:api_path(Host, ["bridges"]),
    ct:pal("list bridges (http v1)"),
    Res = request(get, Path, _Params = []),
    ct:pal("list bridges (http v1) result:\n  ~p", [Res]),
    Res.

list_bridges_http_api_v2() ->
    Path = emqx_mgmt_api_test_util:api_path(["actions"]),
    ct:pal("list bridges (http v2)"),
    Res = request(get, Path, _Params = []),
    ct:pal("list bridges (http v2) result:\n  ~p", [Res]),
    Res.

list_connectors_http() ->
    Path = emqx_mgmt_api_test_util:api_path(["connectors"]),
    ct:pal("list connectors"),
    Res = request(get, Path, _Params = []),
    ct:pal("list connectors result:\n  ~p", [Res]),
    Res.

get_bridge_http_api_v1(Name) ->
    BridgeId = emqx_bridge_resource:bridge_id(bridge_type(), Name),
    Path = emqx_mgmt_api_test_util:api_path(["bridges", BridgeId]),
    ct:pal("get bridge (http v1) (~p)", [#{name => Name}]),
    Res = request(get, Path, _Params = []),
    ct:pal("get bridge (http v1) (~p) result:\n  ~p", [#{name => Name}, Res]),
    Res.

get_bridge_http_api_v2(Name) ->
    BridgeId = emqx_bridge_resource:bridge_id(bridge_type(), Name),
    Path = emqx_mgmt_api_test_util:api_path(["actions", BridgeId]),
    ct:pal("get bridge (http v2) (~p)", [#{name => Name}]),
    Res = request(get, Path, _Params = []),
    ct:pal("get bridge (http v2) (~p) result:\n  ~p", [#{name => Name}, Res]),
    Res.

get_connector_http(Name) ->
    ConnectorId = emqx_connector_resource:connector_id(con_type(), Name),
    Path = emqx_mgmt_api_test_util:api_path(["connectors", ConnectorId]),
    ct:pal("get connector (~p)", [#{name => Name, id => ConnectorId}]),
    Res = request(get, Path, _Params = []),
    ct:pal("get connector (~p) result:\n  ~p", [#{name => Name}, Res]),
    Res.

create_bridge_http_api_v1(Opts) ->
    Name = maps:get(name, Opts),
    Overrides = maps:get(overrides, Opts, #{}),
    OverrideFn = maps:get(override_fn, Opts, fun(X) -> X end),
    BridgeConfig0 = emqx_utils_maps:deep_merge(bridge_config(), Overrides),
    BridgeConfig = maps:without([<<"connector">>], BridgeConfig0),
    Params0 = BridgeConfig#{<<"type">> => bridge_type_bin(), <<"name">> => Name},
    Params = OverrideFn(Params0),
    Path = emqx_mgmt_api_test_util:api_path(["bridges"]),
    ct:pal("creating bridge (http v1): ~p", [Params]),
    Res = request(post, Path, Params),
    ct:pal("bridge create (http v1) result:\n  ~p", [Res]),
    Res.

create_bridge_http_api_v2(Opts) ->
    Name = maps:get(name, Opts),
    Overrides = maps:get(overrides, Opts, #{}),
    BridgeConfig = emqx_utils_maps:deep_merge(bridge_config(), Overrides),
    Params = BridgeConfig#{<<"type">> => bridge_type_bin(), <<"name">> => Name},
    Path = emqx_mgmt_api_test_util:api_path(["actions"]),
    ct:pal("creating bridge (http v2): ~p", [Params]),
    Res = request(post, Path, Params),
    ct:pal("bridge create (http v2) result:\n  ~p", [Res]),
    Res.

update_bridge_http_api_v1(Opts) ->
    Name = maps:get(name, Opts),
    BridgeId = emqx_bridge_resource:bridge_id(bridge_type(), Name),
    Overrides = maps:get(overrides, Opts, #{}),
    BridgeConfig0 = emqx_utils_maps:deep_merge(bridge_config(), Overrides),
    BridgeConfig = maps:without([<<"connector">>], BridgeConfig0),
    Params = BridgeConfig,
    Path = emqx_mgmt_api_test_util:api_path(["bridges", BridgeId]),
    ct:pal("updating bridge (http v1): ~p", [Params]),
    Res = request(put, Path, Params),
    ct:pal("bridge update (http v1) result:\n  ~p", [Res]),
    Res.

delete_bridge_http_api_v1(Opts) ->
    Name = maps:get(name, Opts),
    BridgeId = emqx_bridge_resource:bridge_id(bridge_type(), Name),
    Path = emqx_mgmt_api_test_util:api_path(["bridges", BridgeId]),
    ct:pal("deleting bridge (http v1)"),
    Res = request(delete, Path, _Params = []),
    ct:pal("bridge delete (http v1) result:\n  ~p", [Res]),
    Res.

delete_bridge_http_api_v2(Opts) ->
    Name = maps:get(name, Opts),
    BridgeId = emqx_bridge_resource:bridge_id(bridge_type(), Name),
    Path = emqx_mgmt_api_test_util:api_path(["actions", BridgeId]),
    ct:pal("deleting bridge (http v2)"),
    Res = request(delete, Path, _Params = []),
    ct:pal("bridge delete (http v2) result:\n  ~p", [Res]),
    Res.

enable_bridge_http_api_v1(Name) ->
    BridgeId = emqx_bridge_resource:bridge_id(bridge_type(), Name),
    Path = emqx_mgmt_api_test_util:api_path(["bridges", BridgeId, "enable", "true"]),
    ct:pal("enabling bridge (http v1)"),
    Res = request(put, Path, _Params = []),
    ct:pal("bridge enable (http v1) result:\n  ~p", [Res]),
    Res.

enable_bridge_http_api_v2(Name) ->
    BridgeId = emqx_bridge_resource:bridge_id(bridge_type(), Name),
    Path = emqx_mgmt_api_test_util:api_path(["actions", BridgeId, "enable", "true"]),
    ct:pal("enabling bridge (http v2)"),
    Res = request(put, Path, _Params = []),
    ct:pal("bridge enable (http v2) result:\n  ~p", [Res]),
    Res.

disable_bridge_http_api_v1(Name) ->
    BridgeId = emqx_bridge_resource:bridge_id(bridge_type(), Name),
    Path = emqx_mgmt_api_test_util:api_path(["bridges", BridgeId, "enable", "false"]),
    ct:pal("disabling bridge (http v1)"),
    Res = request(put, Path, _Params = []),
    ct:pal("bridge disable (http v1) result:\n  ~p", [Res]),
    Res.

disable_bridge_http_api_v2(Name) ->
    BridgeId = emqx_bridge_resource:bridge_id(bridge_type(), Name),
    Path = emqx_mgmt_api_test_util:api_path(["actions", BridgeId, "enable", "false"]),
    ct:pal("disabling bridge (http v2)"),
    Res = request(put, Path, _Params = []),
    ct:pal("bridge disable (http v2) result:\n  ~p", [Res]),
    Res.

bridge_operation_http_api_v1(Name, Op0) ->
    Op = atom_to_list(Op0),
    BridgeId = emqx_bridge_resource:bridge_id(bridge_type(), Name),
    Path = emqx_mgmt_api_test_util:api_path(["bridges", BridgeId, Op]),
    ct:pal("bridge op ~p (http v1)", [Op]),
    Res = request(post, Path, _Params = []),
    ct:pal("bridge op ~p (http v1) result:\n  ~p", [Op, Res]),
    Res.

bridge_operation_http_api_v2(Name, Op0) ->
    Op = atom_to_list(Op0),
    BridgeId = emqx_bridge_resource:bridge_id(bridge_type(), Name),
    Path = emqx_mgmt_api_test_util:api_path(["actions", BridgeId, Op]),
    ct:pal("bridge op ~p (http v2)", [Op]),
    Res = request(post, Path, _Params = []),
    ct:pal("bridge op ~p (http v2) result:\n  ~p", [Op, Res]),
    Res.

bridge_node_operation_http_api_v1(Name, Node0, Op0) ->
    Op = atom_to_list(Op0),
    Node = atom_to_list(Node0),
    BridgeId = emqx_bridge_resource:bridge_id(bridge_type(), Name),
    Path = emqx_mgmt_api_test_util:api_path(["nodes", Node, "bridges", BridgeId, Op]),
    ct:pal("bridge node op ~p (http v1)", [{Node, Op}]),
    Res = request(post, Path, _Params = []),
    ct:pal("bridge node op ~p (http v1) result:\n  ~p", [{Node, Op}, Res]),
    Res.

bridge_node_operation_http_api_v2(Name, Node0, Op0) ->
    Op = atom_to_list(Op0),
    Node = atom_to_list(Node0),
    BridgeId = emqx_bridge_resource:bridge_id(bridge_type(), Name),
    Path = emqx_mgmt_api_test_util:api_path(["nodes", Node, "actions", BridgeId, Op]),
    ct:pal("bridge node op ~p (http v2)", [{Node, Op}]),
    Res = request(post, Path, _Params = []),
    ct:pal("bridge node op ~p (http v2) result:\n  ~p", [{Node, Op}, Res]),
    Res.

is_rule_enabled(RuleId) ->
    {ok, #{enable := Enable}} = emqx_rule_engine:get_rule(RuleId),
    Enable.

update_rule_http(RuleId, Params) ->
    Path = emqx_mgmt_api_test_util:api_path(["rules", RuleId]),
    ct:pal("update rule ~p:\n  ~p", [RuleId, Params]),
    Res = request(put, Path, Params),
    ct:pal("update rule ~p result:\n  ~p", [RuleId, Res]),
    Res.

enable_rule_http(RuleId) ->
    Params = #{<<"enable">> => true},
    update_rule_http(RuleId, Params).

probe_bridge_http_api_v1(Opts) ->
    Name = maps:get(name, Opts),
    Overrides = maps:get(overrides, Opts, #{}),
    BridgeConfig0 = emqx_utils_maps:deep_merge(bridge_config(), Overrides),
    BridgeConfig = maps:without([<<"connector">>], BridgeConfig0),
    Params = BridgeConfig#{<<"type">> => bridge_type_bin(), <<"name">> => Name},
    Path = emqx_mgmt_api_test_util:api_path(["bridges_probe"]),
    ct:pal("probe bridge (http v1) (~p):\n  ~p", [#{name => Name}, Params]),
    Res = request(post, Path, Params),
    ct:pal("probe bridge (http v1) (~p) result:\n  ~p", [#{name => Name}, Res]),
    Res.

probe_action_http_api_v2(Opts) ->
    Name = maps:get(name, Opts),
    Overrides = maps:get(overrides, Opts, #{}),
    BridgeConfig = emqx_utils_maps:deep_merge(bridge_config(), Overrides),
    Params = BridgeConfig#{<<"type">> => bridge_type_bin(), <<"name">> => Name},
    Path = emqx_mgmt_api_test_util:api_path(["actions_probe"]),
    ct:pal("probe action (http v2) (~p):\n  ~p", [#{name => Name}, Params]),
    Res = request(post, Path, Params),
    ct:pal("probe action (http v2) (~p) result:\n  ~p", [#{name => Name}, Res]),
    Res.

deprecated_config() ->
    <<
        "\n"
        "bridges {\n"
        "  mqtt {\n"
        "    test {\n"
        "      bridge_mode = false\n"
        "      clean_start = true\n"
        "      egress {\n"
        "        local {topic=\"hhhhhh\"}\n"
        "        remote {\n"
        "          payload = \"${payload}\"\n"
        "          qos = 1\n"
        "          retain = false\n"
        "          topic = hhhhhhhhhh\n"
        "        }\n"
        "      }\n"
        "      enable = true\n"
        "      keepalive = 300s\n"
        "      proto_ver = v4\n"
        "      resource_opts {\n"
        "        health_check_interval = 15s\n"
        "        inflight_window = 100\n"
        "        max_buffer_bytes = 1GB\n"
        "        query_mode = async\n"
        "        request_ttl = 45s\n"
        "        start_timeout = 5s\n"
        "        worker_pool_size = 4\n"
        "      }\n"
        "      retry_interval = 15s\n"
        "      server = \"127.0.0.1\"\n"
        "      ssl {enable = false, verify = verify_peer}\n"
        "    }\n"
        "  }\n"
        "}\n"
    >>.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_name_too_long(_Config) ->
    LongName = list_to_binary(lists:duplicate(256, $a)),
    ?assertMatch(
        {error, {{_, 400, _}, _, #{<<"message">> := #{<<"reason">> := <<"invalid_map_key">>}}}},
        create_bridge_http_api_v1(#{name => LongName})
    ),
    ok.

t_scenario_1(_Config) ->
    %% ===================================================================================
    %% Pre-conditions
    %% ===================================================================================
    ?assertMatch({ok, {{_, 200, _}, _, []}}, list_bridges_http_api_v1()),
    ?assertMatch({ok, {{_, 200, _}, _, []}}, list_bridges_http_api_v2()),
    %% created in the test case init
    ?assertMatch({ok, {{_, 200, _}, _, [#{}]}}, list_connectors_http()),
    {ok, {{_, 200, _}, _, [#{<<"name">> := PreexistentConnectorName}]}} = list_connectors_http(),

    %% ===================================================================================
    %% Create a single bridge v2.  It should still be listed and functional when using v1
    %% APIs.
    %% ===================================================================================
    NameA = <<"bridgev2a">>,
    ?assertMatch(
        {ok, {{_, 201, _}, _, #{}}},
        create_bridge_http_api_v1(#{name => NameA})
    ),
    ?assertMatch({ok, {{_, 200, _}, _, [#{<<"name">> := NameA}]}}, list_bridges_http_api_v1()),
    ?assertMatch({ok, {{_, 200, _}, _, [#{<<"name">> := NameA}]}}, list_bridges_http_api_v2()),
    %% created a new one from the v1 API
    ?assertMatch({ok, {{_, 200, _}, _, [#{}, #{}]}}, list_connectors_http()),
    ?assertMatch({ok, {{_, 200, _}, _, #{<<"name">> := NameA}}}, get_bridge_http_api_v1(NameA)),
    ?assertMatch({ok, {{_, 200, _}, _, #{<<"name">> := NameA}}}, get_bridge_http_api_v2(NameA)),

    ?assertMatch({ok, {{_, 204, _}, _, _}}, disable_bridge_http_api_v1(NameA)),
    ?assertMatch({ok, {{_, 204, _}, _, _}}, enable_bridge_http_api_v1(NameA)),
    ?assertMatch({ok, {{_, 204, _}, _, _}}, disable_bridge_http_api_v2(NameA)),
    ?assertMatch({ok, {{_, 204, _}, _, _}}, enable_bridge_http_api_v2(NameA)),

    ?assertMatch({ok, {{_, 204, _}, _, _}}, bridge_operation_http_api_v1(NameA, stop)),
    ?assertMatch({ok, {{_, 204, _}, _, _}}, bridge_operation_http_api_v1(NameA, start)),
    ?assertMatch({ok, {{_, 204, _}, _, _}}, bridge_operation_http_api_v1(NameA, restart)),
    %% TODO: currently, only `start' op is supported by the v2 API.
    %% ?assertMatch({ok, {{_, 204, _}, _, _}}, bridge_operation_http_api_v2(NameA, stop)),
    ?assertMatch({ok, {{_, 204, _}, _, _}}, bridge_operation_http_api_v2(NameA, start)),
    %% TODO: currently, only `start' op is supported by the v2 API.
    %% ?assertMatch({ok, {{_, 204, _}, _, _}}, bridge_operation_http_api_v2(NameA, restart)),

    ?assertMatch({ok, {{_, 204, _}, _, _}}, bridge_node_operation_http_api_v1(NameA, node(), stop)),
    ?assertMatch(
        {ok, {{_, 204, _}, _, _}}, bridge_node_operation_http_api_v1(NameA, node(), start)
    ),
    ?assertMatch(
        {ok, {{_, 204, _}, _, _}}, bridge_node_operation_http_api_v1(NameA, node(), restart)
    ),
    %% TODO: currently, only `start' op is supported by the v2 API.
    %% ?assertMatch({ok, {{_, 204, _}, _, _}}, bridge_node_operation_http_api_v2(NameA, stop)),
    ?assertMatch(
        {ok, {{_, 204, _}, _, _}}, bridge_node_operation_http_api_v2(NameA, node(), start)
    ),
    %% TODO: currently, only `start' op is supported by the v2 API.
    %% ?assertMatch({ok, {{_, 204, _}, _, _}}, bridge_node_operation_http_api_v2(NameA, restart)),

    {ok, {{_, 200, _}, _, #{<<"connector">> := GeneratedConnName}}} = get_bridge_http_api_v2(NameA),
    ?assertMatch(
        {ok, {{_, 200, _}, _, #{<<"name">> := GeneratedConnName}}},
        get_connector_http(GeneratedConnName)
    ),

    %% ===================================================================================
    %% Update the bridge using v1 API.
    %% ===================================================================================
    ?assertMatch(
        {ok, {{_, 200, _}, _, _}},
        update_bridge_http_api_v1(#{name => NameA})
    ),
    ?assertMatch({ok, {{_, 200, _}, _, [#{<<"name">> := NameA}]}}, list_bridges_http_api_v1()),
    ?assertMatch({ok, {{_, 200, _}, _, [#{<<"name">> := NameA}]}}, list_bridges_http_api_v2()),
    ?assertMatch({ok, {{_, 200, _}, _, [#{}, #{}]}}, list_connectors_http()),
    ?assertMatch({ok, {{_, 200, _}, _, #{<<"name">> := NameA}}}, get_bridge_http_api_v1(NameA)),
    ?assertMatch({ok, {{_, 200, _}, _, #{<<"name">> := NameA}}}, get_bridge_http_api_v2(NameA)),

    %% ===================================================================================
    %% Now create a new bridge_v2 pointing to the same connector.  It should no longer be
    %% functions via v1 API, nor be listed in it.  The new bridge must create a new
    %% channel, so that this bridge is no longer considered v1.
    %% ===================================================================================
    NameB = <<"bridgev2b">>,
    ?assertMatch(
        {ok, {{_, 201, _}, _, #{}}},
        create_bridge_http_api_v2(#{
            name => NameB, overrides => #{<<"connector">> => GeneratedConnName}
        })
    ),
    ?assertMatch({ok, {{_, 200, _}, _, []}}, list_bridges_http_api_v1()),
    ?assertMatch(
        {ok, {{_, 200, _}, _, [#{<<"name">> := _}, #{<<"name">> := _}]}}, list_bridges_http_api_v2()
    ),
    ?assertMatch({ok, {{_, 200, _}, _, [#{}, #{}]}}, list_connectors_http()),
    ?assertMatch({error, {{_, 404, _}, _, #{}}}, get_bridge_http_api_v1(NameA)),
    ?assertMatch({error, {{_, 404, _}, _, #{}}}, get_bridge_http_api_v1(NameB)),
    ?assertMatch({ok, {{_, 200, _}, _, #{<<"name">> := NameA}}}, get_bridge_http_api_v2(NameA)),
    ?assertMatch({ok, {{_, 200, _}, _, #{<<"name">> := NameB}}}, get_bridge_http_api_v2(NameB)),
    ?assertMatch(
        {ok, {{_, 200, _}, _, #{<<"name">> := GeneratedConnName}}},
        get_connector_http(GeneratedConnName)
    ),

    ?assertMatch({error, {{_, 400, _}, _, _}}, disable_bridge_http_api_v1(NameA)),
    ?assertMatch({error, {{_, 400, _}, _, _}}, enable_bridge_http_api_v1(NameA)),
    ?assertMatch({error, {{_, 400, _}, _, _}}, disable_bridge_http_api_v1(NameB)),
    ?assertMatch({error, {{_, 400, _}, _, _}}, enable_bridge_http_api_v1(NameB)),
    ?assertMatch({ok, {{_, 204, _}, _, _}}, disable_bridge_http_api_v2(NameA)),
    ?assertMatch({ok, {{_, 204, _}, _, _}}, enable_bridge_http_api_v2(NameA)),

    ?assertMatch({error, {{_, 400, _}, _, _}}, bridge_operation_http_api_v1(NameA, stop)),
    ?assertMatch({error, {{_, 400, _}, _, _}}, bridge_operation_http_api_v1(NameA, start)),
    ?assertMatch({error, {{_, 400, _}, _, _}}, bridge_operation_http_api_v1(NameA, restart)),
    ?assertMatch({error, {{_, 400, _}, _, _}}, bridge_operation_http_api_v1(NameB, stop)),
    ?assertMatch({error, {{_, 400, _}, _, _}}, bridge_operation_http_api_v1(NameB, start)),
    ?assertMatch({error, {{_, 400, _}, _, _}}, bridge_operation_http_api_v1(NameB, restart)),
    %% TODO: currently, only `start' op is supported by the v2 API.
    %% ?assertMatch({ok, {{_, 204, _}, _, _}}, bridge_operation_http_api_v2(NameA, stop)),
    ?assertMatch({ok, {{_, 204, _}, _, _}}, bridge_operation_http_api_v2(NameA, start)),
    %% TODO: currently, only `start' op is supported by the v2 API.
    %% ?assertMatch({ok, {{_, 204, _}, _, _}}, bridge_operation_http_api_v2(NameA, restart)),
    %% TODO: currently, only `start' op is supported by the v2 API.
    %% ?assertMatch({ok, {{_, 204, _}, _, _}}, bridge_operation_http_api_v2(NameB, stop)),
    ?assertMatch({ok, {{_, 204, _}, _, _}}, bridge_operation_http_api_v2(NameB, start)),
    %% TODO: currently, only `start' op is supported by the v2 API.
    %% ?assertMatch({ok, {{_, 204, _}, _, _}}, bridge_operation_http_api_v2(NameB, restart)),

    ?assertMatch(
        {error, {{_, 400, _}, _, _}}, bridge_node_operation_http_api_v1(NameA, node(), stop)
    ),
    ?assertMatch(
        {error, {{_, 400, _}, _, _}}, bridge_node_operation_http_api_v1(NameA, node(), start)
    ),
    ?assertMatch(
        {error, {{_, 400, _}, _, _}}, bridge_node_operation_http_api_v1(NameA, node(), restart)
    ),
    ?assertMatch(
        {error, {{_, 400, _}, _, _}}, bridge_node_operation_http_api_v1(NameB, node(), stop)
    ),
    ?assertMatch(
        {error, {{_, 400, _}, _, _}}, bridge_node_operation_http_api_v1(NameB, node(), start)
    ),
    ?assertMatch(
        {error, {{_, 400, _}, _, _}}, bridge_node_operation_http_api_v1(NameB, node(), restart)
    ),
    %% TODO: currently, only `start' op is supported by the v2 API.
    %% ?assertMatch({ok, {{_, 204, _}, _, _}}, bridge_node_operation_http_api_v2(NameA, stop)),
    %% ?assertMatch({ok, {{_, 204, _}, _, _}}, bridge_node_operation_http_api_v2(NameB, stop)),
    ?assertMatch(
        {ok, {{_, 204, _}, _, _}}, bridge_node_operation_http_api_v2(NameA, node(), start)
    ),
    ?assertMatch(
        {ok, {{_, 204, _}, _, _}}, bridge_node_operation_http_api_v2(NameB, node(), start)
    ),
    %% TODO: currently, only `start' op is supported by the v2 API.
    %% ?assertMatch({ok, {{_, 204, _}, _, _}}, bridge_node_operation_http_api_v2(NameA, restart)),
    %% ?assertMatch({ok, {{_, 204, _}, _, _}}, bridge_node_operation_http_api_v2(NameB, restart)),

    %% ===================================================================================
    %% Try to delete the original bridge using V1.  It should fail and its connector
    %% should be preserved.
    %% ===================================================================================
    ?assertMatch(
        {error, {{_, 400, _}, _, _}},
        delete_bridge_http_api_v1(#{name => NameA})
    ),
    ?assertMatch({ok, {{_, 200, _}, _, []}}, list_bridges_http_api_v1()),
    ?assertMatch(
        {ok, {{_, 200, _}, _, [#{<<"name">> := _}, #{<<"name">> := _}]}}, list_bridges_http_api_v2()
    ),
    ?assertMatch({ok, {{_, 200, _}, _, [#{}, #{}]}}, list_connectors_http()),
    ?assertMatch({error, {{_, 404, _}, _, #{}}}, get_bridge_http_api_v1(NameA)),
    ?assertMatch({error, {{_, 404, _}, _, #{}}}, get_bridge_http_api_v1(NameB)),
    ?assertMatch({ok, {{_, 200, _}, _, #{<<"name">> := NameA}}}, get_bridge_http_api_v2(NameA)),
    ?assertMatch({ok, {{_, 200, _}, _, #{<<"name">> := NameB}}}, get_bridge_http_api_v2(NameB)),
    ?assertMatch(
        {ok, {{_, 200, _}, _, #{<<"name">> := GeneratedConnName}}},
        get_connector_http(GeneratedConnName)
    ),

    %% ===================================================================================
    %% Delete the 2nd new bridge so it appears again in the V1 API.
    %% ===================================================================================
    ?assertMatch(
        {ok, {{_, 204, _}, _, _}},
        delete_bridge_http_api_v2(#{name => NameB})
    ),
    ?assertMatch({ok, {{_, 200, _}, _, [#{<<"name">> := NameA}]}}, list_bridges_http_api_v1()),
    ?assertMatch({ok, {{_, 200, _}, _, [#{<<"name">> := NameA}]}}, list_bridges_http_api_v2()),
    ?assertMatch({ok, {{_, 200, _}, _, [#{}, #{}]}}, list_connectors_http()),
    ?assertMatch({ok, {{_, 200, _}, _, #{<<"name">> := NameA}}}, get_bridge_http_api_v1(NameA)),
    ?assertMatch({ok, {{_, 200, _}, _, #{<<"name">> := NameA}}}, get_bridge_http_api_v2(NameA)),
    ?assertMatch(
        {ok, {{_, 200, _}, _, #{<<"name">> := GeneratedConnName}}},
        get_connector_http(GeneratedConnName)
    ),
    ?assertMatch({ok, {{_, 204, _}, _, _}}, disable_bridge_http_api_v1(NameA)),
    ?assertMatch({ok, {{_, 204, _}, _, _}}, enable_bridge_http_api_v1(NameA)),
    ?assertMatch({ok, {{_, 204, _}, _, _}}, disable_bridge_http_api_v2(NameA)),
    ?assertMatch({ok, {{_, 204, _}, _, _}}, enable_bridge_http_api_v2(NameA)),
    ?assertMatch({ok, {{_, 204, _}, _, _}}, bridge_operation_http_api_v1(NameA, stop)),
    ?assertMatch({ok, {{_, 204, _}, _, _}}, bridge_operation_http_api_v1(NameA, start)),
    ?assertMatch({ok, {{_, 204, _}, _, _}}, bridge_operation_http_api_v1(NameA, restart)),
    %% TODO: currently, only `start' op is supported by the v2 API.
    %% ?assertMatch({ok, {{_, 204, _}, _, _}}, bridge_operation_http_api_v2(NameA, stop)),
    ?assertMatch({ok, {{_, 204, _}, _, _}}, bridge_operation_http_api_v2(NameA, start)),
    %% TODO: currently, only `start' op is supported by the v2 API.
    %% ?assertMatch({ok, {{_, 204, _}, _, _}}, bridge_operation_http_api_v2(NameA, restart)),

    %% ===================================================================================
    %% Delete the last bridge using API v1.  The generated connector should also be
    %% removed.
    %% ===================================================================================
    ?assertMatch(
        {ok, {{_, 204, _}, _, _}},
        delete_bridge_http_api_v1(#{name => NameA})
    ),
    ?assertMatch({ok, {{_, 200, _}, _, []}}, list_bridges_http_api_v1()),
    ?assertMatch({ok, {{_, 200, _}, _, []}}, list_bridges_http_api_v2()),
    %% only the pre-existing one should remain.
    ?assertMatch(
        {ok, {{_, 200, _}, _, [#{<<"name">> := PreexistentConnectorName}]}},
        list_connectors_http()
    ),
    ?assertMatch(
        {ok, {{_, 200, _}, _, #{<<"name">> := PreexistentConnectorName}}},
        get_connector_http(PreexistentConnectorName)
    ),
    ?assertMatch({error, {{_, 404, _}, _, _}}, get_bridge_http_api_v1(NameA)),
    ?assertMatch({error, {{_, 404, _}, _, _}}, get_bridge_http_api_v2(NameA)),
    ?assertMatch({error, {{_, 404, _}, _, _}}, get_connector_http(GeneratedConnName)),
    ?assertMatch({error, {{_, 404, _}, _, _}}, disable_bridge_http_api_v1(NameA)),
    ?assertMatch({error, {{_, 404, _}, _, _}}, enable_bridge_http_api_v1(NameA)),
    ?assertMatch({error, {{_, 404, _}, _, _}}, disable_bridge_http_api_v2(NameA)),
    ?assertMatch({error, {{_, 404, _}, _, _}}, enable_bridge_http_api_v2(NameA)),
    ?assertMatch({error, {{_, 404, _}, _, _}}, bridge_operation_http_api_v1(NameA, stop)),
    ?assertMatch({error, {{_, 404, _}, _, _}}, bridge_operation_http_api_v1(NameA, start)),
    ?assertMatch({error, {{_, 404, _}, _, _}}, bridge_operation_http_api_v1(NameA, restart)),
    %% TODO: currently, only `start' op is supported by the v2 API.
    %% ?assertMatch({error, {{_, 404, _}, _, _}}, bridge_operation_http_api_v2(NameA, stop)),
    ?assertMatch({error, {{_, 404, _}, _, _}}, bridge_operation_http_api_v2(NameA, start)),
    %% TODO: currently, only `start' op is supported by the v2 API.
    %% ?assertMatch({error, {{_, 404, _}, _, _}}, bridge_operation_http_api_v2(NameA, restart)),

    ok.

t_scenario_2(Config) ->
    %% ===================================================================================
    %% Pre-conditions
    %% ===================================================================================
    ?assertMatch({ok, {{_, 200, _}, _, []}}, list_bridges_http_api_v1()),
    ?assertMatch({ok, {{_, 200, _}, _, []}}, list_bridges_http_api_v2()),
    %% created in the test case init
    ?assertMatch({ok, {{_, 200, _}, _, [#{}]}}, list_connectors_http()),
    {ok, {{_, 200, _}, _, [#{<<"name">> := _PreexistentConnectorName}]}} = list_connectors_http(),

    %% ===================================================================================
    %% Try to create a rule referencing a non-existent bridge.  It succeeds, but it's
    %% implicitly disabled.  Trying to update it later without creating the bridge should
    %% allow it to be enabled.
    %% ===================================================================================
    BridgeName = <<"scenario2">>,
    RuleTopic = <<"t/scenario2">>,
    {ok, #{<<"id">> := RuleId0}} =
        emqx_bridge_v2_testlib:create_rule_and_action_http(
            bridge_type(),
            RuleTopic,
            [
                {bridge_name, BridgeName}
                | Config
            ],
            #{overrides => #{enable => true}}
        ),
    ?assert(is_rule_enabled(RuleId0)),
    ?assertMatch({ok, {{_, 200, _}, _, _}}, enable_rule_http(RuleId0)),
    ?assert(is_rule_enabled(RuleId0)),

    %% ===================================================================================
    %% Now we create the bridge, and attempt to create a new enabled rule.  It should
    %% start enabled.  Also, updating the previous rule to enable it should work now.
    %% ===================================================================================
    ?assertMatch(
        {ok, {{_, 201, _}, _, #{}}},
        create_bridge_http_api_v1(#{name => BridgeName})
    ),
    {ok, #{<<"id">> := RuleId1}} =
        emqx_bridge_v2_testlib:create_rule_and_action_http(
            bridge_type(),
            RuleTopic,
            [
                {bridge_name, BridgeName}
                | Config
            ],
            #{overrides => #{enable => true}}
        ),
    ?assert(is_rule_enabled(RuleId0)),
    ?assert(is_rule_enabled(RuleId1)),
    ?assertMatch({ok, {{_, 200, _}, _, _}}, enable_rule_http(RuleId0)),
    ?assert(is_rule_enabled(RuleId0)),

    %% ===================================================================================
    %% Creating a rule with mixed existent/non-existent bridges should allow enabling it.
    %% ===================================================================================
    NonExistentBridgeName = <<"scenario2_not_created">>,
    {ok, #{<<"id">> := RuleId2}} =
        emqx_bridge_v2_testlib:create_rule_and_action_http(
            bridge_type(),
            RuleTopic,
            [
                {bridge_name, BridgeName}
                | Config
            ],
            #{
                overrides => #{
                    enable => true,
                    actions => [
                        emqx_bridge_resource:bridge_id(
                            bridge_type(),
                            BridgeName
                        ),
                        emqx_bridge_resource:bridge_id(
                            bridge_type(),
                            NonExistentBridgeName
                        )
                    ]
                }
            }
        ),
    ?assert(is_rule_enabled(RuleId2)),
    ?assertMatch({ok, {{_, 200, _}, _, _}}, enable_rule_http(RuleId2)),
    ?assert(is_rule_enabled(RuleId2)),

    ok.

t_create_with_bad_name(_Config) ->
    BadBridgeName = <<"test_哈哈"/utf8>>,
    %% Note: must contain SSL options to trigger bug.
    Cacertfile = emqx_common_test_helpers:app_path(
        emqx,
        filename:join(["etc", "certs", "cacert.pem"])
    ),
    Opts = #{
        name => BadBridgeName,
        overrides => #{
            <<"ssl">> =>
                #{<<"cacertfile">> => Cacertfile}
        }
    },
    {error,
        {{_, 400, _}, _, #{
            <<"code">> := <<"BAD_REQUEST">>,
            <<"message">> := #{
                <<"kind">> := <<"validation_error">>,
                <<"reason">> := <<"invalid_map_key">>
            }
        }}} = create_bridge_http_api_v1(Opts),
    ok.

t_obfuscated_secrets_probe(_Config) ->
    Name = <<"bridgev2">>,
    Me = self(),
    ets:new(emqx_bridge_v2_SUITE:fun_table_name(), [named_table, public]),
    OnStartFun = emqx_bridge_v2_SUITE:wrap_fun(fun(Conf) ->
        Me ! {on_start, Conf},
        {ok, Conf}
    end),
    OriginalPassword = <<"supersecret">>,
    Overrides = #{<<"password">> => OriginalPassword, <<"on_start_fun">> => OnStartFun},
    %% Using the real password, like when creating the bridge for the first time.
    ?assertMatch(
        {ok, {{_, 204, _}, _, _}},
        probe_bridge_http_api_v1(#{name => Name, overrides => Overrides})
    ),

    %% Check that we still can probe created bridges that use passwords.
    ?assertMatch(
        {ok, {{_, 201, _}, _, #{}}},
        create_bridge_http_api_v1(#{name => Name, overrides => Overrides})
    ),
    %% Password is obfuscated
    ?assertMatch(
        {ok, {{_, 200, _}, _, #{<<"password">> := <<"******">>}}},
        get_bridge_http_api_v1(Name)
    ),
    %% still using the password
    ?assertMatch(
        {ok, {{_, 204, _}, _, _}},
        probe_bridge_http_api_v1(#{name => Name, overrides => Overrides})
    ),
    %% now with obfuscated password (loading the UI again)
    ?assertMatch(
        {ok, {{_, 204, _}, _, _}},
        probe_bridge_http_api_v1(#{
            name => Name,
            overrides => Overrides#{<<"password">> => <<"******">>}
        })
    ),
    ?assertMatch(
        {ok, {{_, 204, _}, _, _}},
        probe_action_http_api_v2(#{
            name => Name,
            overrides => Overrides#{<<"password">> => <<"******">>}
        })
    ),

    %% We have to check that the connector was started with real passwords during dry runs
    StartConfs = [Conf || {on_start, Conf} <- ?drainMailbox()],
    Passwords = lists:map(fun(#{password := P}) -> P end, StartConfs),
    ?assert(lists:all(fun is_function/1, Passwords), #{passwords => Passwords}),
    UnwrappedPasswords = [F() || F <- Passwords],
    ?assertEqual(
        [OriginalPassword],
        lists:usort(UnwrappedPasswords),
        #{passwords => UnwrappedPasswords}
    ),

    ok.

t_v1_api_fill_defaults(_Config) ->
    %% Ensure only one sub-field is used, but we get back the defaults filled in.
    BridgeName = ?FUNCTION_NAME,
    OverrideFn = fun(Params) ->
        ResourceOpts = #{<<"resume_interval">> => 100},
        maps:put(<<"resource_opts">>, ResourceOpts, Params)
    end,
    ?assertMatch(
        {ok,
            {{_, 201, _}, _, #{
                <<"resource_opts">> :=
                    #{
                        <<"resume_interval">> := _,
                        <<"query_mode">> := _,
                        <<"inflight_window">> := _,
                        <<"start_timeout">> := _,
                        <<"start_after_created">> := _,
                        <<"max_buffer_bytes">> := _,
                        <<"batch_size">> := _
                    }
            }}},
        create_bridge_http_api_v1(#{name => BridgeName, override_fn => OverrideFn})
    ),

    ok.

t_upgrade_raw_conf_with_deprecated_files(Config) ->
    %% This verifies that, when a user has a deprecated file such as
    %% `cluster-override.conf' in their data directory and upgrades to a newer EMQX
    %% version, its bridge contents are also upgraded.
    ?check_trace(
        begin
            [Node] = ?config(nodes, Config),

            SchemaMod = emqx_conf_schema,
            erpc:call(Node, fun() ->
                DataDir = emqx:data_dir(),
                Path = filename:join([DataDir, "configs", "cluster-override.conf"]),
                ok = filelib:ensure_dir(Path),
                ok = file:write_file(Path, <<
                    "node.cookie = cookie \n",
                    "node.data_dir = \"/tmp/not/used/here\" \n",
                    (deprecated_config())/binary
                >>),
                %% Attempt to emulate loading the config when starting the node.  The key
                %% is starting `emqx_bridge' so that bridges are loaded.
                ok = application:stop(emqx_bridge),
                ok = application:stop(emqx_connector),
                ok = emqx_config:init_load(SchemaMod),
                ok = application:start(emqx_connector),
                ok = application:start(emqx_bridge),

                ?assertMatch(
                    {ok, {{_, 200, _}, _, [_]}},
                    list_bridges_http_api_v1(_Host = "http://127.0.0.1:18084")
                ),

                ok
            end),

            ok
        end,
        []
    ),

    ok.
