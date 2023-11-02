%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    emqx_mgmt_api_test_util:init_suite(),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_mgmt_api_test_util:end_suite(),
    emqx_cth_suite:stop(Apps),
    ok.

app_specs() ->
    [
        emqx,
        emqx_conf,
        emqx_connector,
        emqx_bridge,
        emqx_rule_engine
    ].

init_per_testcase(_TestCase, Config) ->
    %% Setting up mocks for fake connector and bridge V2
    setup_mocks(),
    ets:new(fun_table_name(), [named_table, public]),
    %% Create a fake connector
    {ok, _} = emqx_connector:create(con_type(), con_name(), con_config()),
    [
        {mocked_mods, [
            emqx_connector_schema,
            emqx_connector_resource,

            emqx_bridge_v2
        ]}
        | Config
    ].

end_per_testcase(_TestCase, _Config) ->
    ets:delete(fun_table_name()),
    delete_all_bridges_and_connectors(),
    meck:unload(),
    emqx_common_test_helpers:call_janitor(),
    ok.

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
        enterprise_api_schemas,
        1,
        fun(Method) -> [{bridge_type_bin(), hoconsc:ref(?MODULE, "api_" ++ Method)}] end
    ),

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
        {resource_opts, hoconsc:mk(map(), #{})}
    ];
fields("api_post") ->
    [
        {connector, hoconsc:mk(binary(), #{})},
        {name, hoconsc:mk(binary(), #{})},
        {type, hoconsc:mk(bridge_type(), #{})},
        {send_to, hoconsc:mk(atom(), #{})}
        | fields("connector")
    ].

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
    emqx_conf:update([bridges_v2], RootConf, #{override_to => cluster}).

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
    lists:foreach(fun emqx_resource:remove/1, emqx_resource:list_instances()),
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
    Path = emqx_mgmt_api_test_util:api_path(["bridges"]),
    ct:pal("list bridges (http v1)"),
    Res = request(get, Path, _Params = []),
    ct:pal("list bridges (http v1) result:\n  ~p", [Res]),
    Res.

list_bridges_http_api_v2() ->
    Path = emqx_mgmt_api_test_util:api_path(["bridges_v2"]),
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
    Path = emqx_mgmt_api_test_util:api_path(["bridges_v2", BridgeId]),
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
    BridgeConfig0 = emqx_utils_maps:deep_merge(bridge_config(), Overrides),
    BridgeConfig = maps:without([<<"connector">>], BridgeConfig0),
    Params = BridgeConfig#{<<"type">> => bridge_type_bin(), <<"name">> => Name},
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
    Path = emqx_mgmt_api_test_util:api_path(["bridges_v2"]),
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
    Path = emqx_mgmt_api_test_util:api_path(["bridges_v2", BridgeId]),
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
    Path = emqx_mgmt_api_test_util:api_path(["bridges_v2", BridgeId, "enable", "true"]),
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
    Path = emqx_mgmt_api_test_util:api_path(["bridges_v2", BridgeId, "enable", "false"]),
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
    Path = emqx_mgmt_api_test_util:api_path(["bridges_v2", BridgeId, Op]),
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
    Path = emqx_mgmt_api_test_util:api_path(["nodes", Node, "bridges_v2", BridgeId, Op]),
    ct:pal("bridge node op ~p (http v2)", [{Node, Op}]),
    Res = request(post, Path, _Params = []),
    ct:pal("bridge node op ~p (http v2) result:\n  ~p", [{Node, Op}, Res]),
    Res.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_name_too_long(_Config) ->
    LongName = list_to_binary(lists:duplicate(256, $a)),
    ?assertMatch(
        {error,
            {{_, 400, _}, _, #{<<"message">> := #{<<"reason">> := <<"Name is too long", _/binary>>}}}},
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
