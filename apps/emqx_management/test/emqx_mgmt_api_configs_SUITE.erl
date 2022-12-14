%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_mgmt_api_configs_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_mgmt_api_test_util:init_suite([emqx_conf]),
    Config.

end_per_suite(_) ->
    emqx_mgmt_api_test_util:end_suite([emqx_conf]).

init_per_testcase(TestCase = t_configs_node, Config) ->
    ?MODULE:TestCase({'init', Config});
init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(TestCase = t_configs_node, Config) ->
    ?MODULE:TestCase({'end', Config});
end_per_testcase(_TestCase, Config) ->
    Config.

t_get(_Config) ->
    {ok, Configs} = get_configs(),
    maps:map(
        fun(Name, Value) ->
            {ok, Config} = get_config(Name),
            ?assertEqual(Value, Config)
        end,
        maps:remove(<<"license">>, Configs)
    ),
    ok.

t_update(_Config) ->
    %% update ok
    {ok, SysMon} = get_config(<<"sysmon">>),
    #{<<"vm">> := #{<<"busy_port">> := BusyPort}} = SysMon,
    NewSysMon = emqx_map_lib:deep_put([<<"vm">>, <<"busy_port">>], SysMon, not BusyPort),
    {ok, #{}} = update_config(<<"sysmon">>, NewSysMon),
    {ok, SysMon1} = get_config(<<"sysmon">>),
    #{<<"vm">> := #{<<"busy_port">> := BusyPort1}} = SysMon1,
    ?assertEqual(BusyPort, not BusyPort1),
    assert_busy_port(BusyPort1),

    %% update failed
    ErrorSysMon = emqx_map_lib:deep_put([<<"vm">>, <<"busy_port">>], SysMon, "123"),
    ?assertMatch(
        {error, {"HTTP/1.1", 400, _}},
        update_config(<<"sysmon">>, ErrorSysMon)
    ),
    {ok, SysMon2} = get_config(<<"sysmon">>),
    ?assertEqual(SysMon1, SysMon2),

    %% reset specific config
    ok = reset_config(<<"sysmon">>, "conf_path=vm.busy_port"),
    {ok, SysMon3} = get_config(<<"sysmon">>),
    ?assertMatch(#{<<"vm">> := #{<<"busy_port">> := true}}, SysMon3),
    assert_busy_port(true),

    %% reset no_default_value config
    NewSysMon1 = emqx_map_lib:deep_put([<<"vm">>, <<"busy_port">>], SysMon, false),
    {ok, #{}} = update_config(<<"sysmon">>, NewSysMon1),
    ?assertMatch({error, {"HTTP/1.1", 400, _}}, reset_config(<<"sysmon">>, "")),
    {ok, SysMon4} = get_config(<<"sysmon">>),
    ?assertMatch(#{<<"vm">> := #{<<"busy_port">> := false}}, SysMon4),
    ok.

assert_busy_port(BusyPort) ->
    {_Pid, Monitors} = erlang:system_monitor(),
    RealBusyPort = proplists:get_value(busy_port, Monitors, false),
    ?assertEqual(BusyPort, RealBusyPort).

t_log(_Config) ->
    {ok, Log} = get_config("log"),
    File = "log/emqx-test.log",
    %% update handler
    Log1 = emqx_map_lib:deep_put([<<"file_handlers">>, <<"default">>, <<"enable">>], Log, true),
    Log2 = emqx_map_lib:deep_put([<<"file_handlers">>, <<"default">>, <<"file">>], Log1, File),
    {ok, #{}} = update_config(<<"log">>, Log2),
    {ok, Log3} = logger:get_handler_config(default),
    ?assertMatch(#{config := #{file := File}}, Log3),
    ErrLog1 = emqx_map_lib:deep_put([<<"file_handlers">>, <<"default">>, <<"enable">>], Log, 1),
    ?assertMatch({error, {"HTTP/1.1", 400, _}}, update_config(<<"log">>, ErrLog1)),
    ErrLog2 = emqx_map_lib:deep_put([<<"file_handlers">>, <<"default">>, <<"enabfe">>], Log, true),
    ?assertMatch({error, {"HTTP/1.1", 400, _}}, update_config(<<"log">>, ErrLog2)),

    %% add new handler
    File1 = "log/emqx-test1.log",
    Handler = emqx_map_lib:deep_get([<<"file_handlers">>, <<"default">>], Log2),
    NewLog1 = emqx_map_lib:deep_put([<<"file_handlers">>, <<"new">>], Log2, Handler),
    NewLog2 = emqx_map_lib:deep_put([<<"file_handlers">>, <<"new">>, <<"file">>], NewLog1, File1),
    {ok, #{}} = update_config(<<"log">>, NewLog2),
    {ok, Log4} = logger:get_handler_config(new),
    ?assertMatch(#{config := #{file := File1}}, Log4),

    %% disable new handler
    Disable = emqx_map_lib:deep_put([<<"file_handlers">>, <<"new">>, <<"enable">>], NewLog2, false),
    {ok, #{}} = update_config(<<"log">>, Disable),
    ?assertEqual({error, {not_found, new}}, logger:get_handler_config(new)),
    ok.

t_global_zone(_Config) ->
    {ok, Zones} = get_global_zone(),
    ZonesKeys = lists:map(fun({K, _}) -> K end, hocon_schema:roots(emqx_zone_schema)),
    ?assertEqual(lists:usort(ZonesKeys), lists:usort(maps:keys(Zones))),
    ?assertEqual(
        emqx_config:get_zone_conf(no_default, [mqtt, max_qos_allowed]),
        emqx_map_lib:deep_get([<<"mqtt">>, <<"max_qos_allowed">>], Zones)
    ),
    NewZones = emqx_map_lib:deep_put([<<"mqtt">>, <<"max_qos_allowed">>], Zones, 1),
    {ok, #{}} = update_global_zone(NewZones),
    ?assertEqual(1, emqx_config:get_zone_conf(no_default, [mqtt, max_qos_allowed])),

    BadZones = emqx_map_lib:deep_put([<<"mqtt">>, <<"max_qos_allowed">>], Zones, 3),
    ?assertMatch({error, {"HTTP/1.1", 400, _}}, update_global_zone(BadZones)),

    %% Remove max_qos_allowed from raw config, but we still get default value(2).
    Mqtt0 = emqx_conf:get_raw([<<"mqtt">>]),
    ?assertEqual(1, emqx_map_lib:deep_get([<<"max_qos_allowed">>], Mqtt0)),
    Mqtt1 = maps:remove(<<"max_qos_allowed">>, Mqtt0),
    ok = emqx_config:put_raw([<<"mqtt">>], Mqtt1),
    Mqtt2 = emqx_conf:get_raw([<<"mqtt">>]),
    ?assertNot(maps:is_key(<<"max_qos_allowed">>, Mqtt2), Mqtt2),
    {ok, #{<<"mqtt">> := Mqtt3}} = get_global_zone(),
    %% the default value is 2
    ?assertEqual(2, emqx_map_lib:deep_get([<<"max_qos_allowed">>], Mqtt3)),
    ok = emqx_config:put_raw([<<"mqtt">>], Mqtt0),
    ok.

get_global_zone() ->
    get_config("global_zone").

update_global_zone(Change) ->
    update_config("global_zone", Change).

t_zones(_Config) ->
    {ok, Zones} = get_config("zones"),
    {ok, #{<<"mqtt">> := OldMqtt} = Zone1} = get_global_zone(),
    {ok, #{}} = update_config("zones", Zones#{<<"new_zone">> => Zone1}),
    NewMqtt = emqx_config:get_raw([zones, new_zone, mqtt]),
    ?assertEqual(OldMqtt, NewMqtt),
    %% delete the new zones
    {ok, #{}} = update_config("zones", Zones),
    ?assertEqual(undefined, emqx_config:get_raw([new_zone, mqtt], undefined)),
    ok.

t_dashboard(_Config) ->
    {ok, Dashboard = #{<<"listeners">> := Listeners}} = get_config("dashboard"),
    Https1 = #{enable => true, bind => 18084},
    ?assertMatch(
        {error, {"HTTP/1.1", 400, _}},
        update_config("dashboard", Dashboard#{<<"https">> => Https1})
    ),

    Https2 = #{
        enable => true,
        bind => 18084,
        keyfile => "etc/certs/badkey.pem",
        cacertfile => "etc/certs/badcacert.pem",
        certfile => "etc/certs/badcert.pem"
    },
    Dashboard2 = Dashboard#{listeners => Listeners#{https => Https2}},
    ?assertMatch(
        {error, {"HTTP/1.1", 400, _}},
        update_config("dashboard", Dashboard2)
    ),

    Keyfile = emqx_common_test_helpers:app_path(emqx, filename:join(["etc", "certs", "key.pem"])),
    Certfile = emqx_common_test_helpers:app_path(emqx, filename:join(["etc", "certs", "cert.pem"])),
    Cacertfile = emqx_common_test_helpers:app_path(
        emqx, filename:join(["etc", "certs", "cacert.pem"])
    ),
    Https3 = #{
        enable => true,
        bind => 18084,
        keyfile => Keyfile,
        cacertfile => Cacertfile,
        certfile => Certfile
    },
    Dashboard3 = Dashboard#{listeners => Listeners#{https => Https3}},
    ?assertMatch({ok, _}, update_config("dashboard", Dashboard3)),

    Dashboard4 = Dashboard#{listeners => Listeners#{https => #{enable => false}}},
    ?assertMatch({ok, _}, update_config("dashboard", Dashboard4)),

    ?assertMatch({ok, _}, update_config("dashboard", Dashboard)),

    {ok, Dashboard1} = get_config("dashboard"),
    ?assertNotEqual(Dashboard, Dashboard1),
    timer:sleep(1000),
    ok.

t_configs_node({'init', Config}) ->
    Node = node(),
    meck:expect(mria_mnesia, running_nodes, fun() -> [Node, bad_node, other_node] end),
    meck:expect(
        emqx_management_proto_v2,
        get_full_config,
        fun
            (Node0) when Node0 =:= Node -> <<"\"self\"">>;
            (other_node) -> <<"\"other\"">>;
            (bad_node) -> {badrpc, bad}
        end
    ),
    Config;
t_configs_node({'end', _}) ->
    meck:unload([mria_mnesia, emqx_management_proto_v2]);
t_configs_node(_) ->
    Node = atom_to_list(node()),

    ?assertEqual({ok, <<"self">>}, get_configs(Node, #{return_all => true})),
    ?assertEqual({ok, <<"other">>}, get_configs("other_node", #{return_all => true})),

    {ExpType, ExpRes} = get_configs("unknown_node", #{return_all => true}),
    ?assertEqual(error, ExpType),
    ?assertMatch({{_, 404, _}, _, _}, ExpRes),
    {_, _, Body} = ExpRes,
    ?assertMatch(#{<<"code">> := <<"NOT_FOUND">>}, emqx_json:decode(Body, [return_maps])),

    ?assertMatch({error, {_, 500, _}}, get_configs("bad_node")).

%% Helpers

get_config(Name) ->
    Path = emqx_mgmt_api_test_util:api_path(["configs", Name]),
    case emqx_mgmt_api_test_util:request_api(get, Path) of
        {ok, Res} ->
            {ok, emqx_json:decode(Res, [return_maps])};
        Error ->
            Error
    end.

get_configs() ->
    get_configs([], #{}).

get_configs(Node) ->
    get_configs(Node, #{}).

get_configs(Node, Opts) ->
    Path =
        case Node of
            [] -> ["configs"];
            _ -> ["configs?node=" ++ Node]
        end,
    URI = emqx_mgmt_api_test_util:api_path(Path),
    case emqx_mgmt_api_test_util:request_api(get, URI, [], [], [], Opts) of
        {ok, {_, _, Res}} -> {ok, emqx_json:decode(Res, [return_maps])};
        {ok, Res} -> {ok, emqx_json:decode(Res, [return_maps])};
        Error -> Error
    end.

update_config(Name, Change) ->
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    UpdatePath = emqx_mgmt_api_test_util:api_path(["configs", Name]),
    case emqx_mgmt_api_test_util:request_api(put, UpdatePath, "", AuthHeader, Change) of
        {ok, Update} -> {ok, emqx_json:decode(Update, [return_maps])};
        Error -> Error
    end.

reset_config(Name, Key) ->
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    Path = binary_to_list(
        iolist_to_binary(
            emqx_mgmt_api_test_util:api_path(["configs_reset", Name])
        )
    ),
    case emqx_mgmt_api_test_util:request_api(post, Path, Key, AuthHeader, []) of
        {ok, []} -> ok;
        Error -> Error
    end.
