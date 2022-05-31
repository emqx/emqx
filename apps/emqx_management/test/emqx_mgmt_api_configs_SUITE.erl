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

    %% reset no_default_value config
    NewSysMon1 = emqx_map_lib:deep_put([<<"vm">>, <<"busy_port">>], SysMon, false),
    {ok, #{}} = update_config(<<"sysmon">>, NewSysMon1),
    ?assertMatch({error, {"HTTP/1.1", 400, _}}, reset_config(<<"sysmon">>, "")),
    {ok, SysMon4} = get_config(<<"sysmon">>),
    ?assertMatch(#{<<"vm">> := #{<<"busy_port">> := false}}, SysMon4),
    ok.

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

get_config(Name) ->
    Path = emqx_mgmt_api_test_util:api_path(["configs", Name]),
    case emqx_mgmt_api_test_util:request_api(get, Path) of
        {ok, Res} ->
            {ok, emqx_json:decode(Res, [return_maps])};
        Error ->
            Error
    end.

get_configs() ->
    Path = emqx_mgmt_api_test_util:api_path(["configs"]),
    case emqx_mgmt_api_test_util:request_api(get, Path) of
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
