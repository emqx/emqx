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
    emqx_mgmt_api_test_util:init_suite(),
    Config.

end_per_suite(_) ->
    emqx_mgmt_api_test_util:end_suite().

t_get(_Config) ->
    {ok, Configs} = get_configs(),
    maps:map(fun(Name, Value) ->
        {ok, Config} = get_config(Name),
        ?assertEqual(Value, Config)
    end, maps:remove(<<"license">>, Configs)),
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
    ?assertMatch({error, {"HTTP/1.1", 400, _}},
        update_config(<<"sysmon">>, ErrorSysMon)),
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

t_zones(_Config) ->
    {ok, Zones} = get_zones(),
    ZonesKeys = lists:map(fun({K, _}) -> K end, hocon_schema:roots(emqx_zone_schema)),
    ?assertEqual(lists:usort(ZonesKeys), lists:usort(maps:keys(Zones))),
    ?assertEqual(emqx_config:get_zone_conf(no_default, [mqtt, max_qos_allowed]),
        emqx_map_lib:deep_get([<<"mqtt">>, <<"max_qos_allowed">>], Zones)),
    NewZones = emqx_map_lib:deep_put([<<"mqtt">>, <<"max_qos_allowed">>], Zones, 1),
    {ok, #{}} = update_zones(NewZones),
    ?assertEqual(1, emqx_config:get_zone_conf(no_default, [mqtt, max_qos_allowed])),

    BadZones = emqx_map_lib:deep_put([<<"mqtt">>, <<"max_qos_allowed">>], Zones, 3),
    ?assertMatch({error, {"HTTP/1.1", 400, _}}, update_zones(BadZones)),
    ok.

get_zones() ->
    Path = emqx_mgmt_api_test_util:api_path(["configs", "global_zone"]),
    case emqx_mgmt_api_test_util:request_api(get, Path) of
        {ok, Res} -> {ok, emqx_json:decode(Res, [return_maps])};
        Error -> Error
    end.

update_zones(Change) ->
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    UpdatePath = emqx_mgmt_api_test_util:api_path(["configs", "global_zone"]),
    case emqx_mgmt_api_test_util:request_api(put, UpdatePath, "", AuthHeader, Change) of
        {ok, Update} -> {ok, emqx_json:decode(Update, [return_maps])};
        Error -> Error
    end.

get_config(Name) ->
    Path = emqx_mgmt_api_test_util:api_path(["configs", Name]),
    case emqx_mgmt_api_test_util:request_api(get, Path) of
        {ok, Res} ->
            {ok, emqx_json:decode(Res, [return_maps])};
        Error -> Error
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
    Path = binary_to_list(iolist_to_binary(
        emqx_mgmt_api_test_util:api_path(["configs_reset", Name]))),
    case emqx_mgmt_api_test_util:request_api(post, Path, Key, AuthHeader, []) of
        {ok, []} -> ok;
        Error -> Error
    end.
