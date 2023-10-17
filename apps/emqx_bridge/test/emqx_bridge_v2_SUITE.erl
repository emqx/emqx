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

-module(emqx_bridge_v2_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

con_mod() ->
    emqx_bridge_v2_test_connector.

con_type() ->
    test_connector_type.

con_name() ->
    my_connector.

bridge_type() ->
    test_bridge_type.

con_schema() ->
    [
        {
            con_type(),
            hoconsc:mk(
                hoconsc:map(name, typerefl:map()),
                #{
                    desc => <<"Test Connector Config">>,
                    required => false
                }
            )
        }
    ].

con_config() ->
    #{}.

bridge_schema() ->
    [
        {
            bridge_type(),
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
        <<"connector">> => con_name()
    }.

all() ->
    emqx_common_test_helpers:all(?MODULE).

start_apps() -> [emqx, emqx_conf, emqx_connector, emqx_bridge].

init_per_suite(Config) ->
    %% Setting up mocks for fake connector and bridge V2
    meck:new(emqx_connector_schema, [passthrough, no_link]),
    meck:expect(emqx_connector_schema, fields, 1, con_schema()),

    meck:new(emqx_connector_resource, [passthrough, no_link]),
    meck:expect(emqx_connector_resource, connector_to_resource_type, 1, con_mod()),

    meck:new(emqx_bridge_v2_schema, [passthrough, no_link]),
    meck:expect(emqx_bridge_v2_schema, fields, 1, bridge_schema()),

    meck:new(emqx_bridge_v2, [passthrough, no_link]),
    meck:expect(emqx_bridge_v2, bridge_v2_type_to_connector_type, 1, con_type()),

    _ = application:load(emqx_conf),
    ok = emqx_common_test_helpers:start_apps(start_apps()),
    [
        {mocked_mods, [
            emqx_connector_schema,
            emqx_connector_resource,
            emqx_bridge_v2_schema,
            emqx_bridge_v2
        ]}
        | Config
    ].

end_per_suite(Config) ->
    MockedMods = proplists:get_value(mocked_mods, Config),
    meck:unload(MockedMods),
    emqx_common_test_helpers:stop_apps(start_apps()).

init_per_testcase(_TestCase, Config) ->
    %% Create a fake connector
    {ok, _} = emqx_connector:create(con_type(), con_name(), con_config()),
    Config.

end_per_testcase(_TestCase, Config) ->
    %% Remove the fake connector
    {ok, _} = emqx_connector:remove(con_type(), con_name()),
    Config.

t_create_remove(_) ->
    {ok, _} = emqx_bridge_v2:create(bridge_type(), my_test_bridge, bridge_config()),
    {ok, _} = emqx_bridge_v2:remove(bridge_type(), my_test_bridge),
    ok.

t_list(_) ->
    [] = emqx_bridge_v2:list(),
    {ok, _} = emqx_bridge_v2:create(bridge_type(), my_test_bridge, bridge_config()),
    1 = length(emqx_bridge_v2:list()),
    {ok, _} = emqx_bridge_v2:create(bridge_type(), my_test_bridge2, bridge_config()),
    2 = length(emqx_bridge_v2:list()),
    {ok, _} = emqx_bridge_v2:remove(bridge_type(), my_test_bridge),
    1 = length(emqx_bridge_v2:list()),
    {ok, _} = emqx_bridge_v2:remove(bridge_type(), my_test_bridge2),
    0 = length(emqx_bridge_v2:list()),
    ok.

t_create_dry_run(_) ->
    ok = emqx_bridge_v2:create_dry_run(bridge_type(), bridge_config()).

t_create_dry_run_fail_add_channel(_) ->
    Msg = <<"Failed to add channel">>,
    OnAddChannel1 = fun() ->
        {error, Msg}
    end,
    Conf1 = (bridge_config())#{on_add_channel_fun => OnAddChannel1},
    {error, Msg} = emqx_bridge_v2:create_dry_run(bridge_type(), Conf1),
    OnAddChannel2 = fun() ->
        throw(Msg)
    end,
    Conf2 = (bridge_config())#{on_add_channel_fun => OnAddChannel2},
    {error, Msg} = emqx_bridge_v2:create_dry_run(bridge_type(), Conf2),
    ok.

t_create_dry_run_fail_get_channel_status(_) ->
    Msg = <<"Failed to add channel">>,
    Fun1 = fun() ->
        {error, Msg}
    end,
    Conf1 = (bridge_config())#{on_get_channel_status_fun => Fun1},
    {error, Msg} = emqx_bridge_v2:create_dry_run(bridge_type(), Conf1),
    Fun2 = fun() ->
        throw(Msg)
    end,
    Conf2 = (bridge_config())#{on_get_channel_status_fun => Fun2},
    {error, _} = emqx_bridge_v2:create_dry_run(bridge_type(), Conf2),
    ok.

t_create_dry_run_connector_does_not_exist(_) ->
    BridgeConf = (bridge_config())#{<<"connector">> => <<"connector_does_not_exist">>},
    {error, _} = emqx_bridge_v2:create_dry_run(bridge_type(), BridgeConf).
