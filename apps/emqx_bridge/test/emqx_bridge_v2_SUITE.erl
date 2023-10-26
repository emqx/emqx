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

connector_resource_id() ->
    emqx_connector_resource:resource_id(con_type(), con_name()).

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
    #{
        <<"enable">> => true,
        <<"resource_opts">> => #{
            %% Set this to a low value to make the test run faster
            <<"health_check_interval">> => 100
        }
    }.

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
        <<"connector">> => atom_to_binary(con_name()),
        <<"enable">> => true,
        <<"send_to">> => registered_process_name(),
        <<"resource_opts">> => #{
            <<"resume_interval">> => 100
        }
    }.

fun_table_name() ->
    emqx_bridge_v2_SUITE_fun_table.

registered_process_name() ->
    my_registered_process.

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
    meck:expect(emqx_bridge_v2, bridge_v1_type_to_bridge_v2_type, 1, bridge_type()),

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
    ets:new(fun_table_name(), [named_table, public]),
    %% Create a fake connector
    {ok, _} = emqx_connector:create(con_type(), con_name(), con_config()),
    Config.

end_per_testcase(_TestCase, Config) ->
    ets:delete(fun_table_name()),
    %% Remove the fake connector
    {ok, _} = emqx_connector:remove(con_type(), con_name()),
    Config.

%% Hocon does not support placing a fun in a config map so we replace it with a string

wrap_fun(Fun) ->
    UniqRef = make_ref(),
    UniqRefBin = term_to_binary(UniqRef),
    UniqRefStr = iolist_to_binary(base64:encode(UniqRefBin)),
    ets:insert(fun_table_name(), {UniqRefStr, Fun}),
    UniqRefStr.

unwrap_fun(UniqRefStr) ->
    ets:lookup_element(fun_table_name(), UniqRefStr, 2).

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
    OnAddChannel1 = wrap_fun(fun() ->
        {error, Msg}
    end),
    Conf1 = (bridge_config())#{on_add_channel_fun => OnAddChannel1},
    {error, Msg} = emqx_bridge_v2:create_dry_run(bridge_type(), Conf1),
    OnAddChannel2 = wrap_fun(fun() ->
        throw(Msg)
    end),
    Conf2 = (bridge_config())#{on_add_channel_fun => OnAddChannel2},
    {error, Msg} = emqx_bridge_v2:create_dry_run(bridge_type(), Conf2),
    ok.

t_create_dry_run_fail_get_channel_status(_) ->
    Msg = <<"Failed to add channel">>,
    Fun1 = wrap_fun(fun() ->
        {error, Msg}
    end),
    Conf1 = (bridge_config())#{on_get_channel_status_fun => Fun1},
    {error, Msg} = emqx_bridge_v2:create_dry_run(bridge_type(), Conf1),
    Fun2 = wrap_fun(fun() ->
        throw(Msg)
    end),
    Conf2 = (bridge_config())#{on_get_channel_status_fun => Fun2},
    {error, _} = emqx_bridge_v2:create_dry_run(bridge_type(), Conf2),
    ok.

t_create_dry_run_connector_does_not_exist(_) ->
    BridgeConf = (bridge_config())#{<<"connector">> => <<"connector_does_not_exist">>},
    {error, _} = emqx_bridge_v2:create_dry_run(bridge_type(), BridgeConf).

t_is_valid_bridge_v1(_) ->
    {ok, _} = emqx_bridge_v2:create(bridge_type(), my_test_bridge, bridge_config()),
    true = emqx_bridge_v2:is_valid_bridge_v1(bridge_v1_type, my_test_bridge),
    %% Add another channel/bridge to the connector
    {ok, _} = emqx_bridge_v2:create(bridge_type(), my_test_bridge_2, bridge_config()),
    false = emqx_bridge_v2:is_valid_bridge_v1(bridge_v1_type, my_test_bridge),
    {ok, _} = emqx_bridge_v2:remove(bridge_type(), my_test_bridge),
    true = emqx_bridge_v2:is_valid_bridge_v1(bridge_v1_type, my_test_bridge_2),
    {ok, _} = emqx_bridge_v2:remove(bridge_type(), my_test_bridge_2),
    false = emqx_bridge_v2:is_valid_bridge_v1(bridge_v1_type, my_test_bridge),
    ok.

t_manual_health_check(_) ->
    {ok, _} = emqx_bridge_v2:create(bridge_type(), my_test_bridge, bridge_config()),
    %% Run a health check for the bridge
    connected = emqx_bridge_v2:health_check(bridge_type(), my_test_bridge),
    {ok, _} = emqx_bridge_v2:remove(bridge_type(), my_test_bridge),
    ok.

t_manual_health_check_exception(_) ->
    Conf = (bridge_config())#{
        <<"on_get_channel_status_fun">> => wrap_fun(fun() -> throw(my_error) end)
    },
    {ok, _} = emqx_bridge_v2:create(bridge_type(), my_test_bridge, Conf),
    %% Run a health check for the bridge
    {error, _} = emqx_bridge_v2:health_check(bridge_type(), my_test_bridge),
    {ok, _} = emqx_bridge_v2:remove(bridge_type(), my_test_bridge),
    ok.

t_manual_health_check_exception_error(_) ->
    Conf = (bridge_config())#{
        <<"on_get_channel_status_fun">> => wrap_fun(fun() -> error(my_error) end)
    },
    {ok, _} = emqx_bridge_v2:create(bridge_type(), my_test_bridge, Conf),
    %% Run a health check for the bridge
    {error, _} = emqx_bridge_v2:health_check(bridge_type(), my_test_bridge),
    {ok, _} = emqx_bridge_v2:remove(bridge_type(), my_test_bridge),
    ok.

t_manual_health_check_error(_) ->
    Conf = (bridge_config())#{
        <<"on_get_channel_status_fun">> => wrap_fun(fun() -> {error, my_error} end)
    },
    {ok, _} = emqx_bridge_v2:create(bridge_type(), my_test_bridge, Conf),
    %% Run a health check for the bridge
    {error, my_error} = emqx_bridge_v2:health_check(bridge_type(), my_test_bridge),
    {ok, _} = emqx_bridge_v2:remove(bridge_type(), my_test_bridge),
    ok.

t_send_message(_) ->
    {ok, _} = emqx_bridge_v2:create(bridge_type(), my_test_bridge, bridge_config()),
    %% Register name for this process
    register(registered_process_name(), self()),
    _ = emqx_bridge_v2:send_message(bridge_type(), my_test_bridge, <<"my_msg">>, #{}),
    receive
        <<"my_msg">> ->
            ok
    after 10000 ->
        ct:fail("Failed to receive message")
    end,
    unregister(registered_process_name()),
    {ok, _} = emqx_bridge_v2:remove(bridge_type(), my_test_bridge),
    ok.

t_send_message_unhealthy_channel(_) ->
    OnGetStatusResponseETS = ets:new(on_get_status_response_ets, [public]),
    ets:insert(OnGetStatusResponseETS, {status_value, {error, my_error}}),
    OnGetStatusFun = wrap_fun(fun() ->
        ets:lookup_element(OnGetStatusResponseETS, status_value, 2)
    end),
    Conf = (bridge_config())#{<<"on_get_channel_status_fun">> => OnGetStatusFun},
    {ok, _} = emqx_bridge_v2:create(bridge_type(), my_test_bridge, Conf),
    %% Register name for this process
    register(registered_process_name(), self()),
    _ = emqx_bridge_v2:send_message(bridge_type(), my_test_bridge, <<"my_msg">>, #{timeout => 1}),
    receive
        Any ->
            ct:pal("Received message: ~p", [Any]),
            ct:fail("Should not get message here")
    after 1 ->
        ok
    end,
    %% Sending should work again after the channel is healthy
    ets:insert(OnGetStatusResponseETS, {status_value, connected}),
    _ = emqx_bridge_v2:send_message(
        bridge_type(),
        my_test_bridge,
        <<"my_msg">>,
        #{}
    ),
    receive
        <<"my_msg">> ->
            ok
    after 10000 ->
        ct:fail("Failed to receive message")
    end,
    unregister(registered_process_name()),
    {ok, _} = emqx_bridge_v2:remove(bridge_type(), my_test_bridge),
    ok.

t_send_message_unhealthy_connector(_) ->
    ResponseETS = ets:new(response_ets, [public]),
    ets:insert(ResponseETS, {on_start_value, conf}),
    ets:insert(ResponseETS, {on_get_status_value, connecting}),
    OnStartFun = wrap_fun(fun(Conf) ->
        case ets:lookup_element(ResponseETS, on_start_value, 2) of
            conf ->
                {ok, Conf};
            V ->
                V
        end
    end),
    OnGetStatusFun = wrap_fun(fun() ->
        ets:lookup_element(ResponseETS, on_get_status_value, 2)
    end),
    ConConfig = emqx_utils_maps:deep_merge(con_config(), #{
        <<"on_start_fun">> => OnStartFun,
        <<"on_get_status_fun">> => OnGetStatusFun,
        <<"resource_opts">> => #{<<"start_timeout">> => 100}
    }),
    ConName = ?FUNCTION_NAME,
    {ok, _} = emqx_connector:create(con_type(), ConName, ConConfig),
    BridgeConf = (bridge_config())#{
        <<"connector">> => atom_to_binary(ConName)
    },
    {ok, _} = emqx_bridge_v2:create(bridge_type(), my_test_bridge, BridgeConf),
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %% Test that sending does not work when the connector is unhealthy (connecting)
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    register(registered_process_name(), self()),
    _ = emqx_bridge_v2:send_message(bridge_type(), my_test_bridge, <<"my_msg">>, #{timeout => 100}),
    receive
        Any ->
            ct:pal("Received message: ~p", [Any]),
            ct:fail("Should not get message here")
    after 10 ->
        ok
    end,
    %% We should have one alarm
    1 = get_bridge_v2_alarm_cnt(),
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %% Test that sending works again when the connector is healthy (connected)
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    ets:insert(ResponseETS, {on_get_status_value, connected}),

    _ = emqx_bridge_v2:send_message(bridge_type(), my_test_bridge, <<"my_msg">>, #{timeout => 1000}),
    receive
        <<"my_msg">> ->
            ok
    after 1000 ->
        ct:fail("Failed to receive message")
    end,
    %% The alarm should be gone at this point
    0 = get_bridge_v2_alarm_cnt(),
    unregister(registered_process_name()),
    {ok, _} = emqx_bridge_v2:remove(bridge_type(), my_test_bridge),
    {ok, _} = emqx_connector:remove(con_type(), ConName),
    ets:delete(ResponseETS),
    ok.

t_unhealthy_channel_alarm(_) ->
    Conf = (bridge_config())#{
        <<"on_get_channel_status_fun">> =>
            wrap_fun(fun() -> {error, my_error} end)
    },
    0 = get_bridge_v2_alarm_cnt(),
    {ok, _} = emqx_bridge_v2:create(bridge_type(), my_test_bridge, Conf),
    1 = get_bridge_v2_alarm_cnt(),
    {ok, _} = emqx_bridge_v2:remove(bridge_type(), my_test_bridge),
    0 = get_bridge_v2_alarm_cnt(),
    ok.

get_bridge_v2_alarm_cnt() ->
    Alarms = emqx_alarm:get_alarms(activated),
    FilterFun = fun
        (#{name := S}) when is_binary(S) -> string:find(S, "bridge_v2") =/= nomatch;
        (_) -> false
    end,
    length(lists:filter(FilterFun, Alarms)).
