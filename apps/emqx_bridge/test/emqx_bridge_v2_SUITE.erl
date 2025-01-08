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

-module(emqx_bridge_v2_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

con_mod() ->
    emqx_bridge_v2_test_connector.

con_type() ->
    bridge_type().

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
                hoconsc:map(name, hoconsc:ref(?MODULE, connector_config)),
                #{
                    desc => <<"Test Connector Config">>,
                    required => false
                }
            )
        }
    ].

fields(connector_config) ->
    [
        {enable, hoconsc:mk(typerefl:boolean(), #{})},
        {resource_opts, hoconsc:mk(typerefl:map(), #{})},
        {on_start_fun, hoconsc:mk(typerefl:binary(), #{})},
        {on_get_status_fun, hoconsc:mk(typerefl:binary(), #{})},
        {on_add_channel_fun, hoconsc:mk(typerefl:binary(), #{})}
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
    emqx_bridge_v2_SUITE_fun_table.

registered_process_name() ->
    my_registered_process.

all() ->
    emqx_common_test_helpers:all(?MODULE).

start_apps() ->
    [
        emqx,
        emqx_conf,
        emqx_connector,
        emqx_bridge,
        emqx_rule_engine
    ].

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
    BridgeType = bridge_type(),
    BridgeTypeBin = atom_to_binary(BridgeType),
    meck:expect(
        emqx_bridge_v2,
        bridge_v2_type_to_connector_type,
        fun(Type) when Type =:= BridgeType; Type =:= BridgeTypeBin -> con_type() end
    ),
    meck:expect(emqx_bridge_v2, bridge_v1_type_to_bridge_v2_type, 1, bridge_type()),

    meck:expect(emqx_bridge_v2, is_bridge_v2_type, fun(Type) -> Type =:= BridgeType end),
    ok.

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        app_specs(),
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
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
        emqx_rule_engine
    ].

init_per_testcase(_TestCase, Config) ->
    %% Setting up mocks for fake connector and bridge V2
    setup_mocks(),
    ets:new(fun_table_name(), [named_table, public]),
    %% Create a fake connector
    {ok, _} = emqx_connector:create(con_type(), con_name(), con_config()),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ets:delete(fun_table_name()),
    delete_all_bridges_and_connectors(),
    meck:unload(),
    emqx_common_test_helpers:call_janitor(),
    ok.

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

update_root_connectors_config(RootConf) ->
    emqx_conf:update([connectors], RootConf, #{override_to => cluster}).

t_create_remove(_) ->
    {ok, _} = emqx_bridge_v2:create(bridge_type(), my_test_bridge, bridge_config()),
    ok = emqx_bridge_v2:remove(bridge_type(), my_test_bridge),
    ok.

t_create_disabled_bridge(_) ->
    Config = #{<<"connector">> := Connector} = bridge_config(),
    Disable = Config#{<<"enable">> => false},
    BridgeType = bridge_type(),
    {ok, _} = emqx_bridge_v2:create(BridgeType, my_enable_bridge, Config),
    {ok, _} = emqx_bridge_v2:create(BridgeType, my_disable_bridge, Disable),
    ConnectorId = emqx_connector_resource:resource_id(con_type(), Connector),
    ?assertMatch(
        [
            {_, #{
                enable := true,
                connector := Connector,
                bridge_type := _
            }},
            {_, #{
                enable := false,
                connector := Connector,
                bridge_type := _
            }}
        ],
        emqx_bridge_v2:get_channels_for_connector(ConnectorId)
    ),
    ok = emqx_bridge_v2:remove(bridge_type(), my_enable_bridge),
    ok = emqx_bridge_v2:remove(bridge_type(), my_disable_bridge),
    ok.

t_list(_) ->
    [] = emqx_bridge_v2:list(),
    {ok, _} = emqx_bridge_v2:create(bridge_type(), my_test_bridge, bridge_config()),
    1 = length(emqx_bridge_v2:list()),
    {ok, _} = emqx_bridge_v2:create(bridge_type(), my_test_bridge2, bridge_config()),
    2 = length(emqx_bridge_v2:list()),
    ok = emqx_bridge_v2:remove(bridge_type(), my_test_bridge),
    1 = length(emqx_bridge_v2:list()),
    ok = emqx_bridge_v2:remove(bridge_type(), my_test_bridge2),
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
    {error, _} = emqx_bridge_v2:create_dry_run(bridge_type(), Conf1),
    OnAddChannel2 = wrap_fun(fun() ->
        throw(Msg)
    end),
    Conf2 = (bridge_config())#{on_add_channel_fun => OnAddChannel2},
    {error, _} = emqx_bridge_v2:create_dry_run(bridge_type(), Conf2),
    ok.

t_create_dry_run_fail_get_channel_status(_) ->
    Msg = <<"Failed to add channel">>,
    Fun1 = wrap_fun(fun() ->
        {error, Msg}
    end),
    Conf1 = (bridge_config())#{on_get_channel_status_fun => Fun1},
    {error, _} = emqx_bridge_v2:create_dry_run(bridge_type(), Conf1),
    Fun2 = wrap_fun(fun() ->
        throw(Msg)
    end),
    Conf2 = (bridge_config())#{on_get_channel_status_fun => Fun2},
    {error, _} = emqx_bridge_v2:create_dry_run(bridge_type(), Conf2),
    ok.

t_create_dry_run_connector_does_not_exist(_) ->
    BridgeConf = (bridge_config())#{<<"connector">> => <<"connector_does_not_exist">>},
    {error, _} = emqx_bridge_v2:create_dry_run(bridge_type(), BridgeConf).

t_bridge_v1_is_valid(_) ->
    {ok, _} = emqx_bridge_v2:create(bridge_type(), my_test_bridge, bridge_config()),
    true = emqx_bridge_v2:bridge_v1_is_valid(bridge_v1_type, my_test_bridge),
    %% Add another channel/bridge to the connector
    {ok, _} = emqx_bridge_v2:create(bridge_type(), my_test_bridge_2, bridge_config()),
    false = emqx_bridge_v2:bridge_v1_is_valid(bridge_v1_type, my_test_bridge),
    ok = emqx_bridge_v2:remove(bridge_type(), my_test_bridge),
    true = emqx_bridge_v2:bridge_v1_is_valid(bridge_v1_type, my_test_bridge_2),
    ok = emqx_bridge_v2:remove(bridge_type(), my_test_bridge_2),
    %% Non existing bridge is a valid Bridge V1
    true = emqx_bridge_v2:bridge_v1_is_valid(bridge_v1_type, my_test_bridge),
    ok.

t_manual_health_check(_) ->
    {ok, _} = emqx_bridge_v2:create(bridge_type(), my_test_bridge, bridge_config()),
    %% Run a health check for the bridge
    #{error := undefined, status := connected} = emqx_bridge_v2:health_check(
        bridge_type(), my_test_bridge
    ),
    ok = emqx_bridge_v2:remove(bridge_type(), my_test_bridge),
    ok.

t_manual_health_check_exception(_) ->
    Conf = (bridge_config())#{
        <<"on_get_channel_status_fun">> => wrap_fun(fun() -> throw(my_error) end)
    },
    {ok, _} = emqx_bridge_v2:create(bridge_type(), my_test_bridge, Conf),
    %% Run a health check for the bridge
    #{error := my_error, status := disconnected} = emqx_bridge_v2:health_check(
        bridge_type(), my_test_bridge
    ),
    ok = emqx_bridge_v2:remove(bridge_type(), my_test_bridge),
    ok.

t_manual_health_check_exception_error(_) ->
    Conf = (bridge_config())#{
        <<"on_get_channel_status_fun">> => wrap_fun(fun() -> error(my_error) end)
    },
    {ok, _} = emqx_bridge_v2:create(bridge_type(), my_test_bridge, Conf),
    %% Run a health check for the bridge
    #{error := _, status := disconnected} = emqx_bridge_v2:health_check(
        bridge_type(), my_test_bridge
    ),
    ok = emqx_bridge_v2:remove(bridge_type(), my_test_bridge),
    ok.

t_manual_health_check_error(_) ->
    Conf = (bridge_config())#{
        <<"on_get_channel_status_fun">> => wrap_fun(fun() -> {error, my_error} end)
    },
    {ok, _} = emqx_bridge_v2:create(bridge_type(), my_test_bridge, Conf),
    %% Run a health check for the bridge
    #{error := my_error, status := disconnected} = emqx_bridge_v2:health_check(
        bridge_type(), my_test_bridge
    ),
    ok = emqx_bridge_v2:remove(bridge_type(), my_test_bridge),
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
    ok = emqx_bridge_v2:remove(bridge_type(), my_test_bridge).

t_send_message_through_rule(_) ->
    BridgeName = my_test_bridge,
    BridgeType = bridge_type(),
    BridgeConfig0 =
        emqx_utils_maps:deep_merge(
            bridge_config(),
            #{<<"resource_opts">> => #{<<"query_mode">> => <<"async">>}}
        ),
    {ok, _} = emqx_bridge_v2:create(BridgeType, BridgeName, BridgeConfig0),
    %% Create a rule to send message to the bridge
    {ok, #{id := RuleId}} = emqx_rule_engine:create_rule(
        #{
            sql => <<"select * from \"t/a\"">>,
            id => atom_to_binary(?FUNCTION_NAME),
            actions => [
                <<
                    (atom_to_binary(bridge_type()))/binary,
                    ":",
                    (atom_to_binary(BridgeName))/binary
                >>
            ],
            description => <<"bridge_v2 test rule">>
        }
    ),
    on_exit(fun() -> emqx_rule_engine:delete_rule(RuleId) end),
    %% Register name for this process
    register(registered_process_name(), self()),
    %% Send message to the topic
    ClientId = atom_to_binary(?FUNCTION_NAME),
    Payload = <<"hello">>,
    Msg = emqx_message:make(ClientId, 0, <<"t/a">>, Payload),
    emqx:publish(Msg),
    receive
        #{payload := Payload} ->
            ok
    after 10000 ->
        ct:fail("Failed to receive message")
    end,
    ?assertMatch(
        #{
            counters :=
                #{
                    'matched' := 1,
                    'failed' := 0,
                    'passed' := 1,
                    'actions.success' := 1,
                    'actions.failed' := 0,
                    'actions.failed.unknown' := 0,
                    'actions.failed.out_of_service' := 0
                }
        },
        get_rule_metrics(RuleId)
    ),
    %% Now, turn off connector.  Should increase `actions.out_of_service' metric.
    {ok, _} = emqx_connector:create(con_type(), con_name(), (con_config())#{<<"enable">> := false}),
    emqx:publish(Msg),
    ?retry(
        100,
        10,
        ?assertMatch(
            #{
                counters :=
                    #{
                        'matched' := 2,
                        'failed' := 0,
                        'passed' := 2,
                        'actions.success' := 1,
                        'actions.failed' := 1,
                        'actions.failed.unknown' := 0,
                        'actions.failed.out_of_service' := 1,
                        'actions.discarded' := 0
                    }
            },
            get_rule_metrics(RuleId)
        )
    ),
    %% Sync query
    BridgeConfig1 =
        emqx_utils_maps:deep_merge(
            bridge_config(),
            #{<<"resource_opts">> => #{<<"query_mode">> => <<"sync">>}}
        ),
    {ok, _} = emqx_bridge_v2:create(BridgeType, BridgeName, BridgeConfig1),
    emqx:publish(Msg),
    ?retry(
        100,
        10,
        ?assertMatch(
            #{
                counters :=
                    #{
                        'matched' := 3,
                        'failed' := 0,
                        'passed' := 3,
                        'actions.success' := 1,
                        'actions.failed' := 2,
                        'actions.failed.unknown' := 0,
                        'actions.failed.out_of_service' := 2,
                        'actions.discarded' := 0
                    }
            },
            get_rule_metrics(RuleId)
        )
    ),
    unregister(registered_process_name()),
    ok = emqx_bridge_v2:remove(bridge_type(), BridgeName),
    ok.

t_send_message_through_local_topic(_) ->
    %% Bridge configuration with local topic
    BridgeName = my_test_bridge,
    TopicName = <<"t/b">>,
    BridgeConfig = (bridge_config())#{
        <<"local_topic">> => TopicName
    },
    {ok, _} = emqx_bridge_v2:create(bridge_type(), BridgeName, BridgeConfig),
    %% Register name for this process
    register(registered_process_name(), self()),
    %% Send message to the topic
    ClientId = atom_to_binary(?FUNCTION_NAME),
    Payload = <<"hej">>,
    Msg = emqx_message:make(ClientId, 0, TopicName, Payload),
    emqx:publish(Msg),
    receive
        #{payload := Payload} ->
            ok
    after 10000 ->
        ct:fail("Failed to receive message")
    end,
    unregister(registered_process_name()),
    ok = emqx_bridge_v2:remove(bridge_type(), BridgeName),
    ok.

t_send_message_unhealthy_channel(_) ->
    OnGetStatusResponseETS = ets:new(on_get_status_response_ets, [public]),
    ets:insert(OnGetStatusResponseETS, {status_value, {error, my_error}}),
    OnGetStatusFun = wrap_fun(fun() ->
        ets:lookup_element(OnGetStatusResponseETS, status_value, 2)
    end),
    Name = my_test_bridge,
    Conf = (bridge_config())#{<<"on_get_channel_status_fun">> => OnGetStatusFun},
    {ok, _} = emqx_bridge_v2:create(bridge_type(), Name, Conf),
    %% Register name for this process
    register(registered_process_name(), self()),
    _ = emqx_bridge_v2:send_message(bridge_type(), Name, <<"my_msg">>, #{timeout => 1}),
    receive
        Any ->
            ct:pal("Received message: ~p", [Any]),
            ct:fail("Should not get message here")
    after 1 ->
        ok
    end,
    %% Sending should work again after the channel is healthy
    ets:insert(OnGetStatusResponseETS, {status_value, connected}),
    ?retry(
        200,
        5,
        ?assertMatch(
            #{status := connected},
            emqx_bridge_v2:health_check(bridge_type(), Name)
        )
    ),
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
    ok = emqx_bridge_v2:remove(bridge_type(), my_test_bridge).

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
    ok = emqx_bridge_v2:remove(bridge_type(), my_test_bridge),
    ok = emqx_connector:remove(con_type(), ConName),
    ets:delete(ResponseETS),
    ok.

t_connector_connected_to_connecting_to_connected_no_channel_restart(_) ->
    ResponseETS = ets:new(response_ets, [public]),
    ets:insert(ResponseETS, {on_start_value, conf}),
    ets:insert(ResponseETS, {on_get_status_value, connected}),
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
    OnAddChannelCntr = counters:new(1, []),
    OnAddChannelFun = wrap_fun(fun(_InstId, ConnectorState, _ChannelId, _ChannelConfig) ->
        counters:add(OnAddChannelCntr, 1, 1),
        {ok, ConnectorState}
    end),
    ConConfig = emqx_utils_maps:deep_merge(con_config(), #{
        <<"on_start_fun">> => OnStartFun,
        <<"on_get_status_fun">> => OnGetStatusFun,
        <<"on_add_channel_fun">> => OnAddChannelFun,
        <<"resource_opts">> => #{<<"start_timeout">> => 100}
    }),
    ConName = ?FUNCTION_NAME,
    {ok, _} = emqx_connector:create(con_type(), ConName, ConConfig),
    BridgeConf = (bridge_config())#{
        <<"connector">> => atom_to_binary(ConName)
    },
    {ok, _} = emqx_bridge_v2:create(bridge_type(), my_test_bridge, BridgeConf),
    %% Wait until on_add_channel_fun is called at least once
    wait_until(fun() ->
        counters:get(OnAddChannelCntr, 1) =:= 1
    end),
    1 = counters:get(OnAddChannelCntr, 1),
    %% We change the status of the connector
    ets:insert(ResponseETS, {on_get_status_value, connecting}),
    %% Wait until the status is changed
    wait_until(fun() ->
        {ok, BridgeData} = emqx_bridge_v2:lookup(bridge_type(), my_test_bridge),
        maps:get(status, BridgeData) =:= connecting
    end),
    {ok, BridgeData1} = emqx_bridge_v2:lookup(bridge_type(), my_test_bridge),
    ct:pal("Bridge V2 status changed to: ~p", [maps:get(status, BridgeData1)]),
    %% We change the status again back to connected
    ets:insert(ResponseETS, {on_get_status_value, connected}),
    %% Wait until the status is connected again
    wait_until(fun() ->
        {ok, BridgeData2} = emqx_bridge_v2:lookup(bridge_type(), my_test_bridge),
        maps:get(status, BridgeData2) =:= connected
    end),
    %% On add channel should not have been called again
    1 = counters:get(OnAddChannelCntr, 1),
    %% We change the status to an error
    ets:insert(ResponseETS, {on_get_status_value, {error, my_error}}),
    %% Wait until the status is changed
    wait_until(fun() ->
        {ok, BridgeData2} = emqx_bridge_v2:lookup(bridge_type(), my_test_bridge),
        maps:get(status, BridgeData2) =:= disconnected
    end),
    %% Now we go back to connected
    ets:insert(ResponseETS, {on_get_status_value, connected}),
    wait_until(fun() ->
        {ok, BridgeData2} = emqx_bridge_v2:lookup(bridge_type(), my_test_bridge),
        maps:get(status, BridgeData2) =:= connected
    end),
    %% Now the channel should have been removed and added again
    wait_until(fun() ->
        counters:get(OnAddChannelCntr, 1) =:= 2
    end),
    ok = emqx_bridge_v2:remove(bridge_type(), my_test_bridge),
    ok = emqx_connector:remove(con_type(), ConName),
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
    ok = emqx_bridge_v2:remove(bridge_type(), my_test_bridge),
    0 = get_bridge_v2_alarm_cnt(),
    ok.

get_bridge_v2_alarm_cnt() ->
    Alarms = emqx_alarm:get_alarms(activated),
    FilterFun = fun
        (#{name := S}) when is_binary(S) -> string:find(S, "action") =/= nomatch;
        (_) -> false
    end,
    length(lists:filter(FilterFun, Alarms)).

t_load_no_matching_connector(_Config) ->
    Conf = bridge_config(),
    BridgeTypeBin = atom_to_binary(bridge_type()),
    BridgeNameBin0 = <<"my_test_bridge_update">>,
    ?assertMatch({ok, _}, emqx_bridge_v2:create(bridge_type(), BridgeNameBin0, Conf)),

    %% updating to invalid reference
    RootConf0 = #{
        BridgeTypeBin =>
            #{BridgeNameBin0 => Conf#{<<"connector">> := <<"unknown">>}}
    },
    ?assertMatch(
        {error,
            {post_config_update, _HandlerMod, [
                #{
                    errors := [
                        {
                            {_, my_test_bridge_update},
                            {error, #{
                                bridge_name := my_test_bridge_update,
                                connector_name := <<"unknown">>,
                                bridge_type := _,
                                reason := <<"connector_not_found_or_wrong_type">>
                            }}
                        }
                    ],
                    action := update
                }
            ]}},
        update_root_config(RootConf0)
    ),

    %% creating new with invalid reference
    BridgeNameBin1 = <<"my_test_bridge_new">>,
    RootConf1 = #{
        BridgeTypeBin =>
            #{BridgeNameBin1 => Conf#{<<"connector">> := <<"unknown">>}}
    },
    ?assertMatch(
        {error,
            {post_config_update, _HandlerMod, [
                #{
                    errors := [
                        {
                            {_, my_test_bridge_new},
                            {error, #{
                                bridge_name := my_test_bridge_new,
                                connector_name := <<"unknown">>,
                                bridge_type := _,
                                reason := <<"connector_not_found_or_wrong_type">>
                            }}
                        }
                    ],
                    action := create
                }
            ]}},
        update_root_config(RootConf1)
    ),

    ok.

%% tests root config handler post config update hook
t_load_config_success(_Config) ->
    Conf = bridge_config(),
    BridgeType = bridge_type(),
    BridgeTypeBin = atom_to_binary(BridgeType),
    BridgeName = my_test_bridge_root,
    BridgeNameBin = atom_to_binary(BridgeName),

    %% pre-condition
    ?assertEqual(#{}, emqx_config:get([actions])),

    %% create
    RootConf0 = #{BridgeTypeBin => #{BridgeNameBin => Conf}},
    ?assertMatch(
        {ok, _},
        update_root_config(RootConf0)
    ),
    BridgeTypeBin = bin(BridgeType),
    BridgeNameBin = bin(BridgeName),
    ?assertMatch(
        {ok, #{
            type := BridgeTypeBin,
            name := BridgeNameBin,
            raw_config := #{},
            resource_data := #{}
        }},
        emqx_bridge_v2:lookup(BridgeType, BridgeName)
    ),

    %% update
    RootConf1 = #{BridgeTypeBin => #{BridgeNameBin => Conf#{<<"some_key">> => <<"new_value">>}}},
    ?assertMatch(
        {ok, _},
        update_root_config(RootConf1)
    ),
    ?assertMatch(
        {ok, #{
            type := BridgeTypeBin,
            name := BridgeNameBin,
            raw_config := #{<<"some_key">> := <<"new_value">>},
            resource_data := #{}
        }},
        emqx_bridge_v2:lookup(BridgeType, BridgeName)
    ),

    %% delete
    RootConf2 = #{},
    ?assertMatch(
        {ok, _},
        update_root_config(RootConf2)
    ),
    ?assertMatch(
        {error, not_found},
        emqx_bridge_v2:lookup(BridgeType, BridgeName)
    ),

    ok.

t_create_no_matching_connector(_Config) ->
    Conf = (bridge_config())#{<<"connector">> => <<"wrong_connector_name">>},
    ?assertMatch(
        {error,
            {pre_config_update, _HandlerMod, #{
                bridge_name := _,
                connector_name := _,
                bridge_type := _,
                reason := <<"connector_not_found_or_wrong_type">>
            }}},
        emqx_bridge_v2:create(bridge_type(), my_test_bridge, Conf)
    ),
    ok.

t_create_wrong_connector_type(_Config) ->
    meck:expect(
        emqx_bridge_v2_schema,
        fields,
        1,
        bridge_schema(#{bridge_type => wrong_type})
    ),
    Conf = bridge_config(),
    ?assertMatch(
        {error,
            {pre_config_update, _HandlerMod, #{
                bridge_name := _,
                connector_name := _,
                bridge_type := wrong_type,
                reason := <<"connector_not_found_or_wrong_type">>
            }}},
        emqx_bridge_v2:create(wrong_type, my_test_bridge, Conf)
    ),
    ok.

t_update_connector_not_found(_Config) ->
    Conf = bridge_config(),
    ?assertMatch({ok, _}, emqx_bridge_v2:create(bridge_type(), my_test_bridge, Conf)),
    BadConf = Conf#{<<"connector">> => <<"wrong_connector_name">>},
    ?assertMatch(
        {error,
            {pre_config_update, _HandlerMod, #{
                bridge_name := _,
                connector_name := _,
                bridge_type := _,
                reason := <<"connector_not_found_or_wrong_type">>
            }}},
        emqx_bridge_v2:create(bridge_type(), my_test_bridge, BadConf)
    ),
    ok.

%% Check that https://emqx.atlassian.net/browse/EMQX-12376 is fixed
t_update_concurrent_health_check(_Config) ->
    Msg = <<"Channel status check failed">>,
    ok = meck:expect(
        emqx_bridge_v2_test_connector,
        on_get_channel_status,
        fun(
            _ResId,
            ChannelId,
            #{channels := Channels}
        ) ->
            #{
                is_conf_for_connected := Connected
            } = maps:get(ChannelId, Channels),
            case Connected of
                true ->
                    connected;
                false ->
                    {error, Msg}
            end
        end
    ),
    BaseConf = (bridge_config())#{
        is_conf_for_connected => false
    },
    ?assertMatch({ok, _}, emqx_bridge_v2:create(bridge_type(), my_test_bridge, BaseConf)),
    SetStatusConnected =
        fun
            (true) ->
                Conf = BaseConf#{is_conf_for_connected => true},
                %% Update the config
                ?assertMatch({ok, _}, emqx_bridge_v2:create(bridge_type(), my_test_bridge, Conf)),
                ?assertMatch(
                    #{status := connected},
                    emqx_bridge_v2:health_check(bridge_type(), my_test_bridge)
                );
            (false) ->
                Conf = BaseConf#{is_conf_for_connected => false},
                %% Update the config
                ?assertMatch({ok, _}, emqx_bridge_v2:create(bridge_type(), my_test_bridge, Conf)),
                ?assertMatch(
                    #{status := disconnected},
                    emqx_bridge_v2:health_check(bridge_type(), my_test_bridge)
                )
        end,
    [
        begin
            Connected = (N rem 2) =:= 0,
            SetStatusConnected(Connected)
        end
     || N <- lists:seq(0, 20)
    ],
    ok.

t_remove_single_connector_being_referenced_with_active_channels(_Config) ->
    %% we test the connector post config update here because we also need bridges.
    Conf = bridge_config(),
    ?assertMatch({ok, _}, emqx_bridge_v2:create(bridge_type(), my_test_bridge, Conf)),
    ?assertMatch(
        {error, {post_config_update, _HandlerMod, {active_channels, [_ | _]}}},
        emqx_connector:remove(con_type(), con_name())
    ),
    ok.

t_remove_single_connector_being_referenced_without_active_channels(_Config) ->
    %% we test the connector post config update here because we also need bridges.
    Conf = bridge_config(),
    BridgeName = my_test_bridge,
    ?assertMatch({ok, _}, emqx_bridge_v2:create(bridge_type(), BridgeName, Conf)),
    emqx_common_test_helpers:with_mock(
        emqx_bridge_v2_test_connector,
        on_get_channels,
        fun(_ResId) -> [] end,
        fun() ->
            ?assertMatch(ok, emqx_connector:remove(con_type(), con_name())),
            %% we no longer have connector data if this happens...
            ?assertMatch(
                {ok, #{resource_data := #{}}},
                emqx_bridge_v2:lookup(bridge_type(), BridgeName)
            ),
            ok
        end
    ),
    ok.

t_remove_multiple_connectors_being_referenced_with_channels(_Config) ->
    Conf = bridge_config(),
    BridgeName = my_test_bridge,
    ?assertMatch({ok, _}, emqx_bridge_v2:create(bridge_type(), BridgeName, Conf)),
    ?assertMatch(
        {error,
            {post_config_update, _HandlerMod, #{
                reason := "connector_has_active_channels",
                type := _,
                connector_name := _,
                active_channels := [_ | _]
            }}},
        update_root_connectors_config(#{})
    ),
    ok.

t_remove_multiple_connectors_being_referenced_without_channels(_Config) ->
    Conf = bridge_config(),
    BridgeName = my_test_bridge,
    ?assertMatch({ok, _}, emqx_bridge_v2:create(bridge_type(), BridgeName, Conf)),
    emqx_common_test_helpers:with_mock(
        emqx_bridge_v2_test_connector,
        on_get_channels,
        fun(_ResId) -> [] end,
        fun() ->
            ?assertMatch(
                {ok, _},
                update_root_connectors_config(#{})
            ),
            %% we no longer have connector data if this happens...
            ?assertMatch(
                {ok, #{resource_data := #{}}},
                emqx_bridge_v2:lookup(bridge_type(), BridgeName)
            ),
            ok
        end
    ),
    ok.

t_start_operation_when_on_add_channel_gives_error(_Config) ->
    Conf = bridge_config(),
    BridgeName = my_test_bridge,
    emqx_common_test_helpers:with_mock(
        emqx_bridge_v2_test_connector,
        on_add_channel,
        fun(_, _, _ResId, _Channel) -> {error, <<"some_error">>} end,
        fun() ->
            %% We can crete the bridge event though on_add_channel returns error
            ?assertMatch({ok, _}, emqx_bridge_v2:create(bridge_type(), BridgeName, Conf)),
            ?assertMatch(
                #{
                    status := disconnected,
                    error := <<"some_error">>
                },
                emqx_bridge_v2:health_check(bridge_type(), BridgeName)
            ),
            ?assertMatch(
                {ok, #{
                    status := disconnected,
                    error := <<"some_error">>
                }},
                emqx_bridge_v2:lookup(bridge_type(), BridgeName)
            ),
            %% emqx_bridge_v2:start/2 should return ok if bridge if connected after
            %% start and otherwise and error
            ?assertMatch({error, _}, emqx_bridge_v2:start(bridge_type(), BridgeName)),
            %% Let us change on_add_channel to be successful and try again
            ok = meck:expect(
                emqx_bridge_v2_test_connector,
                on_add_channel,
                fun(_, _, _ResId, _Channel) -> {ok, #{}} end
            ),
            ?assertMatch(ok, emqx_bridge_v2:start(bridge_type(), BridgeName))
        end
    ),
    ok.

t_lookup_status_when_connecting(_Config) ->
    ResponseETS = ets:new(response_ets, [public]),
    ets:insert(ResponseETS, {on_get_status_value, ?status_connecting}),
    OnGetStatusFun = wrap_fun(fun() ->
        ets:lookup_element(ResponseETS, on_get_status_value, 2)
    end),

    ConnectorConfig = emqx_utils_maps:deep_merge(con_config(), #{
        <<"on_get_status_fun">> => OnGetStatusFun,
        <<"resource_opts">> => #{<<"start_timeout">> => 100}
    }),
    ConnectorName = ?FUNCTION_NAME,
    ct:pal("connector config:\n  ~p", [ConnectorConfig]),
    {ok, _} = emqx_connector:create(con_type(), ConnectorName, ConnectorConfig),

    ActionName = my_test_action,
    ChanStatusFun = wrap_fun(fun() -> ?status_disconnected end),
    ActionConfig = (bridge_config())#{
        <<"on_get_channel_status_fun">> => ChanStatusFun,
        <<"connector">> => atom_to_binary(ConnectorName)
    },
    ct:pal("action config:\n  ~p", [ActionConfig]),
    {ok, _} = emqx_bridge_v2:create(bridge_type(), ActionName, ActionConfig),

    %% Top-level status is connecting if the connector status is connecting, but the
    %% channel is not yet installed.  `resource_data.added_channels.$channel_id.status'
    %% contains true internal status.
    {ok, Res} = emqx_bridge_v2:lookup(bridge_type(), ActionName),
    ?assertMatch(
        #{
            %% This is the action's public status
            status := ?status_connecting,
            resource_data :=
                #{
                    %% This is the connector's status
                    status := ?status_connecting
                }
        },
        Res
    ),
    #{resource_data := #{added_channels := Channels}} = Res,
    [{_Id, ChannelData}] = maps:to_list(Channels),
    ?assertMatch(#{status := ?status_disconnected}, ChannelData),
    ok.

t_rule_pointing_to_non_operational_channel(_Config) ->
    %% Check that, if a rule sends a message to an action that is not yet installed and
    %% uses `simple_async_internal_buffer', then it eventually increments the rule's
    %% failed counter.
    ResponseETS = ets:new(response_ets, [public]),
    ets:insert(ResponseETS, {on_get_status_value, ?status_connecting}),
    OnGetStatusFun = wrap_fun(fun() ->
        ets:lookup_element(ResponseETS, on_get_status_value, 2)
    end),

    ConnectorConfig = emqx_utils_maps:deep_merge(con_config(), #{
        <<"on_get_status_fun">> => OnGetStatusFun,
        <<"resource_opts">> => #{<<"start_timeout">> => 100}
    }),
    ConnectorName = ?FUNCTION_NAME,
    ct:pal("connector config:\n  ~p", [ConnectorConfig]),
    ?check_trace(
        begin
            {ok, _} = emqx_connector:create(con_type(), ConnectorName, ConnectorConfig),

            ActionName = my_test_action,
            ChanStatusFun = wrap_fun(fun() -> ?status_disconnected end),
            ActionConfig = (bridge_config())#{
                <<"on_get_channel_status_fun">> => ChanStatusFun,
                <<"connector">> => atom_to_binary(ConnectorName)
            },
            ct:pal("action config:\n  ~p", [ActionConfig]),

            meck:new(con_mod(), [passthrough, no_history, non_strict]),
            on_exit(fun() -> catch meck:unload([con_mod()]) end),
            meck:expect(con_mod(), query_mode, 1, simple_async_internal_buffer),
            meck:expect(con_mod(), callback_mode, 0, async_if_possible),

            {ok, _} = emqx_bridge_v2:create(bridge_type(), ActionName, ActionConfig),

            ?assertMatch(
                {ok, #{
                    error := <<"Not installed">>,
                    status := ?status_connecting,
                    resource_data := #{status := ?status_connecting}
                }},
                emqx_bridge_v2:lookup(bridge_type(), ActionName)
            ),

            {ok, #{id := RuleId}} = emqx_rule_engine:create_rule(
                #{
                    sql => <<"select * from \"t/a\"">>,
                    id => atom_to_binary(?FUNCTION_NAME),
                    actions => [
                        <<
                            (atom_to_binary(bridge_type()))/binary,
                            ":",
                            (atom_to_binary(ActionName))/binary
                        >>
                    ]
                }
            ),
            on_exit(fun() -> emqx_rule_engine:delete_rule(RuleId) end),

            Msg = emqx_message:make(<<"t/a">>, <<"payload">>),
            emqx:publish(Msg),

            ActionId = emqx_bridge_v2:id(bridge_type(), ActionName, ConnectorName),
            ?assertEqual(1, emqx_resource_metrics:matched_get(ActionId)),
            ?assertEqual(1, emqx_resource_metrics:failed_get(ActionId)),
            ?retry(
                _Sleep0 = 100,
                _Attempts = 20,
                ?assertMatch(
                    #{
                        counters :=
                            #{
                                matched := 1,
                                'actions.failed' := 1,
                                'actions.failed.unknown' := 1
                            }
                    },
                    emqx_metrics_worker:get_metrics(rule_metrics, RuleId)
                )
            ),

            ok
        end,
        []
    ),

    ok.

t_query_uses_action_query_mode(_Config) ->
    %% Check that we compute the query mode from the action and not from the connector
    %% when querying the resource.

    %% Set one query mode for the connector...
    meck:new(con_mod(), [passthrough, no_history]),
    on_exit(fun() -> catch meck:unload([con_mod()]) end),
    meck:expect(con_mod(), query_mode, 1, sync),
    meck:expect(con_mod(), callback_mode, 0, always_sync),

    ConnectorConfig = emqx_utils_maps:deep_merge(con_config(), #{
        <<"resource_opts">> => #{<<"start_timeout">> => 100}
    }),
    ConnectorName = ?FUNCTION_NAME,
    ct:pal("connector config:\n  ~p", [ConnectorConfig]),
    ?check_trace(
        begin
            {ok, _} = emqx_connector:create(con_type(), ConnectorName, ConnectorConfig),

            ActionName = my_test_action,
            ActionConfig = (bridge_config())#{
                <<"connector">> => atom_to_binary(ConnectorName)
            },
            ct:pal("action config:\n  ~p", [ActionConfig]),

            %% ... now we use a quite different query mode for the action
            meck:expect(con_mod(), query_mode, 1, simple_async_internal_buffer),
            meck:expect(con_mod(), resource_type, 0, dummy),
            meck:expect(con_mod(), callback_mode, 0, async_if_possible),

            {ok, _} = emqx_bridge_v2:create(bridge_type(), ActionName, ActionConfig),

            {ok, #{id := RuleId}} = emqx_rule_engine:create_rule(
                #{
                    sql => <<"select * from \"t/a\"">>,
                    id => atom_to_binary(?FUNCTION_NAME),
                    actions => [
                        <<
                            (atom_to_binary(bridge_type()))/binary,
                            ":",
                            (atom_to_binary(ActionName))/binary
                        >>
                    ]
                }
            ),
            on_exit(fun() -> emqx_rule_engine:delete_rule(RuleId) end),

            Msg = emqx_message:make(<<"t/a">>, <<"payload">>),
            {_, {ok, _}} =
                ?wait_async_action(
                    emqx:publish(Msg),
                    #{?snk_kind := call_query},
                    2_000
                ),

            ok
        end,
        fun(Trace) ->
            ?assertMatch(
                [#{query_mode := simple_async_internal_buffer}],
                ?of_kind(simple_query_override, Trace)
            ),
            ok
        end
    ),
    ok.

%% Helper Functions

wait_until(Fun) ->
    wait_until(Fun, 5000).

wait_until(Fun, Timeout) when Timeout >= 0 ->
    case Fun() of
        true ->
            ok;
        false ->
            IdleTime = 100,
            timer:sleep(IdleTime),
            wait_until(Fun, Timeout - IdleTime)
    end;
wait_until(_, _) ->
    ct:fail("Wait until event did not happen").

bin(Bin) when is_binary(Bin) -> Bin;
bin(Str) when is_list(Str) -> list_to_binary(Str);
bin(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8).

get_rule_metrics(RuleId) ->
    emqx_metrics_worker:get_metrics(rule_metrics, RuleId).
