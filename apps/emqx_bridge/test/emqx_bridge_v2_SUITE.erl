%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_v2_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").
-include_lib("hocon/include/hocon.hrl").
-include_lib("emqx/include/emqx_config.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

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
    {ok, _} = emqx_connector:create(?global_ns, con_type(), con_name(), con_config()),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ets:delete(fun_table_name()),
    emqx_common_test_helpers:call_janitor(),
    delete_all_bridges_and_connectors(),
    meck:unload(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

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
    ];
fields(action) ->
    emqx_bridge_v2_schema:make_producer_action_schema(
        hoconsc:mk(hoconsc:ref(?MODULE, action_parameters), #{})
    );
fields(action_parameters) ->
    [
        {send_to, hoconsc:mk(atom(), #{required => false})},
        {is_conf_for_connected, hoconsc:mk(boolean(), #{required => false})},
        {on_query_fn, hoconsc:mk(binary(), #{required => false})},
        {on_get_channel_status_fun, hoconsc:mk(binary(), #{required => false})}
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
                hoconsc:map(name, hoconsc:ref(?MODULE, action)),
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
        <<"parameters">> => #{<<"send_to">> => registered_process_name()},
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
    meck:expect(emqx_bridge_v2_schema, fields, fun(Struct) ->
        case Struct of
            actions ->
                bridge_schema();
            sources ->
                bridge_schema();
            _ ->
                meck:passthrough([Struct])
        end
    end),
    meck:expect(emqx_bridge_v2_schema, registered_action_types, 0, [bridge_type()]),

    catch meck:new(emqx_bridge_v2, MeckOpts),
    BridgeType = bridge_type(),
    BridgeTypeBin = atom_to_binary(BridgeType),
    meck:expect(
        emqx_bridge_v2,
        bridge_v2_type_to_connector_type,
        fun
            (Type) when Type =:= BridgeType; Type =:= BridgeTypeBin ->
                con_type();
            (Type) ->
                meck:passthrough([Type])
        end
    ),
    meck:expect(emqx_bridge_v2, bridge_v1_type_to_bridge_v2_type, 1, bridge_type()),

    meck:expect(emqx_bridge_v2, is_bridge_v2_type, fun(Type) -> Type =:= BridgeType end),
    ok.

delete_all_bridges_and_connectors() ->
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
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

get_bridge_v2_alarm_cnt() ->
    Alarms = emqx_alarm:get_alarms(activated),
    FilterFun = fun
        (#{name := S}) when is_binary(S) -> string:find(S, "action") =/= nomatch;
        (_) -> false
    end,
    length(lists:filter(FilterFun, Alarms)).

send_message(Type, Name, Msg, QueryOpts) ->
    emqx_bridge_v2:send_message(?global_ns, Type, Name, Msg, QueryOpts).

list_actions() ->
    emqx_bridge_v2:list(?global_ns, actions).

create(Type, Name, Config) ->
    create(#{type => Type, name => Name, config => Config}).

create(Opts) ->
    #{
        type := Type,
        name := Name,
        config := Config
    } = Opts,
    Namespace = maps:get(namespace, Opts, ?global_ns),
    ConfRootKey =
        case maps:get(kind, Opts, action) of
            action -> actions;
            source -> sources
        end,
    emqx_bridge_v2:create(Namespace, ConfRootKey, Type, Name, Config).

remove(Type, Name) ->
    emqx_bridge_v2:remove(?global_ns, actions, Type, Name).

remove_connector(Type, Name) ->
    emqx_connector:remove(?global_ns, Type, Name).

lookup_action(Type, Name) ->
    emqx_bridge_v2:lookup(?global_ns, actions, Type, Name).

create_dry_run(Type, Config) ->
    emqx_bridge_v2:create_dry_run(?global_ns, actions, Type, Config).

health_check(Type, Name) ->
    emqx_bridge_v2_testlib:force_health_check(#{
        type => Type,
        name => Name,
        resource_namespace => ?global_ns,
        kind => action
    }).

id(Type, Name) ->
    emqx_bridge_v2_testlib:lookup_chan_id_in_conf(#{
        kind => action,
        type => Type,
        name => Name
    }).

id(Type, Name, ConnName) ->
    emqx_bridge_v2_testlib:make_chan_id(#{
        kind => action,
        type => Type,
        name => Name,
        connector_name => ConnName
    }).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_create_remove(_) ->
    {ok, _} = create(bridge_type(), my_test_bridge, bridge_config()),
    ok = remove(bridge_type(), my_test_bridge),
    ok.

t_create_disabled_bridge(_) ->
    Config = #{<<"connector">> := Connector} = bridge_config(),
    Disable = Config#{<<"enable">> => false},
    BridgeType = bridge_type(),
    {ok, _} = create(BridgeType, my_enable_bridge, Config),
    {ok, _} = create(BridgeType, my_disable_bridge, Disable),
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
    ok = remove(bridge_type(), my_enable_bridge),
    ok = remove(bridge_type(), my_disable_bridge),
    ok.

t_list(_) ->
    [] = list_actions(),
    {ok, _} = create(bridge_type(), my_test_bridge, bridge_config()),
    1 = length(list_actions()),
    {ok, _} = create(bridge_type(), my_test_bridge2, bridge_config()),
    2 = length(list_actions()),
    ok = remove(bridge_type(), my_test_bridge),
    1 = length(list_actions()),
    ok = remove(bridge_type(), my_test_bridge2),
    0 = length(list_actions()),
    ok.

t_create_dry_run(_) ->
    ok = create_dry_run(bridge_type(), bridge_config()).

t_create_dry_run_fail_add_channel(_) ->
    Msg = <<"Failed to add channel">>,
    OnAddChannel1 = wrap_fun(fun() ->
        {error, Msg}
    end),
    Conf1 = (bridge_config())#{on_add_channel_fun => OnAddChannel1},
    {error, _} = create_dry_run(bridge_type(), Conf1),
    OnAddChannel2 = wrap_fun(fun() ->
        throw(Msg)
    end),
    Conf2 = (bridge_config())#{on_add_channel_fun => OnAddChannel2},
    {error, _} = create_dry_run(bridge_type(), Conf2),
    ok.

t_create_dry_run_fail_get_channel_status(_) ->
    Msg = <<"Failed to add channel">>,
    Fun1 = wrap_fun(fun() ->
        {error, Msg}
    end),
    Conf1 = (bridge_config())#{parameters => #{on_get_channel_status_fun => Fun1}},
    {error, _} = create_dry_run(bridge_type(), Conf1),
    Fun2 = wrap_fun(fun() ->
        throw(Msg)
    end),
    Conf2 = (bridge_config())#{parameters => #{on_get_channel_status_fun => Fun2}},
    {error, _} = create_dry_run(bridge_type(), Conf2),
    ok.

t_create_dry_run_connector_does_not_exist(_) ->
    BridgeConf = (bridge_config())#{<<"connector">> => <<"connector_does_not_exist">>},
    {error, _} = create_dry_run(bridge_type(), BridgeConf).

t_bridge_v1_is_valid(_) ->
    {ok, _} = create(bridge_type(), my_test_bridge, bridge_config()),
    true = emqx_bridge_v2:bridge_v1_is_valid(bridge_v1_type, my_test_bridge),
    %% Add another channel/bridge to the connector
    {ok, _} = create(bridge_type(), my_test_bridge_2, bridge_config()),
    false = emqx_bridge_v2:bridge_v1_is_valid(bridge_v1_type, my_test_bridge),
    ok = remove(bridge_type(), my_test_bridge),
    true = emqx_bridge_v2:bridge_v1_is_valid(bridge_v1_type, my_test_bridge_2),
    ok = remove(bridge_type(), my_test_bridge_2),
    %% Non existing bridge is a valid Bridge V1
    true = emqx_bridge_v2:bridge_v1_is_valid(bridge_v1_type, my_test_bridge),
    ok.

t_manual_health_check(_) ->
    {ok, _} = create(bridge_type(), my_test_bridge, bridge_config()),
    %% Run a health check for the bridge
    #{error := undefined, status := connected} = health_check(
        bridge_type(), my_test_bridge
    ),
    ok = remove(bridge_type(), my_test_bridge),
    ok.

t_manual_health_check_exception(_) ->
    Conf = (bridge_config())#{
        <<"parameters">> => #{
            <<"on_get_channel_status_fun">> => wrap_fun(fun() -> throw(my_error) end)
        }
    },
    {ok, _} = create(bridge_type(), my_test_bridge, Conf),
    %% Run a health check for the bridge
    #{error := my_error, status := disconnected} = health_check(
        bridge_type(), my_test_bridge
    ),
    ok = remove(bridge_type(), my_test_bridge),
    ok.

t_manual_health_check_exception_error(_) ->
    Conf = (bridge_config())#{
        <<"parameters">> => #{
            <<"on_get_channel_status_fun">> => wrap_fun(fun() -> error(my_error) end)
        }
    },
    {ok, _} = create(bridge_type(), my_test_bridge, Conf),
    %% Run a health check for the bridge
    #{error := _, status := disconnected} = health_check(
        bridge_type(), my_test_bridge
    ),
    ok = remove(bridge_type(), my_test_bridge),
    ok.

t_manual_health_check_error(_) ->
    Conf = (bridge_config())#{
        <<"parameters">> => #{
            <<"on_get_channel_status_fun">> => wrap_fun(fun() -> {error, my_error} end)
        }
    },
    {ok, _} = create(bridge_type(), my_test_bridge, Conf),
    %% Run a health check for the bridge
    #{error := my_error, status := disconnected} = health_check(
        bridge_type(), my_test_bridge
    ),
    ok = remove(bridge_type(), my_test_bridge),
    ok.

t_send_message(_) ->
    {ok, _} = create(bridge_type(), my_test_bridge, bridge_config()),
    %% Register name for this process
    register(registered_process_name(), self()),
    _ = send_message(bridge_type(), my_test_bridge, <<"my_msg">>, #{}),
    ?assertReceive({query_called, #{message := <<"my_msg">>}}),
    unregister(registered_process_name()),
    ok = remove(bridge_type(), my_test_bridge).

t_send_message_through_rule(_) ->
    BridgeName = my_test_bridge,
    BridgeType = bridge_type(),
    BridgeConfig0 =
        emqx_utils_maps:deep_merge(
            bridge_config(),
            #{<<"resource_opts">> => #{<<"query_mode">> => <<"async">>}}
        ),
    {ok, _} = create(BridgeType, BridgeName, BridgeConfig0),
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
    ?assertReceive({query_called, #{message := #{payload := Payload}}}),
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
    {ok, _} = emqx_connector:create(?global_ns, con_type(), con_name(), (con_config())#{
        <<"enable">> := false
    }),
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
    {ok, _} = create(BridgeType, BridgeName, BridgeConfig1),
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
    ok = remove(bridge_type(), BridgeName),
    ok.

t_send_message_through_local_topic(_) ->
    %% Bridge configuration with local topic
    BridgeName = my_test_bridge,
    TopicName = <<"t/b">>,
    BridgeConfig = (bridge_config())#{
        <<"local_topic">> => TopicName
    },
    {ok, _} = create(bridge_type(), BridgeName, BridgeConfig),
    %% Register name for this process
    register(registered_process_name(), self()),
    %% Send message to the topic
    ClientId = atom_to_binary(?FUNCTION_NAME),
    Payload = <<"hej">>,
    Msg = emqx_message:make(ClientId, 0, TopicName, Payload),
    emqx:publish(Msg),
    ?assertReceive({query_called, #{message := #{payload := Payload}}}),
    unregister(registered_process_name()),
    ok = remove(bridge_type(), BridgeName),
    ok.

t_send_message_unhealthy_channel(_) ->
    OnGetStatusResponseETS = ets:new(on_get_status_response_ets, [public]),
    ets:insert(OnGetStatusResponseETS, {status_value, {error, my_error}}),
    OnGetStatusFun = wrap_fun(fun() ->
        ets:lookup_element(OnGetStatusResponseETS, status_value, 2)
    end),
    Name = my_test_bridge,
    Conf = emqx_utils_maps:deep_merge(
        bridge_config(),
        #{
            <<"parameters">> => #{<<"on_get_channel_status_fun">> => OnGetStatusFun}
        }
    ),
    {ok, _} = create(bridge_type(), Name, Conf),
    %% Register name for this process
    register(registered_process_name(), self()),
    _ = send_message(bridge_type(), Name, <<"my_msg">>, #{timeout => 1}),
    ?assertNotReceive({query_called, _}),
    %% Sending should work again after the channel is healthy
    ets:insert(OnGetStatusResponseETS, {status_value, connected}),
    ?retry(
        200,
        5,
        ?assertMatch(
            #{status := connected},
            health_check(bridge_type(), Name)
        )
    ),
    _ = send_message(
        bridge_type(),
        my_test_bridge,
        <<"my_msg">>,
        #{}
    ),
    ?assertReceive({query_called, #{message := <<"my_msg">>}}),
    unregister(registered_process_name()),
    ok = remove(bridge_type(), my_test_bridge).

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
    {ok, _} = emqx_connector:create(?global_ns, con_type(), ConName, ConConfig),
    BridgeConf = (bridge_config())#{
        <<"connector">> => atom_to_binary(ConName)
    },
    {ok, _} = create(bridge_type(), my_test_bridge, BridgeConf),
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %% Test that sending does not work when the connector is unhealthy (connecting)
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    register(registered_process_name(), self()),
    _ = send_message(bridge_type(), my_test_bridge, <<"my_msg">>, #{timeout => 100}),
    ?assertNotReceive({query_called, _}),
    %% We should have one alarm
    1 = get_bridge_v2_alarm_cnt(),
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %% Test that sending works again when the connector is healthy (connected)
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    ets:insert(ResponseETS, {on_get_status_value, connected}),

    _ = send_message(bridge_type(), my_test_bridge, <<"my_msg">>, #{timeout => 1000}),
    ?assertReceive({query_called, #{message := <<"my_msg">>}}),
    %% The alarm should be gone at this point
    ?retry(100, 10, ?assertEqual(0, get_bridge_v2_alarm_cnt())),
    unregister(registered_process_name()),
    ok = remove(bridge_type(), my_test_bridge),
    ok = remove_connector(con_type(), ConName),
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
    {ok, _} = emqx_connector:create(?global_ns, con_type(), ConName, ConConfig),
    BridgeConf = (bridge_config())#{
        <<"connector">> => atom_to_binary(ConName)
    },
    {ok, _} = create(bridge_type(), my_test_bridge, BridgeConf),
    %% Wait until on_add_channel_fun is called at least once
    ?retry(100, 50, ?assertEqual(1, counters:get(OnAddChannelCntr, 1))),
    1 = counters:get(OnAddChannelCntr, 1),
    %% We change the status of the connector
    ets:insert(ResponseETS, {on_get_status_value, connecting}),
    %% Wait until the status is changed
    ?retry(
        100,
        50,
        ?assertMatch(
            {ok, #{status := connecting}},
            lookup_action(bridge_type(), my_test_bridge)
        )
    ),
    {ok, BridgeData1} = lookup_action(bridge_type(), my_test_bridge),
    ct:pal("Bridge V2 status changed to: ~p", [maps:get(status, BridgeData1)]),
    %% We change the status again back to connected
    ets:insert(ResponseETS, {on_get_status_value, connected}),
    %% Wait until the status is connected again
    ?retry(
        100,
        50,
        ?assertMatch(
            {ok, #{status := connected}},
            lookup_action(bridge_type(), my_test_bridge)
        )
    ),
    %% On add channel should not have been called again
    1 = counters:get(OnAddChannelCntr, 1),
    %% We change the status to an error
    ets:insert(ResponseETS, {on_get_status_value, {error, my_error}}),
    %% Wait until the status is changed
    ?retry(
        100,
        50,
        ?assertMatch(
            {ok, #{status := disconnected}},
            lookup_action(bridge_type(), my_test_bridge)
        )
    ),
    %% Now we go back to connected
    ets:insert(ResponseETS, {on_get_status_value, connected}),
    ?retry(
        100,
        50,
        ?assertMatch(
            {ok, #{status := connected}},
            lookup_action(bridge_type(), my_test_bridge)
        )
    ),
    %% Now the channel should have been removed and added again
    ?retry(100, 50, ?assertEqual(2, counters:get(OnAddChannelCntr, 1))),
    ok = remove(bridge_type(), my_test_bridge),
    ok = remove_connector(con_type(), ConName),
    ets:delete(ResponseETS),
    ok.

t_unhealthy_channel_alarm(_) ->
    Conf = (bridge_config())#{
        <<"parameters">> => #{
            <<"on_get_channel_status_fun">> =>
                wrap_fun(fun() -> {error, my_error} end)
        }
    },
    0 = get_bridge_v2_alarm_cnt(),
    {ok, _} = create(bridge_type(), my_test_bridge, Conf),
    ok = emqx_bridge_v2_testlib:kickoff_action_health_check(bridge_type(), my_test_bridge),
    1 = get_bridge_v2_alarm_cnt(),
    ok = remove(bridge_type(), my_test_bridge),
    0 = get_bridge_v2_alarm_cnt(),
    ok.

t_load_no_matching_connector(_Config) ->
    Conf = bridge_config(),
    BridgeTypeBin = atom_to_binary(bridge_type()),
    BridgeNameBin0 = <<"my_test_bridge_update">>,
    ?assertMatch({ok, _}, create(bridge_type(), BridgeNameBin0, Conf)),

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
    ?assertEqual(#{}, maps:remove(?COMPUTED, emqx_config:get([actions]))),

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
        lookup_action(BridgeType, BridgeName)
    ),

    %% update
    RootConf1 = #{BridgeTypeBin => #{BridgeNameBin => Conf#{<<"description">> => <<"new_value">>}}},
    ?assertMatch(
        {ok, _},
        update_root_config(RootConf1)
    ),
    ?assertMatch(
        {ok, #{
            type := BridgeTypeBin,
            name := BridgeNameBin,
            raw_config := #{<<"description">> := <<"new_value">>},
            resource_data := #{}
        }},
        lookup_action(BridgeType, BridgeName)
    ),

    %% delete
    RootConf2 = #{},
    ?assertMatch(
        {ok, _},
        update_root_config(RootConf2)
    ),
    ?assertMatch(
        {error, not_found},
        lookup_action(BridgeType, BridgeName)
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
        create(bridge_type(), my_test_bridge, Conf)
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
        create(wrong_type, my_test_bridge, Conf)
    ),
    ok.

t_update_connector_not_found(_Config) ->
    Conf = bridge_config(),
    ?assertMatch({ok, _}, create(bridge_type(), my_test_bridge, Conf)),
    BadConf = Conf#{<<"connector">> => <<"wrong_connector_name">>},
    ?assertMatch(
        {error,
            {pre_config_update, _HandlerMod, #{
                bridge_name := _,
                connector_name := _,
                bridge_type := _,
                reason := <<"connector_not_found_or_wrong_type">>
            }}},
        create(bridge_type(), my_test_bridge, BadConf)
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
            #{parameters := #{is_conf_for_connected := Connected}} =
                maps:get(ChannelId, Channels),
            case Connected of
                true ->
                    connected;
                false ->
                    {error, Msg}
            end
        end
    ),
    BaseConf0 = bridge_config(),
    ConfFor = fun(Val) ->
        emqx_utils_maps:deep_put(
            [<<"parameters">>, <<"is_conf_for_connected">>], BaseConf0, Val
        )
    end,
    ?assertMatch({ok, _}, create(bridge_type(), my_test_bridge, ConfFor(true))),
    SetStatusConnected =
        fun
            (true) ->
                Conf = ConfFor(true),
                %% Update the config
                ?assertMatch({ok, _}, create(bridge_type(), my_test_bridge, Conf)),
                ?assertMatch(
                    #{status := connected},
                    health_check(bridge_type(), my_test_bridge)
                );
            (false) ->
                Conf = ConfFor(false),
                %% Update the config
                ?assertMatch({ok, _}, create(bridge_type(), my_test_bridge, Conf)),
                ?assertMatch(
                    #{status := disconnected},
                    health_check(bridge_type(), my_test_bridge)
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
    ?assertMatch({ok, _}, create(bridge_type(), my_test_bridge, Conf)),
    ?assertMatch(
        {error, {post_config_update, _HandlerMod, {active_channels, [_ | _]}}},
        remove_connector(con_type(), con_name())
    ),
    ok.

t_remove_single_connector_being_referenced_without_active_channels(_Config) ->
    %% we test the connector post config update here because we also need bridges.
    Conf = bridge_config(),
    BridgeName = my_test_bridge,
    ?assertMatch({ok, _}, create(bridge_type(), BridgeName, Conf)),
    emqx_common_test_helpers:with_mock(
        emqx_bridge_v2_test_connector,
        on_get_channels,
        fun(_ResId) -> [] end,
        fun() ->
            ?assertMatch(ok, remove_connector(con_type(), con_name())),
            %% we no longer have connector data if this happens...
            ?assertMatch(
                {ok, #{resource_data := #{}}},
                lookup_action(bridge_type(), BridgeName)
            ),
            ok
        end
    ),
    ok.

t_remove_multiple_connectors_being_referenced_with_channels(_Config) ->
    Conf = bridge_config(),
    BridgeName = my_test_bridge,
    ?assertMatch({ok, _}, create(bridge_type(), BridgeName, Conf)),
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
    ?assertMatch({ok, _}, create(bridge_type(), BridgeName, Conf)),
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
                lookup_action(bridge_type(), BridgeName)
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
            ?assertMatch({ok, _}, create(bridge_type(), BridgeName, Conf)),
            ?assertMatch(
                #{
                    status := disconnected,
                    error := <<"some_error">>
                },
                health_check(bridge_type(), BridgeName)
            ),
            ?assertMatch(
                {ok, #{
                    status := disconnected,
                    error := <<"some_error">>
                }},
                lookup_action(bridge_type(), BridgeName)
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
    {ok, _} = emqx_connector:create(?global_ns, con_type(), ConnectorName, ConnectorConfig),

    ActionName = my_test_action,
    ChanStatusFun = wrap_fun(fun() -> ?status_disconnected end),
    ActionConfig = (bridge_config())#{
        <<"parameters">> => #{<<"on_get_channel_status_fun">> => ChanStatusFun},
        <<"connector">> => atom_to_binary(ConnectorName)
    },
    ct:pal("action config:\n  ~p", [ActionConfig]),
    {ok, _} = create(bridge_type(), ActionName, ActionConfig),

    %% Top-level status is connecting if the connector status is connecting, but the
    %% channel is not yet installed.  `resource_data.added_channels.$channel_id.status'
    %% contains true internal status.
    {ok, Res} = lookup_action(bridge_type(), ActionName),
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
            {ok, _} = emqx_connector:create(?global_ns, con_type(), ConnectorName, ConnectorConfig),

            ActionName = my_test_action,
            ChanStatusFun = wrap_fun(fun() -> ?status_disconnected end),
            ActionConfig =
                emqx_utils_maps:deep_merge(
                    bridge_config(),
                    #{
                        <<"connector">> => atom_to_binary(ConnectorName),
                        <<"parameters">> => #{
                            <<"on_get_channel_status_fun">> => ChanStatusFun
                        }
                    }
                ),
            ct:pal("action config:\n  ~p", [ActionConfig]),

            meck:new(con_mod(), [passthrough, no_history, non_strict]),
            on_exit(fun() -> catch meck:unload([con_mod()]) end),
            meck:expect(con_mod(), query_mode, 1, simple_async_internal_buffer),
            meck:expect(con_mod(), callback_mode, 0, async_if_possible),

            {ok, _} = create(bridge_type(), ActionName, ActionConfig),

            ?assertMatch(
                {ok, #{
                    error := <<"Not installed">>,
                    status := ?status_connecting,
                    resource_data := #{status := ?status_connecting}
                }},
                lookup_action(bridge_type(), ActionName)
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

            ActionId = id(bridge_type(), ActionName, ConnectorName),
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
            {ok, _} = emqx_connector:create(?global_ns, con_type(), ConnectorName, ConnectorConfig),

            ActionName = my_test_action,
            ActionConfig = (bridge_config())#{
                <<"connector">> => atom_to_binary(ConnectorName)
            },
            ct:pal("action config:\n  ~p", [ActionConfig]),

            %% ... now we use a quite different query mode for the action
            meck:expect(con_mod(), query_mode, 1, simple_async_internal_buffer),
            meck:expect(con_mod(), resource_type, 0, dummy),
            meck:expect(con_mod(), callback_mode, 0, async_if_possible),

            {ok, _} = create(bridge_type(), ActionName, ActionConfig),

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

t_async_load_config_cli(_Config) ->
    ResponseETS = ets:new(response_ets, [public]),
    ets:insert(ResponseETS, {on_start_value, hang}),
    TestPid = self(),
    OnStartFun = wrap_fun(fun(_Conf) ->
        case ets:lookup_element(ResponseETS, on_start_value, 2) of
            hang ->
                persistent_term:put({?MODULE, res_pid}, self()),
                TestPid ! hanging,
                receive
                    go ->
                        ets:insert(ResponseETS, {on_start_value, continue})
                end;
            continue ->
                ok
        end,
        {ok, connector_state}
    end),
    on_exit(fun() ->
        case persistent_term:get({?MODULE, res_pid}, undefined) of
            undefined ->
                ct:fail("resource didn't start");
            ResPid ->
                ResPid ! go,
                ok
        end
    end),

    ConnectorType = con_type(),
    ConnectorName = atom_to_binary(?FUNCTION_NAME),
    ConnectorConfig = emqx_utils_maps:deep_merge(con_config(), #{
        <<"resource_opts">> => #{<<"start_timeout">> => 100},
        <<"on_start_fun">> => OnStartFun
    }),
    %% Make the resource stuck while starting
    spawn_link(fun() ->
        {ok, _} = emqx_connector:create(?global_ns, ConnectorType, ConnectorName, ConnectorConfig)
    end),
    receive
        hanging ->
            ok
    after 1_000 ->
        ct:fail("connector not started and stuck")
    end,

    ActionType = bridge_type(),
    ActionName = ConnectorName,
    ActionConfig = bridge_config(),

    SourceType = bridge_type(),
    SourceName = ConnectorName,
    SourceConfig = bridge_config(),

    RawConfig = #{
        <<"actions">> => #{ActionType => #{ActionName => ActionConfig}},
        <<"sources">> => #{SourceType => #{SourceName => SourceConfig}}
    },
    ConfigToLoadBin = iolist_to_binary(hocon_pp:do(RawConfig, #{})),
    ct:pal("loading config..."),
    ct:timetrap(5_000),
    ?assertMatch(ok, emqx_conf_cli:load_config(ConfigToLoadBin, #{mode => merge})),
    ct:pal("config loaded"),

    ok.

%% Checks that we avoid touching `created_at' and `last_modified_at' when replicating
%% cluster RPC configurations.
t_modification_dates_when_replicating(Config) ->
    AppSpecs = app_specs() ++ [emqx_management],
    ClusterSpec = [
        {mod_dates1, #{apps => AppSpecs ++ [emqx_mgmt_api_test_util:emqx_dashboard()]}},
        {mod_dates2, #{apps => AppSpecs}}
    ],
    [N1, N2] =
        Nodes = emqx_cth_cluster:start(
            ClusterSpec,
            #{work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, Config)}
        ),
    Fun = fun() -> ?ON(N1, emqx_mgmt_api_test_util:auth_header_()) end,
    emqx_bridge_v2_testlib:set_auth_header_getter(Fun),
    snabbkaffe:start_trace(),
    try
        ConnectorName = mod_dates,
        {201, _} = emqx_bridge_mqtt_v2_publisher_SUITE:create_connector_api(
            [
                {connector_type, mqtt},
                {connector_name, ConnectorName},
                {connector_config, emqx_bridge_mqtt_v2_publisher_SUITE:connector_config()}
            ],
            #{}
        ),
        snabbkaffe_nemesis:inject_crash(
            ?match_event(#{?snk_kind := bridge_post_config_update_done}),
            fun(_) ->
                %% Only introduces a bit of delay, so that we are guaranteed some time
                %% difference when replicating.
                ct:sleep(100),
                false
            end
        ),
        {201, _} = emqx_bridge_mqtt_v2_publisher_SUITE:create_action_api(
            [
                {bridge_kind, action},
                {action_type, mqtt},
                {action_name, ConnectorName},
                {action_config,
                    emqx_bridge_mqtt_v2_publisher_SUITE:action_config(#{
                        <<"connector">> => ConnectorName
                    })}
            ],
            #{}
        ),
        #{
            <<"created_at">> := CreatedAt1,
            <<"last_modified_at">> := LastModifiedAt1
        } = ?ON(N1, emqx_config:get_raw([actions, mqtt, ConnectorName])),
        #{
            <<"created_at">> := CreatedAt2,
            <<"last_modified_at">> := LastModifiedAt2
        } = ?ON(N2, emqx_config:get_raw([actions, mqtt, ConnectorName])),
        ?assertEqual(CreatedAt1, CreatedAt2),
        ?assertEqual(LastModifiedAt1, LastModifiedAt2),
        ok
    after
        emqx_cth_cluster:stop(Nodes),
        snabbkaffe:stop()
    end.

%% Happy path smoke tests for fallback actions.
t_fallback_actions(_Config) ->
    ConnectorConfig = emqx_utils_maps:deep_merge(con_config(), #{
        <<"resource_opts">> => #{<<"start_timeout">> => 100}
    }),
    ConnectorName = ?FUNCTION_NAME,
    ct:pal("connector config:\n  ~p", [ConnectorConfig]),
    ?check_trace(
        #{timetrap => 3_000},
        begin
            {ok, _} = emqx_connector:create(?global_ns, con_type(), ConnectorName, ConnectorConfig),

            FallbackActionName = <<"my_fallback">>,
            FallbackActionConfig = (bridge_config())#{
                <<"connector">> => atom_to_binary(ConnectorName)
            },
            ct:pal("fallback action config:\n  ~p", [FallbackActionConfig]),

            OnQueryFn = wrap_fun(fun(_Ctx) ->
                {error, {unrecoverable_error, fallback_time}}
            end),

            RepublishTopic = <<"republish/fallback">>,
            RepublishArgs = #{
                <<"topic">> => RepublishTopic,
                <<"qos">> => 1,
                <<"retain">> => false,
                <<"payload">> => <<"${payload}">>,
                <<"mqtt_properties">> => #{},
                <<"user_properties">> => <<"${pub_props.'User-Property'}">>,
                <<"direct_dispatch">> => false
            },

            ActionName = my_test_action,
            ActionTypeBin = atom_to_binary(bridge_type()),
            ActionConfig = (bridge_config())#{
                <<"connector">> => atom_to_binary(ConnectorName),
                <<"parameters">> => #{<<"on_query_fn">> => OnQueryFn},
                <<"fallback_actions">> => [
                    #{
                        <<"kind">> => <<"reference">>,
                        <<"type">> => ActionTypeBin,
                        <<"name">> => FallbackActionName
                    },
                    #{
                        <<"kind">> => <<"republish">>,
                        <<"args">> => RepublishArgs
                    }
                ]
            },
            ct:pal("action config:\n  ~p", [ActionConfig]),

            {ok, _} = create(
                bridge_type(), FallbackActionName, FallbackActionConfig
            ),
            _ = emqx_bridge_v2_testlib:kickoff_action_health_check(
                bridge_type(), FallbackActionName
            ),
            {ok, _} = create(bridge_type(), ActionName, ActionConfig),
            _ = emqx_bridge_v2_testlib:kickoff_action_health_check(bridge_type(), ActionName),

            {ok, #{id := RuleId}} = emqx_rule_engine:create_rule(
                #{
                    sql => <<"select * from \"t/a\"">>,
                    id => atom_to_binary(?FUNCTION_NAME),
                    actions => [
                        emqx_bridge_resource:bridge_id(bridge_type(), ActionName)
                    ]
                }
            ),
            on_exit(fun() -> emqx_rule_engine:delete_rule(RuleId) end),

            ct:pal("publishing initial message"),
            register(registered_process_name(), self()),
            emqx:subscribe(RepublishTopic, #{qos => 1}),
            Payload = <<"payload">>,
            Msg = emqx_message:make(<<"t/a">>, Payload),
            emqx:publish(Msg),
            ct:pal("waiting for fallback actions effects..."),
            ?assertReceive({query_called, #{message := #{payload := Payload}}}),
            ?assertReceive({deliver, RepublishTopic, #message{payload = Payload}}),

            ok
        end,
        []
    ),
    ok.

%% Checks that fallback action cycles are not allowed.
t_fallback_actions_cycles(_Config) ->
    ConnectorConfig = emqx_utils_maps:deep_merge(con_config(), #{
        <<"resource_opts">> => #{<<"start_timeout">> => 100}
    }),
    ConnectorName = ?FUNCTION_NAME,
    ct:pal("connector config:\n  ~p", [ConnectorConfig]),
    ?check_trace(
        #{timetrap => 10_000},
        begin
            {ok, _} = emqx_connector:create(?global_ns, con_type(), ConnectorName, ConnectorConfig),

            TestPid = self(),
            OnQueryFn = wrap_fun(fun(Ctx) ->
                TestPid ! {query_called, Ctx},
                {error, {unrecoverable_error, fallback_time}}
            end),

            ActionTypeBin = atom_to_binary(bridge_type()),
            PrimaryActionName = <<"primary">>,
            FallbackActionName = <<"fallback">>,
            FallbackActionConfig = (bridge_config())#{
                <<"connector">> => atom_to_binary(ConnectorName),
                <<"parameters">> => #{<<"on_query_fn">> => OnQueryFn},
                <<"resource_opts">> => #{<<"metrics_flush_interval">> => <<"100ms">>},
                <<"fallback_actions">> => [
                    %% Fallback back to primary
                    #{
                        <<"kind">> => <<"reference">>,
                        <<"type">> => ActionTypeBin,
                        <<"name">> => PrimaryActionName
                    },
                    %% Fallback back to self
                    #{
                        <<"kind">> => <<"reference">>,
                        <<"type">> => ActionTypeBin,
                        <<"name">> => FallbackActionName
                    }
                ]
            },
            ct:pal("fallback action config:\n  ~p", [FallbackActionConfig]),

            PrimaryActionConfig = (bridge_config())#{
                <<"connector">> => atom_to_binary(ConnectorName),
                <<"parameters">> => #{<<"on_query_fn">> => OnQueryFn},
                <<"resource_opts">> => #{<<"metrics_flush_interval">> => <<"100ms">>},
                <<"fallback_actions">> => [
                    %% Fallback back to self
                    #{
                        <<"kind">> => <<"reference">>,
                        <<"type">> => ActionTypeBin,
                        <<"name">> => PrimaryActionName
                    },
                    %% This fallback ends up falling back to primary
                    #{
                        <<"kind">> => <<"reference">>,
                        <<"type">> => ActionTypeBin,
                        <<"name">> => FallbackActionName
                    }
                ]
            },
            ct:pal("primary action config:\n  ~p", [PrimaryActionConfig]),

            {ok, _} = create(
                bridge_type(), FallbackActionName, FallbackActionConfig
            ),
            _ = emqx_bridge_v2_testlib:kickoff_action_health_check(
                bridge_type(), FallbackActionName
            ),
            {ok, _} = create(bridge_type(), PrimaryActionName, PrimaryActionConfig),
            _ = emqx_bridge_v2_testlib:kickoff_action_health_check(
                bridge_type(), PrimaryActionName
            ),

            {ok, #{id := RuleId}} = emqx_rule_engine:create_rule(
                #{
                    sql => <<"select * from \"t/a\"">>,
                    id => atom_to_binary(?FUNCTION_NAME),
                    actions => [
                        emqx_bridge_resource:bridge_id(bridge_type(), PrimaryActionName)
                    ]
                }
            ),
            on_exit(fun() -> emqx_rule_engine:delete_rule(RuleId) end),

            ct:pal("publishing initial message"),
            Payload = <<"payload">>,
            Msg = emqx_message:make(<<"t/a">>, Payload),
            emqx:publish(Msg),

            ct:pal("waiting for fallback actions effects..."),
            PrimaryResId = id(bridge_type(), PrimaryActionName),
            FallbackResId = id(bridge_type(), FallbackActionName),
            %% Should trigger the primary and fallback actions exactly one each.
            ?assertReceive(
                {query_called, #{action_res_id := PrimaryResId, message := #{payload := Payload}}}
            ),
            ct:pal("primary called"),
            ?assertReceive(
                {query_called, #{action_res_id := FallbackResId, message := #{payload := Payload}}}
            ),
            ct:pal("fallback called"),
            ?assertNotReceive({query_called, _}),
            ct:pal("no spurious invocations found"),

            ?assertMatch(
                #{
                    counters :=
                        #{
                            'matched' := 1,
                            'failed' := 0,
                            'failed.exception' := 0,
                            'failed.no_result' := 0,
                            'passed' := 1,
                            'actions.total' := 1,
                            'actions.success' := 0,
                            'actions.failed' := 1,
                            'actions.failed.unknown' := 1,
                            'actions.failed.out_of_service' := 0,
                            'actions.discarded' := 0
                        }
                },
                get_rule_metrics(RuleId)
            ),
            ct:pal("checking primary action metrics"),
            ?retry(
                500,
                10,
                ?assertMatch(
                    #{
                        counters :=
                            #{
                                'matched' := 1,
                                'success' := 0,
                                'failed' := 1,
                                'dropped' := 0,
                                'received' := 0,
                                'late_reply' := 0,
                                'retried' := 0
                            }
                    },
                    emqx_bridge_v2_testlib:get_metrics(#{
                        type => ActionTypeBin,
                        name => PrimaryActionName
                    })
                )
            ),
            ct:pal("primary action metrics ok"),
            ct:pal("checking fallback action metrics"),
            ?retry(
                500,
                10,
                ?assertMatch(
                    #{
                        counters :=
                            #{
                                'matched' := 1,
                                'success' := 0,
                                'failed' := 1,
                                'dropped' := 0,
                                'received' := 0,
                                'late_reply' := 0,
                                'retried' := 0
                            }
                    },
                    emqx_bridge_v2_testlib:get_metrics(#{
                        type => ActionTypeBin,
                        name => FallbackActionName
                    })
                )
            ),
            ct:pal("fallback action metrics ok"),

            ok
        end,
        []
    ),
    ok.

-doc """
Checks that, when fallback actions are triggered, their references are resolved within the
same namespace as primary action's.
""".
t_fallback_actions_different_namespaces(_Config) ->
    Namespace = <<"somens">>,
    ConnectorConfig = emqx_utils_maps:deep_merge(con_config(), #{
        <<"resource_opts">> => #{<<"start_timeout">> => 100}
    }),
    ConnectorName = ?FUNCTION_NAME,
    ct:pal("connector config:\n  ~p", [ConnectorConfig]),
    ?check_trace(
        #{timetrap => 3_000},
        begin
            %% One for the primary action (namespaced), the other for the "fallback" (global).
            {ok, _} = emqx_connector:create(Namespace, con_type(), ConnectorName, ConnectorConfig),
            {ok, _} = emqx_connector:create(?global_ns, con_type(), ConnectorName, ConnectorConfig),

            FallbackActionName = <<"my_fallback">>,
            FallbackActionConfig = (bridge_config())#{
                <<"connector">> => atom_to_binary(ConnectorName)
            },
            ct:pal("fallback action config:\n  ~p", [FallbackActionConfig]),

            OnQueryFn = wrap_fun(fun(_Ctx) ->
                {error, {unrecoverable_error, fallback_time}}
            end),

            RepublishTopic = <<"republish/fallback">>,
            RepublishArgs = #{
                <<"topic">> => RepublishTopic,
                <<"qos">> => 1,
                <<"retain">> => false,
                <<"payload">> => <<"${payload}">>,
                <<"mqtt_properties">> => #{},
                <<"user_properties">> => <<"${pub_props.'User-Property'}">>,
                <<"direct_dispatch">> => false
            },

            ActionName = my_test_action,
            ActionTypeBin = atom_to_binary(bridge_type()),
            ActionConfig = (bridge_config())#{
                <<"connector">> => atom_to_binary(ConnectorName),
                <<"parameters">> => #{<<"on_query_fn">> => OnQueryFn},
                <<"fallback_actions">> => [
                    #{
                        <<"kind">> => <<"reference">>,
                        <<"type">> => ActionTypeBin,
                        <<"name">> => FallbackActionName
                    },
                    #{
                        <<"kind">> => <<"republish">>,
                        <<"args">> => RepublishArgs
                    }
                ]
            },
            ct:pal("action config:\n  ~p", [ActionConfig]),
            %% Note: creating fallback in global namespace; should not be invoked.
            ct:pal("creating fallback (global namespace)"),
            {ok, _} = create(#{
                namespace => ?global_ns,
                kind => action,
                type => bridge_type(),
                name => FallbackActionName,
                config => FallbackActionConfig
            }),
            _ = emqx_bridge_v2_testlib:kickoff_kind_health_check(#{
                namespace => ?global_ns,
                kind => action,
                type => bridge_type(),
                name => FallbackActionName
            }),
            ct:pal("creating primary (namespaced)"),
            {ok, _} = create(#{
                namespace => Namespace,
                kind => action,
                type => bridge_type(),
                name => ActionName,
                config => ActionConfig
            }),
            _ = emqx_bridge_v2_testlib:kickoff_kind_health_check(#{
                namespace => Namespace,
                kind => action,
                type => bridge_type(),
                name => ActionName
            }),

            %% NOTE: we're temporarily sending the message directly to the action.  This
            %% should **not** be done in tests: they should use rules for that.  However,
            %% at the time of writing, rule engine is not yet namespaced.
            TMPSendMessage = fun(Name, Payload) ->
                Message = #{
                    payload => Payload,
                    metadata => #{rule_id => <<"ns_support_pending">>}
                },
                emqx_bridge_v2:send_message(
                    Namespace,
                    bridge_type(),
                    Name,
                    Message,
                    #{query_mode => sync}
                )
            end,

            %% {ok, #{id := RuleId}} = emqx_rule_engine:create_rule(
            %%     #{
            %%         sql => <<"select * from \"t/a\"">>,
            %%         id => atom_to_binary(?FUNCTION_NAME),
            %%         actions => [
            %%             emqx_bridge_resource:bridge_id(bridge_type(), ActionName)
            %%         ]
            %%     }
            %% ),
            %% on_exit(fun() -> emqx_rule_engine:delete_rule(RuleId) end),

            register(registered_process_name(), self()),
            emqx:subscribe(RepublishTopic, #{qos => 1}),
            Payload0 = <<"payload">>,

            ct:pal("publishing initial message"),
            TMPSendMessage(ActionName, Payload0),
            ct:pal("waiting to assert fallback was not called"),
            ?assertNotReceive({query_called, _}),
            %% This one is received because republishes don't have namespaces...
            ?assertReceive({deliver, RepublishTopic, #message{payload = Payload0}}),

            %% Now, let's create a fallback action in the same namespace.  That should be
            %% triggered.
            ct:pal("creating fallback (namespaced)"),
            {ok, _} = create(#{
                namespace => Namespace,
                kind => action,
                type => bridge_type(),
                name => FallbackActionName,
                config => FallbackActionConfig
            }),
            _ = emqx_bridge_v2_testlib:kickoff_kind_health_check(#{
                namespace => Namespace,
                kind => action,
                type => bridge_type(),
                name => FallbackActionName
            }),

            ct:pal("publishing second message"),
            Payload1 = <<"payload1">>,
            TMPSendMessage(ActionName, Payload1),
            ct:pal("waiting to assert fallback was called"),
            ?assertReceive({query_called, #{message := #{payload := Payload1}}}),
            ?assertReceive({deliver, RepublishTopic, #message{payload = Payload1}}),
            %% Checking that `Payload0` didn't arrive later.
            ?assertNotReceive({query_called, #{message := #{payload := Payload0}}}),

            ok
        end,
        []
    ),
    ok.
