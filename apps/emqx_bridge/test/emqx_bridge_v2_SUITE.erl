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

start_apps() -> [emqx, emqx_conf, emqx_connector, emqx_bridge].

setup_mocks() ->
    MeckOpts = [passthrough, no_link, no_history, non_strict],

    catch meck:new(emqx_connector_schema, MeckOpts),
    meck:expect(emqx_connector_schema, fields, 1, con_schema()),

    catch meck:new(emqx_connector_resource, MeckOpts),
    meck:expect(emqx_connector_resource, connector_to_resource_type, 1, con_mod()),

    catch meck:new(emqx_bridge_v2_schema, MeckOpts),
    meck:expect(emqx_bridge_v2_schema, fields, 1, bridge_schema()),

    catch meck:new(emqx_bridge_v2, MeckOpts),
    meck:expect(emqx_bridge_v2, bridge_v2_type_to_connector_type, 1, con_type()),
    meck:expect(emqx_bridge_v2, bridge_v1_type_to_bridge_v2_type, 1, bridge_type()),

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
        emqx_bridge
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

update_root_connectors_config(RootConf) ->
    emqx_conf:update([connectors], RootConf, #{override_to => cluster}).

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
            {post_config_update, _HandlerMod, #{
                bridge_name := my_test_bridge_update,
                connector_name := unknown,
                type := _,
                reason := "connector_not_found_or_wrong_type"
            }}},
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
            {post_config_update, _HandlerMod, #{
                bridge_name := my_test_bridge_new,
                connector_name := unknown,
                type := _,
                reason := "connector_not_found_or_wrong_type"
            }}},
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
    ?assertEqual(#{}, emqx_config:get([bridges_v2])),

    %% create
    RootConf0 = #{BridgeTypeBin => #{BridgeNameBin => Conf}},
    ?assertMatch(
        {ok, _},
        update_root_config(RootConf0)
    ),
    ?assertMatch(
        {ok, #{
            type := BridgeType,
            name := BridgeName,
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
            type := BridgeType,
            name := BridgeName,
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
            {post_config_update, _HandlerMod, #{
                bridge_name := _,
                connector_name := _,
                type := _,
                reason := "connector_not_found_or_wrong_type"
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
            {post_config_update, _HandlerMod, #{
                bridge_name := _,
                connector_name := _,
                type := wrong_type,
                reason := "connector_not_found_or_wrong_type"
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
            {post_config_update, _HandlerMod, #{
                bridge_name := _,
                connector_name := _,
                type := _,
                reason := "connector_not_found_or_wrong_type"
            }}},
        emqx_bridge_v2:create(bridge_type(), my_test_bridge, BadConf)
    ),
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
            ?assertMatch({ok, _}, emqx_connector:remove(con_type(), con_name())),
            %% we no longer have connector data if this happens...
            ?assertMatch(
                {ok, #{resource_data := undefined}},
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
                {ok, #{resource_data := undefined}},
                emqx_bridge_v2:lookup(bridge_type(), BridgeName)
            ),
            ok
        end
    ),
    ok.
