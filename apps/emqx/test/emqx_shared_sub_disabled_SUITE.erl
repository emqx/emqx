%%--------------------------------------------------------------------
%% Copyright (c) 2018-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_shared_sub_disabled_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(SHARED_TOPIC, <<"$share/g/t/1">>).
-define(NORMAL_TOPIC, <<"t/1">>).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx], #{work_dir => emqx_cth_suite:work_dir(Config)}),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_TestCase, Config) ->
    process_flag(trap_exit, true),
    [{old_zones, emqx:get_config([zones], #{})} | Config].

end_per_testcase(_TestCase, Config) ->
    emqx_config:put([zones], ?config(old_zones, Config)),
    ok.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

-doc """
An MQTT v5 client attempting a shared subscription while shared subscriptions
are disabled receives DISCONNECT with reason code 0x9E and the connection is
closed, no SUBACK is delivered.
""".
t_v5_shared_sub_disabled(_Config) ->
    ok = disable_shared_subscription(),
    C = connect(v5),
    MRef = erlang:monitor(process, C),
    _ = catch emqtt:subscribe(C, ?SHARED_TOPIC, 0),
    ?assertReceive({disconnected, ?RC_SHARED_SUBSCRIPTIONS_NOT_SUPPORTED, _}),
    ?assertReceive(
        {'DOWN', MRef, process, C,
            {shutdown, {disconnected, ?RC_SHARED_SUBSCRIPTIONS_NOT_SUPPORTED, _}}}
    ),
    ?assertEqual([], emqx_cm:lookup_channels(clientid(v5))).

-doc """
An MQTT v3.1.1 client attempting a shared subscription while shared
subscriptions are disabled has its connection closed, v3 has no DISCONNECT
packet from server to client.
""".
t_v3_shared_sub_disabled(_Config) ->
    ok = disable_shared_subscription(),
    C = connect(v4),
    MRef = erlang:monitor(process, C),
    _ = catch emqtt:subscribe(C, ?SHARED_TOPIC, 0),
    ?assertReceive({'DOWN', MRef, process, C, {shutdown, tcp_closed}}),
    ?assertEqual([], emqx_cm:lookup_channels(clientid(v4))).

-doc """
A SUBSCRIBE carrying both a normal and a shared topic filter still closes the
connection when shared subscriptions are disabled.
""".
t_mixed_sub_disabled(_Config) ->
    ok = disable_shared_subscription(),
    C = connect(v5),
    MRef = erlang:monitor(process, C),
    _ = catch emqtt:subscribe(C, [{?NORMAL_TOPIC, 0}, {?SHARED_TOPIC, 0}]),
    ?assertReceive({disconnected, ?RC_SHARED_SUBSCRIPTIONS_NOT_SUPPORTED, _}),
    ?assertReceive({'DOWN', MRef, process, C, {shutdown, {disconnected, _, _}}}).

-doc """
A normal (non-shared) subscription is unaffected when shared subscriptions are
disabled.
""".
t_normal_sub_unaffected(_Config) ->
    ok = disable_shared_subscription(),
    C = connect(v5),
    ?assertMatch({ok, _, [0]}, emqtt:subscribe(C, ?NORMAL_TOPIC, 0)),
    ok = emqtt:disconnect(C).

-doc """
With shared subscriptions enabled (the default) a shared subscription still
succeeds and the connection is kept.
""".
t_shared_sub_enabled(_Config) ->
    C = connect(v5),
    ?assertMatch({ok, _, [0]}, emqtt:subscribe(C, ?SHARED_TOPIC, 0)),
    ?assertMatch([_], emqx_cm:lookup_channels(clientid(v5))),
    ok = emqtt:disconnect(C).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

disable_shared_subscription() ->
    emqx_config:put_zone_conf(default, [mqtt, shared_subscription], false).

clientid(Vsn) ->
    iolist_to_binary([atom_to_list(Vsn), "-shared-sub-disabled"]).

connect(Vsn) ->
    {ok, C} = emqtt:start_link([{clientid, clientid(Vsn)}, {proto_ver, Vsn}]),
    {ok, _} = emqtt:connect(C),
    C.
