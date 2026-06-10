%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_session_expiry_clamp_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-define(HOST, "127.0.0.1").
-define(PORT, 1883).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [{emqx, "mqtt { session_expiry_interval = 1h }"}],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_TC, Config) ->
    %% Default: no clamp. Each case sets its own value.
    ok = emqx_config:put_zone_conf(default, [mqtt, max_session_expiry_interval], infinity),
    Config.

end_per_testcase(_TC, _Config) ->
    ok = emqx_config:put_zone_conf(default, [mqtt, max_session_expiry_interval], infinity),
    ok.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

connect_v5(ClientId, CleanStart, Props) ->
    {ok, Client} = emqx_mqtt_test_client:start_link(?HOST, ?PORT),
    Pkt = ?CONNECT_PACKET(
        #mqtt_packet_connect{
            proto_ver = ?MQTT_PROTO_V5,
            properties = Props,
            clientid = ClientId,
            clean_start = CleanStart
        }
    ),
    ok = emqx_mqtt_test_client:send(Client, Pkt),
    {ok, Connack} = emqx_mqtt_test_client:receive_packet(),
    {Client, Connack}.

connect_v311(ClientId, CleanSession) ->
    {ok, Client} = emqx_mqtt_test_client:start_link(?HOST, ?PORT),
    Pkt = ?CONNECT_PACKET(
        #mqtt_packet_connect{
            proto_ver = ?MQTT_PROTO_V4,
            clientid = ClientId,
            clean_start = CleanSession
        }
    ),
    ok = emqx_mqtt_test_client:send(Client, Pkt),
    {ok, Connack} = emqx_mqtt_test_client:receive_packet(),
    {Client, Connack}.

stored_expiry_ms(ClientId) ->
    #{conninfo := ConnInfo} = emqx_cm:get_chan_info(ClientId),
    maps:get(expiry_interval, ConnInfo).

connack_props(?CONNACK_PACKET(?RC_SUCCESS, _SP, Props)) ->
    Props.

set_max(Value) ->
    ok = emqx_config:put_zone_conf(default, [mqtt, max_session_expiry_interval], Value).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

-doc """
MQTT 5.0 request below the configured cap is not clamped;
CONNACK does not include a Session-Expiry-Interval property.
""".
t_v5_request_within_cap_not_clamped(_Config) ->
    set_max(timer:hours(1)),
    ClientId = <<"v5-within-cap">>,
    RequestedSec = 60,
    {Client, Connack} = connect_v5(
        ClientId, true, #{'Session-Expiry-Interval' => RequestedSec}
    ),
    Props = connack_props(Connack),
    ?assertNot(maps:is_key('Session-Expiry-Interval', Props)),
    ?assertEqual(timer:seconds(RequestedSec), stored_expiry_ms(ClientId)),
    ok = emqx_mqtt_test_client:stop(Client).

-doc """
MQTT 5.0 request above the cap is clamped;
CONNACK reflects the clamped value in seconds.
""".
t_v5_request_above_cap_is_clamped(_Config) ->
    MaxMs = timer:seconds(30),
    set_max(MaxMs),
    ClientId = <<"v5-above-cap">>,
    RequestedSec = 3600,
    {Client, Connack} = connect_v5(
        ClientId, true, #{'Session-Expiry-Interval' => RequestedSec}
    ),
    Props = connack_props(Connack),
    ExpectedClampedSec = MaxMs div 1000,
    ?assertEqual(ExpectedClampedSec, maps:get('Session-Expiry-Interval', Props)),
    ?assertEqual(MaxMs, stored_expiry_ms(ClientId)),
    ok = emqx_mqtt_test_client:stop(Client).

-doc """
MQTT 5.0 connect without Session-Expiry-Interval property expires the session
immediately; CONNACK does not include the property.
""".
t_v5_no_property_unchanged(_Config) ->
    set_max(timer:seconds(30)),
    ClientId = <<"v5-no-prop">>,
    {Client, Connack} = connect_v5(ClientId, true, #{}),
    Props = connack_props(Connack),
    ?assertNot(maps:is_key('Session-Expiry-Interval', Props)),
    ?assertEqual(0, stored_expiry_ms(ClientId)),
    ok = emqx_mqtt_test_client:stop(Client).

-doc """
MQTT 3.1.1 connect with clean_session=0 takes session_expiry_interval from
the zone config and is unaffected by max_session_expiry_interval.
""".
t_v311_clean_session_false_ignores_max(_Config) ->
    set_max(timer:seconds(1)),
    ClientId = <<"v311-non-clean">>,
    {Client, _Connack} = connect_v311(ClientId, false),
    %% session_expiry_interval is 1h per init_per_suite; max is 1s but must not apply to v3.
    ?assertEqual(timer:hours(1), stored_expiry_ms(ClientId)),
    ok = emqx_mqtt_test_client:stop(Client).

-doc """
MQTT 5.0 connect with clean_start=true and no Session-Expiry-Interval expires
immediately regardless of the cap; CONNACK does not include the property.
""".
t_v5_clean_start_unchanged(_Config) ->
    set_max(timer:hours(1)),
    ClientId = <<"v5-clean">>,
    {Client, Connack} = connect_v5(ClientId, true, #{}),
    Props = connack_props(Connack),
    ?assertNot(maps:is_key('Session-Expiry-Interval', Props)),
    ?assertEqual(0, stored_expiry_ms(ClientId)),
    ok = emqx_mqtt_test_client:stop(Client).

-doc """
Default config value `infinity` honors any v5 request verbatim and CONNACK
does not include Session-Expiry-Interval.
""".
t_default_infinity_preserves_behavior(_Config) ->
    %% init_per_testcase already set infinity.
    ClientId = <<"v5-infinity">>,
    RequestedSec = 86400,
    {Client, Connack} = connect_v5(
        ClientId, true, #{'Session-Expiry-Interval' => RequestedSec}
    ),
    Props = connack_props(Connack),
    ?assertNot(maps:is_key('Session-Expiry-Interval', Props)),
    ?assertEqual(timer:seconds(RequestedSec), stored_expiry_ms(ClientId)),
    ok = emqx_mqtt_test_client:stop(Client).

-doc """
Setting max_session_expiry_interval to 0 forces every v5 session to expire
immediately, regardless of the value the client requested; CONNACK reflects
the clamped 0.
""".
t_max_zero_clamps_all_v5(_Config) ->
    set_max(0),
    ClientId = <<"v5-zero-max">>,
    RequestedSec = 600,
    {Client, Connack} = connect_v5(
        ClientId, true, #{'Session-Expiry-Interval' => RequestedSec}
    ),
    Props = connack_props(Connack),
    ?assertEqual(0, maps:get('Session-Expiry-Interval', Props)),
    ?assertEqual(0, stored_expiry_ms(ClientId)),
    ok = emqx_mqtt_test_client:stop(Client).
