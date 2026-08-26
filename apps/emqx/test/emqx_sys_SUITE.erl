%%--------------------------------------------------------------------
%% Copyright (c) 2019-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_sys_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").

-define(CONNECTED_TOPIC, <<"$SYS/brokers/+/clients/+/connected">>).
-define(DISCONNECTED_TOPIC, <<"$SYS/brokers/+/clients/+/disconnected">>).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx], #{work_dir => emqx_cth_suite:work_dir(Config)}),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = emqx_broker:unsubscribe(?CONNECTED_TOPIC),
    ok = emqx_broker:unsubscribe(?DISCONNECTED_TOPIC),
    ok.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

-doc """
The connected system message payload has the expected shape:
`peername` carries the client's source IP and port, `ipaddress` stays a bare IP.
""".
t_connected_message(_Config) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    ok = emqx_broker:subscribe(?CONNECTED_TOPIC),
    {ok, Client} = emqtt:start_link([{clientid, ClientId}, {username, <<"u1">>}]),
    {ok, _} = emqtt:connect(Client),
    Peername = peername_bin(channel_peername(ClientId)),
    Payload = receive_sys_payload(<<"connected">>),
    ?assertMatch(
        #{
            <<"clientid">> := ClientId,
            <<"username">> := <<"u1">>,
            <<"ipaddress">> := <<"127.0.0.1">>,
            <<"peername">> := Peername,
            <<"sockport">> := 1883,
            <<"protocol">> := <<"mqtt">>,
            <<"proto_name">> := <<"MQTT">>,
            <<"proto_ver">> := 4,
            <<"connected_at">> := ConnectedAt,
            <<"ts">> := Ts,
            <<"conn_props">> := #{},
            <<"receive_maximum">> := _,
            <<"keepalive">> := 60,
            <<"clean_start">> := true,
            <<"expiry_interval">> := 0,
            <<"client_attrs">> := #{}
        } when is_integer(ConnectedAt) andalso is_integer(Ts),
        Payload
    ),
    ok = emqtt:disconnect(Client).

-doc """
The disconnected system message payload has the expected shape:
`peername` carries the client's source IP and port, `ipaddress` stays a bare IP.
""".
t_disconnected_message(_Config) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    ok = emqx_broker:subscribe(?DISCONNECTED_TOPIC),
    {ok, Client} = emqtt:start_link([{clientid, ClientId}, {username, <<"u1">>}]),
    {ok, _} = emqtt:connect(Client),
    Peername = peername_bin(channel_peername(ClientId)),
    ok = emqtt:disconnect(Client),
    Payload = receive_sys_payload(<<"disconnected">>),
    ?assertMatch(
        #{
            <<"clientid">> := ClientId,
            <<"username">> := <<"u1">>,
            <<"ipaddress">> := <<"127.0.0.1">>,
            <<"peername">> := Peername,
            <<"sockport">> := 1883,
            <<"protocol">> := <<"mqtt">>,
            <<"proto_name">> := <<"MQTT">>,
            <<"proto_ver">> := 4,
            <<"connected_at">> := ConnectedAt,
            <<"disconnected_at">> := DisconnectedAt,
            <<"ts">> := Ts,
            <<"disconn_props">> := #{},
            <<"client_attrs">> := #{},
            <<"reason">> := <<"normal">>
        } when
            is_integer(ConnectedAt) andalso is_integer(DisconnectedAt) andalso is_integer(Ts),
        Payload
    ).

-doc """
A client info without the `peername` key does not crash the hook;
the message is published with `peername` set to `undefined`.
""".
t_absent_peername_no_crash(_Config) ->
    ok = emqx_broker:subscribe(?CONNECTED_TOPIC),
    ok = emqx_broker:subscribe(?DISCONNECTED_TOPIC),
    ClientInfo = #{
        clientid => <<"no-peername">>,
        username => <<"u">>,
        peerhost => {127, 0, 0, 1},
        sockport => 1883,
        protocol => mqtt
    },
    ConnInfo = #{
        proto_name => <<"MQTT">>,
        proto_ver => 5,
        connected_at => erlang:system_time(millisecond),
        disconnected_at => erlang:system_time(millisecond)
    },
    _ = emqx_sys:on_client_connected(ClientInfo, ConnInfo),
    ConnectedPayload = receive_sys_payload(<<"connected">>),
    ?assertEqual(<<"undefined">>, maps:get(<<"peername">>, ConnectedPayload)),
    _ = emqx_sys:on_client_disconnected(ClientInfo, normal, ConnInfo),
    DisconnectedPayload = receive_sys_payload(<<"disconnected">>),
    ?assertEqual(<<"undefined">>, maps:get(<<"peername">>, DisconnectedPayload)).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

channel_peername(ClientId) ->
    [ChanPid] = emqx_cm:lookup_channels(ClientId),
    #{conninfo := #{peername := Peername}} = emqx_cm:get_chan_info(ClientId, ChanPid),
    Peername.

peername_bin({IpAddr, Port}) ->
    iolist_to_binary([inet:ntoa(IpAddr), ":", integer_to_list(Port)]).

receive_sys_payload(Event) ->
    Suffix = <<"/", Event/binary>>,
    receive
        {deliver, _Sub, #message{topic = Topic, payload = Payload}} ->
            case binary:longest_common_suffix([Topic, Suffix]) =:= byte_size(Suffix) of
                true -> emqx_utils_json:decode(Payload);
                false -> receive_sys_payload(Event)
            end
    after 5000 ->
        ct:fail({timeout_waiting_for_sys_message, Event})
    end.
