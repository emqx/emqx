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

-doc "The connected system message carries `peername` with the client's source IP and port.".
t_connected_message_peername(_Config) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    ok = emqx_broker:subscribe(?CONNECTED_TOPIC),
    {ok, Client} = emqtt:start_link([{clientid, ClientId}]),
    {ok, _} = emqtt:connect(Client),
    Peername = channel_peername(ClientId),
    Payload = receive_sys_payload(<<"connected">>),
    ?assertEqual(peername_bin(Peername), maps:get(<<"peername">>, Payload)),
    ok = emqtt:disconnect(Client).

-doc "The disconnected system message carries `peername` with the client's source IP and port.".
t_disconnected_message_peername(_Config) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    ok = emqx_broker:subscribe(?DISCONNECTED_TOPIC),
    {ok, Client} = emqtt:start_link([{clientid, ClientId}]),
    {ok, _} = emqtt:connect(Client),
    Peername = channel_peername(ClientId),
    ok = emqtt:disconnect(Client),
    Payload = receive_sys_payload(<<"disconnected">>),
    ?assertEqual(peername_bin(Peername), maps:get(<<"peername">>, Payload)).

-doc """
The `ipaddress` field stays a bare IP address without a port
in both the connected and the disconnected system messages.
""".
t_ipaddress_has_no_port(_Config) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    ok = emqx_broker:subscribe(?CONNECTED_TOPIC),
    ok = emqx_broker:subscribe(?DISCONNECTED_TOPIC),
    {ok, Client} = emqtt:start_link([{clientid, ClientId}]),
    {ok, _} = emqtt:connect(Client),
    ok = emqtt:disconnect(Client),
    ConnectedPayload = receive_sys_payload(<<"connected">>),
    DisconnectedPayload = receive_sys_payload(<<"disconnected">>),
    ?assertEqual(<<"127.0.0.1">>, maps:get(<<"ipaddress">>, ConnectedPayload)),
    ?assertEqual(<<"127.0.0.1">>, maps:get(<<"ipaddress">>, DisconnectedPayload)).

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
