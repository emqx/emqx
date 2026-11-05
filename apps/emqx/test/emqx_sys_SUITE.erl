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
    %% Forward every `$SYS` message that reaches the publish hooks, the way the
    %% rule engine sees them.
    ok = emqx_hooks:add('message.publish', {?MODULE, on_message_publish, [self()]}, 0),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = emqx_hooks:del('message.publish', {?MODULE, on_message_publish}),
    emqx_config:erase(rule_engine),
    ok = emqx_broker:unsubscribe(?CONNECTED_TOPIC),
    ok = emqx_broker:unsubscribe(?DISCONNECTED_TOPIC),
    ok.

on_message_publish(Msg, Pid) ->
    emqx_message:is_sys(Msg) andalso (Pid ! {sys_publish, emqx_message:topic(Msg)}),
    {ok, Msg}.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

-doc """
Without a subscriber, and with the rule engine ignoring `$SYS` messages, the
client events are not published.
""".
t_client_event_skipped_without_subscriber(_Config) ->
    ClientId = connect_and_disconnect(),
    ?assertEqual([], client_event_topics(ClientId, 500)).

-doc """
A subscriber on the event topic receives the connected event. The disconnected
event still has no subscriber.
""".
t_client_event_published_to_subscriber(_Config) ->
    Filter = <<"$SYS/brokers/+/clients/+/connected">>,
    ok = emqx:subscribe(Filter),
    try
        ClientId = connect_and_disconnect(),
        Expected = systop(<<"clients/", ClientId/binary, "/connected">>),
        receive
            {deliver, Filter, #message{topic = Topic}} -> ?assertEqual(Expected, Topic)
        after 1000 -> ct:fail("connected event not delivered")
        end,
        ?assertEqual([Expected], client_event_topics(ClientId, 500))
    after
        ok = emqx:unsubscribe(Filter)
    end.

-doc """
With `rule_engine.ignore_sys_message = false` the client events are published
for the rule engine, also with no subscriber.
""".
t_client_event_published_for_rule_engine(_Config) ->
    %% The rule engine application is not started here, so its config root does not exist yet.
    ok = emqx_config:force_put([rule_engine, ignore_sys_message], false),
    ClientId = connect_and_disconnect(),
    ?assertEqual(
        [
            systop(<<"clients/", ClientId/binary, "/connected">>),
            systop(<<"clients/", ClientId/binary, "/disconnected">>)
        ],
        client_event_topics(ClientId, 500)
    ).

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

connect_and_disconnect() ->
    ClientId = emqx_guid:to_hexstr(emqx_guid:gen()),
    {ok, Client} = emqtt:start_link([{clientid, ClientId}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(Client),
    ok = emqtt:disconnect(Client),
    ClientId.

%% The `$SYS` topics that carry `ClientId`, in the order the publish hooks saw them.
client_event_topics(ClientId, Timeout) ->
    receive
        {sys_publish, Topic} ->
            case binary:match(Topic, ClientId) of
                nomatch -> client_event_topics(ClientId, Timeout);
                _ -> [Topic | client_event_topics(ClientId, Timeout)]
            end
    after Timeout ->
        []
    end.

systop(Suffix) ->
    emqx_topic:systop(Suffix).

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
