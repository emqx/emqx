%%--------------------------------------------------------------------
%% Copyright (c) 2019-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mqtt_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").

-define(STATS_KYES, [
    recv_pkt,
    recv_msg,
    send_pkt,
    send_msg,
    recv_oct,
    recv_cnt,
    send_oct,
    send_cnt,
    send_pend
]).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx], #{work_dir => emqx_cth_suite:work_dir(Config)}),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(proplists:get_value(apps, Config)).

init_per_testcase(TestCase, Config) ->
    case erlang:function_exported(?MODULE, TestCase, 2) of
        true -> ?MODULE:TestCase(init, Config);
        false -> Config
    end.

end_per_testcase(TestCase, Config) ->
    case erlang:function_exported(?MODULE, TestCase, 2) of
        true -> ?MODULE:TestCase('end', Config);
        false -> ok
    end,
    Config.

t_conn_stats(_) ->
    with_client(
        fun(CPid) ->
            Stats = emqx_connection:stats(CPid),
            ct:pal("==== stats: ~p", [Stats]),
            [?assert(proplists:get_value(Key, Stats) >= 0) || Key <- ?STATS_KYES]
        end,
        []
    ).

t_tcp_sock_passive(_) ->
    with_client(fun(CPid) -> CPid ! {tcp_passive, sock} end, []).

-doc "A message queued for an offline session is dropped on resume once its Message-Expiry-Interval has elapsed.".
t_message_expiry_interval(_) ->
    run_message_expiry_interval(<<"exp">>, fun message_expiry_interval_exipred/4).

-doc "A message queued for an offline session is delivered on resume while still within its Message-Expiry-Interval.".
t_message_not_expiry_interval(_) ->
    run_message_expiry_interval(<<"noexp">>, fun message_expiry_interval_not_exipred/4).

%% Each QoS iteration uses a fresh set of clientids (tagged with the test name
%% and the QoS) so that no session/mqueue/subscription state leaks from one
%% iteration to the next, nor between the two expiry-interval tests. Reusing a
%% fixed clientid across iterations was racy: a resumed `Client-Verify` session
%% could carry stale queued state, and rapid reconnects of the same clientid
%% raced session takeover on slow CI.
run_message_expiry_interval(Name, Fun) ->
    lists:foreach(
        fun(QoS) ->
            Tag = <<Name/binary, "-", (integer_to_binary(QoS))/binary>>,
            {CPublish, CControl} = message_expiry_interval_init(Tag),
            Fun(CPublish, CControl, QoS, Tag),
            emqtt:stop(CPublish),
            emqtt:stop(CControl)
        end,
        [0, 1, 2]
    ).

message_expiry_interval_init(Tag) ->
    {ok, CPublish} = emqtt:start_link([
        {proto_ver, v5},
        {clientid, <<"Client-Publish-", Tag/binary>>},
        {clean_start, false},
        {properties, #{'Session-Expiry-Interval' => 360}}
    ]),
    {ok, CVerify} = emqtt:start_link([
        {proto_ver, v5},
        {clientid, <<"Client-Verify-", Tag/binary>>},
        {clean_start, false},
        {properties, #{'Session-Expiry-Interval' => 360}}
    ]),
    {ok, CControl} = emqtt:start_link([
        {proto_ver, v5},
        {clientid, <<"Client-Control-", Tag/binary>>},
        {clean_start, false},
        {properties, #{'Session-Expiry-Interval' => 360}}
    ]),
    {ok, _} = emqtt:connect(CPublish),
    {ok, _} = emqtt:connect(CVerify),
    {ok, _} = emqtt:connect(CControl),
    %% subscribe and disconnect Client-verify
    emqtt:subscribe(CControl, <<"t/a">>, 1),
    emqtt:subscribe(CVerify, <<"t/a">>, 1),
    emqtt:stop(CVerify),
    %% Wait until the broker has fully processed Client-Verify's DISCONNECT and
    %% moved its (retained) session into the `disconnected` state before any
    %% message is published. Until then the channel is still `connected`, so a
    %% delivered QoS>0 message is placed into the session's inflight window
    %% instead of the mqueue. Inflight messages are replayed verbatim on resume,
    %% bypassing the mqueue's Message-Expiry-Interval check -- which made the
    %% expiry assertions race (most visibly for QoS=2, whose longer publish
    %% handshake widens the window).
    ok = wait_until_session_disconnected(<<"Client-Verify-", Tag/binary>>),
    {CPublish, CControl}.

wait_until_session_disconnected(ClientId) ->
    wait_until_session_disconnected(ClientId, 100).

wait_until_session_disconnected(ClientId, 0) ->
    error({session_still_connected, ClientId});
wait_until_session_disconnected(ClientId, N) ->
    case emqx_cm:lookup_channels(ClientId) of
        [_ | _] = Pids ->
            case lists:any(fun emqx_cm:is_channel_connected/1, Pids) of
                false -> ok;
                true -> retry_until_session_disconnected(ClientId, N)
            end;
        [] ->
            retry_until_session_disconnected(ClientId, N)
    end.

retry_until_session_disconnected(ClientId, N) ->
    ct:sleep(50),
    wait_until_session_disconnected(ClientId, N - 1).

message_expiry_interval_exipred(CPublish, CControl, QoS, Tag) ->
    ct:pal("~p ~p", [?FUNCTION_NAME, QoS]),
    %% publish to t/a and waiting for the message expired
    _ = emqtt:publish(
        CPublish,
        <<"t/a">>,
        #{'Message-Expiry-Interval' => 1},
        <<"this will be purged in 1s">>,
        [{qos, QoS}]
    ),
    %% CControl make sure publish already store in broker.
    receive
        {publish, #{client_pid := CControl, topic := <<"t/a">>}} ->
            ok
    after 1000 ->
        ct:fail(should_receive_publish)
    end,
    %% Sleep well past Message-Expiry-Interval (1s) with wide margin so the
    %% wall-clock-based expiry check at session resume is not racy on slow CI.
    ct:sleep(2000),

    %% resume the session for Client-Verify
    {ok, CVerify} = emqtt:start_link([
        {proto_ver, v5},
        {clientid, <<"Client-Verify-", Tag/binary>>},
        {clean_start, false},
        {properties, #{'Session-Expiry-Interval' => 360}}
    ]),
    {ok, _} = emqtt:connect(CVerify),

    %% verify Client-Verify could not receive the publish message
    receive
        {publish, #{client_pid := CVerify, topic := <<"t/a">>}} ->
            ct:fail(should_have_expired)
    after 300 ->
        ok
    end,
    emqtt:stop(CVerify).

message_expiry_interval_not_exipred(CPublish, CControl, QoS, Tag) ->
    ct:pal("~p ~p", [?FUNCTION_NAME, QoS]),
    %% publish to t/a
    _ = emqtt:publish(
        CPublish,
        <<"t/a">>,
        #{'Message-Expiry-Interval' => 20},
        <<"this will be purged in 20s">>,
        [{qos, QoS}]
    ),

    %% CControl make sure publish already store in broker.
    receive
        {publish, #{client_pid := CControl, topic := <<"t/a">>}} ->
            ok
    after 1000 ->
        ct:fail(should_receive_publish)
    end,

    %% wait for 1.2s and then resume the session for Client-Verify, the message should not expires
    %% as Message-Expiry-Interval = 20s
    ct:sleep(1200),
    {ok, CVerify} = emqtt:start_link([
        {proto_ver, v5},
        {clientid, <<"Client-Verify-", Tag/binary>>},
        {clean_start, false},
        {properties, #{'Session-Expiry-Interval' => 360}}
    ]),
    {ok, _} = emqtt:connect(CVerify),

    %% verify Client-Verify could receive the publish message and the Message-Expiry-Interval is set
    %% Use a generous receive window: the session resume + queued-message dispatch
    %% can take longer than a few hundred ms on a busy CI runner. A late (but
    %% present) delivery here is a pass, so a wider window only removes flakiness.
    receive
        {publish, #{
            client_pid := CVerify,
            topic := <<"t/a">>,
            properties := #{'Message-Expiry-Interval' := MsgExpItvl}
        }} when
            MsgExpItvl =< 20
        ->
            ok;
        {publish, _} = Msg ->
            ct:fail({incorrect_publish, Msg})
    after 2000 ->
        ct:fail(no_publish_received)
    end,
    emqtt:stop(CVerify).

t_puback_not_lost_on_disconnect(_) ->
    %% Client with persistent in-mem session, subscribing to t/1, manual acking
    {ok, C0} = emqtt:start_link([
        {clientid, <<"puback_not_lost">>},
        {proto_ver, v5},
        {auto_ack, false},
        {clean_start, false},
        {properties, #{'Session-Expiry-Interval' => 10000}}
    ]),
    {ok, _} = emqtt:connect(C0),
    {ok, _, [1]} = emqtt:subscribe(C0, <<"t/1">>, 1),
    %% Publish 100 messages to t/1
    spawn_link(fun() ->
        lists:foreach(
            fun(I) ->
                Payload = integer_to_binary(I),
                Message = emqx_message:make(<<"from">>, 1, <<"t/1">>, Payload),
                emqx_broker:publish(Message)
            end,
            lists:seq(1, 100)
        )
    end),
    %% Receive the first message and acknowledge it, then disconnect
    receive
        {publish, #{topic := <<"t/1">>, payload := <<"1">>, packet_id := PacketId}} ->
            emqtt:puback(C0, PacketId),
            emqtt:disconnect(C0)
    after 1000 ->
        ct:fail("Message not received")
    end,
    %% Wait for the old channel to fully shut down before reconnecting
    ?retry(100, 20, [] =:= emqx_cm:lookup_channels(<<"puback_not_lost">>)),
    %% Restore the session
    {ok, C1} = emqtt:start_link([
        {clientid, <<"puback_not_lost">>},
        {proto_ver, v5},
        {auto_ack, true},
        {clean_start, false},
        {properties, #{'Session-Expiry-Interval' => 10000}}
    ]),
    {ok, _} = emqtt:connect(C1),
    %% Verify that the first message is not received, i.e. that our ack was not lost
    Msgs1 = drain_messages(C1),
    ?assertEqual(99, length(Msgs1)),
    ?assertNot(lists:member(<<"1">>, Msgs1)),
    emqtt:stop(C1).

-doc """
An inflight (sent-but-unacked) QoS-1 message is retried at takeover even after
its Message-Expiry-Interval elapsed: per [MQTT-3.3.2-5], expiry only drops
messages whose onward delivery has not started.
""".
t_inflight_retried_after_expiry_on_takeover(_) ->
    ClientId = <<"inflight_retry_after_expiry">>,
    {ok, CPub} = emqtt:start_link([
        {proto_ver, v5},
        {clientid, <<"inflight_retry_pub">>}
    ]),
    {ok, _} = emqtt:connect(CPub),
    %% Subscriber with a persistent in-mem session, manual acking so the broker
    %% keeps the delivered message in its inflight set awaiting PUBACK.
    {ok, C0} = emqtt:start_link([
        {proto_ver, v5},
        {clientid, ClientId},
        {auto_ack, false},
        {clean_start, false},
        {properties, #{'Session-Expiry-Interval' => 360}}
    ]),
    {ok, _} = emqtt:connect(C0),
    {ok, _, [1]} = emqtt:subscribe(C0, <<"t/a">>, 1),
    %% Publish a QoS-1 message that expires in 1s.
    _ = emqtt:publish(
        CPub,
        <<"t/a">>,
        #{'Message-Expiry-Interval' => 1},
        <<"inflight, retried after expiry">>,
        [{qos, 1}]
    ),
    %% The message reaches the subscriber and enters the session inflight set;
    %% onward delivery has started, but we deliberately do NOT acknowledge it.
    {publish, #{client_pid := C0}} = ?assertReceive({publish, #{topic := <<"t/a">>}}),
    %% Disconnect (keep the session); the un-acked message stays in inflight.
    ok = emqtt:disconnect(C0),
    ?retry(100, 20, [] =:= emqx_cm:lookup_channels(ClientId)),
    %% Sleep well past Message-Expiry-Interval (1s) so the message is expired.
    ct:sleep(2000),
    %% Resume the session, triggering takeover/replay of the inflight set.
    {ok, C1} = emqtt:start_link([
        {proto_ver, v5},
        {clientid, ClientId},
        {auto_ack, true},
        {clean_start, false},
        {properties, #{'Session-Expiry-Interval' => 360}}
    ]),
    {ok, _} = emqtt:connect(C1),
    %% Onward delivery had already started, so the message MUST still be retried
    %% (as a DUP) despite having expired.
    receive
        {publish, #{client_pid := C1, topic := <<"t/a">>, dup := Dup}} ->
            ?assertEqual(true, Dup)
    after 2000 ->
        ct:fail(inflight_message_not_retried)
    end,
    emqtt:stop(C1),
    emqtt:stop(CPub).

drain_messages(C) ->
    receive
        {publish, #{topic := <<"t/1">>, payload := IBin, client_pid := C}} ->
            [IBin | drain_messages(C)]
    after 500 ->
        []
    end.

with_client(TestFun, _Options) ->
    ClientId = <<"t_conn">>,
    {ok, C} = emqtt:start_link([{clientid, ClientId}]),
    {ok, _} = emqtt:connect(C),
    timer:sleep(50),
    case emqx_cm:lookup_channels(ClientId) of
        [] ->
            ct:fail({client_not_started, ClientId});
        [ChanPid] ->
            TestFun(ChanPid),
            emqtt:stop(C)
    end.

t_async_set_keepalive(init, Config) ->
    ok = snabbkaffe:start_trace(),
    Config;
t_async_set_keepalive('end', _Config) ->
    snabbkaffe:stop(),
    ok.

t_async_set_keepalive(_) ->
    case os:type() of
        {unix, darwin} ->
            do_async_set_keepalive(16#10, 16#101, 16#102);
        {unix, linux} ->
            do_async_set_keepalive(4, 5, 6);
        _ ->
            %% don't support the feature on other OS
            ok
    end.

do_async_set_keepalive(OptKeepIdle, OptKeepInterval, OptKeepCount) ->
    ClientID = <<"client-tcp-keepalive">>,
    {ok, Client} = emqtt:start_link([
        {host, "localhost"},
        {proto_ver, v5},
        {clientid, ClientID},
        {clean_start, false}
    ]),
    {ok, _} = emqtt:connect(Client),
    {ok, _} = ?block_until(
        #{
            ?snk_kind := insert_channel_info,
            clientid := ClientID
        },
        2000,
        100
    ),
    [Pid] = emqx_cm:lookup_channels(ClientID),
    State = emqx_connection:get_state(Pid),
    Transport = maps:get(transport, State),
    Socket = maps:get(socket, State),
    ?assert(is_port(Socket)),
    Opts = [{raw, 6, OptKeepIdle, 4}, {raw, 6, OptKeepInterval, 4}, {raw, 6, OptKeepCount, 4}],
    {ok, [
        {raw, 6, OptKeepIdle, <<Idle:32/native>>},
        {raw, 6, OptKeepInterval, <<Interval:32/native>>},
        {raw, 6, OptKeepCount, <<Probes:32/native>>}
    ]} = Transport:getopts(Socket, Opts),
    ct:pal("Idle=~p, Interval=~p, Probes=~p", [Idle, Interval, Probes]),
    emqx_connection:async_set_keepalive(os:type(), Pid, Idle + 1, Interval + 1, Probes + 1),
    {ok, _} = ?block_until(#{?snk_kind := "custom_socket_options_successfully"}, 1000),
    {ok, [
        {raw, 6, OptKeepIdle, <<NewIdle:32/native>>},
        {raw, 6, OptKeepInterval, <<NewInterval:32/native>>},
        {raw, 6, OptKeepCount, <<NewProbes:32/native>>}
    ]} = Transport:getopts(Socket, Opts),
    ?assertEqual(NewIdle, Idle + 1),
    ?assertEqual(NewInterval, Interval + 1),
    ?assertEqual(NewProbes, Probes + 1),
    emqtt:stop(Client),
    ok.
