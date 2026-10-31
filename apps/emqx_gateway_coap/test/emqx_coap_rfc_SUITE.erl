%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_coap_rfc_SUITE).
-compile(export_all).
-compile(nowarn_export_all).
-include("emqx_coap.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    [
        t_sequence_after_retransmission,
        t_deregister_wrong_token,
        t_two_tokens_same_resource,
        t_deregister_stops_retransmission,
        t_non_notification_pacing,
        t_periodic_non_confirmation,
        t_confirmation_after_udp_migration,
        t_independent_subopts,
        t_api_cancel_with_mountpoint,
        t_cancel_one_token,
        t_repeated_registration,
        t_reregister_current_representation,
        t_confirmation_before_next_non,
        t_confirmation_with_full_queue,
        t_confirmation_after_idle,
        t_con_reset,
        t_non_reset,
        t_old_non_reset
    ].
init_per_suite(Config) -> emqx_coap_SUITE:init_per_suite(Config).
end_per_suite(Config) -> emqx_coap_SUITE:end_per_suite(Config).

-doc "Queued notifications remain newer than the retransmission that precedes them.".
t_sequence_after_retransmission(_) ->
    with_conn(fun(S, Auth) ->
        Topic = <<"review/sequence">>,
        Token = <<"seq">>,
        sub(S, Auth, Topic, Token, 0),
        emqx_coap_SUITE:publish(Topic, 1, <<"first">>),
        First = recv(S, 2000),
        emqx_coap_SUITE:publish(Topic, 1, <<"latest">>),
        [Pid] = emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>),
        wait_pending(Pid),
        Retry = recv(S, 4000),
        ?assertEqual(First#coap_message.id, Retry#coap_message.id),
        send(S, emqx_coap_message:ack(Retry)),
        Next = recv(S, 2000),
        send(S, emqx_coap_message:ack(Next)),
        ?assertEqual(<<"latest">>, Next#coap_message.payload),
        RetrySeq = emqx_coap_message:get_option(observe, Retry),
        NextSeq = emqx_coap_message:get_option(observe, Next),
        Delta = (NextSeq - RetrySeq) band 16#FFFFFF,
        ct:pal("Retry Observe=~p, latest Observe=~p, serial delta=~p", [RetrySeq, NextSeq, Delta]),
        ?assert(Delta > 0 andalso Delta < 16#800000)
    end).

-doc "A deregistration with a different Token leaves the observation intact.".
t_deregister_wrong_token(_) ->
    with_conn(fun(S, Auth) ->
        Topic = <<"review/cancel-token">>,
        sub(S, Auth, Topic, <<"right">>, 0),
        sub(S, Auth, Topic, <<"wrong">>, 1),
        [Pid] = emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>),
        ?assertEqual([Topic], [T || {T, _} <- emqx_broker:subscriptions(Pid)])
    end).

-doc "Two accepted Tokens observe the same resource independently.".
t_two_tokens_same_resource(_) ->
    with_conn(fun(S, Auth) ->
        Topic = <<"review/two-tokens">>,
        A = sub(S, Auth, Topic, <<"first">>, 0),
        B = sub(S, Auth, Topic, <<"second">>, 0),
        ?assertEqual(<<"first">>, A#coap_message.token),
        ?assertEqual(<<"second">>, B#coap_message.token),
        ?assert(is_integer(emqx_coap_message:get_option(observe, A))),
        ?assert(is_integer(emqx_coap_message:get_option(observe, B))),
        emqx_coap_SUITE:publish(Topic, 0, <<"change">>),
        Messages = [recv(S, 2000), recv(S, 5000)],
        Tokens = lists:sort([M#coap_message.token || M <- Messages]),
        ct:pal("Two accepted Observe registrations, notified Tokens: ~p", [Tokens]),
        ?assertEqual([<<"first">>, <<"second">>], Tokens)
    end).

-doc "Explicit deregistration stops outstanding notification retransmissions.".
t_deregister_stops_retransmission(_) ->
    with_conn(fun(S, Auth) ->
        Topic = <<"review/cancel-inflight">>,
        Token = <<"cancel">>,
        sub(S, Auth, Topic, Token, 0),
        emqx_coap_SUITE:publish(Topic, 1, <<"first">>),
        _ = recv(S, 2000),
        sub(S, Auth, Topic, Token, 1),
        Received = gen_udp:recv(S, 0, 3500),
        case Received of
            {ok, {_, _, Data}} ->
                ct:pal("After deregistration: ~p", [emqx_coap_SUITE:parse_udp_message(Data)]);
            _ ->
                ok
        end,
        ?assertEqual({error, timeout}, Received)
    end).

-doc "NON notifications share a waiting window across one client.".
t_non_notification_pacing(_) ->
    with_conn(fun(S, Auth) ->
        Topic = <<"review/non-pacing">>,
        sub(S, Auth, Topic, <<"pace">>, 0),
        Start = erlang:monotonic_time(millisecond),
        lists:foreach(
            fun(N) ->
                emqx_coap_SUITE:publish(Topic, 0, integer_to_binary(N))
            end,
            lists:seq(1, 5)
        ),
        Messages = collect(S, 5, []),
        Elapsed = erlang:monotonic_time(millisecond) - Start,
        ct:pal("Received ~p NON notifications in ~p ms without RTT estimation", [
            length(Messages), Elapsed
        ]),
        ?assertEqual(1, length(Messages)),
        Remaining = [recv(S, 5000) || _ <- lists:seq(1, 4)],
        ?assert(lists:all(fun(M) -> M#coap_message.type =:= non end, Remaining)),
        ?assert(erlang:monotonic_time(millisecond) - Start >= 11000)
    end).

-doc "The confirmation timer sends the latest NON representation as CON and stops after ACK.".
t_periodic_non_confirmation(_) ->
    with_confirmation_timer(fun(S, Auth) ->
        Topic = <<"review/confirmation">>,
        Token = <<"confirm">>,
        sub(S, Auth, Topic, Token, 0),
        emqx_coap_SUITE:publish(Topic, 0, <<"first">>),
        ?assertEqual(non, (recv(S, 2000))#coap_message.type),
        emqx_coap_SUITE:publish(Topic, 0, <<"latest">>),
        Probe = recv_confirmable(S),
        ?assertEqual(<<"latest">>, Probe#coap_message.payload),
        ?assertEqual(Token, Probe#coap_message.token),
        send(S, emqx_coap_message:ack(Probe)),
        ?assertEqual({error, timeout}, gen_udp:recv(S, 0, 500)),
        sub(S, Auth, Topic, Token, 1),
        ?assertEqual({error, timeout}, gen_udp:recv(S, 0, 500))
    end).

-doc "Periodic Block2 confirmation uses the current peer after an authenticated UDP migration.".
t_confirmation_after_udp_migration(_) ->
    with_confirmation_timer(fun(S, Auth) ->
        Topic = <<"review/confirm-migrate">>,
        Token = <<"migrate">>,
        sub(S, Auth, Topic, Token, 0),
        emqx_coap_SUITE:publish(
            Topic,
            0,
            emqx_coap_SUITE:blockwise_payload(<<"A">>, <<"B">>, <<"C">>)
        ),
        _ = recv(S, 2000),
        _ = emqx_coap_SUITE:raw_observe_request(S, Auth, Topic, Token, [{block2, {1, false, 16}}]),
        _ = emqx_coap_SUITE:raw_observe_request(S, Auth, Topic, Token, [{block2, {2, false, 16}}]),
        {ok, NewSocket} = gen_udp:open(0, [binary, {active, false}]),
        try
            URI = emqx_coap_SUITE:compose_uri(
                "coap://127.0.0.1/mqtt/connection",
                #{"clientid" => <<"client1">>, "token" => Auth},
                false
            ),
            {ok, changed, _} = emqx_coap_SUITE:raw_udp_request(
                NewSocket,
                URI,
                emqx_coap_SUITE:make_req(put)
            ),
            Probe = recv_confirmable(NewSocket),
            ?assertEqual({0, true, 16}, emqx_coap_message:get_option(block2, Probe)),
            send(NewSocket, emqx_coap_message:ack(Probe)),
            Follow = emqx_coap_SUITE:raw_observe_request(
                NewSocket,
                Auth,
                Topic,
                Token,
                [{block2, {1, false, 16}}]
            ),
            ?assertEqual({ok, content}, Follow#coap_message.method),
            ?assertEqual(binary:copy(<<"B">>, 16), Follow#coap_message.payload)
        after
            gen_udp:close(NewSocket)
        end
    end).

with_confirmation_timer(Fun) ->
    ok = meck:new(emqx_coap_observe_res, [passthrough, no_link, non_strict]),
    ok = meck:expect(emqx_coap_observe_res, confirmation_interval, fun() -> 300 end),
    try
        with_conn(Fun)
    after
        meck:unload(emqx_coap_observe_res)
    end.

recv_confirmable(S) ->
    case recv(S, 10000) of
        #coap_message{type = con} = Msg -> Msg;
        #coap_message{type = non} -> recv_confirmable(S)
    end.

-doc "Administrative cancellation removes mounted observations and their outstanding retransmissions.".
t_api_cancel_with_mountpoint(_) ->
    OldConf = emqx:get_raw_config([gateway, coap]),
    {ok, _} = emqx_coap_SUITE:update_coap_with_mountpoint(<<"mount/">>),
    try
        with_conn(fun(S, Auth) ->
            Topic = <<"api-cancel">>,
            Mounted = <<"mount/api-cancel">>,
            sub(S, Auth, Topic, <<"api">>, 0),
            emqx_coap_SUITE:publish(Mounted, 1, <<"pending">>),
            _ = recv(S, 2000),
            Path = "/gateways/coap/clients/client1/subscriptions",
            {204, _} = emqx_gateway_test_utils:request(delete, Path ++ "/api-cancel"),
            {200, []} = emqx_gateway_test_utils:request(get, Path),
            [Pid] = emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>),
            ?assertEqual([], emqx_broker:subscriptions(Pid)),
            ?assertEqual({error, timeout}, gen_udp:recv(S, 0, 3500))
        end)
    after
        {ok, _} = emqx_gateway_conf:update_gateway(coap, OldConf)
    end.

-doc "Observers retain their own No Local option while sharing broker QoS and lifecycle.".
t_independent_subopts(_) ->
    with_conn(fun(S, Auth) ->
        Topic = <<"review/subopts">>,
        emqx_coap_SUITE:raw_observe_request(
            S,
            Auth,
            Topic,
            <<"local">>,
            [{observe, 0}],
            #{"nl" => <<"1">>, "qos" => <<"0">>}
        ),
        emqx_coap_SUITE:raw_observe_request(
            S,
            Auth,
            Topic,
            <<"all">>,
            [{observe, 0}],
            #{"nl" => <<"0">>, "qos" => <<"1">>}
        ),
        [Pid] = emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>),
        [{Topic, Opts}] = emqx_broker:subscriptions(Pid),
        ?assertMatch(#{qos := 1, nl := 0}, Opts),
        emqx:publish2(emqx_message:make(<<"client1">>, 1, Topic, <<"own">>)),
        Msg = recv(S, 2000),
        ?assertEqual(<<"all">>, Msg#coap_message.token),
        send(S, emqx_coap_message:ack(Msg)),
        ?assertEqual({error, timeout}, gen_udp:recv(S, 0, 300)),
        Mismatched = emqx_coap_SUITE:raw_observe_request(S, Auth, Topic, <<"all">>, [{observe, 1}]),
        ?assertEqual({error, bad_request}, Mismatched#coap_message.method),
        ?assertMatch([{Topic, #{qos := 1, nl := 0}}], emqx_broker:subscriptions(Pid)),
        Cancel = emqx_coap_SUITE:raw_observe_request(
            S, Auth, Topic, <<"all">>, [{observe, 1}], #{"nl" => <<"0">>, "qos" => <<"1">>}
        ),
        ?assertEqual({ok, content}, Cancel#coap_message.method),
        ?assertEqual(undefined, emqx_coap_message:get_option(observe, Cancel, undefined)),
        [{Topic, Remaining}] = emqx_broker:subscriptions(Pid),
        ?assertMatch(#{qos := 0, nl := 1}, Remaining),
        {200, [ApiSub]} = emqx_gateway_test_utils:request(
            get,
            "/gateways/coap/clients/client1/subscriptions"
        ),
        ?assertMatch(#{qos := 0, nl := 1}, ApiSub)
    end).

-doc "Reset removes only one of two accepted observations of the same resource.".
t_cancel_one_token(_) ->
    with_conn(fun(S, Auth) ->
        Topic = <<"review/two-reset">>,
        sub(S, Auth, Topic, <<"first">>, 0),
        sub(S, Auth, Topic, <<"second">>, 0),
        emqx_coap_SUITE:publish(Topic, 1, <<"initial">>),
        First = recv(S, 2000),
        send(S, emqx_coap_message:reset(First)),
        Second = recv(S, 2000),
        ?assertNotEqual(First#coap_message.token, Second#coap_message.token),
        send(S, emqx_coap_message:ack(Second)),
        emqx_coap_SUITE:publish(Topic, 1, <<"remaining">>),
        Next = recv(S, 2000),
        ?assertEqual(Second#coap_message.token, Next#coap_message.token),
        ?assertEqual(<<"remaining">>, Next#coap_message.payload),
        send(S, emqx_coap_message:ack(Next)),
        sub(S, Auth, Topic, Second#coap_message.token, 1),
        [Pid] = emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>),
        ?assertEqual([], emqx_broker:subscriptions(Pid))
    end).

-doc "Registering the same endpoint/Token again updates one observation without duplicating delivery.".
t_repeated_registration(_) ->
    with_conn(fun(S, Auth) ->
        Topic = <<"review/repeated">>,
        Token = <<"repeat">>,
        First = sub(S, Auth, Topic, Token, 0),
        Second = sub(S, Auth, Topic, Token, 0),
        A = emqx_coap_message:get_option(observe, First),
        B = emqx_coap_message:get_option(observe, Second),
        ?assert(((B - A) band 16#FFFFFF) > 0),
        emqx_coap_SUITE:publish(Topic, 1, <<"once">>),
        Msg = recv(S, 2000),
        send(S, emqx_coap_message:ack(Msg)),
        ?assertEqual({error, timeout}, gen_udp:recv(S, 0, 300))
    end).

-doc "CON Reset removes an Observe and its transport and queue state.".
t_con_reset(Config) -> emqx_coap_SUITE:t_observe_con_reset_cancels_relationship(Config).
-doc "NON Reset aborts later CON notifications of the same observation.".
t_non_reset(Config) -> emqx_coap_SUITE:t_observe_non_reset_cancels_relationship(Config).
-doc "A delayed Reset preserves a newly established observation.".
t_old_non_reset(Config) ->
    emqx_coap_SUITE:t_observe_old_non_reset_preserves_new_relationship(Config).

with_conn(Fun) -> emqx_coap_SUITE:with_raw_observe_connection(Fun).
sub(S, Auth, Topic, Token, Value) ->
    Reply = emqx_coap_SUITE:raw_observe_request(S, Auth, Topic, Token, [{observe, Value}]),
    ?assertEqual({ok, content}, Reply#coap_message.method),
    case Value of
        1 -> ?assertEqual(undefined, emqx_coap_message:get_option(observe, Reply, undefined));
        0 -> ok
    end,
    Reply.
recv(S, Timeout) -> emqx_coap_SUITE:recv_raw_observe(S, Timeout).
send(S, Msg) -> emqx_coap_SUITE:send_raw_observe(S, Msg).
wait_pending(Pid) ->
    wait_pending(Pid, 100).
wait_pending(Pid, N) when N > 0 ->
    case proplists:get_value(mqueue_len, emqx_gateway_conn:stats(Pid)) of
        1 ->
            ok;
        _ ->
            timer:sleep(10),
            wait_pending(Pid, N - 1)
    end;
wait_pending(Pid, 0) ->
    ?assertEqual(1, proplists:get_value(mqueue_len, emqx_gateway_conn:stats(Pid))).
collect(_S, 0, Acc) ->
    lists:reverse(Acc);
collect(S, N, Acc) ->
    case gen_udp:recv(S, 0, 200) of
        {ok, {_, _, Data}} -> collect(S, N - 1, [emqx_coap_SUITE:parse_udp_message(Data) | Acc]);
        {error, timeout} -> lists:reverse(Acc)
    end.

-doc "Re-registration returns the latest resource representation.".
t_reregister_current_representation(_) ->
    with_conn(fun(S, Auth) ->
        Topic = <<"review/reregister-current">>,
        Token = <<"refresh">>,
        sub(S, Auth, Topic, Token, 0),
        emqx_coap_SUITE:publish(Topic, 1, <<"current">>),
        Notification = recv(S, 2000),
        send(S, emqx_coap_message:ack(Notification)),
        Reply = sub(S, Auth, Topic, Token, 0),
        ct:pal("Current representation=~p, registration payload=~p", [
            Notification#coap_message.payload, Reply#coap_message.payload
        ]),
        ?assertEqual(<<"current">>, Reply#coap_message.payload)
    end).

-doc "A due confirmation takes the next transmission opportunity and coalesces older states.".
t_confirmation_before_next_non(_) ->
    with_confirmation_timer(fun(S, Auth) ->
        Topic = <<"review/deadline">>,
        sub(S, Auth, Topic, <<"due">>, 0),
        emqx_coap_SUITE:publish(Topic, 0, <<"first">>),
        First = recv(S, 2000),
        ?assertEqual(non, First#coap_message.type),
        with_suspended_channel(fun() ->
            emqx_coap_SUITE:publish(Topic, 0, <<"latest">>),
            emqx_coap_SUITE:publish(Topic, 0, <<"newest">>)
        end),
        Next = recv(S, 5000),
        ct:pal("First eligible notification after confirmation deadline: ~p", [Next]),
        ?assertEqual(con, Next#coap_message.type),
        ?assertEqual(<<"newest">>, Next#coap_message.payload),
        send(S, emqx_coap_message:ack(Next)),
        ?assertEqual({error, timeout}, gen_udp:recv(S, 0, 3300))
    end).

-doc "A full notification queue cannot defer an overdue confirmation behind NON notifications.".
t_confirmation_with_full_queue(_) ->
    with_confirmation_timer(fun(S, Auth) ->
        Topic = <<"review/full-confirmation">>,
        sub(S, Auth, Topic, <<"full">>, 0),
        emqx_coap_SUITE:publish(Topic, 0, <<"first">>),
        ?assertEqual(non, (recv(S, 2000))#coap_message.type),
        with_suspended_channel(fun() ->
            lists:foreach(
                fun(N) -> emqx_coap_SUITE:publish(Topic, 0, integer_to_binary(N)) end,
                lists:seq(1, 101)
            )
        end),
        Next = recv(S, 5000),
        ?assertEqual(con, Next#coap_message.type),
        ?assertEqual(<<"101">>, Next#coap_message.payload),
        send(S, emqx_coap_message:ack(Next)),
        ?assertEqual({error, timeout}, gen_udp:recv(S, 0, 3300))
    end).

-doc "After an idle period, the next overdue NON update is immediately sent as CON.".
t_confirmation_after_idle(_) ->
    with_confirmation_timer(fun(S, Auth) ->
        Topic = <<"review/idle-confirmation">>,
        sub(S, Auth, Topic, <<"idle">>, 0),
        emqx_coap_SUITE:publish(Topic, 0, <<"first">>),
        ?assertEqual(non, (recv(S, 2000))#coap_message.type),
        Probe = recv_confirmable(S),
        send(S, emqx_coap_message:ack(Probe)),
        ?assertEqual({error, timeout}, gen_udp:recv(S, 0, 500)),
        emqx_coap_SUITE:publish(Topic, 0, <<"after-idle">>),
        Next = recv(S, 2000),
        ?assertEqual(con, Next#coap_message.type),
        ?assertEqual(<<"after-idle">>, Next#coap_message.payload),
        send(S, emqx_coap_message:ack(Next))
    end).

with_suspended_channel(Fun) ->
    [Pid] = emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>),
    ok = sys:suspend(Pid),
    try
        Fun()
    after
        ok = sys:resume(Pid)
    end.
