%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
%% @doc Connector process tests with emqtt mocked via meck.
%%
%% We mock emqtt so no real TCP connections are made. The test process
%% acts as the buffer, sending publish_batch messages and receiving
%% {batch_ack, Ref, ok} replies.
%%--------------------------------------------------------------------
-module(emqx_bridge_mqtt_dq_connector_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Port = emqx_common_test_helpers:select_free_port(tcp),
    Apps = emqx_cth_suite:start(
        [
            {emqx, #{
                config => #{
                    listeners => #{
                        tcp => #{default => #{bind => "127.0.0.1:" ++ integer_to_list(Port)}},
                        ssl => #{default => #{enable => false}},
                        ws => #{default => #{enable => false}},
                        wss => #{default => #{enable => false}}
                    }
                }
            }}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)),
    Config.

init_per_testcase(_Case, Config) ->
    setup_emqtt_mock(),
    Config.

end_per_testcase(_Case, _Config) ->
    meck:unload(emqtt),
    ok.

%%--------------------------------------------------------------------
%% Tests
%%--------------------------------------------------------------------

-doc "All items published successfully; batch acked back to buffer.".
t_batch_ack_on_all_items_published(_Config) ->
    Conn = start_connector(#{}),
    wait_connected(),
    Ref = make_ref(),
    Conn ! {publish_batch, [make_item(I) || I <- lists:seq(1, 5)], self(), Ref},
    receive
        {batch_ack, Ref, ok} -> ok
    after 3000 ->
        ct:fail("batch_ack not received")
    end,
    stop_connector(Conn).

-doc "PUBACK error triggers retry; succeeds on third attempt.".
t_retry_on_puback_error(_Config) ->
    AttemptCounter = counters:new(1, [atomics]),
    meck:expect(emqtt, publish_async, fun(
        _Pid, _Topic, _Props, _Payload, _Opts, _Timeout, {Cb, Args}
    ) ->
        counters:add(AttemptCounter, 1, 1),
        Result =
            case counters:get(AttemptCounter, 1) =< 2 of
                true -> {error, {puback_error, 131}};
                false -> ok
            end,
        erlang:apply(Cb, Args ++ [Result]),
        ok
    end),
    Conn = start_connector(#{}),
    wait_connected(),
    Ref = make_ref(),
    Conn ! {publish_batch, [make_item(1)], self(), Ref},
    receive
        {batch_ack, Ref, ok} -> ok
    after 3000 ->
        ct:fail("batch_ack not received after retries")
    end,
    ?assertEqual(3, counters:get(AttemptCounter, 1)),
    stop_connector(Conn).

-doc "Default max_publish_retries = -1 means retry forever and do not batch-ack on repeated errors.".
t_infinite_retries_by_default(_Config) ->
    AttemptCounter = counters:new(1, [atomics]),
    meck:expect(emqtt, publish_async, fun(
        _Pid, _Topic, _Props, _Payload, _Opts, _Timeout, {Cb, Args}
    ) ->
        counters:add(AttemptCounter, 1, 1),
        erlang:apply(Cb, Args ++ [{error, {puback_error, 131}}]),
        ok
    end),
    Conn = start_connector(#{}),
    wait_connected(),
    Ref = make_ref(),
    Conn ! {publish_batch, [make_item(1)], self(), Ref},
    receive
        {batch_ack, Ref, ok} ->
            ct:fail("batch_ack unexpectedly received under infinite retries")
    after 3500 ->
        ok
    end,
    ?assert(counters:get(AttemptCounter, 1) >= 3),
    stop_connector(Conn).

-doc "Finite max_publish_retries drops the item after retries are exhausted; batch still completes.".
t_drop_after_max_retries(_Config) ->
    meck:expect(emqtt, publish_async, fun(
        _Pid, _Topic, _Props, _Payload, _Opts, _Timeout, {Cb, Args}
    ) ->
        erlang:apply(Cb, Args ++ [{error, {puback_error, 131}}]),
        ok
    end),
    Conn = start_connector(#{max_publish_retries => 3}),
    wait_connected(),
    Ref = make_ref(),
    Conn ! {publish_batch, [make_item(1)], self(), Ref},
    receive
        {batch_ack, Ref, ok} -> ok
    after 3000 ->
        ct:fail("batch_ack not received after drop")
    end,
    stop_connector(Conn).

-doc "At most max_inflight items dispatched concurrently to emqtt.".
t_inflight_cap_respected(_Config) ->
    TestPid = self(),
    Holder = spawn_link(fun() -> hold_callbacks(TestPid, []) end),
    meck:expect(emqtt, publish_async, fun(
        _Pid, _Topic, _Props, _Payload, _Opts, _Timeout, {Cb, Args}
    ) ->
        Holder ! {hold, Cb, Args},
        ok
    end),
    Conn = start_connector(#{max_inflight => 2}),
    wait_connected(),
    Ref = make_ref(),
    Conn ! {publish_batch, [make_item(I) || I <- lists:seq(1, 5)], self(), Ref},
    wait_held_callbacks(2),
    %% Exactly 2 should be held (inflight cap)
    Holder ! {get_count, self()},
    receive
        {count, 2} -> ok;
        {count, N} -> ct:fail("expected 2 held callbacks, got ~p", [N])
    after 1000 ->
        ct:fail("no count reply")
    end,
    %% Release in rounds until batch completes
    release_until_batch_ack(Holder, Ref, 10),
    Holder ! stop,
    stop_connector(Conn).

-doc "Inflight items lose one retry credit on client EXIT; re-dispatched after reconnect.".
t_client_exit_decrements_retries(_Config) ->
    TestPid = self(),
    ClientHolder = spawn(fun() -> client_holder_loop(undefined) end),
    meck:expect(emqtt, start_link, fun(_Opts) ->
        ClientHolder ! {new_client, self()},
        receive
            {client_pid, Pid} ->
                link(Pid),
                {ok, Pid}
        after 5000 -> {error, timeout}
        end
    end),
    meck:expect(emqtt, connect, fun(Pid) ->
        TestPid ! {emqtt_connect, Pid},
        {ok, #{}}
    end),
    %% Hold all callbacks so items stay inflight
    Holder = spawn_link(fun() -> hold_callbacks(TestPid, []) end),
    meck:expect(emqtt, publish_async, fun(
        _Pid, _Topic, _Props, _Payload, _Opts, _Timeout, {Cb, Args}
    ) ->
        Holder ! {hold, Cb, Args},
        ok
    end),
    Conn = start_connector(#{max_inflight => 3}),
    wait_connected(),
    Ref = make_ref(),
    Conn ! {publish_batch, [make_item(I) || I <- lists:seq(1, 3)], self(), Ref},
    wait_held_callbacks(3),
    %% All 3 should be dispatched and held
    Holder ! {get_count, self()},
    receive
        {count, 3} -> ok
    after 1000 -> ct:fail("expected 3 held")
    end,
    %% Kill the current fake client
    ClientHolder ! {kill_current, connection_closed},
    %% Connector reconnects (new client from holder), re-dispatches with retries-1
    wait_connected(7000),
    wait_held_callbacks(3),
    %% Release all held callbacks (stale ones from dead client + new ones)
    release_until_batch_ack(Holder, Ref, 10),
    Holder ! stop,
    ClientHolder ! stop,
    stop_connector(Conn).

-doc "Connector survives emqtt EXIT, reconnects, and continues publishing.".
t_emqtt_exit_and_reconnect(_Config) ->
    TestPid = self(),
    ClientHolder = spawn(fun() -> client_holder_loop(undefined) end),
    meck:expect(emqtt, start_link, fun(_Opts) ->
        ClientHolder ! {new_client, self()},
        receive
            {client_pid, Pid} ->
                link(Pid),
                {ok, Pid}
        after 5000 -> {error, timeout}
        end
    end),
    meck:expect(emqtt, connect, fun(Pid) ->
        TestPid ! {emqtt_connect, Pid},
        {ok, #{}}
    end),
    Conn = start_connector(#{}),
    wait_connected(),
    %% First batch — should succeed
    Ref1 = make_ref(),
    Conn ! {publish_batch, [make_item(1)], self(), Ref1},
    receive
        {batch_ack, Ref1, ok} -> ok
    after 3000 -> ct:fail("first batch_ack not received")
    end,
    %% Kill the emqtt client
    ClientHolder ! {kill_current, connection_closed},
    %% Connector should reconnect automatically
    wait_connected(7000),
    %% Second batch after reconnect — should also succeed
    Ref2 = make_ref(),
    Conn ! {publish_batch, [make_item(2)], self(), Ref2},
    receive
        {batch_ack, Ref2, ok} -> ok
    after 3000 -> ct:fail("second batch_ack not received after reconnect")
    end,
    ?assert(is_process_alive(Conn)),
    ClientHolder ! stop,
    stop_connector(Conn).

-doc "Two batches from different buffers are tracked and acked independently.".
t_multiple_batches_interleaved(_Config) ->
    Conn = start_connector(#{}),
    wait_connected(),
    Ref1 = make_ref(),
    Ref2 = make_ref(),
    Conn ! {publish_batch, [make_item(I) || I <- lists:seq(1, 3)], self(), Ref1},
    Conn ! {publish_batch, [make_item(I) || I <- lists:seq(4, 6)], self(), Ref2},
    Acks = collect_batch_acks(2, 5000),
    AckedRefs = [R || {R, ok} <- Acks],
    ?assert(lists:member(Ref1, AckedRefs)),
    ?assert(lists:member(Ref2, AckedRefs)),
    stop_connector(Conn).

-doc "MQTT reason codes 0 (success) and 16 (no subscribers) are not retried.".
t_success_reason_codes_not_retried(_Config) ->
    AttemptCounter = counters:new(1, [atomics]),
    meck:expect(emqtt, publish_async, fun(
        _Pid, _Topic, _Props, _Payload, _Opts, _Timeout, {Cb, Args}
    ) ->
        counters:add(AttemptCounter, 1, 1),
        erlang:apply(Cb, Args ++ [{ok, #{reason_code => 16}}]),
        ok
    end),
    Conn = start_connector(#{}),
    wait_connected(),
    Ref = make_ref(),
    Conn ! {publish_batch, [make_item(1), make_item(2)], self(), Ref},
    receive
        {batch_ack, Ref, ok} -> ok
    after 3000 ->
        ct:fail("batch_ack not received")
    end,
    ?assertEqual(2, counters:get(AttemptCounter, 1)),
    stop_connector(Conn).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

make_item(Seq) ->
    #{
        topic => <<"test/topic">>,
        payload => integer_to_binary(Seq),
        qos => 1,
        retain => false,
        timestamp => erlang:system_time(millisecond),
        properties => #{}
    }.

make_connector_config(Overrides) ->
    Default = #{
        name => <<"conn_test">>,
        enable => true,
        server => "127.0.0.1:1883",
        proto_ver => v4,
        clientid_prefix => <<"test_conn">>,
        username => <<>>,
        password => <<>>,
        keepalive_s => 60,
        ssl => #{enable => false},
        pool_size => 1,
        buffer_pool_size => 1,
        max_inflight => 32,
        filter_topic => <<"#">>,
        remote_topic => <<"${topic}">>,
        enqueue_timeout_ms => 5000,
        remote_qos => 1,
        remote_retain => false,
        queue_base_dir => <<"/tmp/unused">>,
        seg_bytes => 1048576,
        max_total_bytes => 10485760
    },
    maps:merge(Default, Overrides).

start_connector(Overrides) ->
    Config = make_connector_config(Overrides),
    {ok, Pid} = emqx_bridge_mqtt_dq_connector:start_link(Config, 0),
    unlink(Pid),
    Pid.

stop_connector(Pid) ->
    MRef = monitor(process, Pid),
    exit(Pid, shutdown),
    receive
        {'DOWN', MRef, process, Pid, _} -> ok
    after 5000 ->
        ct:fail("connector did not stop")
    end.

wait_connected() ->
    wait_connected(3000).

wait_connected(Timeout) ->
    receive
        {emqtt_connect, _Pid} -> ok
    after Timeout ->
        ct:fail("connector did not connect")
    end.

setup_emqtt_mock() ->
    TestPid = self(),
    meck:new(emqtt, [passthrough, no_link]),
    meck:expect(emqtt, start_link, fun(_Opts) ->
        Pid = spawn(fun() -> fake_client_loop() end),
        {ok, Pid}
    end),
    meck:expect(emqtt, connect, fun(Pid) ->
        TestPid ! {emqtt_connect, Pid},
        {ok, #{}}
    end),
    meck:expect(emqtt, publish_async, fun(
        _Pid, _Topic, _Props, _Payload, _Opts, _Timeout, {Cb, Args}
    ) ->
        erlang:apply(Cb, Args ++ [ok]),
        ok
    end),
    meck:expect(emqtt, info, fun(_Pid, _Attr) -> 0 end),
    meck:expect(emqtt, disconnect, fun(_Pid) -> ok end),
    meck:expect(emqtt, stop, fun(_Pid) -> ok end),
    ok.

fake_client_loop() ->
    receive
        stop -> ok;
        _ -> fake_client_loop()
    end.

%% Manages fake client pids for connector tests that need to kill/replace clients.
client_holder_loop(CurrentClient) ->
    receive
        {new_client, From} ->
            Pid = spawn(fun() -> fake_client_loop() end),
            From ! {client_pid, Pid},
            client_holder_loop(Pid);
        {kill_current, Reason} ->
            case CurrentClient of
                undefined -> ok;
                Pid -> exit(Pid, Reason)
            end,
            client_holder_loop(undefined);
        stop ->
            ok
    end.

%% A process that holds callbacks until told to release them.
hold_callbacks(Owner, Held) ->
    receive
        {hold, Cb, Args} ->
            Owner ! held_callback,
            hold_callbacks(Owner, [{Cb, Args} | Held]);
        {get_count, From} ->
            From ! {count, length(Held)},
            hold_callbacks(Owner, Held);
        {release_all, Result} ->
            lists:foreach(
                fun({Cb, Args}) ->
                    erlang:apply(Cb, Args ++ [Result])
                end,
                Held
            ),
            hold_callbacks(Owner, []);
        stop ->
            ok
    end.

wait_held_callbacks(0) ->
    ok;
wait_held_callbacks(N) ->
    receive
        held_callback -> wait_held_callbacks(N - 1)
    after 3000 ->
        ct:fail("expected ~p held callbacks", [N])
    end.

%% Release held callbacks in rounds until batch_ack arrives.
release_until_batch_ack(_Holder, _Ref, 0) ->
    ct:fail("batch_ack not received after max release rounds");
release_until_batch_ack(Holder, Ref, N) ->
    Holder ! {release_all, ok},
    receive
        {batch_ack, Ref, ok} -> ok
    after 300 ->
        release_until_batch_ack(Holder, Ref, N - 1)
    end.

collect_batch_acks(0, _Timeout) ->
    [];
collect_batch_acks(N, Timeout) ->
    receive
        {batch_ack, Ref, Result} ->
            [{Ref, Result} | collect_batch_acks(N - 1, Timeout)]
    after Timeout ->
        ct:fail("expected ~p batch_ack(s), timed out", [N])
    end.
