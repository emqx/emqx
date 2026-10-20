%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_extsub_delivery_SUITE).

-moduledoc """
Tests of ExtSub delivery tracking: acknowledgements, NACKs, session drops, the delivery
rate limit and handler removal.

The suite module is the ExtSub handler under test. The handler reports its callbacks
to the test process.

The `client` and `rate_limited` groups use MQTT clients. The test sends messages for the
handler to the channel process.

The `unit` group covers checks that no client can reach. The test process acts as the
channel process and runs the ExtSub hooks directly.
""".

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-include("../src/emqx_extsub_internal.hrl").

-define(TOPIC, <<"extsub_delivery/t">>).
-define(TEST_PID_KEY, {?MODULE, test_pid}).

all() ->
    emqx_common_test_helpers:all_with_matrix(?MODULE).

groups() ->
    emqx_common_test_helpers:groups_with_matrix(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [emqx, emqx_extsub],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_group(rate_limited, Config) ->
    ok = set_delivery_rate(<<"1/1h">>),
    Config;
init_per_group(_Group, Config) ->
    Config.

end_per_group(rate_limited, _Config) ->
    ok = set_delivery_rate(<<"infinity">>);
end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    ok = snabbkaffe:start_trace(),
    _ = persistent_term:put(?TEST_PID_KEY, self()),
    ok = emqx_extsub_handler_registry:register(?MODULE, #{
        handle_generic_messages => true, multi_topic => false
    }),
    [{clientid, atom_to_binary(TestCase)} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok = emqx_common_test_helpers:call_janitor(),
    ok = emqx_extsub_handler_registry:unregister(?MODULE),
    _ = persistent_term:erase(?TEST_PID_KEY),
    ok = snabbkaffe:stop().

%%--------------------------------------------------------------------
%% Test cases: MQTT clients
%%--------------------------------------------------------------------

t_stale_completion() ->
    [{matrix, true}].

-doc """
After takeover, the acknowledgement of a replayed message does not change the new delivery
that has the same sequence number.
""".
t_stale_completion(matrix) ->
    [[client]];
t_stale_completion(Config) ->
    %% The first client receives a message and does not acknowledge it
    {C0, ChanPid0} = connect_and_subscribe(Config, []),
    true = unlink(C0),
    ok = handler_deliver(ChanPid0, [message(<<"old">>)]),
    #{packet_id := PacketIdOld} = receive_publish(C0, <<"old">>),

    %% The second client takes over the session and receives the replayed message
    {C1, ChanPid1} = connect(Config, []),
    ?assertMatch(#{packet_id := PacketIdOld, dup := true}, receive_publish(C1, <<"old">>)),

    %% The new handler delivers a message with the same sequence number
    ok = handler_deliver(ChanPid1, [message(<<"new">>)]),
    #{packet_id := PacketIdNew} = receive_publish(C1, <<"new">>),
    ?assertMatch({ok, #{unacked_count := 1}}, emqx_extsub:inspect(ChanPid1)),

    %% The acknowledgement of the replayed message changes nothing
    ok = puback(C1, PacketIdOld),
    ?assertMatch({ok, #{unacked_count := 1}}, emqx_extsub:inspect(ChanPid1)),
    assert_no_handle_delivered(),

    %% The handler receives the acknowledgement of the new message
    ok = puback(C1, PacketIdNew),
    assert_handle_delivered(#{<<"new">> => ?RC_SUCCESS}),
    ?assertMatch({ok, #{unacked_count := 0}}, emqx_extsub:inspect(ChanPid1)),

    %% Clean up
    ok = emqtt:disconnect(C1).

t_dropped_no_local() ->
    [{matrix, true}].

-doc "The handler receives {dropped, no_local} for a message that No Local drops.".
t_dropped_no_local(matrix) ->
    [[client]];
t_dropped_no_local(Config) ->
    ClientId = ?config(clientid, Config),
    {C, ChanPid} = connect_and_subscribe(Config, [], [{nl, true}]),
    ok = handler_deliver(ChanPid, [message(ClientId, <<"own">>)]),
    assert_handle_delivered(#{<<"own">> => {dropped, no_local}}),
    ?assertMatch({ok, #{unacked_count := 0}}, emqx_extsub:inspect(ChanPid)),
    assert_no_publish(),
    ok = emqtt:disconnect(C).

t_dropped_expired_on_retry() ->
    [{matrix, true}].

-doc "The handler receives {dropped, expired} for an inflight message that expires before retry.".
t_dropped_expired_on_retry(matrix) ->
    [[client]];
t_dropped_expired_on_retry(Config) ->
    ok = put_zone_conf(retry_interval, 2000),
    {C, ChanPid} = connect_and_subscribe(Config, []),

    %% The client receives the message and does not acknowledge it
    ok = handler_deliver(ChanPid, [expiring_message(<<"expiring">>)]),
    _ = receive_publish(C, <<"expiring">>),

    %% The delivery retry finds the message expired
    assert_handle_delivered(#{<<"expiring">> => {dropped, expired}}, 5000),
    ?assertMatch({ok, #{unacked_count := 0}}, emqx_extsub:inspect(ChanPid)),
    ok = emqtt:disconnect(C).

t_unsubscribe_drops_buffered() ->
    [{matrix, true}].

-doc "Unsubscribing from the last topic filter of a handler removes its buffered messages.".
t_unsubscribe_drops_buffered(matrix) ->
    [[client]];
t_unsubscribe_drops_buffered(Config) ->
    %% A full inflight window returns the messages to the handler buffer
    {C, ChanPid} = connect_and_subscribe(Config, [{properties, #{'Receive-Maximum' => 1}}]),
    ok = fill_buffer(ChanPid),

    %% The unsubscribe terminates the handler and removes its buffered messages
    {ok, _, _} = emqtt:unsubscribe(C, ?TOPIC),
    assert_terminated(),
    ?assertMatch(
        {ok, #{
            registry := #{by_ref := [], buffer := #{message_buffer_size := 0, delivering := #{}}}
        }},
        emqx_extsub:inspect(ChanPid)
    ),
    ok = emqtt:disconnect(C).

t_recreate_drops_old_handler() ->
    [{matrix, true}].

-doc "Recreate terminates the old handler and removes its buffered messages and counters.".
t_recreate_drops_old_handler(matrix) ->
    [[client]];
t_recreate_drops_old_handler(Config) ->
    %% A full inflight window returns the messages to the handler buffer
    {C, ChanPid} = connect_and_subscribe(Config, [{properties, #{'Receive-Maximum' => 1}}]),
    ok = fill_buffer(ChanPid),
    {ok, #{registry := #{by_ref := [OldRef]}}} = emqx_extsub:inspect(ChanPid),

    %% Recreate replaces the old handler and removes its state
    ChanPid ! {?MODULE, recreate},
    assert_terminated(),
    {ok, #{
        registry := #{
            by_ref := [NewRef],
            generic_message_handlers := GenericHandlers,
            buffer := #{message_buffer_size := BufferSize, delivering := Delivering}
        }
    }} = emqx_extsub:inspect(ChanPid),
    ?assertNotEqual(OldRef, NewRef),
    ?assertEqual([NewRef], GenericHandlers),
    ?assertEqual(0, BufferSize),
    ?assertNot(maps:is_key(OldRef, Delivering)),
    ok = emqtt:disconnect(C).

%%--------------------------------------------------------------------
%% Test cases: MQTT clients with a delivery rate limit
%%--------------------------------------------------------------------

t_rate_limited_returns_to_buffer() ->
    [{matrix, true}].

-doc """
Messages that the delivery rate limit refuses go back to the ExtSub buffer and do not
enter the session mqueue. The handler receives no drop for them.
""".
t_rate_limited_returns_to_buffer(matrix) ->
    [[rate_limited]];
t_rate_limited_returns_to_buffer(Config) ->
    %% The first message takes the only token. The rate limit refuses the other messages.
    {C, ChanPid} = connect_and_subscribe(Config, []),
    ok = handler_deliver(ChanPid, [
        message(<<"sent">>), message(<<"refused">>), qos0_message(<<"qos0">>)
    ]),
    _ = receive_publish(C, <<"sent">>),

    %% The refused messages are in the ExtSub buffer, not in the mqueue
    ?assertMatch(
        {ok, #{unacked_count := 1, registry := #{buffer := #{message_buffer_size := 2}}}},
        emqx_extsub:inspect(ChanPid)
    ),
    assert_no_publish(),
    assert_no_handle_delivered(),
    ok = emqtt:disconnect(C).

%%--------------------------------------------------------------------
%% Test cases: ExtSub hooks in the test process
%%--------------------------------------------------------------------

t_duplicate_completion() ->
    [{matrix, true}].

-doc "A second acknowledgement does not call handle_delivered and does not change the ExtSub state.".
t_duplicate_completion(matrix) ->
    [[unit]];
t_duplicate_completion(_Config) ->
    %% The handler receives the first acknowledgement
    ok = hook_subscribe(),
    [Msg] = hook_deliver([message(<<"msg">>)]),
    ok = emqx_hooks:run('delivery.completed', [Msg, #{}]),
    assert_handle_delivered(#{<<"msg">> => ?RC_SUCCESS}),
    Info0 = hook_inspect(),

    %% The second acknowledgement changes nothing
    ok = emqx_hooks:run('delivery.completed', [Msg, #{}]),
    ?assertEqual(Info0, hook_inspect()),
    assert_no_handle_delivered().

t_removed_handler_nack() ->
    [{matrix, true}].

-doc "A NACK for a removed handler removes the delivery from unacked and does not buffer the message.".
t_removed_handler_nack(matrix) ->
    [[unit]];
t_removed_handler_nack(_Config) ->
    ok = hook_subscribe(),
    [Msg] = hook_deliver([message(<<"msg">>)]),
    ok = emqx_hooks:run('session.unsubscribed', [#{}, ?TOPIC, #{}]),
    ?assert(hook_nack(Msg)),
    ?assertMatch(
        #{
            unacked_count := 0,
            deliver_retry_scheduled := false,
            registry := #{buffer := #{message_buffer_size := 0}}
        },
        hook_inspect()
    ).

%%--------------------------------------------------------------------
%% Helpers: MQTT clients
%%--------------------------------------------------------------------

connect(Config, Opts) ->
    C = emqx_extsub_test_utils:emqtt_connect(
        [
            {clientid, ?config(clientid, Config)},
            {auto_ack, false},
            {clean_start, false},
            {properties, #{'Session-Expiry-Interval' => 1000}}
        ] ++ Opts
    ),
    [ChanPid] = emqx_cm:lookup_channels(?config(clientid, Config)),
    {C, ChanPid}.

connect_and_subscribe(Config, Opts) ->
    connect_and_subscribe(Config, Opts, []).

connect_and_subscribe(Config, Opts, SubOpts) ->
    {C, ChanPid} = connect(Config, Opts),
    {ok, _, _} = emqtt:subscribe(C, ?TOPIC, [{qos, ?QOS_1} | SubOpts]),
    {C, ChanPid}.

%% Make the handler in the channel return the messages for delivery.
handler_deliver(ChanPid, Msgs) ->
    ChanPid ! {?MODULE, {messages, Msgs}},
    ok.

%% Deliver as many messages as ExtSub allows, so that the inflight window is full and
%% the session returns the other messages to the handler buffer.
fill_buffer(ChanPid) ->
    MaxUnacked = emqx_extsub:max_unacked(),
    Msgs = [message(integer_to_binary(N)) || N <- lists:seq(1, MaxUnacked + 1)],
    ok = handler_deliver(ChanPid, Msgs),
    ?retry(
        100,
        10,
        ?assertMatch(
            {ok, #{registry := #{buffer := #{message_buffer_size := BufferSize}}}} when
                BufferSize > 0,
            emqx_extsub:inspect(ChanPid)
        )
    ),
    ok.

puback(C, PacketId) ->
    {_, {ok, _}} = ?wait_async_action(
        emqtt:puback(C, PacketId),
        #{?snk_kind := extsub_delivery_completed},
        1000
    ),
    ok.

receive_publish(C, Payload) ->
    receive
        {publish, #{client_pid := C, payload := Payload} = Publish} -> Publish
    after 3000 -> ct:fail({publish_timeout, Payload})
    end.

assert_no_publish() ->
    receive
        {publish, Publish} -> ct:fail({unexpected_publish, Publish})
    after 300 -> ok
    end.

put_zone_conf(Key, Value) ->
    OldValue = emqx_config:get_zone_conf(default, [mqtt, Key]),
    ok = emqx_config:put_zone_conf(default, [mqtt, Key], Value),
    emqx_common_test_helpers:on_exit(fun() ->
        emqx_config:put_zone_conf(default, [mqtt, Key], OldValue)
    end).

set_delivery_rate(Rate) ->
    {ok, _} = emqx:update_config(
        [listeners, tcp, default],
        {update, #{<<"delivery_messages_rate">> => Rate}}
    ),
    ok.

%%--------------------------------------------------------------------
%% Helpers: ExtSub hooks in the test process
%%--------------------------------------------------------------------

hook_subscribe() ->
    emqx_hooks:run('session.subscribed', [#{}, ?TOPIC, #{}]).

%% Make the handler return the messages, and return the messages that ExtSub delivers.
hook_deliver(Msgs) ->
    #{deliver := Delivers} = emqx_hooks:run_fold(
        'client.handle_info',
        #{
            session_info_fn => fun(_) -> 0 end,
            chan_info_fn => fun(_) -> #{} end
        },
        [#{}, {?MODULE, {messages, Msgs}}],
        #{deliver => []}
    ),
    [Msg || {deliver, _Topic, Msg} <- Delivers].

hook_nack(Msg) ->
    emqx_hooks:run_fold('message.nack', [Msg], false).

hook_inspect() ->
    _ = emqx_extsub:on_client_handle_info(#{}, #info_extsub_inspect{receiver = self()}, #{}),
    receive
        {_Receiver, Info} when is_map(Info) -> Info
    after 1000 -> ct:fail(inspect_timeout)
    end.

%%--------------------------------------------------------------------
%% Helpers: messages and handler reports
%%--------------------------------------------------------------------

message(Payload) ->
    message(<<"publisher">>, Payload).

message(From, Payload) ->
    emqx_message:make(From, ?QOS_1, ?TOPIC, Payload).

qos0_message(Payload) ->
    emqx_message:make(<<"publisher">>, ?QOS_0, ?TOPIC, Payload).

expiring_message(Payload) ->
    emqx_message:set_header(properties, #{'Message-Expiry-Interval' => 1}, message(Payload)).

%% Wait for one handle_delivered call for each message, identified by its payload.
assert_handle_delivered(Expected) ->
    assert_handle_delivered(Expected, 3000).

assert_handle_delivered(Expected, Timeout) ->
    ?assertEqual(Expected, receive_handle_delivered(maps:size(Expected), Timeout, #{})),
    assert_no_handle_delivered().

receive_handle_delivered(0, _Timeout, Acc) ->
    Acc;
receive_handle_delivered(N, Timeout, Acc) ->
    receive
        {handle_delivered, #message{payload = Payload}, Ack} ->
            receive_handle_delivered(N - 1, Timeout, Acc#{Payload => Ack})
    after Timeout -> ct:fail({handle_delivered_timeout, Acc})
    end.

assert_no_handle_delivered() ->
    receive
        {handle_delivered, _, _} = Report -> ct:fail({unexpected_handle_delivered, Report})
    after 0 -> ok
    end.

assert_terminated() ->
    receive
        terminated -> ok
    after 3000 -> ct:fail(terminate_timeout)
    end.

notify(Report) ->
    persistent_term:get(?TEST_PID_KEY) ! Report,
    ok.

%%--------------------------------------------------------------------
%% ExtSub handler callbacks
%%--------------------------------------------------------------------

handle_subscribe(_SubscribeType, _SubscribeCtx, _State, ?TOPIC) ->
    {ok, state};
handle_subscribe(_SubscribeType, _SubscribeCtx, _State, _TopicFilter) ->
    ignore.

handle_unsubscribe(_UnsubscribeType, _UnsubscribeCtx, State, _TopicFilter) ->
    State.

handle_terminate(_State) ->
    notify(terminated).

handle_info(State, _InfoCtx, {generic, {?MODULE, {messages, Msgs}}}) ->
    {ok, State, Msgs};
handle_info(_State, _InfoCtx, {generic, {?MODULE, recreate}}) ->
    recreate;
handle_info(State, _InfoCtx, {generic, _Info}) ->
    {ok, State}.

handle_delivered(State, _AckCtx, Msg, Ack) ->
    ok = notify({handle_delivered, Msg, Ack}),
    {ok, State}.
