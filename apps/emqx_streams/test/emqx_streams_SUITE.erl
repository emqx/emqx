%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_SUITE).

-moduledoc """
Main test suite for the Streams application.
""".

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-include("../src/emqx_streams_internal.hrl").

-define(PUBLISH_AND_CONSUME_CASES, [
    t_publish_and_consume_regular_many_generations,
    t_publish_and_consume_lastvalue
]).

-define(READ_CASES, [
    t_read_earliest,
    t_read_latest,
    t_read_timestamp
]).

-define(MANY, 999_999_999_999).

all() ->
    All = emqx_common_test_helpers:all(?MODULE) -- (?PUBLISH_AND_CONSUME_CASES ++ ?READ_CASES),
    [{group, pub_and_consume}, {group, read}] ++ All.

groups() ->
    emqx_common_test_helpers:nested_groups([
        [pub_and_consume],
        [limited, unlimited],
        [subscribe_all, subscribe_shard],
        ?PUBLISH_AND_CONSUME_CASES
    ]) ++
        emqx_common_test_helpers:nested_groups([
            [read],
            [subscribe_all, subscribe_shard],
            ?READ_CASES
        ]).

init_per_suite(Config) ->
    Apps =
        emqx_cth_suite:start(
            [
                emqx_durable_storage,
                {emqx, emqx_streams_test_utils:cth_config(emqx)},
                {emqx_mq, emqx_streams_test_utils:cth_config(emqx_mq)},
                {emqx_streams, emqx_streams_test_utils:cth_config(emqx_streams)}
            ],
            #{work_dir => emqx_cth_suite:work_dir(Config)}
        ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_group(limited, Config) ->
    [{limits, #{max_shard_message_bytes => ?MANY, max_shard_message_count => ?MANY}} | Config];
init_per_group(unlimited, Config) ->
    [
        {limits, #{max_shard_message_bytes => infinity, max_shard_message_count => infinity}}
        | Config
    ];
init_per_group(subscribe_all, Config) ->
    [{subscribe, all} | Config];
init_per_group(subscribe_shard, Config) ->
    [{subscribe, shard} | Config];
init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_CaseName, Config) ->
    ok = emqx_streams_test_utils:cleanup_streams(),
    ok = snabbkaffe:start_trace(),
    ExtSubMaxUnacked = emqx_extsub:max_unacked(),
    LimitOpts =
        case ?config(limits, Config) of
            undefined -> #{};
            Limits -> #{limits => Limits}
        end,
    [{ext_sub_max_unacked, ExtSubMaxUnacked}, {limits, LimitOpts} | Config].

end_per_testcase(_CaseName, Config) ->
    ok = snabbkaffe:stop(),
    ok = emqx_streams_test_utils:cleanup_streams(),
    ok = emqx_streams_test_utils:reset_config(),
    ok = emqx_streams_test_utils:reregister_extsub_handler(#{}),
    ok = emqx_extsub:set_max_unacked(?config(ext_sub_max_unacked, Config)).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

%% Very basic test, publish some messages to the stream and look into the DB
%% to verify that the messages are stored.
t_smoke(_Config) ->
    Stream = emqx_streams_test_utils:create_stream(#{topic_filter => <<"t/#">>}),
    ok = emqx_streams_test_utils:populate(10, #{topic_prefix => <<"t/">>}),
    AllMessages = emqx_streams_message_db:dirty_read_all(Stream),
    ?assertEqual(10, length(AllMessages)),
    ok.

%% Verify reading stream messages from the earliest timestamp.
t_read_earliest(Config) ->
    %% Create a stream
    _Stream = emqx_streams_test_utils:create_stream(#{topic_filter => <<"t/#">>}),
    ok = emqx_streams_test_utils:populate(50, #{topic_prefix => <<"t/">>, different_clients => true}),

    %% Subscribe to the stream, either to all shards at once or to each shard separately.
    CSub = emqx_streams_test_utils:emqtt_connect([]),
    case ?config(subscribe, Config) of
        all ->
            emqx_streams_test_utils:emqtt_sub(CSub, [<<"$s/earliest/t/#">>]);
        shard ->
            emqx_streams_test_utils:emqtt_sub(
                CSub,
                [<<"$sp/0/earliest/t/#">>, <<"$sp/1/earliest/t/#">>]
            )
    end,

    %% Drain the messages from the stream and verify that they are received.
    {ok, Msgs0} = emqx_streams_test_utils:emqtt_drain(_MinMsg0 = 50, _Timeout1 = 500),
    ok = validate_headers(Msgs0),

    %% Publish more messages while the clients are still subscribed.
    %% We should receive all the messages.
    ok = emqx_streams_test_utils:populate(50, #{topic_prefix => <<"t/">>, different_clients => true}),
    {ok, Msgs1} = emqx_streams_test_utils:emqtt_drain(_MinMsg1 = 50, _Timeout1 = 500),
    ok = validate_headers(Msgs1),
    ok = emqtt:disconnect(CSub).

%% Verify reading stream messages from the latest timestamp.
t_read_latest(Config) ->
    %% Create a stream
    _Stream = emqx_streams_test_utils:create_stream(#{topic_filter => <<"t/#">>}),
    ok = emqx_streams_test_utils:populate(50, #{topic_prefix => <<"t/">>, different_clients => true}),

    %% Subscribe to the stream, either to all shards at once or to each shard separately.
    CSub = emqx_streams_test_utils:emqtt_connect([]),
    case ?config(subscribe, Config) of
        all ->
            emqx_streams_test_utils:emqtt_sub(CSub, [<<"$s/latest/t/#">>]);
        shard ->
            emqx_streams_test_utils:emqtt_sub(
                CSub,
                [<<"$sp/0/latest/t/#">>, <<"$sp/1/latest/t/#">>]
            )
    end,

    %% Drain the messages from the stream
    %% and verify that they are NOT received — we subscribed to the latest offset.
    {ok, Msgs0} = emqx_streams_test_utils:emqtt_drain(_MinMsg0 = 0, _Timeout1 = 500),
    ?assertEqual(0, length(Msgs0)),

    %% Publish more messages while the clients are still subscribed.
    %% We should receive all the messages.
    ok = emqx_streams_test_utils:populate(50, #{topic_prefix => <<"t/">>, different_clients => true}),
    {ok, Msgs1} = emqx_streams_test_utils:emqtt_drain(_MinMsg1 = 50, _Timeout1 = 500),
    ok = validate_headers(Msgs1),

    %% Clean up
    ok = emqtt:disconnect(CSub).

%% Verify reading stream messages from a specific offset.
t_read_timestamp(Config) ->
    %% Create a stream
    Stream = emqx_streams_test_utils:create_stream(#{topic_filter => <<"t/#">>}),
    %% Publish 1st portion of messages to the stream
    ok = emqx_streams_test_utils:populate(50, #{topic_prefix => <<"t/">>, different_clients => true}),

    %% Get the last message timestamp
    AllMessages = emqx_streams_message_db:dirty_read_all(Stream),
    ?assertEqual(50, length(AllMessages)),
    {_, Offset, _} = lists:last(AllMessages),

    %% Publish 2nd portion of messages to the stream
    ok = emqx_streams_test_utils:populate(50, #{topic_prefix => <<"t/">>, different_clients => true}),

    %% Subscribe to the stream, either to all shards at once or to each shard separately.
    CSub = emqx_streams_test_utils:emqtt_connect([]),
    OffsetBin = integer_to_binary(Offset + 1),
    case ?config(subscribe, Config) of
        all ->
            emqx_streams_test_utils:emqtt_sub(CSub, [<<"$s/", OffsetBin/binary, "/t/#">>]);
        shard ->
            emqx_streams_test_utils:emqtt_sub(CSub, [
                <<"$sp/0/", OffsetBin/binary, "/t/#">>, <<"$sp/1/", OffsetBin/binary, "/t/#">>
            ])
    end,

    %% Drain the messages from the stream and verify that we receive the messages from
    %% the second portion, but not the first portion.
    {ok, Msgs0} = emqx_streams_test_utils:emqtt_drain(_MinMsg0 = 50, _Timeout0 = 1000),
    ?assertEqual(50, length(Msgs0)),
    ok = validate_headers(Msgs0),

    %% Publish more messages to the stream
    ok = emqx_streams_test_utils:populate(50, #{topic_prefix => <<"t/">>, different_clients => true}),

    %% Drain the messages from the stream and verify that we receive the messages
    {ok, Msgs1} = emqx_streams_test_utils:emqtt_drain(_MinMsg1 = 50, _Timeout1 = 1000),
    ok = validate_headers(Msgs1),

    %% Clean up
    ok = emqtt:disconnect(CSub).

%% Consume some history messages from a non-lastvalue(regular) stream
t_publish_and_consume_regular_many_generations(Config) ->
    %% Create a non-lastvalue Stream
    LimitOpts = ?config(limits, Config),
    _ = emqx_streams_test_utils:create_stream(LimitOpts#{
        topic_filter => <<"t/#">>, is_lastvalue => false
    }),

    %% Publish 100 messages to the stream
    emqx_streams_test_utils:populate(50, #{topic_prefix => <<"t/">>}),
    emqx_streams_test_utils:populate(50, #{topic_prefix => <<"t/">>, different_clients => true}),

    %% Consume the messages from the stream
    CSub = emqx_streams_test_utils:emqtt_connect([]),
    case ?config(subscribe, Config) of
        all ->
            emqx_streams_test_utils:emqtt_sub(CSub, [<<"$s/earliest/t/#">>]);
        shard ->
            emqx_streams_test_utils:emqtt_sub(CSub, [
                <<"$sp/0/earliest/t/#">>, <<"$sp/1/earliest/t/#">>
            ])
    end,
    {ok, Msgs0} = emqx_streams_test_utils:emqtt_drain(_MinMsg0 = 100, _Timeout0 = 5000),

    %% Verify the messages
    ?assertEqual(100, length(Msgs0)),

    %% Add a generation
    ok = emqx_streams_message_db:add_regular_db_generation(),
    % %% And another one
    ok = emqx_streams_message_db:add_regular_db_generation(),

    %% Publish 100 more messages to the stream
    emqx_streams_test_utils:populate(50, #{topic_prefix => <<"t/">>}),
    emqx_streams_test_utils:populate(50, #{topic_prefix => <<"t/">>, different_clients => true}),

    %% Consume the rest messages
    {ok, Msgs1} = emqx_streams_test_utils:emqtt_drain(_MinMsg1 = 100, _Timeout1 = 1000),

    %% Verify the messages
    ?assertEqual(100, length(Msgs1)),

    %% Clean up
    ok = emqtt:disconnect(CSub).

%% Consume some history messages from a lastvalue stream
t_publish_and_consume_lastvalue(Config) ->
    %% Create a lastvalue Stream
    LimitOpts = ?config(limits, Config),
    _ = emqx_streams_test_utils:create_stream(LimitOpts#{
        topic_filter => <<"t/#">>, is_lastvalue => true
    }),

    %% Publish 100 messages to the stream
    emqx_streams_test_utils:populate_lastvalue(100, #{
        topic_prefix => <<"t/">>,
        payload_prefix => <<"payload-">>,
        n_keys => 10
    }),

    %% Consume the messages from the stream
    CSub = emqx_streams_test_utils:emqtt_connect([]),
    case ?config(subscribe, Config) of
        all ->
            emqx_streams_test_utils:emqtt_sub(CSub, [<<"$s/earliest/t/#">>]);
        shard ->
            emqx_streams_test_utils:emqtt_sub(CSub, [
                <<"$sp/0/earliest/t/#">>, <<"$sp/1/earliest/t/#">>
            ])
    end,
    {ok, Msgs} = emqx_streams_test_utils:emqtt_drain(_MinMsg = 10, _Timeout = 100),
    ok = emqtt:disconnect(CSub),

    %% Verify the messages
    ?assertEqual(10, length(Msgs)).

%% Verify that the stream extsub stops consuming DS messages once there is
%% a critical amount of unacked messages
t_backpressure(_Config) ->
    %% Set max_inflight to 0 to avoid nacking messages by the client's session
    emqx_config:put([mqtt, max_inflight], 0),
    %% Set max_unacked to 100 to allow extsub to send all the messages to the client
    emqx_extsub:set_max_unacked(100),
    %% With the settings above, the handler's buffer size should be the limiting factor
    BufferSize = 20,
    DSStreamMaxUnacked = 1,
    emqx_streams_test_utils:reregister_extsub_handler(#{
        buffer_size => BufferSize
    }),

    %% Create a non-lastvalue Stream
    _ =
        emqx_streams_test_utils:create_stream(#{
            topic_filter => <<"t/#">>,
            is_lastvalue => false,
            read_max_unacked => DSStreamMaxUnacked
        }),

    %% Publish 100 messages to the stream
    emqx_streams_test_utils:populate(100, #{topic_prefix => <<"t/">>}),

    %% Consume the messages from the stream
    CSub = emqx_streams_test_utils:emqtt_connect([{auto_ack, false}]),
    emqx_streams_test_utils:emqtt_sub(CSub, <<"$s/earliest/t/#">>),
    {ok, Msgs0} =
        emqx_streams_test_utils:emqtt_drain(_MinMsg = BufferSize, _Timeout = 200),

    %% Messages should stop being dispatched once the buffer is full and we acked nothing
    %% After the buffer is full, the handler will stop acknowledging messages from the DS streams,
    %% but they still arrive till DSStreamMaxUnacked messages are unacked. So the mzximum total number of
    %% messages is BufferSize + DSStreamMaxUnacked * 2. We multiply by 2 because we have two active DS streams,
    %% one for each shard.
    ?assert(
        length(Msgs0) =< BufferSize + DSStreamMaxUnacked * 2,
        binfmt(
            "Msgs received: ~p, expected less than or equal to: ~p",
            [length(Msgs0), BufferSize + DSStreamMaxUnacked * 2]
        )
    ),

    %% Acknowledge the messages
    ok = emqx_streams_test_utils:emqtt_ack(Msgs0),

    %% After acknowledging, the messages should start being received again
    {ok, Msgs1} =
        emqx_streams_test_utils:emqtt_drain(_MinMsg = BufferSize, _Timeout = 200),
    ?assert(
        length(Msgs1) =< BufferSize + DSStreamMaxUnacked * 2,
        binfmt(
            "Msgs received: ~p, expected less than or equal to: ~p",
            [length(Msgs1), BufferSize + DSStreamMaxUnacked]
        )
    ),

    %% Clean up
    ok = emqtt:disconnect(CSub).

%% Verify that the stream is eventually found even if it is not present when
%% subscribing
t_find_stream(_Config) ->
    emqx_config:put([streams, check_stream_status_interval], 100),

    %% Connect a client and subscribe to a non-existent stream
    CSub = emqx_streams_test_utils:emqtt_connect([]),
    emqx_streams_test_utils:emqtt_sub(CSub, <<"$s/earliest/t/#">>),

    %% Create the stream
    emqx_streams_test_utils:create_stream(#{topic_filter => <<"t/#">>}),
    emqx_streams_test_utils:populate(1, #{topic_prefix => <<"t/">>}),

    %% Verify that the message is received
    {ok, _Msgs} = emqx_streams_test_utils:emqtt_drain(_MinMsg = 1, _Timeout = 1000),

    %% Clean up
    ok = emqtt:disconnect(CSub).

%% Verify that if the stream is recreated, the subscribers are eventually
%% resubscribe to the new stream.
t_stream_recreate(_Config) ->
    emqx_config:put([streams, check_stream_status_interval], 100),

    %% Create the stream
    emqx_streams_test_utils:create_stream(#{topic_filter => <<"t/#">>, is_lastvalue => false}),
    emqx_streams_test_utils:populate(1, #{
        topic_prefix => <<"t/">>, payload_prefix => <<"payload-1-">>
    }),

    %% Subscribe to the stream
    CSub = emqx_streams_test_utils:emqtt_connect([]),
    emqx_streams_test_utils:emqtt_sub(CSub, <<"$s/earliest/t/#">>),

    %% Verify that the message is received
    {ok, [#{payload := <<"payload-1-", _/binary>>}]} = emqx_streams_test_utils:emqtt_drain(
        _MinMsg0 = 1, _Timeout0 = 1000
    ),

    %% Recreate the stream
    emqx_streams_registry:delete(<<"t/#">>),
    ?retry(5, 100, ?assertNot(emqx_streams_registry:is_present(<<"t/#">>))),
    emqx_streams_test_utils:create_stream(#{topic_filter => <<"t/#">>, is_lastvalue => true}),
    emqx_streams_test_utils:populate_lastvalue(1, #{
        topic_prefix => <<"t/">>, payload_prefix => <<"payload-2-">>
    }),

    %% Verify that the messages are received from the new stream
    {ok, [#{payload := <<"payload-2-", _/binary>>}]} = emqx_streams_test_utils:emqtt_drain(
        _MinMsg1 = 1, _Timeout1 = 1000
    ),

    %% Recreate the stream again, but with a pause > check_stream_status_interval
    emqx_streams_registry:delete(<<"t/#">>),
    ?retry(5, 100, ?assertNot(emqx_streams_registry:is_present(<<"t/#">>))),
    ct:sleep(emqx_config:get([streams, check_stream_status_interval]) * 3),
    emqx_streams_test_utils:create_stream(#{topic_filter => <<"t/#">>, is_lastvalue => false}),
    emqx_streams_test_utils:populate(1, #{
        topic_prefix => <<"t/">>, payload_prefix => <<"payload-3-">>
    }),

    %% Verify that the message are received from the new stream
    {ok, [#{payload := <<"payload-3-", _/binary>>}]} = emqx_streams_test_utils:emqtt_drain(
        _MinMsg3 = 1, _Timeout3 = 1000
    ),

    %% Clean up
    ok = emqtt:disconnect(CSub).

%% Verify that the metrics are updated correctly
t_metrics(_Config) ->
    #{received_messages := ReceivedMessages0, inserted_messages := InsertedMessages0} =
        emqx_streams_metrics:get_counters(ds),

    %% Create a stream, publish and consume some messages
    _Stream = emqx_streams_test_utils:create_stream(#{topic_filter => <<"t/#">>}),
    emqx_streams_test_utils:populate(10, #{topic_prefix => <<"t/">>}),
    CSub = emqx_streams_test_utils:emqtt_connect([]),
    emqx_streams_test_utils:emqtt_sub(CSub, [<<"$s/earliest/t/#">>]),
    {ok, Msgs} = emqx_streams_test_utils:emqtt_drain(_MinMsg = 10, _Timeout = 1000),
    ok = emqtt:disconnect(CSub),
    ?assertEqual(10, length(Msgs)),

    %% Verify that the metrics are updated correctly
    #{received_messages := ReceivedMessages1, inserted_messages := InsertedMessages1} =
        emqx_streams_metrics:get_counters(ds),
    ?assertEqual(10, ReceivedMessages1 - ReceivedMessages0),
    ?assertEqual(10, InsertedMessages1 - InsertedMessages0),
    #{received_messages := #{current := Current}} = emqx_streams_metrics:get_rates(ds),
    ?assert(Current > 0),

    %% Verify that other accessors work
    ?assert(is_integer(emqx_streams_metrics:get_quota_buffer_inbox_size())),
    emqx_streams_metrics:print_common_hists(),
    emqx_streams_metrics:print_flush_quota_hist(),
    emqx_streams_metrics:print_common_hists(regular_limited).

%% Verify that invalid topics are rejected without crashing
%% and that invalid timestamps do not prevent further subscription
%% with correct timestamps.
t_subscribe_invalid_topic(_Config) ->
    %% Create a stream
    _Stream = emqx_streams_test_utils:create_stream(#{topic_filter => <<"t/#">>}),
    emqx_streams_test_utils:populate(1, #{topic_prefix => <<"t/">>}),

    %% Subscribe to the stream with invalid timestamps
    CSub = emqx_streams_test_utils:emqtt_connect([]),
    emqx_streams_test_utils:emqtt_sub(CSub, [<<"$s/invalid-ts/t/#">>]),
    emqx_streams_test_utils:emqtt_sub(CSub, [<<"$sp/0/invalid-ts/t/#">>]),
    emqx_streams_test_utils:emqtt_sub(CSub, [<<"$sp/9999/earliest/t/#">>]),

    %% Subscribe to the stream with valid timestamp
    emqx_streams_test_utils:emqtt_sub(CSub, [<<"$sp/all/earliest/t/#">>]),

    %% Drain the messages from the stream and verify that we receive the messages
    {ok, Msgs} = emqx_streams_test_utils:emqtt_drain(_MinMsg = 1, _Timeout = 1000),
    ?assertEqual(1, length(Msgs)),

    %% Clean up
    ok = emqtt:disconnect(CSub).

t_autocreate_stream(_Config) ->
    %% Create a client
    CSub = emqx_streams_test_utils:emqtt_connect([]),

    %% Autocreate some lastvalue streams
    {ok, _} = emqx:update_config([streams], #{
        <<"auto_create">> => #{
            <<"regular">> => false,
            <<"lastvalue">> => #{
                <<"key_expression">> => <<"message.headers.properties.User-Property.stream-key">>
            }
        }
    }),
    ok = emqx_streams_test_utils:emqtt_sub(CSub, [
        <<"$sp/0/latest/a/#">>,
        <<"$sp/1/latest/a/#">>,
        <<"$s/earliest/b/#">>,
        <<"$s/0/c/#">>
    ]),

    %% Autocreate some regular streams
    {ok, _} = emqx:update_config([streams], #{
        <<"auto_create">> => #{
            <<"regular">> => #{},
            <<"lastvalue">> => false
        }
    }),
    ok = emqx_streams_test_utils:emqtt_sub(CSub, [
        <<"$sp/0/latest/d/#">>,
        <<"$sp/1/latest/d/#">>,
        <<"$s/earliest/e/#">>,
        <<"$s/0/f/#">>
    ]),

    %% Verify that all 6 streams are created
    ?assertMatch(
        [
            #{topic_filter := <<"a/#">>},
            #{topic_filter := <<"b/#">>},
            #{topic_filter := <<"c/#">>},
            #{topic_filter := <<"d/#">>},
            #{topic_filter := <<"e/#">>},
            #{topic_filter := <<"f/#">>}
        ],
        emqx_utils_stream:consume(emqx_streams_registry:list())
    ),

    %% Publish some messages to the streams
    emqx_streams_test_utils:populate_lastvalue(10, #{topic_prefix => <<"a/">>}),
    emqx_streams_test_utils:populate_lastvalue(10, #{topic_prefix => <<"b/">>}),
    emqx_streams_test_utils:populate_lastvalue(10, #{topic_prefix => <<"c/">>}),
    emqx_streams_test_utils:populate(10, #{topic_prefix => <<"d/">>}),
    emqx_streams_test_utils:populate(10, #{topic_prefix => <<"e/">>}),
    emqx_streams_test_utils:populate(10, #{topic_prefix => <<"f/">>}),

    %% Verify that we receive all the messages
    {ok, _Msgs0} = emqx_streams_test_utils:emqtt_drain(60, 1000),

    %% Clean up
    ok = emqtt:disconnect(CSub).

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

validate_headers(Msgs) when is_list(Msgs) ->
    lists:foreach(fun validate_msg_headers/1, Msgs).

validate_msg_headers(Msg) ->
    case user_properties(Msg) of
        #{<<"ts">> := _Ts, <<"key">> := _Key} ->
            ok;
        _ ->
            ct:fail("Message does not have required user properties (part, ts, key): ~p", [Msg])
    end.

user_properties(_Msg = #{properties := #{'User-Property' := UserProperties}}) ->
    maps:from_list(UserProperties);
user_properties(_Msg) ->
    #{}.

binfmt(Format, Args) ->
    iolist_to_binary(io_lib:format(Format, Args)).
