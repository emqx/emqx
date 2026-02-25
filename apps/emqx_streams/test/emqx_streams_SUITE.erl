%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    [
        {group, pub_and_consume},
        {group, read}
    ] ++ All.

groups() ->
    emqx_common_test_helpers:nested_groups([
        [pub_and_consume],
        [limited, unlimited],
        [pc_subscribe_all, pc_subscribe_all_stream],
        ?PUBLISH_AND_CONSUME_CASES
    ]) ++
        emqx_common_test_helpers:nested_groups([
            [read],
            [read_subscribe_all, read_subscribe_all_stream],
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
init_per_group(pc_subscribe_all, Config) ->
    [{subscribe, all} | Config];
init_per_group(pc_subscribe_all_stream, Config) ->
    [{subscribe, all_stream} | Config];
init_per_group(read_subscribe_all, Config) ->
    [{subscribe, all} | Config];
init_per_group(read_subscribe_all_stream, Config) ->
    [{subscribe, all_stream} | Config];
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
    Stream = emqx_streams_test_utils:ensure_stream_created(#{
        name => <<"smoke">>, topic_filter => <<"t/#">>
    }),
    ok = emqx_streams_test_utils:populate(10, #{topic_prefix => <<"t/">>}),
    AllMessages = emqx_streams_message_db:dirty_read_all(Stream),
    ?assertEqual(10, length(AllMessages)),
    ok.

%% Verify reading stream messages from the earliest timestamp.
t_read_earliest(Config) ->
    %% Create a stream
    StreamOpts = #{topic_filter => <<"t/#">>, name => <<"t_read_earliest">>},
    case ?config(subscribe, Config) of
        all ->
            _ = emqx_streams_test_utils:ensure_legacy_stream_created(StreamOpts);
        all_stream ->
            _ = emqx_streams_test_utils:ensure_stream_created(StreamOpts)
    end,
    ok = emqx_streams_test_utils:populate(50, #{topic_prefix => <<"t/">>, different_clients => true}),

    %% Subscribe to the stream, either to all shards at once or to each shard separately.
    CSub = emqx_streams_test_utils:emqtt_connect([]),
    case ?config(subscribe, Config) of
        all ->
            emqx_streams_test_utils:emqtt_sub(CSub, [<<"$s/earliest/t/#">>]);
        all_stream ->
            emqx_streams_test_utils:emqtt_sub(
                CSub,
                [<<"$stream/t_read_earliest/t/#">>],
                [{<<"stream-offset">>, <<"earliest">>}]
            )
    end,

    %% Drain the messages from the stream and verify that they are received.
    {ok, Msgs0} = emqx_streams_test_utils:emqtt_drain(_MinMsg0 = 50, _Timeout0 = 500),
    ok = validate_headers(Msgs0),

    %% Publish more messages while the clients are still subscribed.
    %% We should receive all the messages
    ok = emqx_streams_test_utils:populate(50, #{topic_prefix => <<"t/">>, different_clients => true}),
    {ok, Msgs1} = emqx_streams_test_utils:emqtt_drain(_MinMsg1 = 50, _Timeout1 = 500),
    ok = validate_headers(Msgs1),

    %% Now, unsubscribe from the stream
    case ?config(subscribe, Config) of
        all ->
            emqtt:unsubscribe(CSub, <<"$s/earliest/t/#">>);
        all_stream ->
            emqtt:unsubscribe(CSub, <<"$stream/t_read_earliest/t/#">>)
    end,

    %% Publish more messages, we should not receive any
    ok = emqx_streams_test_utils:populate(50, #{topic_prefix => <<"t/">>, different_clients => true}),
    {ok, []} = emqx_streams_test_utils:emqtt_drain(_MinMsg2 = 0, _Timeout2 = 1000),

    %% Clean up
    ok = emqtt:disconnect(CSub).

%% Verify reading stream messages from the latest timestamp.
t_read_latest(Config) ->
    %% Create a stream
    StreamOpts = #{topic_filter => <<"t/#">>, name => <<"t_read_latest">>},
    case ?config(subscribe, Config) of
        all ->
            _ = emqx_streams_test_utils:ensure_legacy_stream_created(StreamOpts);
        all_stream ->
            _ = emqx_streams_test_utils:ensure_stream_created(StreamOpts)
    end,
    ok = emqx_streams_test_utils:populate(50, #{topic_prefix => <<"t/">>, different_clients => true}),

    %% Subscribe to the stream, either to all shards at once or to each shard separately.
    CSub = emqx_streams_test_utils:emqtt_connect([]),
    case ?config(subscribe, Config) of
        all ->
            emqx_streams_test_utils:emqtt_sub(CSub, [<<"$s/latest/t/#">>]);
        all_stream ->
            emqx_streams_test_utils:emqtt_sub(
                CSub,
                [<<"$stream/t_read_latest/t/#">>],
                [{<<"dummy-prop">>, <<"latest">>}, {<<"stream-offset">>, <<"latest">>}]
            )
    end,

    %% Drain the messages from the stream
    %% and verify that they are NOT received — we subscribed to the latest offset.
    {ok, Msgs0} = emqx_streams_test_utils:emqtt_drain(_MinMsg0 = 0, _Timeout0 = 500),
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
    StreamOpts = #{topic_filter => <<"t/#">>, name => <<"t_read_timestamp">>},
    Stream =
        case ?config(subscribe, Config) of
            all ->
                emqx_streams_test_utils:ensure_legacy_stream_created(StreamOpts);
            all_stream ->
                emqx_streams_test_utils:ensure_stream_created(StreamOpts)
        end,

    %% Publish 1st portion of messages to the stream
    ok = emqx_streams_test_utils:populate(50, #{topic_prefix => <<"t/">>, different_clients => true}),

    %% Get the last message timestamp
    AllMessages = emqx_streams_message_db:dirty_read_all(Stream),
    ?assertEqual(50, length(AllMessages)),
    Offsets = [Offset || {_, Offset, _} <- AllMessages],
    Offset = lists:max(Offsets),

    %% Publish 2nd portion of messages to the stream
    ok = emqx_streams_test_utils:populate(50, #{topic_prefix => <<"t/">>, different_clients => true}),

    %% Subscribe to the stream, either to all shards at once or to each shard separately.
    CSub = emqx_streams_test_utils:emqtt_connect([]),
    OffsetBin = integer_to_binary(Offset + 1),
    case ?config(subscribe, Config) of
        all ->
            emqx_streams_test_utils:emqtt_sub(CSub, [<<"$s/", OffsetBin/binary, "/t/#">>]);
        all_stream ->
            emqx_streams_test_utils:emqtt_sub(
                CSub,
                [<<"$stream/t_read_timestamp/t/#">>],
                [{<<"stream-offset">>, OffsetBin}]
            )
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
    StreamOpts = LimitOpts#{
        name => <<"test_publish_and_consume_regular_many_generations">>,
        topic_filter => <<"t/#">>,
        is_lastvalue => false
    },
    case ?config(subscribe, Config) of
        all ->
            _ = emqx_streams_test_utils:ensure_legacy_stream_created(StreamOpts);
        all_stream ->
            _ = emqx_streams_test_utils:ensure_stream_created(StreamOpts)
    end,

    %% Publish 100 messages to the stream
    emqx_streams_test_utils:populate(50, #{topic_prefix => <<"t/">>}),
    emqx_streams_test_utils:populate(50, #{topic_prefix => <<"t/">>, different_clients => true}),

    %% Consume the messages from the stream
    CSub = emqx_streams_test_utils:emqtt_connect([]),
    case ?config(subscribe, Config) of
        all ->
            emqx_streams_test_utils:emqtt_sub(CSub, [<<"$s/earliest/t/#">>]);
        all_stream ->
            emqx_streams_test_utils:emqtt_sub(
                CSub,
                [<<"$stream/test_publish_and_consume_regular_many_generations/t/#">>],
                [{<<"stream-offset">>, <<"earliest">>}]
            )
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
    StreamOpts = LimitOpts#{
        name => <<"test_publish_and_consume_lastvalue">>,
        topic_filter => <<"t/#">>,
        is_lastvalue => true
    },
    case ?config(subscribe, Config) of
        all ->
            _ = emqx_streams_test_utils:ensure_legacy_stream_created(StreamOpts);
        all_stream ->
            _ = emqx_streams_test_utils:ensure_stream_created(StreamOpts)
    end,

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
        all_stream ->
            emqx_streams_test_utils:emqtt_sub(
                CSub,
                [<<"$stream/test_publish_and_consume_lastvalue/t/#">>],
                [{<<"stream-offset">>, <<"earliest">>}]
            )
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
        emqx_streams_test_utils:ensure_stream_created(#{
            name => <<"t_backpressure">>,
            topic_filter => <<"t/#">>,
            is_lastvalue => false,
            read_max_unacked => DSStreamMaxUnacked
        }),

    %% Publish 100 messages to the stream
    emqx_streams_test_utils:populate(100, #{topic_prefix => <<"t/">>}),

    %% Consume the messages from the stream
    CSub = emqx_streams_test_utils:emqtt_connect([{auto_ack, false}]),
    emqx_streams_test_utils:emqtt_sub(CSub, <<"$stream/t_backpressure">>, [
        {<<"stream-offset">>, <<"earliest">>}
    ]),
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
        emqx_streams_test_utils:emqtt_drain(_MinMsg1 = BufferSize, _Timeout1 = 200),
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
    emqx_streams_test_utils:emqtt_sub(CSub, <<"$stream/t_find_stream">>, [
        {<<"stream-offset">>, <<"earliest">>}
    ]),

    %% Create the stream
    emqx_streams_test_utils:ensure_stream_created(#{
        name => <<"t_find_stream">>, topic_filter => <<"t/#">>
    }),
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
    emqx_streams_test_utils:ensure_stream_created(#{
        name => <<"t_stream_recreate">>,
        topic_filter => <<"t/#">>,
        is_lastvalue => false
    }),
    emqx_streams_test_utils:populate(1, #{
        topic_prefix => <<"t/">>, payload_prefix => <<"payload-1-">>
    }),

    %% Subscribe to the stream
    CSub = emqx_streams_test_utils:emqtt_connect([]),
    emqx_streams_test_utils:emqtt_sub(CSub, <<"$stream/t_stream_recreate">>, [
        {<<"stream-offset">>, <<"earliest">>}
    ]),

    %% Verify that the message is received
    {ok, [#{payload := <<"payload-1-", _/binary>>}]} = emqx_streams_test_utils:emqtt_drain(
        _MinMsg0 = 1, _Timeout0 = 1000
    ),

    %% Recreate the stream
    emqx_streams_registry:delete(<<"t_stream_recreate">>),
    ?retry(
        5, 100, ?assertNot(emqx_streams_registry:is_present(<<"t_stream_recreate">>, <<"t/#">>))
    ),
    emqx_streams_test_utils:ensure_stream_created(#{
        name => <<"t_stream_recreate">>, topic_filter => <<"t/#">>, is_lastvalue => true
    }),
    emqx_streams_test_utils:populate_lastvalue(1, #{
        topic_prefix => <<"t/">>, payload_prefix => <<"payload-2-">>
    }),

    %% Verify that the messages are received from the new stream
    {ok, [#{payload := <<"payload-2-", _/binary>>}]} = emqx_streams_test_utils:emqtt_drain(
        _MinMsg1 = 1, _Timeout1 = 1000
    ),

    %% Recreate the stream again, but with a pause > check_stream_status_interval
    emqx_streams_registry:delete(<<"t_stream_recreate">>),
    ?retry(
        5, 100, ?assertNot(emqx_streams_registry:is_present(<<"t_stream_recreate">>, <<"t/#">>))
    ),
    ct:sleep(emqx_config:get([streams, check_stream_status_interval]) * 3),
    emqx_streams_test_utils:ensure_stream_created(#{
        name => <<"t_stream_recreate">>,
        topic_filter => <<"t/#">>,
        is_lastvalue => false
    }),
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
    _Stream = emqx_streams_test_utils:ensure_stream_created(#{
        name => <<"t_metrics">>, topic_filter => <<"t/#">>
    }),
    emqx_streams_test_utils:populate(10, #{topic_prefix => <<"t/">>}),
    CSub = emqx_streams_test_utils:emqtt_connect([]),
    emqx_streams_test_utils:emqtt_sub(CSub, <<"$stream/t_metrics">>, [
        {<<"stream-offset">>, <<"earliest">>}
    ]),
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

%% Verify that subscription with invalid topic filter
%% does not prevent further subscription with valid topic filter.
t_subscribe_invalid_topic(_Config) ->
    %% Create a stream
    _Stream = emqx_streams_test_utils:ensure_stream_created(#{
        name => <<"t_subscribe_invalid_topic">>, topic_filter => <<"t/#">>
    }),
    emqx_streams_test_utils:populate(1, #{topic_prefix => <<"t/">>}),

    %% Subscribe to the stream with invalid topic
    CSub = emqx_streams_test_utils:emqtt_connect([]),
    emqx_streams_test_utils:emqtt_sub(CSub, <<"$stream/t_subscribe_invalid_topic/xxx/#">>, [
        {<<"stream-offset">>, <<"earliest">>}
    ]),

    %% Subscribe to the stream with valid topic filter
    emqx_streams_test_utils:emqtt_sub(CSub, <<"$stream/t_subscribe_invalid_topic/t/#">>, [
        {<<"stream-offset">>, <<"earliest">>}
    ]),

    %% Drain the messages from the stream and verify that we receive the messages
    {ok, Msgs} = emqx_streams_test_utils:emqtt_drain(_MinMsg = 1, _Timeout = 1000),
    ?assertEqual(1, length(Msgs)),

    %% Clean up
    ok = emqtt:disconnect(CSub).

t_autocreate_stream(_Config) ->
    %% Create a client
    CSub = emqx_streams_test_utils:emqtt_connect([]),

    %% Autocreate some lastvalue streams
    RawConfig = emqx:get_raw_config([streams]),
    {ok, _} = emqx:update_config([streams], RawConfig#{
        <<"auto_create">> => #{
            <<"regular">> => false,
            <<"lastvalue">> => #{
                <<"key_expression">> => <<"message.headers.properties.User-Property.stream-key">>
            }
        }
    }),
    ok = emqx_streams_test_utils:emqtt_sub(CSub, [
        <<"$stream/auto_create_lastvalue/a/#">>
    ]),

    %% Autocreate some regular streams
    {ok, _} = emqx:update_config([streams], RawConfig#{
        <<"auto_create">> => #{
            <<"regular">> => #{},
            <<"lastvalue">> => false
        }
    }),
    ok = emqx_streams_test_utils:emqtt_sub(CSub, [
        <<"$stream/auto_create_regular/d/#">>
    ]),

    %% Verify that all 6 streams are created
    ?assertMatch(
        [
            #{topic_filter := <<"a/#">>},
            #{topic_filter := <<"d/#">>}
        ],
        emqx_utils_stream:consume(emqx_streams_registry:list())
    ),

    %% Publish some messages to the streams
    emqx_streams_test_utils:populate_lastvalue(10, #{topic_prefix => <<"a/">>}),
    emqx_streams_test_utils:populate(10, #{topic_prefix => <<"d/">>}),

    %% Verify that we receive all the messages
    {ok, _Msgs0} = emqx_streams_test_utils:emqtt_drain(20, 1000),

    %% Clean up
    ok = emqtt:disconnect(CSub).

%% Verify that the data retention period is applied correctly
t_data_retention_period(_Config) ->
    %% Create a lastvalue and a regular streams with 1s data retention period
    StreamLV = emqx_streams_test_utils:ensure_stream_created(#{
        name => <<"data_retention_period_lastvalue">>,
        topic_filter => <<"t1/#">>,
        is_lastvalue => true,
        data_retention_period => 1000
    }),
    StreamR = emqx_streams_test_utils:ensure_stream_created(#{
        name => <<"data_retention_period_regular">>,
        topic_filter => <<"t2/#">>,
        is_lastvalue => false,
        data_retention_period => 1000
    }),

    %% Timestamp before the message was published
    TimestampBeforeMessage = erlang:system_time(microsecond) - 1_000_000,
    TimestampBeforeMessageBin = integer_to_binary(TimestampBeforeMessage),

    %% Publish 1 message to the streams
    emqx_streams_test_utils:populate_lastvalue(1, #{
        topic_prefix => <<"t1/">>, key_start_from => 0, payload_prefix => <<"payload-old-">>
    }),
    emqx_streams_test_utils:populate(1, #{
        topic_prefix => <<"t2/">>, payload_prefix => <<"payload-old-">>
    }),
    emqx_streams_message_db:add_regular_db_generation(),

    %% Wait for 1s to let the data expire
    ct:sleep(1000),

    %% Publish 1 more message to the streams
    emqx_streams_test_utils:populate_lastvalue(1, #{
        topic_prefix => <<"t1/">>, key_start_from => 1, payload_prefix => <<"payload-new-">>
    }),
    emqx_streams_test_utils:populate(1, #{
        topic_prefix => <<"t2/">>, payload_prefix => <<"payload-new-">>
    }),

    %% Try to read the messages as earliest and see that the limit is applied.
    %% We should see the second message from each stream, totaling 2 messages.
    CSub0 = emqx_streams_test_utils:emqtt_connect([]),
    emqx_streams_test_utils:emqtt_sub(
        CSub0,
        [<<"$stream/data_retention_period_lastvalue">>],
        [{<<"stream-offset">>, <<"earliest">>}]
    ),
    emqx_streams_test_utils:emqtt_sub(
        CSub0,
        [<<"$stream/data_retention_period_regular">>],
        [{<<"stream-offset">>, <<"earliest">>}]
    ),

    {ok, Msgs0} = emqx_streams_test_utils:emqtt_drain(_MinMsg0 = 2, _Timeout0 = 100),
    ?assertEqual(2, length(Msgs0)),
    ok = emqtt:disconnect(CSub0),

    %% Try to read the messages from timestamp 2s ago and see that the limit is applied
    CSub1 = emqx_streams_test_utils:emqtt_connect([]),
    emqx_streams_test_utils:emqtt_sub(
        CSub1,
        [<<"$stream/data_retention_period_lastvalue">>],
        [{<<"stream-offset">>, TimestampBeforeMessageBin}]
    ),
    emqx_streams_test_utils:emqtt_sub(
        CSub1,
        [<<"$stream/data_retention_period_regular">>],
        [{<<"stream-offset">>, TimestampBeforeMessageBin}]
    ),
    {ok, Msgs1} = emqx_streams_test_utils:emqtt_drain(_MinMsg1 = 2, _Timeout1 = 100),
    ?assertEqual(2, length(Msgs1)),
    ok = emqtt:disconnect(CSub1),

    %% Now update the data retention period to 1m and verify that we see all the messages
    %% (because stream was not yet GC-ed)
    emqx_streams_registry:update(
        <<"data_retention_period_lastvalue">>,
        StreamLV#{data_retention_period => 60_000}
    ),
    emqx_streams_registry:update(
        <<"data_retention_period_regular">>,
        StreamR#{data_retention_period => 60_000}
    ),

    %% Try to read the messages as earliest and get all the messages
    CSub2 = emqx_streams_test_utils:emqtt_connect([]),
    emqx_streams_test_utils:emqtt_sub(
        CSub2,
        [<<"$stream/data_retention_period_lastvalue">>],
        [{<<"stream-offset">>, <<"earliest">>}]
    ),
    emqx_streams_test_utils:emqtt_sub(
        CSub2,
        [<<"$stream/data_retention_period_regular">>],
        [{<<"stream-offset">>, <<"earliest">>}]
    ),
    {ok, _Msgs2} = emqx_streams_test_utils:emqtt_drain(_MinMsg2 = 4, _Timeout2 = 200),
    ok = emqtt:disconnect(CSub2),

    %% Try to read the messages from timestamp TimestampBeforeMessage and get all the messages
    CSub3 = emqx_streams_test_utils:emqtt_connect([]),
    emqx_streams_test_utils:emqtt_sub(
        CSub3,
        [<<"$stream/data_retention_period_lastvalue">>],
        [{<<"stream-offset">>, TimestampBeforeMessageBin}]
    ),
    emqx_streams_test_utils:emqtt_sub(
        CSub3,
        [<<"$stream/data_retention_period_regular">>],
        [{<<"stream-offset">>, TimestampBeforeMessageBin}]
    ),
    {ok, _Msgs3} = emqx_streams_test_utils:emqtt_drain(_MinMsg3 = 4, _Timeout3 = 1000),

    %% Clean up
    ok = emqtt:disconnect(CSub3).

%% Verify that we can subscribe to a stream using the legacy start-from user property
t_legacy_start_from_user_property(_Config) ->
    %% Create a stream
    _ = emqx_streams_test_utils:ensure_stream_created(#{
        name => <<"legacy_start_from_user_property">>,
        topic_filter => <<"t/#">>
    }),

    %% Publish a message to the stream
    emqx_streams_test_utils:populate(1, #{topic_prefix => <<"t/">>}),

    %% Subscribe to the stream
    CSub = emqx_streams_test_utils:emqtt_connect([]),
    emqx_streams_test_utils:emqtt_sub(
        CSub,
        [<<"$stream/legacy_start_from_user_property">>],
        [{<<"$stream.start-from">>, <<"earliest">>}]
    ),
    {ok, Msgs} = emqx_streams_test_utils:emqtt_drain(_MinMsg = 1, _Timeout = 1000),
    ?assertEqual(1, length(Msgs)),
    ok = emqtt:disconnect(CSub).

%% Verify that subscription restoration works correctly for stream topics
t_sub_restoration(_Config) ->
    %% Create a non-lastvalue stream
    emqx_streams_test_utils:ensure_stream_created(#{
        name => <<"offline_session">>,
        topic_filter => <<"t/#">>,
        is_lastvalue => false
    }),
    emqx_streams_test_utils:populate(10, #{topic_prefix => <<"t/">>}),

    %% Subscribe to the stream
    CSub0 = emqx_streams_test_utils:emqtt_connect([
        {clientid, <<"csub">>},
        {properties, #{'Session-Expiry-Interval' => 1000}},
        {clean_start, false}
    ]),
    emqx_streams_test_utils:emqtt_sub(CSub0, <<"$stream/offline_session">>, [
        {<<"stream-offset">>, <<"earliest">>}
    ]),

    %% Verify that we receive all the messages
    {ok, _Msgs0} = emqx_streams_test_utils:emqtt_drain(_MinMsg0 = 10, _Timeout0 = 1000),

    %% Disconnect the client
    ok = emqtt:disconnect(CSub0),

    %% Reconnect the client
    CSub1 = emqx_streams_test_utils:emqtt_connect([
        {clientid, <<"csub">>},
        {properties, #{'Session-Expiry-Interval' => 1000}},
        {clean_start, false}
    ]),

    %% Receive the messages again
    {ok, _Msgs1} = emqx_streams_test_utils:emqtt_drain(_MinMsg1 = 10, _Timeout1 = 1000),

    %% Clean up
    ok = emqtt:disconnect(CSub1).

%% Verify that only MQTT v5 clients are allowed to subscribe to streams
t_allow_only_mqtt_v5(_Config) ->
    %% Connect a client and subscribe to a queue
    {ok, CSub} = emqtt:start_link([{proto_ver, v3}]),
    {ok, _} = emqtt:connect(CSub),

    %% Try to subscribe to a queue with MQTT v3
    {ok, _, [?RC_UNSPECIFIED_ERROR]} = emqtt:subscribe(CSub, {<<"$stream/some_stream/t/#">>, 1}),

    %% Clean up
    ok = emqtt:disconnect(CSub).

t_subscribe_unsubscribe_to_many_streams(_Config) ->
    NStreams = 5,

    %% Create NStreams streams with different names
    lists:foreach(
        fun(N) ->
            NBin = integer_to_binary(N),
            emqx_streams_test_utils:ensure_stream_created(#{
                name => <<"stream_", NBin/binary>>,
                topic_filter => <<"t/", NBin/binary>>
            })
        end,
        lists:seq(1, NStreams)
    ),

    %% Subscribe to each stream, receive and unsubscribe from the stream,
    %% all by the same client.
    CSub = emqx_streams_test_utils:emqtt_connect([{clientid, <<"csub">>}]),
    CPub = emqx_streams_test_utils:emqtt_connect([{clientid, <<"cpub">>}]),
    lists:foreach(
        fun(N) ->
            NBin = integer_to_binary(N),
            Topic = <<"$stream/stream_", NBin/binary>>,
            emqx_streams_test_utils:emqtt_sub(CSub, [Topic], [
                {<<"stream-offset">>, <<"earliest">>}
            ]),
            emqtt:publish(CPub, <<"t/", NBin/binary>>, <<"payload">>, ?QOS_1),
            {ok, _Msgs} = emqx_streams_test_utils:emqtt_drain(_MinMsg = 1, _Timeout = 5000),
            emqtt:unsubscribe(CSub, Topic)
        end,
        lists:seq(1, NStreams)
    ),
    ok = emqtt:disconnect(CPub),

    %% Obtain the extsub info
    [ChannelPid] = emqx_cm:lookup_channels(<<"csub">>),
    {ok, Info} = emqx_extsub:inspect(ChannelPid),

    %% Verify that there are no leftovers

    #{
        unacked_count := UnackedCount,
        deliver_retry_scheduled := DeliverRetryScheduled,
        registry := #{
            buffer := #{delivering := Delivering, message_buffer_size := MessageBufferSize},
            by_ref := ByRef,
            by_topic_cbm := ByTopicCBM,
            generic_message_handlers := GenericMessageHandlers
        }
    } = Info,
    ?assertEqual(0, maps:size(Delivering)),
    ?assertEqual(0, UnackedCount),
    ?assertEqual(false, DeliverRetryScheduled),
    ?assertEqual([], ByRef),
    ?assertEqual(0, maps:size(ByTopicCBM)),
    ?assertEqual([], GenericMessageHandlers),
    ?assertEqual(0, MessageBufferSize),

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
