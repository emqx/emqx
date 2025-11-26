%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-include("../src/emqx_streams_internal.hrl").

-define(COMMON_LIMITED_TESTS, [
    t_publish_and_consume_regular_many_generations,
    t_publish_and_consume_lastvalue
]).

-define(MANY, 999_999_999_999).

all() ->
    All =
        emqx_common_test_helpers:all(?MODULE) -- ?COMMON_LIMITED_TESTS,
    [
        {group, unlimited},
        {group, limited}
    ] ++ All.

groups() ->
    [
        {limited, [], ?COMMON_LIMITED_TESTS},
        {unlimited, [], ?COMMON_LIMITED_TESTS}
    ].

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
init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_CaseName, Config) ->
    ok = emqx_streams_test_utils:cleanup_streams(),
    ok = snabbkaffe:start_trace(),
    ExtSubMaxUnacked = emqx_extsub:max_unacked(),
    [{ext_sub_max_unacked, ExtSubMaxUnacked} | Config].

end_per_testcase(_CaseName, Config) ->
    ok = snabbkaffe:stop(),
    ok = emqx_streams_test_utils:cleanup_streams(),
    ok = emqx_streams_test_utils:reset_config(),
    ok = emqx_streams_test_utils:reregister_extsub_handler(#{}),
    ok = emqx_extsub:set_max_unacked(?config(ext_sub_max_unacked, Config)).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_smoke(_Config) ->
    Stream = emqx_streams_test_utils:create_stream(#{topic_filter => <<"t/#">>}),
    ok = emqx_streams_test_utils:populate(10, #{topic_prefix => <<"t/">>}),
    AllMessages = emqx_streams_message_db:dirty_read_all(Stream),
    ?assertEqual(10, length(AllMessages)),
    ok.

t_read_earliest(_Config) ->
    _Stream = emqx_streams_test_utils:create_stream(#{topic_filter => <<"t/#">>}),
    ok = emqx_streams_test_utils:populate(50, #{topic_prefix => <<"t/">>, different_clients => true}),

    CSub = emqx_streams_test_utils:emqtt_connect([]),
    ok = emqx_streams_test_utils:emqtt_sub_stream(CSub, <<"0/earliest/t/#">>),
    ok = emqx_streams_test_utils:emqtt_sub_stream(CSub, <<"1/earliest/t/#">>),

    {ok, Msgs0} = emqx_streams_test_utils:emqtt_drain(_MinMsg0 = 50, _Timeout1 = 500),
    ok = validate_headers(Msgs0),

    ok = emqx_streams_test_utils:populate(50, #{topic_prefix => <<"t/">>, different_clients => true}),
    {ok, Msgs1} = emqx_streams_test_utils:emqtt_drain(_MinMsg1 = 50, _Timeout1 = 500),
    ok = validate_headers(Msgs1),
    ok = emqtt:disconnect(CSub).

t_read_latest(_Config) ->
    _Stream = emqx_streams_test_utils:create_stream(#{topic_filter => <<"t/#">>}),
    ok = emqx_streams_test_utils:populate(50, #{topic_prefix => <<"t/">>, different_clients => true}),

    CSub = emqx_streams_test_utils:emqtt_connect([]),
    ok = emqx_streams_test_utils:emqtt_sub_stream(CSub, <<"0/latest/t/#">>),
    ok = emqx_streams_test_utils:emqtt_sub_stream(CSub, <<"1/latest/t/#">>),

    {ok, Msgs0} = emqx_streams_test_utils:emqtt_drain(_MinMsg0 = 0, _Timeout1 = 500),
    ?assertEqual(0, length(Msgs0)),

    ok = emqx_streams_test_utils:populate(50, #{topic_prefix => <<"t/">>, different_clients => true}),
    {ok, Msgs1} = emqx_streams_test_utils:emqtt_drain(_MinMsg1 = 50, _Timeout1 = 500),
    ok = validate_headers(Msgs1),

    ok = emqtt:disconnect(CSub).

t_read_offset(_Config) ->
    Stream = emqx_streams_test_utils:create_stream(#{topic_filter => <<"t/#">>}),
    ok = emqx_streams_test_utils:populate(50, #{topic_prefix => <<"t/">>, different_clients => true}),

    AllMessages = emqx_streams_message_db:dirty_read_all(Stream),
    ?assertEqual(50, length(AllMessages)),
    {_, Offset, _} = lists:last(AllMessages),

    ok = emqx_streams_test_utils:populate(50, #{topic_prefix => <<"t/">>, different_clients => true}),

    CSub = emqx_streams_test_utils:emqtt_connect([]),
    OffsetBin = integer_to_binary(Offset),
    ok = emqx_streams_test_utils:emqtt_sub_stream(CSub, <<"0/", OffsetBin/binary, "/t/#">>),
    ok = emqx_streams_test_utils:emqtt_sub_stream(CSub, <<"1/", OffsetBin/binary, "/t/#">>),

    {ok, Msgs0} = emqx_streams_test_utils:emqtt_drain(_MinMsg0 = 50, _Timeout0 = 1000),
    ok = validate_headers(Msgs0),

    ok = emqx_streams_test_utils:populate(50, #{topic_prefix => <<"t/">>, different_clients => true}),

    {ok, Msgs1} = emqx_streams_test_utils:emqtt_drain(_MinMsg1 = 50, _Timeout1 = 1000),
    ok = validate_headers(Msgs1),

    ok = emqtt:disconnect(CSub).

%% Consume some history messages from a non-lastvalue(regular) queue
t_publish_and_consume_regular_many_generations(Config) ->
    %% Create a non-lastvalue Stream
    _ = emqx_streams_test_utils:create_stream(#{
        topic_filter => <<"t/#">>, is_lastvalue => false, limits => ?config(limits, Config)
    }),

    %% Publish 100 messages to the stream
    emqx_streams_test_utils:populate(50, #{topic_prefix => <<"t/">>}),
    emqx_streams_test_utils:populate(50, #{topic_prefix => <<"t/">>, different_clients => true}),

    %% Consume the messages from the queue
    CSub = emqx_streams_test_utils:emqtt_connect([]),
    emqx_streams_test_utils:emqtt_sub_stream(CSub, <<"0/earliest/t/#">>),
    emqx_streams_test_utils:emqtt_sub_stream(CSub, <<"1/earliest/t/#">>),
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

%% Consume some history messages from a lastvalue queue
t_publish_and_consume_lastvalue(Config) ->
    %% Create a lastvalue Stream
    _ = emqx_streams_test_utils:create_stream(#{
        topic_filter => <<"t/#">>, is_lastvalue => true, limits => ?config(limits, Config)
    }),

    %% Publish 100 messages to the stream
    emqx_streams_test_utils:populate_lastvalue(100, #{
        topic_prefix => <<"t/">>,
        payload_prefix => <<"payload-">>,
        n_keys => 10
    }),

    %% Consume the messages from the stream
    CSub = emqx_streams_test_utils:emqtt_connect([]),
    emqx_streams_test_utils:emqtt_sub_stream(CSub, <<"0/earliest/t/#">>),
    emqx_streams_test_utils:emqtt_sub_stream(CSub, <<"1/earliest/t/#">>),
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

    %% Publish 100 messages to the queue
    emqx_streams_test_utils:populate(100, #{topic_prefix => <<"t/">>}),

    %% Consume the messages from the queue
    CSub = emqx_streams_test_utils:emqtt_connect([{auto_ack, false}]),
    emqx_streams_test_utils:emqtt_sub_stream(CSub, [<<"0/earliest/t/#">>, <<"1/earliest/t/#">>]),
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

    %% Connect a client and subscribe to a non-existent queue
    CSub = emqx_streams_test_utils:emqtt_connect([]),
    emqx_streams_test_utils:emqtt_sub_stream(CSub, [<<"0/earliest/t/#">>, <<"1/earliest/t/#">>]),

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
    emqx_streams_test_utils:emqtt_sub_stream(CSub, <<"0/earliest/t/#">>),
    emqx_streams_test_utils:emqtt_sub_stream(CSub, <<"1/earliest/t/#">>),

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

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

validate_headers(Msgs) when is_list(Msgs) ->
    lists:foreach(fun validate_msg_headers/1, Msgs).

validate_msg_headers(Msg) ->
    case user_properties(Msg) of
        #{<<"part">> := _Part, <<"offset">> := _Offset} ->
            ok;
        _ ->
            ct:fail("Message does not have required user properties (part and offset): ~p", [Msg])
    end.

user_properties(_Msg = #{properties := #{'User-Property' := UserProperties}}) ->
    maps:from_list(UserProperties);
user_properties(_Msg) ->
    #{}.

binfmt(Format, Args) ->
    iolist_to_binary(io_lib:format(Format, Args)).
