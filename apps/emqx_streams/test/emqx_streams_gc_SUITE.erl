%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_gc_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-include("../src/emqx_streams_internal.hrl").

-define(N_SHARDS, 2).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_testcase(TestCase, Config) ->
    Apps =
        emqx_cth_suite:start(
            [
                emqx_durable_storage,
                {emqx,
                    emqx_streams_test_utils:cth_config(emqx, #{
                        <<"durable_storage">> => #{
                            <<"streams_messages">> => #{<<"n_shards">> => ?N_SHARDS}
                        }
                    })},
                {emqx_mq, emqx_streams_test_utils:cth_config(emqx_mq)},
                {emqx_streams, emqx_streams_test_utils:cth_config(emqx_streams)}
            ],
            #{work_dir => emqx_cth_suite:work_dir(TestCase, Config)}
        ),
    ok = snabbkaffe:start_trace(),
    [{suite_apps, Apps} | Config].

end_per_testcase(_TestCase, Config) ->
    ok = snabbkaffe:stop(),
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

%% Verify that the GC works as expected:
%% * drops expired generations for regular streams
%% * drops expired messages for lastvalue streams
t_gc(_Config) ->
    emqx_config:put([streams, regular_stream_retention_period], 1000),
    ct:sleep(500),
    %% Create a lastvalue Stream
    StreamLV = emqx_streams_test_utils:create_stream(#{
        topic_filter => <<"tc/#">>, is_lastvalue => true
    }),
    %% Create a non-lastvalue Stream
    StreamR = emqx_streams_test_utils:create_stream(#{
        topic_filter => <<"tr/#">>, is_lastvalue => false, data_retention_period => 1000
    }),

    % Publish 10 messages to the streams
    emqx_streams_test_utils:populate_lastvalue(10, #{
        topic_prefix => <<"tc/">>,
        payload_prefix => <<"payload-old-">>
    }),
    emqx_streams_test_utils:populate(10, #{
        topic_prefix => <<"tr/">>,
        payload_prefix => <<"payload-old-">>
    }),

    %% Wait for data retention period
    ct:sleep(1000),
    %% This gc should create a new generation
    ?assertWaitEvent(emqx_streams_gc:gc(), #{?snk_kind := streams_gc_regular_done}, 1000),
    RegularDBGens0 = maps:values(emqx_streams_message_db:initial_generations(StreamR)),
    ?assertEqual([1], lists:usort(RegularDBGens0)),

    % Publish 10 messages to the stream
    emqx_streams_test_utils:populate_lastvalue(10, #{
        topic_prefix => <<"tc/">>,
        payload_prefix => <<"payload-new-">>
    }),
    emqx_streams_test_utils:populate(10, #{
        topic_prefix => <<"tr/">>,
        payload_prefix => <<"payload-new-">>
    }),

    %% Wait for the data retention period
    ct:sleep(1000),
    %% This gc should also create a new generation and drop the first one
    ?assertWaitEvent(emqx_streams_gc:gc(), #{?snk_kind := streams_gc_done}, 1000),
    RegularDBGens1 = maps:values(emqx_streams_message_db:initial_generations(StreamR)),
    ?assertEqual([2], lists:usort(RegularDBGens1)),

    %% Check that only last messages are available
    Records = emqx_streams_message_db:dirty_read_all(StreamLV),
    ?assertEqual(10, length(Records)).

%% Verify that the GC successfully completes when there are no streams
t_gc_noop(_Config) ->
    ?assertWaitEvent(emqx_streams_gc:gc(), #{?snk_kind := streams_gc_done}, 1000).

%% Verify that the GC collects data of regular streams limited by count or byte size
t_limited_regular(_Config) ->
    %% Create a regular stream limited by count
    %% 50 messages per shard maximum
    %% We have ?N_SHARDS = 2 shards, so 50 * 2 = 100 messages maximum
    StreamRC = emqx_streams_test_utils:create_stream(
        #{
            topic_filter => <<"tc/#">>,
            is_lastvalue => false,
            limits => #{
                max_shard_message_count => 50,
                max_shard_message_bytes => infinity
            }
        }
    ),

    %% Publish 200 messages to the stream and run GC
    emqx_streams_test_utils:populate(200, #{
        topic_prefix => <<"tc/">>, payload_prefix => <<"payload-">>, different_clients => true
    }),
    ct:sleep(1100),
    ?assertWaitEvent(emqx_streams_gc:gc(), #{?snk_kind := streams_gc_done}, 1000),

    %% Check that only the last 100 + threshold messages are available
    Records0 = emqx_streams_message_db:dirty_read_all(StreamRC),
    RecordCount0 = length(Records0),
    ct:pal("Record count: ~p", [RecordCount0]),
    ?assert(RecordCount0 =< (100 + 10)),

    %% Create a regular stream limited by bytes
    %% 50KB per shard maximum
    %% We have ?N_SHARDS = 2 shards, so 50KB * 2 = 100KB maximum
    StreamRB = emqx_streams_test_utils:create_stream(
        #{
            topic_filter => <<"tb/#">>,
            is_lastvalue => false,
            limits => #{
                max_shard_message_bytes => 50 * 1024,
                max_shard_message_count => infinity
            }
        }
    ),

    %% Publish 200KB messages to the stream and run GC
    Bin1K = <<1:512>>,
    emqx_streams_test_utils:populate(400, #{
        topic_prefix => <<"tb/">>, payload_prefix => Bin1K, different_clients => true
    }),
    ok = emqx_mq_quota_buffer:flush(?STREAMS_QUOTA_BUFFER),
    ?assertWaitEvent(emqx_streams_gc:gc(), #{?snk_kind := streams_gc_done}, 1000),

    %% Check that only the last 100KB + threshold of messages are available
    Records1 = emqx_streams_message_db:dirty_read_all(StreamRB),
    RecordCount1 = length(Records1),
    TotalBytes1 = lists:sum([byte_size(Value) || {_Topic, _TS, Value} <- Records1]),
    ct:pal("Record count: ~p, total bytes: ~p", [RecordCount1, TotalBytes1]),
    ?assert(TotalBytes1 =< (100 * 1024 * 1.1)).

%% Verify that the GC collects data of lastvalue streams limited by count or byte size
t_limited_lastvalue(_Config) ->
    %% Create a lastvalue stream limited by count
    %% 100 messages per shard maximum
    %% We have ?N_SHARDS = 2 shards, so 100 * 2 = 200 messages maximum
    _StreamLV = emqx_streams_test_utils:create_stream(#{
        topic_filter => <<"tc/#">>,
        is_lastvalue => true,
        key_expression =>
            <<"concat(message.topic, message.headers.properties.User-Property.stream-key)">>,
        limits => #{
            max_shard_message_count => 100,
            max_shard_message_bytes => infinity
        }
    }),
    %% Publish 1st portion of 80 messages to the stream
    emqx_streams_test_utils:populate_lastvalue(80, #{topic_prefix => <<"tc/1/">>}),
    %% Publish 2nd portion of 80 messages to the stream
    emqx_streams_test_utils:populate_lastvalue(80, #{topic_prefix => <<"tc/2/">>}),
    %% Republish 1st portion of 80 messages to the stream with new payloads
    emqx_streams_test_utils:populate_lastvalue(80, #{topic_prefix => <<"tc/1/">>}),
    %% Publish 3rd portion of 80 messages to the stream
    emqx_streams_test_utils:populate_lastvalue(80, #{topic_prefix => <<"tc/3/">>}),
    ct:sleep(1100),

    %% Run GC
    ?assertWaitEvent(emqx_streams_gc:gc(), #{?snk_kind := streams_gc_done}, 1000),

    %% Now we should have 200 + threshold messages in the stream.
    %%
    %% 3rd portion should be at the top of the stream
    %% the republished 1st portion should go next,
    %% and the 2nd portion should be partially evicted
    CSub = emqx_streams_test_utils:emqtt_connect([]),
    emqx_streams_test_utils:emqtt_sub(CSub, <<"$sp/0/earliest/tc/#">>),
    emqx_streams_test_utils:emqtt_sub(CSub, <<"$sp/1/earliest/tc/#">>),
    {ok, Msgs} = emqx_streams_test_utils:emqtt_drain(_MinMsg = 200, _Timeout = 1000),
    ?assert(length(Msgs) < 200 + 20),
    PortionCounts = lists:foldl(
        fun(#{topic := Topic}, Acc) ->
            [<<"tc">>, Portion | _] = binary:split(Topic, <<"/">>, [global]),
            maps:update_with(Portion, fun(X) -> X + 1 end, 1, Acc)
        end,
        #{},
        Msgs
    ),
    ?assertEqual(80, maps:get(<<"3">>, PortionCounts)),
    ?assertEqual(80, maps:get(<<"1">>, PortionCounts)),
    %% Should be partially evicted
    ?assert(maps:get(<<"2">>, PortionCounts) < 80),

    %% Clean up
    ok = emqtt:disconnect(CSub).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

binfmt(Format, Args) ->
    iolist_to_binary(io_lib:format(Format, Args)).

now_ms() ->
    erlang:system_time(millisecond).
