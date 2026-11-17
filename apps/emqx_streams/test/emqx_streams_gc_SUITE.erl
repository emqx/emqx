%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_gc_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").

-include("../src/emqx_streams_internal.hrl").

-define(N_SHARDS, 2).

all() ->
    [{group, mixed_version}, {group, fully_transitioned}].

groups() ->
    Cases = emqx_common_test_helpers:all(?MODULE),
    [{mixed_version, [], Cases}, {fully_transitioned, [], Cases}].

init_per_group(Mode, Config) ->
    [{index_mode, Mode} | Config].

end_per_group(_Mode, _Config) ->
    ok.

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
                {emqx_streams,
                    emqx_streams_test_utils:cth_config(emqx_streams, #{
                        <<"streams">> => #{
                            <<"quota">> => #{
                                <<"buffer_max_size">> => emqx_mq_quota_test_utils:buffer_max_size()
                            }
                        }
                    })}
            ],
            #{work_dir => emqx_cth_suite:work_dir(TestCase, Config)}
        ),
    ok = emqx_mq_quota_test_utils:setup_index_mode(?config(index_mode, Config)),
    ok = snabbkaffe:start_trace(),
    [{suite_apps, Apps} | Config].

end_per_testcase(_TestCase, Config) ->
    ok = emqx_common_test_helpers:call_janitor(),
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
    StreamLV = emqx_streams_test_utils:ensure_stream_created(#{
        name => <<"t_gc_lastvalue">>,
        topic_filter => <<"tc/#">>,
        is_lastvalue => true
    }),
    %% Create a non-lastvalue Stream
    StreamR = emqx_streams_test_utils:ensure_stream_created(#{
        name => <<"t_gc_regular">>,
        topic_filter => <<"tr/#">>,
        is_lastvalue => false,
        data_retention_period => 1000
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

%% Verify that retention removes expired last-value records and preserves newer records.
t_retention_lastvalue(_Config) ->
    test_retention(#{is_lastvalue => true, key_expression => <<"message.topic">>}).

%% Verify that retention removes expired regular records below the count limit.
t_retention_regular_count(_Config) ->
    test_retention(#{
        is_lastvalue => false,
        limits => #{max_shard_message_count => 1000, max_shard_message_bytes => infinity}
    }).

%% Verify that retention removes expired regular records below the byte limit.
t_retention_regular_bytes(_Config) ->
    test_retention(#{
        is_lastvalue => false,
        limits => #{max_shard_message_count => infinity, max_shard_message_bytes => 1024 * 1024}
    }).

%% Verify that the GC successfully completes when there are no streams
t_gc_noop(_Config) ->
    ?assertWaitEvent(emqx_streams_gc:gc(), #{?snk_kind := streams_gc_done}, 10_000).

%% Verify that regular stream GC is skipped when slab information is incomplete.
t_gc_regular_list_slabs_error(_Config) ->
    {ok, SlabInfo0} = emqx_streams_message_db:regular_db_slab_info(),
    [{{Shard, _Generation}, _} | _] = maps:to_list(SlabInfo0),
    ok = meck:new(emqx_ds, [passthrough, no_link]),
    ok = emqx_common_test_helpers:on_exit(fun() -> meck:unload(emqx_ds) end),
    ok = meck:expect(
        emqx_ds,
        list_slabs,
        fun
            (?STREAMS_MESSAGE_REGULAR_DB, #{}) ->
                {#{}, [{Shard, {error, recoverable, test_error}}]};
            (DB, Opts) ->
                meck:passthrough([DB, Opts])
        end
    ),
    ?check_trace(
        ?assertWaitEvent(
            emqx_streams_gc:gc(),
            #{?snk_kind := streams_gc_worker_terminated, reason := normal},
            1000
        ),
        fun(Trace) ->
            ?assertMatch(
                [#{reason := list_slabs_failed, errors := _}],
                ?of_kind(streams_gc_regular_streams_skipped, Trace)
            ),
            ?assertMatch([_], ?of_kind(streams_gc_done, Trace)),
            ?assertEqual([], ?of_kind(mq_gc_regular, Trace))
        end
    ),
    ?assert(not meck:called(emqx_ds, add_generation, [?STREAMS_MESSAGE_REGULAR_DB])),
    ?assert(not meck:called(emqx_ds, drop_slab, [?STREAMS_MESSAGE_REGULAR_DB, '_'])).

%% Verify that the GC collects data of regular streams limited by count or byte size
t_limited_regular(Config) ->
    Mode = ?config(index_mode, Config),
    %% Create a regular stream limited by count: 100 messages.
    %% Stream resides to a single shard
    StreamRC = emqx_streams_test_utils:ensure_stream_created(
        #{
            name => <<"t_limited_regular">>,
            topic_filter => <<"tc/#">>,
            is_lastvalue => false,
            limits => #{
                max_shard_message_count => 100,
                max_shard_message_bytes => infinity
            }
        }
    ),

    %% Publish enough messages to close several buckets before GC.
    emqx_streams_test_utils:populate(400, #{
        topic_prefix => <<"tc/">>, payload_prefix => <<"payload-">>, different_clients => true
    }),
    ct:sleep(1100),
    assert_index_mode(Mode, StreamRC),
    ?assertWaitEvent(emqx_streams_gc:gc(), #{?snk_kind := streams_gc_done}, 1000),

    %% GC keeps at least the limit and at most the limit plus the index error.
    Records0 = emqx_streams_message_db:dirty_read_all(StreamRC),
    RecordCount0 = length(Records0),
    MaxRecordCount0 = emqx_mq_quota_test_utils:max_kept(Mode, 100, buffer_max_size()),
    ct:pal("Record count: ~p, max: ~p", [RecordCount0, MaxRecordCount0]),
    ?assert(RecordCount0 >= 100),
    ?assert(RecordCount0 =< MaxRecordCount0),

    %% Create a regular stream limited by bytes: 50KB.
    %% Stream resides to a single shard
    StreamRB = emqx_streams_test_utils:ensure_stream_created(
        #{
            topic_filter => <<"tb/#">>,
            is_lastvalue => false,
            limits => #{
                max_shard_message_bytes => 50 * 1024,
                max_shard_message_count => infinity
            }
        }
    ),

    %% Publish about 200KB, four times the limit.
    Bin1K = <<0:8192>>,
    emqx_streams_test_utils:populate(200, #{
        topic_prefix => <<"tb/">>, payload_prefix => Bin1K, different_clients => true
    }),
    ok = emqx_mq_quota_buffer:flush(?STREAMS_QUOTA_BUFFER),
    assert_index_mode(Mode, StreamRB),
    ?assertWaitEvent(emqx_streams_gc:gc(), #{?snk_kind := streams_gc_done}, 1000),

    %% GC deletes messages, keeps at least the limit, and at most the limit plus the index error.
    Records1 = emqx_streams_message_db:dirty_read_all(StreamRB),
    RecordCount1 = length(Records1),
    Sizes1 = [byte_size(Value) || {_Topic, _TS, Value} <- Records1],
    TotalBytes1 = lists:sum(Sizes1),
    FlushBytes = buffer_max_size() * lists:max(Sizes1),
    MaxTotalBytes1 = emqx_mq_quota_test_utils:max_kept(Mode, 50 * 1024, FlushBytes),
    ct:pal("Record count: ~p, total bytes: ~p, max: ~p", [RecordCount1, TotalBytes1, MaxTotalBytes1]),
    ?assert(RecordCount1 < 200),
    ?assert(TotalBytes1 >= 50 * 1024),
    ?assert(TotalBytes1 =< MaxTotalBytes1).

%% Verify that the GC collects data of lastvalue streams limited by count or byte size
t_limited_lastvalue(Config) ->
    Mode = ?config(index_mode, Config),
    %% Create a lastvalue stream limited by count: 250 messages.
    %% Stream resides to a single shard
    StreamLV = emqx_streams_test_utils:ensure_stream_created(#{
        name => <<"t_limited_lastvalue">>,
        topic_filter => <<"tc/#">>,
        is_lastvalue => true,
        key_expression =>
            <<"concat(message.topic, message.headers.properties.User-Property.stream-key)">>,
        limits => #{
            max_shard_message_count => 250,
            max_shard_message_bytes => infinity
        }
    }),
    %% Make the oldest portion large enough that GC must remove some of it in either mode.
    emqx_streams_test_utils:populate_lastvalue(100, #{topic_prefix => <<"tc/1/">>}),
    emqx_streams_test_utils:populate_lastvalue(300, #{topic_prefix => <<"tc/2/">>}),
    %% Replace the first portion and append a new portion. Both must survive GC.
    emqx_streams_test_utils:populate_lastvalue(100, #{topic_prefix => <<"tc/1/">>}),
    emqx_streams_test_utils:populate_lastvalue(100, #{topic_prefix => <<"tc/3/">>}),
    ct:sleep(1100),
    assert_index_mode(Mode, StreamLV),

    %% Run GC
    ?assertWaitEvent(emqx_streams_gc:gc(), #{?snk_kind := streams_gc_done}, 1000),

    %% GC keeps at least the limit and at most the limit plus the index error.
    %%
    %% 3rd portion should be at the top of the stream
    %% the republished 1st portion should go next,
    %% and the 2nd portion should be partially evicted
    CSub = emqx_streams_test_utils:emqtt_connect([]),
    emqx_streams_test_utils:emqtt_sub(CSub, <<"$stream/t_limited_lastvalue">>, [
        {<<"stream-offset">>, <<"earliest">>}
    ]),
    {ok, Msgs} = emqx_streams_test_utils:emqtt_drain(_MinMsg = 250, _Timeout = 1000),
    MaxMsgs = emqx_mq_quota_test_utils:max_kept(Mode, 250, buffer_max_size()),
    ct:pal("Delivered: ~p, max: ~p", [length(Msgs), MaxMsgs]),
    ?assert(length(Msgs) =< MaxMsgs),
    PortionCounts = lists:foldl(
        fun(#{topic := Topic}, Acc) ->
            [<<"tc">>, Portion | _] = binary:split(Topic, <<"/">>, [global]),
            maps:update_with(Portion, fun(X) -> X + 1 end, 1, Acc)
        end,
        #{},
        Msgs
    ),
    ?assertEqual(100, maps:get(<<"3">>, PortionCounts)),
    ?assertEqual(100, maps:get(<<"1">>, PortionCounts)),
    %% Should be partially evicted
    ?assert(maps:get(<<"2">>, PortionCounts) < 300),

    %% Clean up
    ok = emqtt:disconnect(CSub).

%% Verify that when updating the GC interval, the GC is rescheduled with the new interval
t_update_gc_interval(_Config) ->
    OldConfig = emqx:get_raw_config([streams]),
    ?assertWaitEvent(
        emqx:update_config([streams], OldConfig#{<<"gc_interval">> => <<"1s">>}),
        #{?snk_kind := streams_gc_done},
        2000
    ).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

buffer_max_size() ->
    emqx_mq_quota_test_utils:buffer_max_size().

assert_index_mode(Mode, Stream) ->
    emqx_mq_quota_test_utils:assert_index_mode(
        Mode, ?STREAMS_MESSAGE_LASTVALUE_DB, Stream, emqx_streams_prop:quota_index_opts(Stream)
    ).

test_retention(StreamOpts) ->
    RetentionMs = 3600_000,
    Stream = emqx_streams_test_utils:ensure_stream_created(StreamOpts#{
        topic_filter => <<"retention/#">>, data_retention_period => RetentionMs
    }),
    NowUs = erlang:system_time(microsecond),
    RetentionUs = erlang:convert_time_unit(RetentionMs, millisecond, microsecond),
    ok = meck:new(emqx_ds, [passthrough, no_link]),
    ok = emqx_common_test_helpers:on_exit(fun() -> meck:unload(emqx_ds) end),

    %% Store distinct keys with controlled timestamps on both sides of the retention cutoff.
    lists:foreach(
        fun({Prefix, Timestamp}) ->
            ok = meck:expect(emqx_ds, tx_write, fun
                ({Topic, ?ds_tx_ts_monotonic, Value}) ->
                    meck:passthrough([{Topic, Timestamp, Value}]);
                (Record) ->
                    meck:passthrough([Record])
            end),
            ok = emqx_streams_test_utils:populate(10, #{
                topic_prefix => Prefix, different_clients => true
            })
        end,
        [
            {<<"retention/old/">>, NowUs - 2 * RetentionUs},
            {<<"retention/fresh/">>, NowUs - RetentionUs div 2}
        ]
    ),
    ok = meck:delete(emqx_ds, tx_write, 1),
    ok = emqx_mq_quota_buffer:flush(?STREAMS_QUOTA_BUFFER),
    Records = emqx_streams_message_db:dirty_read_all(Stream),
    ?assertEqual(20, length(Records)),
    FreshRecords = [
        Record
     || {_, Timestamp, _} = Record <- Records, Timestamp > NowUs - RetentionUs
    ],
    ?assertEqual(10, length(FreshRecords)),

    %% Check stored records after GC completes, without consumer-side expiry filtering.
    ?assertWaitEvent(emqx_streams_gc:gc(), #{?snk_kind := streams_gc_done}, 5000),
    ?assertEqual(
        lists:sort(FreshRecords),
        lists:sort(emqx_streams_message_db:dirty_read_all(Stream))
    ).

binfmt(Format, Args) ->
    iolist_to_binary(io_lib:format(Format, Args)).

now_ms() ->
    erlang:system_time(millisecond).
