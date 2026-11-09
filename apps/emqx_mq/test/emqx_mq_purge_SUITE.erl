%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_purge_SUITE).

-moduledoc """
Purge of queue messages, for every queue type.
All times are DS storage times. Records are written directly to DS with chosen timestamps.
""".

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("../src/emqx_mq_internal.hrl").
-include("../gen_src/MessageQueue.hrl").

all() ->
    [
        {group, regular},
        {group, limited_regular},
        {group, lastvalue},
        {group, limited_lastvalue},
        t_concurrent_write_survives,
        t_stale_fence_retry,
        t_failed_deletion,
        t_refused_during_upgrade
    ].

groups() ->
    Common = [t_full_purge, t_bounded_purge_connected_client, t_full_purge_connected_client],
    [
        {regular, [], [t_bounded_purge_unlimited | Common]},
        {limited_regular, [], [t_bounded_purge, t_bounded_purge_no_boundary | Common]},
        {lastvalue, [], [t_bounded_purge_unlimited | Common]},
        {limited_lastvalue, [], [t_bounded_purge, t_bounded_purge_no_boundary | Common]}
    ].

init_per_group(Group, Config) ->
    {IsLastvalue, IsLimited} =
        case Group of
            regular -> {false, false};
            limited_regular -> {false, true};
            lastvalue -> {true, false};
            limited_lastvalue -> {true, true}
        end,
    [
        {queue_name, atom_to_binary(Group)},
        {is_lastvalue, IsLastvalue},
        {is_limited, IsLimited}
        | Config
    ].

end_per_group(_Group, _Config) ->
    ok.

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_durable_storage,
            {emqx,
                emqx_mq_test_utils:cth_config(emqx, #{
                    <<"durable_storage">> => #{<<"mq_messages">> => #{<<"n_shards">> => 2}}
                })},
            {emqx_mq,
                emqx_mq_test_utils:cth_config(emqx_mq, #{
                    <<"mq">> => #{<<"quota">> => #{<<"buffer_flush_interval">> => <<"1h">>}}
                })}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(_Case, Config) ->
    ok = emqx_mq_test_utils:cleanup_mqs(),
    ok = snabbkaffe:start_trace(),
    Config.

end_per_testcase(_Case, _Config) ->
    ok = snabbkaffe:stop(),
    ok = emqx_common_test_helpers:call_janitor(),
    ok = emqx_mq_test_utils:cleanup_mqs().

%%------------------------------------------------------------------------------
%% Bounded purge
%%------------------------------------------------------------------------------

-doc """
Bounded purge of a limited queue deletes the messages before the newest index boundary
at or before the requested time. Delivery skips every message stored before the requested time.
""".
t_bounded_purge(Config) ->
    {ok, #{name := Name} = MQ} = create_mq(Config),
    DB = message_db(MQ),
    Slabs = slabs(DB),
    %% Index boundaries T1 < T2 < T3. The request is between T2 and T3.
    NowMs = erlang:system_time(millisecond),
    T1 = (NowMs - 30_000) * 1000,
    T2 = (NowMs - 20_000) * 1000,
    T3 = (NowMs - 10_000) * 1000,
    BeforeMs = NowMs - 15_000,
    BeforeUs = BeforeMs * 1000,
    BeforeT1 = message_record(MQ, <<"before_t1">>, T1 - 1),
    AfterT1 = message_record(MQ, <<"after_t1">>, T1 + 1),
    AfterT2 = message_record(MQ, <<"after_t2">>, T2 + 1),
    BeforeRequest = message_record(MQ, <<"before_request">>, BeforeUs - 1),
    AtRequest = message_record(MQ, <<"at_request">>, BeforeUs),
    AfterT3 = message_record(MQ, <<"after_t3">>, T3 + 1),
    Records = [BeforeT1, AfterT1, AfterT2, BeforeRequest, AtRequest, AfterT3],
    Buckets = [bucket(3, T3, 1), bucket(2, T2, 3), bucket(1, T1, 1), bucket(0, 0, 1)],
    OtherMQ = MQ#{id => emqx_guid:gen()},
    Other = message_record(OtherMQ, <<"other_queue">>, T1 - 1),
    lists:foreach(
        fun(Slab) ->
            write_records(DB, Slab, [index_record(MQ, Buckets), Other | Records])
        end,
        Slabs
    ),

    %% Purge deletes the messages before T2 and keeps the buckets from T2.
    ?assertEqual(
        {ok, MQ#{deliver_cutoff_date => BeforeMs}}, emqx_mq_registry:purge(Name, BeforeMs)
    ),
    Kept = [AfterT2, BeforeRequest, AtRequest, AfterT3],
    ?assertEqual(repeated(Kept, Slabs), lists:sort(emqx_mq_message_db:dirty_read_all(MQ))),
    lists:foreach(
        fun(Slab) ->
            ?assertEqual(lists:sublist(Buckets, 2), read_buckets(DB, Slab, MQ))
        end,
        Slabs
    ),
    %% Another queue is not changed.
    ?assertEqual(repeated([Other], Slabs), lists:sort(emqx_mq_message_db:dirty_read_all(OtherMQ))),
    %% Repeating the purge changes nothing.
    {ok, _} = emqx_mq_registry:purge(Name, BeforeMs),
    ?assertEqual(repeated(Kept, Slabs), lists:sort(emqx_mq_message_db:dirty_read_all(MQ))),

    %% Messages kept because of the index boundary are not delivered.
    ?assertEqual(
        lists:sort(lists:append([[<<"at_request">>, <<"after_t3">>] || _ <- Slabs])),
        drain_payloads(Name, 2 * length(Slabs))
    ).

-doc "Bounded purge of a limited queue keeps all messages when no index boundary is that early".
t_bounded_purge_no_boundary(Config) ->
    {ok, #{name := Name} = MQ} = create_mq(Config),
    DB = message_db(MQ),
    Slabs = slabs(DB),
    BeforeMs = erlang:system_time(millisecond),
    Old = message_record(MQ, <<"old">>, BeforeMs * 1000 - 1000),
    %% The first slab has an index with only the bucket at (0, 0). The others have no index.
    [FirstSlab | OtherSlabs] = Slabs,
    write_records(DB, FirstSlab, [index_record(MQ, [bucket(0, 0, 1)]), Old]),
    lists:foreach(fun(Slab) -> write_records(DB, Slab, [Old]) end, OtherSlabs),
    {ok, _} = emqx_mq_registry:purge(Name, BeforeMs),
    ?assertEqual(repeated([Old], Slabs), lists:sort(emqx_mq_message_db:dirty_read_all(MQ))),
    %% The index is not changed, and no index is created.
    ?assertEqual([bucket(0, 0, 1)], read_buckets(DB, FirstSlab, MQ)),
    lists:foreach(
        fun(Slab) -> ?assertEqual([], dirty_read(DB, Slab, index_topic(MQ))) end, OtherSlabs
    ).

-doc "Bounded purge of an unlimited queue deletes the messages stored before the requested time".
t_bounded_purge_unlimited(Config) ->
    {ok, #{name := Name} = MQ} = create_mq(Config),
    DB = message_db(MQ),
    Slabs = slabs(DB),
    BeforeMs = erlang:system_time(millisecond) - 10_000,
    BeforeUs = BeforeMs * 1000,
    Old = message_record(MQ, <<"old">>, BeforeUs - 1),
    AtRequest = message_record(MQ, <<"at_request">>, BeforeUs),
    Fresh = message_record(MQ, <<"fresh">>, BeforeUs + 1),
    lists:foreach(fun(Slab) -> write_records(DB, Slab, [Old, AtRequest, Fresh]) end, Slabs),
    ?assertEqual(
        {ok, MQ#{deliver_cutoff_date => BeforeMs}}, emqx_mq_registry:purge(Name, BeforeMs)
    ),
    ?assertEqual(
        repeated([AtRequest, Fresh], Slabs), lists:sort(emqx_mq_message_db:dirty_read_all(MQ))
    ),
    %% No index is created.
    ?assertEqual([], emqx_ds:dirty_read(DB, index_topic(MQ))),
    ?assertEqual(
        lists:sort(lists:append([[<<"at_request">>, <<"fresh">>] || _ <- Slabs])),
        drain_payloads(Name, 2 * length(Slabs))
    ).

%%------------------------------------------------------------------------------
%% Full purge
%%------------------------------------------------------------------------------

-doc """
Full purge deletes all messages stored before it.
A limited queue's index then has one empty bucket at the purge fence.
The delivery cutoff is the earliest fence timestamp of all slabs.
""".
t_full_purge(Config) ->
    {ok, #{name := Name} = MQ} = create_mq(Config),
    DB = message_db(MQ),
    Slabs = slabs(DB),
    OldUs = (erlang:system_time(millisecond) - 10_000) * 1000,
    Old = message_record(MQ, <<"old">>, OldUs),
    OtherMQ = MQ#{id => emqx_guid:gen()},
    Other = message_record(OtherMQ, <<"other">>, OldUs),
    Indices =
        case ?config(is_limited, Config) of
            true -> [index_record(MQ, [bucket(1, OldUs - 1, 1), bucket(0, 0, 5)])];
            false -> []
        end,
    lists:foreach(fun(Slab) -> write_records(DB, Slab, [Old, Other | Indices]) end, Slabs),

    {ok, #{deliver_cutoff_date := CutoffMs}} = emqx_mq_registry:purge(Name),
    ?assertEqual([], emqx_mq_message_db:dirty_read_all(MQ)),
    ?assertEqual(repeated([Other], Slabs), lists:sort(emqx_mq_message_db:dirty_read_all(OtherMQ))),
    ?assert(CutoffMs > OldUs div 1000),
    ?assert(CutoffMs =< erlang:system_time(millisecond)),
    case ?config(is_limited, Config) of
        true ->
            %% The fence stays. The index has one empty bucket at the fence and has seen it.
            FenceTsUs = [check_full_purge_index(DB, Slab, MQ) || Slab <- Slabs],
            ?assertEqual(lists:min(FenceTsUs) div 1000, CutoffMs);
        false ->
            %% An unlimited queue gets no index, and its fence is deleted.
            ?assertEqual([], emqx_ds:dirty_read(DB, index_topic(MQ))),
            ?assertEqual([], emqx_ds:dirty_read(DB, fence_topic(MQ)))
    end,

    %% Repeating the purge works and keeps the queue empty.
    {ok, #{deliver_cutoff_date := CutoffMs1}} = emqx_mq_registry:purge(Name),
    ?assert(CutoffMs1 >= CutoffMs),
    ?assertEqual([], emqx_mq_message_db:dirty_read_all(MQ)).

%% Check that the index has one empty bucket at the stored fence. Return the fence timestamp.
check_full_purge_index(DB, Slab, MQ) ->
    [{_, FenceTsUs, FenceSerialBin}] = dirty_read(DB, Slab, fence_topic(MQ)),
    FenceSerial = binary:decode_unsigned(FenceSerialBin),
    ?assertEqual({[bucket(FenceSerial, FenceTsUs, 0)], FenceSerial}, read_index(DB, Slab, MQ)),
    FenceTsUs.

-doc """
A message stored after the purge fence survives the purge, and the index counts it.
The concurrent write runs between the fence write and the index rotation.
""".
t_concurrent_write_survives(_Config) ->
    {ok, MQ} = create_mq(limited_lastvalue_config(<<"concurrent_write">>)),
    DB = message_db(MQ),
    %% Write messages to both shards after the fence of the first purged slab.
    %% The second slab gets its fence after these messages.
    Payloads = payloads_for_all_shards(DB),
    ?force_ordering(
        #{?snk_kind := mq_message_db_purge_fence_written},
        #{?snk_kind := test_concurrent_write, ?snk_span := start}
    ),
    ?force_ordering(
        #{?snk_kind := test_concurrent_write, ?snk_span := {complete, _}},
        #{?snk_kind := mq_message_db_purge_rotate}
    ),
    _ = spawn_link(fun() ->
        ?tp_span(test_concurrent_write, #{}, insert_messages(MQ, Payloads))
    end),
    {ok, _} = emqx_mq_message_db:purge(MQ),
    [#{shard := FirstShard} | _] = ?of_kind(
        mq_message_db_purge_fence_written, snabbkaffe:collect_trace()
    ),
    %% Messages stored after the fence of their slab survive. The others are deleted.
    Survivors = [P || P <- Payloads, emqx_ds:shard_of(DB, P) =:= FirstShard],
    ?assertEqual(lists:sort(Survivors), stored_payloads(MQ)),
    %% The buffered updates of the survivors are counted in the new bucket of their slab.
    ok = emqx_mq_quota_buffer:flush(?MQ_QUOTA_BUFFER),
    lists:foreach(
        fun({Shard, _} = Slab) ->
            Expected =
                case Shard of
                    FirstShard -> length(Survivors);
                    _ -> 0
                end,
            ?assertMatch([#'SerialIndexBucket'{count = Expected}], read_buckets(DB, Slab, MQ))
        end,
        slabs(DB)
    ),
    ?assertEqual([], ?of_kind(mq_message_db_purge_stale_fence, snabbkaffe:collect_trace())).

-doc """
When a flush sees the purge fence before the rotation and does not use it,
the purge writes another fence. The messages flushed in between are then deleted too.
""".
t_stale_fence_retry(_Config) ->
    {ok, MQ} = create_mq(limited_lastvalue_config(<<"stale_fence">>)),
    DB = message_db(MQ),
    %% Write and flush messages in both shards after the fence of the first purged slab.
    %% The flush sees that fence. The head is not full, so the flush only marks it seen.
    Payloads = payloads_for_all_shards(DB),
    ?force_ordering(
        #{?snk_kind := mq_message_db_purge_fence_written},
        #{?snk_kind := test_write_and_flush, ?snk_span := start}
    ),
    ?force_ordering(
        #{?snk_kind := test_write_and_flush, ?snk_span := {complete, _}},
        #{?snk_kind := mq_message_db_purge_rotate}
    ),
    _ = spawn_link(fun() ->
        ?tp_span(test_write_and_flush, #{}, begin
            ok = insert_messages(MQ, Payloads),
            ok = emqx_mq_quota_buffer:flush(?MQ_QUOTA_BUFFER)
        end)
    end),
    {ok, _} = emqx_mq_message_db:purge(MQ),
    Trace = snabbkaffe:collect_trace(),
    [#{shard := FirstShard} | _] = ?of_kind(mq_message_db_purge_fence_written, Trace),
    %% Only the first slab needed a second fence. All messages are deleted.
    ?assertMatch([#{shard := FirstShard}], ?of_kind(mq_message_db_purge_stale_fence, Trace)),
    ?assertEqual([], stored_payloads(MQ)),
    %% Every slab's index has one empty bucket.
    lists:foreach(
        fun(Slab) ->
            ?assertMatch(
                [#'SerialIndexBucket'{bytes = 0, count = 0}], read_buckets(DB, Slab, MQ)
            )
        end,
        slabs(DB)
    ).

-doc "A failed deletion keeps the delivery cutoff, and a repeated purge completes the deletion".
t_failed_deletion(_Config) ->
    {ok, #{name := Name} = MQ} = create_mq(limited_lastvalue_config(<<"failed_deletion">>)),
    DB = message_db(MQ),
    [Slab | _] = slabs(DB),
    BeforeMs = erlang:system_time(millisecond),
    T1 = (BeforeMs - 2000) * 1000,
    Old = message_record(MQ, <<"old">>, T1 - 1),
    Fresh = message_record(MQ, <<"fresh">>, T1 + 1),
    write_records(DB, Slab, [index_record(MQ, [bucket(1, T1, 1), bucket(0, 0, 1)]), Old, Fresh]),
    ok = meck:new(emqx_ds, [passthrough, no_link]),
    ok = emqx_common_test_helpers:on_exit(fun() -> meck:unload(emqx_ds) end),
    %% Make the deletion transaction fail its precondition: the index exists.
    IndexTopic = index_topic(MQ),
    ok = meck:expect(emqx_ds, tx_del_topic, fun(Topic, 0, To) ->
        ok = emqx_ds:tx_ttv_assert_absent(IndexTopic, ?QUOTA_INDEX_TS),
        meck:passthrough([Topic, 0, To])
    end),
    ?assertMatch(
        {error, [{Slab, {unrecoverable, {precondition_failed, _}}}]},
        emqx_mq_registry:purge(Name, BeforeMs)
    ),
    %% The index cut is committed. Both messages and the queue settings stay.
    ?assertEqual([bucket(1, T1, 1)], read_buckets(DB, Slab, MQ)),
    ?assertEqual(lists:sort([Old, Fresh]), lists:sort(emqx_mq_message_db:dirty_read_all(MQ))),
    ?assertEqual({ok, MQ}, emqx_mq_registry:find(Name)),
    %% A repeated purge finds the same boundary and deletes the old message.
    ok = meck:delete(emqx_ds, tx_del_topic, 3),
    {ok, _} = emqx_mq_registry:purge(Name, BeforeMs),
    ?assertEqual([Fresh], emqx_mq_message_db:dirty_read_all(MQ)).

-doc "Purge is refused, and nothing changes, while some cluster node does not support the serial index".
t_refused_during_upgrade(_Config) ->
    {ok, #{name := Name} = MQ} = create_mq(limited_lastvalue_config(<<"refused">>)),
    DB = message_db(MQ),
    [Slab | _] = slabs(DB),
    Old = message_record(MQ, <<"old">>, 1000),
    write_records(DB, Slab, [Old]),
    ok = meck:new(emqx_bpapi, [passthrough, no_link]),
    ok = emqx_common_test_helpers:on_exit(fun() -> meck:unload(emqx_bpapi) end),
    ok = meck:expect(emqx_bpapi, supported_version, fun
        (_, emqx_mq_quota_index) -> undefined;
        (Node, API) -> meck:passthrough([Node, API])
    end),
    ?assertEqual({error, cluster_upgrade_in_progress}, emqx_mq_registry:purge(Name)),
    ?assertEqual(
        {error, cluster_upgrade_in_progress},
        emqx_mq_registry:purge(Name, erlang:system_time(millisecond))
    ),
    ?assertEqual([Old], emqx_mq_message_db:dirty_read_all(MQ)),
    ?assertEqual({ok, MQ}, emqx_mq_registry:find(Name)).

%%------------------------------------------------------------------------------
%% Connected clients
%%------------------------------------------------------------------------------

-doc "Bounded purge restarts the consumer, and the connected subscriber receives later messages".
t_bounded_purge_connected_client(Config) ->
    test_purge_connected_client(bounded, Config).

-doc "Full purge restarts the consumer, and the connected subscriber receives later messages".
t_full_purge_connected_client(Config) ->
    test_purge_connected_client(full, Config).

test_purge_connected_client(Purge, Config) ->
    BeforePayloads = [<<"before-", (integer_to_binary(I))/binary>> || I <- lists:seq(1, 5)],
    AfterPayloads = [<<"after-", (integer_to_binary(I))/binary>> || I <- lists:seq(1, 3)],
    #{name := Name, id := Id} = emqx_mq_test_utils:ensure_mq_created(
        (queue_opts(Config))#{ping_interval => 200}
    ),
    %% Disable MQTT reconnect so that a disconnected subscriber cannot recover silently.
    Publisher = emqx_mq_test_utils:emqtt_connect([]),
    Subscriber = emqx_mq_test_utils:emqtt_connect([{reconnect, 0}]),
    ok = emqx_mq_test_utils:emqtt_sub_mq(Subscriber, Name),
    ok = publish(Publisher, BeforePayloads),
    {ok, BeforeMessages} = emqx_mq_test_utils:emqtt_drain(length(BeforePayloads), 1000),
    ?assertEqual(BeforePayloads, lists:sort(payloads(Subscriber, BeforeMessages))),

    %% Purge stops the consumer that serves the subscription.
    {ok, Consumer} = emqx_mq_consumer:find(Id),
    ConsumerMonitor = monitor(process, Consumer),
    {ok, _} =
        case Purge of
            bounded -> emqx_mq_registry:purge(Name, erlang:system_time(millisecond));
            full -> emqx_mq_registry:purge(Name)
        end,
    ok = publish(Publisher, AfterPayloads),
    receive
        {'DOWN', ConsumerMonitor, process, Consumer, _} -> ok
    after 5000 -> ct:fail(consumer_not_stopped)
    end,

    %% The same subscriber receives every message published after the purge.
    {ok, AfterMessages} = emqx_mq_test_utils:emqtt_drain(length(AfterPayloads), 5000),
    ?assertEqual(AfterPayloads, lists:sort(payloads(Subscriber, AfterMessages))),
    ?assertEqual(connected, emqtt:status(Subscriber)),
    ?assertEqual(pong, emqtt:ping(Subscriber)),
    ok = emqtt:disconnect(Subscriber),
    ok = emqtt:disconnect(Publisher).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

limited_lastvalue_config(Name) ->
    [{queue_name, Name}, {is_lastvalue, true}, {is_limited, true}].

queue_opts(Config) ->
    Opts0 = #{
        name => ?config(queue_name, Config),
        topic_filter => <<"purge/#">>,
        is_lastvalue => ?config(is_lastvalue, Config)
    },
    Opts =
        case ?config(is_lastvalue, Config) of
            true -> Opts0#{key_expression => <<"message.payload">>};
            false -> Opts0
        end,
    case ?config(is_limited, Config) of
        true ->
            Opts#{
                limits => #{max_shard_message_count => 1000, max_shard_message_bytes => infinity}
            };
        false ->
            Opts
    end.

%% Payloads whose last-value keys cover every shard.
payloads_for_all_shards(DB) ->
    Payloads = [<<"p", (integer_to_binary(I))/binary>> || I <- lists:seq(1, 10)],
    ?assertEqual(
        lists:sort(emqx_ds:list_shards(DB)),
        lists:usort([emqx_ds:shard_of(DB, P) || P <- Payloads])
    ),
    Payloads.

insert_messages(MQ, Payloads) ->
    lists:foreach(
        fun(Payload) ->
            Message = emqx_message:make(<<"publisher">>, 1, <<"purge/1">>, Payload),
            ok = emqx_mq_message_db:insert(MQ, Message)
        end,
        Payloads
    ).

stored_payloads(MQ) ->
    lists:sort([
        emqx_message:payload(emqx_mq_message_db:decode_message(Bin))
     || {_, _, Bin} <- emqx_mq_message_db:dirty_read_all(MQ)
    ]).

create_mq(Config) ->
    emqx_mq_test_utils:create_mq(queue_opts(Config)).

message_db(MQ) ->
    case emqx_mq_prop:is_append_only(MQ) of
        true -> ?MQ_MESSAGE_REGULAR_DB;
        false -> ?MQ_MESSAGE_LASTVALUE_DB
    end.

slabs(DB) ->
    lists:sort(maps:keys(emqx_ds:list_slabs(DB))).

message_record(#{topic_filter := TopicFilter, id := Id}, Payload, StoredUs) ->
    Message = emqx_message:make(<<"publisher">>, 1, <<"purge/1">>, Payload),
    {
        [<<"topic">>, TopicFilter, Id, <<"key">>, Payload],
        StoredUs,
        emqx_mq_message_db:encode_message(Message)
    }.

%% Bucket counters are not checked against the stored messages.
bucket(Serial, TsUs, Count) ->
    #'SerialIndexBucket'{serial = Serial, tsUs = TsUs, bytes = Count * 100, count = Count}.

index_topic(#{topic_filter := TopicFilter, id := Id}) ->
    [<<"topic">>, TopicFilter, Id, <<"index">>].

fence_topic(#{topic_filter := TopicFilter, id := Id}) ->
    [<<"topic">>, TopicFilter, Id, <<"fence">>].

index_record(MQ, Buckets) ->
    Index = #'StorageIndex'{maxTsUs = 0, serialBuckets = Buckets, seenFenceSerial = 0},
    {index_topic(MQ), ?QUOTA_INDEX_TS, 'MessageQueue':encode('StorageIndex', Index)}.

write_records(DB, {Shard, Generation}, Records) ->
    ?assertMatch(
        {atomic, _, ok},
        emqx_ds:trans(#{db => DB, shard => Shard, generation => Generation}, fun() ->
            lists:foreach(fun emqx_ds:tx_write/1, Records)
        end)
    ).

dirty_read(DB, {Shard, Generation}, Topic) ->
    emqx_ds:dirty_read(#{db => DB, shard => Shard, generation => Generation}, Topic).

%% Return the stored buckets and the serial of the last seen fence.
read_index(DB, Slab, MQ) ->
    [{_, ?QUOTA_INDEX_TS, Bin}] = dirty_read(DB, Slab, index_topic(MQ)),
    #'StorageIndex'{serialBuckets = Buckets, seenFenceSerial = SeenFenceSerial} =
        'MessageQueue':decode('StorageIndex', Bin),
    {Buckets, SeenFenceSerial}.

read_buckets(DB, Slab, MQ) ->
    {Buckets, _} = read_index(DB, Slab, MQ),
    Buckets.

repeated(Records, Slabs) ->
    lists:sort(lists:append([Records || _ <- Slabs])).

drain_payloads(Name, Count) ->
    Client = emqx_mq_test_utils:emqtt_connect([]),
    ok = emqx_mq_test_utils:emqtt_sub_mq(Client, Name),
    {ok, Messages} = emqx_mq_test_utils:emqtt_drain(Count, 1000),
    ok = emqtt:disconnect(Client),
    lists:sort([Payload || #{payload := Payload} <- Messages]).

publish(Publisher, Payloads) ->
    lists:foreach(
        fun(Payload) ->
            {ok, _} = emqx_mq_test_utils:emqtt_pub_mq(
                Publisher, <<"purge/1">>, Payload, #{key => Payload}
            )
        end,
        Payloads
    ).

payloads(Client, Messages) ->
    [Payload || #{client_pid := Pid, payload := Payload} <- Messages, Pid =:= Client].
