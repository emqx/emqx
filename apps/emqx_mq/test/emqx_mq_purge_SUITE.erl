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
-include_lib("emqx/include/asserts.hrl").
-include("../src/emqx_mq_internal.hrl").
-include("../gen_src/MessageQueue.hrl").

all() ->
    [
        {group, regular},
        {group, limited_regular},
        {group, lastvalue},
        {group, limited_lastvalue},
        t_rotate_seen_fence,
        t_failed_deletion_limited,
        t_failed_deletion_unlimited_lastvalue,
        t_refused_during_upgrade,
        t_refused_being_deleted,
        t_gc_limited_cutoff
    ].

groups() ->
    Common = [t_full_purge, t_bounded_purge_connected_client, t_full_purge_connected_client],
    [
        {regular, [], [t_bounded_purge_unlimited, t_unlimited_clock_boundary | Common]},
        {limited_regular, [], [t_bounded_purge, t_bounded_purge_no_boundary | Common]},
        {lastvalue, [], [t_bounded_purge_unlimited, t_unlimited_clock_boundary | Common]},
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
Bounded purge of a limited queue deletes the messages stored before the requested time,
and drops the index buckets that end at or before it.
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

    %% Purge deletes the messages before the requested time and keeps the buckets from T2.
    ?assertEqual({ok, MQ#{deliver_cutoff_date => BeforeMs}}, purge_and_wait(MQ, BeforeMs)),
    Kept = [AtRequest, AfterT3],
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
    {ok, _} = purge_and_wait(MQ, BeforeMs),
    ?assertEqual(repeated(Kept, Slabs), lists:sort(emqx_mq_message_db:dirty_read_all(MQ))),

    %% Only the kept messages are delivered.
    ?assertEqual(
        lists:sort(lists:append([[<<"at_request">>, <<"after_t3">>] || _ <- Slabs])),
        drain_payloads(Name, 2 * length(Slabs))
    ).

-doc """
Bounded purge of a limited queue deletes the messages before the requested time,
and keeps the index when only the bucket at (0, 0) starts that early.
""".
t_bounded_purge_no_boundary(Config) ->
    {ok, MQ} = create_mq(Config),
    DB = message_db(MQ),
    Slabs = slabs(DB),
    BeforeMs = erlang:system_time(millisecond),
    Old = message_record(MQ, <<"old">>, BeforeMs * 1000 - 1000),
    %% The first slab has an index with only the bucket at (0, 0). The others have no index.
    [FirstSlab | OtherSlabs] = Slabs,
    write_records(DB, FirstSlab, [index_record(MQ, [bucket(0, 0, 1)]), Old]),
    lists:foreach(fun(Slab) -> write_records(DB, Slab, [Old]) end, OtherSlabs),
    {ok, _} = purge_and_wait(MQ, BeforeMs),
    ?assertEqual([], emqx_mq_message_db:dirty_read_all(MQ)),
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
    ?assertEqual({ok, MQ#{deliver_cutoff_date => BeforeMs}}, purge_and_wait(MQ, BeforeMs)),
    ?assertEqual(
        repeated([AtRequest, Fresh], Slabs), lists:sort(emqx_mq_message_db:dirty_read_all(MQ))
    ),
    %% No index is created.
    ?assertEqual([], emqx_ds:dirty_read(DB, index_topic(MQ))),
    ?assertEqual(
        lists:sort(lists:append([[<<"at_request">>, <<"fresh">>] || _ <- Slabs])),
        drain_payloads(Name, 2 * length(Slabs))
    ).

-doc "Purge of an unlimited queue uses the local time without fences, and clamps a future cutoff to it".
t_unlimited_clock_boundary(Config) ->
    {ok, MQ0} = create_mq(Config),
    ok = meck:new(emqx_mq_quota_index_tx, [passthrough, no_link]),
    ok = emqx_common_test_helpers:on_exit(fun() -> meck:unload(emqx_mq_quota_index_tx) end),
    ok = meck:expect(emqx_mq_quota_index_tx, write_fence_tx, fun(_) -> error(unexpected_fence) end),
    StartMs = erlang:system_time(millisecond),
    {ok, #{deliver_cutoff_date := CutoffMs0} = MQ1} = purge_and_wait(MQ0),
    ?assert(CutoffMs0 >= StartMs),
    ?assert(CutoffMs0 =< erlang:system_time(millisecond)),
    {ok, #{deliver_cutoff_date := CutoffMs1}} = purge_and_wait(MQ1, StartMs + 3600_000),
    ?assert(CutoffMs1 >= CutoffMs0),
    ?assert(CutoffMs1 =< erlang:system_time(millisecond)).

%%------------------------------------------------------------------------------
%% Full purge
%%------------------------------------------------------------------------------

-doc """
Full purge deletes all messages stored before it.
A limited queue's index then has one empty bucket at the purge fence.
The delivery cutoff is the latest fence timestamp of all slabs, rounded up to a millisecond.
""".
t_full_purge(Config) ->
    {ok, MQ} = create_mq(Config),
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

    {ok, #{deliver_cutoff_date := CutoffMs}} = purge_and_wait(MQ),
    ?assertEqual([], emqx_mq_message_db:dirty_read_all(MQ)),
    ?assertEqual(repeated([Other], Slabs), lists:sort(emqx_mq_message_db:dirty_read_all(OtherMQ))),
    ?assert(CutoffMs > OldUs div 1000),
    %% Rounding up can put the cutoff at most 1 ms after the current time.
    ?assert(CutoffMs =< erlang:system_time(millisecond) + 1),
    case ?config(is_limited, Config) of
        true ->
            %% The fence stays. The index has one empty bucket at the fence and has seen it.
            FenceTsUs = [check_full_purge_index(DB, Slab, MQ) || Slab <- Slabs],
            ?assertEqual((lists:max(FenceTsUs) + 999) div 1000, CutoffMs);
        false ->
            %% An unlimited queue gets no index and no fence.
            ?assertEqual([], emqx_ds:dirty_read(DB, index_topic(MQ))),
            ?assertEqual([], emqx_ds:dirty_read(DB, fence_topic(MQ)))
    end,

    %% Repeating the purge works and keeps the queue empty.
    {ok, #{deliver_cutoff_date := CutoffMs1}} = purge_and_wait(MQ),
    ?assert(CutoffMs1 >= CutoffMs),
    ?assertEqual([], emqx_mq_message_db:dirty_read_all(MQ)).

%% Check that the index has one empty bucket at the stored fence. Return the fence timestamp.
check_full_purge_index(DB, Slab, MQ) ->
    [{_, FenceTsUs, FenceSerialBin}] = dirty_read(DB, Slab, fence_topic(MQ)),
    FenceSerial = binary:decode_unsigned(FenceSerialBin),
    ?assertEqual({[bucket(FenceSerial, FenceTsUs, 0)], FenceSerial}, read_index(DB, Slab, MQ)),
    FenceTsUs.

-doc "A full purge rotates at the fence it wrote, also when a flush has seen that fence before".
t_rotate_seen_fence(_Config) ->
    {ok, MQ} = create_mq(limited_lastvalue_config(<<"seen_fence">>)),
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
    {ok, _} = emqx_mq_message_db:rotate_quota_indexes(MQ),
    WrittenFences = maps:from_list([
        {Shard, FenceSerial}
     || #{shard := Shard, fence_serial := FenceSerial} <- ?of_kind(
            mq_message_db_purge_fence_written, snabbkaffe:collect_trace()
        )
    ]),
    %% Each slab keeps the fence that the purge wrote, and its index has one empty bucket there.
    lists:foreach(
        fun({Shard, _} = Slab) ->
            [{_, _, FenceSerialBin}] = dirty_read(DB, Slab, fence_topic(MQ)),
            ?assertEqual(maps:get(Shard, WrittenFences), binary:decode_unsigned(FenceSerialBin)),
            _ = check_full_purge_index(DB, Slab, MQ)
        end,
        slabs(DB)
    ).

-doc """
When the deletion worker fails, a limited queue keeps the delivery cutoff and the truncated index,
and GC deletes the messages before the delivery cutoff.
""".
t_failed_deletion_limited(_Config) ->
    {ok, MQ} = create_mq(limited_lastvalue_config(<<"failed_deletion_limited">>)),
    DB = message_db(MQ),
    [Slab | _] = slabs(DB),
    BeforeMs = erlang:system_time(millisecond) - 1000,
    BeforeUs = BeforeMs * 1000,
    T1 = BeforeUs - 2000_000,
    Old = message_record(MQ, <<"old">>, T1 - 1),
    Hidden = message_record(MQ, <<"hidden">>, T1 + 1),
    Fresh = message_record(MQ, <<"fresh">>, BeforeUs + 1),
    write_records(DB, Slab, [
        index_record(MQ, [bucket(1, T1, 1), bucket(0, 0, 1)]), Old, Hidden, Fresh
    ]),
    %% The purge succeeds, but the deletion worker fails.
    ok = fail_del_batch(),
    ?assertEqual({ok, MQ#{deliver_cutoff_date => BeforeMs}}, purge_and_wait(MQ, BeforeMs)),
    ?assertEqual([bucket(1, T1, 1)], read_buckets(DB, Slab, MQ)),
    ?assertEqual(
        lists:sort([Old, Hidden, Fresh]), lists:sort(emqx_mq_message_db:dirty_read_all(MQ))
    ),
    %% GC deletes the messages before the delivery cutoff, also inside the kept bucket.
    ok = meck:unload(emqx_ds),
    ?assertWaitEvent(emqx_mq_gc:gc(), #{?snk_kind := mq_gc_done}, 5000),
    ?assertEqual([Fresh], emqx_mq_message_db:dirty_read_all(MQ)).

-doc """
When the deletion worker fails, an unlimited last-value queue keeps the delivery cutoff,
and GC deletes the messages before it.
""".
t_failed_deletion_unlimited_lastvalue(_Config) ->
    {ok, MQ} = create_mq([
        {queue_name, <<"failed_deletion_unlimited">>}, {is_lastvalue, true}, {is_limited, false}
    ]),
    DB = message_db(MQ),
    [Slab | _] = slabs(DB),
    BeforeMs = erlang:system_time(millisecond) - 2000,
    BeforeUs = BeforeMs * 1000,
    Old = message_record(MQ, <<"old">>, BeforeUs - 1),
    AtCutoff = message_record(MQ, <<"at_cutoff">>, BeforeUs),
    write_records(DB, Slab, [Old, AtCutoff]),
    %% The purge succeeds, but the deletion worker fails.
    ok = fail_del_batch(),
    ?assertEqual({ok, MQ#{deliver_cutoff_date => BeforeMs}}, purge_and_wait(MQ, BeforeMs)),
    ?assertEqual(lists:sort([Old, AtCutoff]), lists:sort(emqx_mq_message_db:dirty_read_all(MQ))),
    %% GC deletes the message before the delivery cutoff.
    ok = meck:unload(emqx_ds),
    ?assertWaitEvent(emqx_mq_gc:gc(), #{?snk_kind := mq_gc_done}, 5000),
    ?assertEqual([AtCutoff], emqx_mq_message_db:dirty_read_all(MQ)).

-doc "Purge is refused, and nothing changes, while some cluster node does not support the serial index".
t_refused_during_upgrade(_Config) ->
    {ok, #{name := Name} = MQ} = create_mq(limited_lastvalue_config(<<"refused">>)),
    DB = message_db(MQ),
    [Slab | _] = slabs(DB),
    Old = message_record(MQ, <<"old">>, 1000),
    write_records(DB, Slab, [Old]),
    ok = emqx_mq_test_utils:stub_bpapi_supported_version(emqx_mq_consumer, 1),
    ?assertEqual({error, cluster_upgrade_in_progress}, emqx_mq_registry:purge(Name)),
    ?assertEqual(
        {error, cluster_upgrade_in_progress},
        emqx_mq_registry:purge(Name, erlang:system_time(millisecond))
    ),
    ?assertEqual([Old], emqx_mq_message_db:dirty_read_all(MQ)),
    ?assertEqual({ok, MQ}, emqx_mq_registry:find(Name)).

-doc "Purge is refused, and nothing changes, while the queue is being deleted".
t_refused_being_deleted(_Config) ->
    {ok, #{name := Name} = MQ} = create_mq(limited_lastvalue_config(<<"refused_deleting">>)),
    DB = message_db(MQ),
    [Slab | _] = slabs(DB),
    Old = message_record(MQ, <<"old">>, 1000),
    write_records(DB, Slab, [Old]),
    ok = emqx_mq_registry:mark_deleting(MQ),
    ?assertEqual({error, being_deleted}, emqx_mq_registry:purge(Name)),
    ?assertEqual(
        {error, being_deleted}, emqx_mq_registry:purge(Name, erlang:system_time(millisecond))
    ),
    ?assertEqual([Old], emqx_mq_message_db:dirty_read_all(MQ)),
    ?assertEqual([], dirty_read(DB, Slab, fence_topic(MQ))),
    ?assertEqual({ok, MQ}, emqx_mq_registry:find(Name)).

-doc "GC of a limited queue deletes the messages stored before the delivery cutoff, also inside an index bucket".
t_gc_limited_cutoff(_Config) ->
    NowMs = erlang:system_time(millisecond),
    %% Index boundaries T1 < T2 < T3. The delivery cutoff is between T2 and T3.
    T1 = (NowMs - 30_000) * 1000,
    T2 = (NowMs - 20_000) * 1000,
    T3 = (NowMs - 10_000) * 1000,
    {ok, MQ} = emqx_mq_test_utils:create_mq(
        maps:merge(queue_opts(limited_lastvalue_config(<<"gc_cutoff">>)), #{
            deliver_cutoff_date => NowMs - 15_000
        })
    ),
    DB = message_db(MQ),
    [Slab | _] = slabs(DB),
    BeforeT2 = message_record(MQ, <<"before_t2">>, T2 - 1),
    AfterT2 = message_record(MQ, <<"after_t2">>, T2 + 1),
    AfterT3 = message_record(MQ, <<"after_t3">>, T3 + 1),
    Buckets = [bucket(3, T3, 1), bucket(2, T2, 1), bucket(1, T1, 1), bucket(0, 0, 1)],
    write_records(DB, Slab, [index_record(MQ, Buckets), BeforeT2, AfterT2, AfterT3]),
    ?assertWaitEvent(emqx_mq_gc:gc(), #{?snk_kind := mq_gc_done}, 5000),
    ?assertEqual([AfterT3], emqx_mq_message_db:dirty_read_all(MQ)).

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

%% Purge through the registry and wait until the deletion worker finishes.
purge_and_wait(#{name := Name, id := Id}) ->
    wait_purge_finished(Id, fun() -> emqx_mq_registry:purge(Name) end).

purge_and_wait(#{name := Name, id := Id}, BeforeMs) ->
    wait_purge_finished(Id, fun() -> emqx_mq_registry:purge(Name, BeforeMs) end).

wait_purge_finished(Id, PurgeFun) ->
    {Result, {ok, _}} = ?wait_async_action(
        PurgeFun(), #{?snk_kind := mq_purge_finished, id := Id}, 5000
    ),
    Result.

%% Make every batch deletion fail, so the deletion worker gives up.
fail_del_batch() ->
    ok = meck:new(emqx_ds, [passthrough, no_link]),
    ok = emqx_common_test_helpers:on_exit(fun() -> catch meck:unload(emqx_ds) end),
    ok = meck:expect(emqx_ds, del_batch, fun(_DB, _Stream, _Topic, _From, _To, _Opts) ->
        {error, unrecoverable, injected}
    end).

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
