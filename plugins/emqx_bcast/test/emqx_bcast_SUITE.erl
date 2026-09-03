%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("emqx_bcast.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

%%--------------------------------------------------------------------
%% Setup / Teardown
%%--------------------------------------------------------------------

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [emqx, mria],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    ok = emqx_bcast:init_tables(),
    init_test_config(),
    application:load(prometheus),
    {ok, _} = application:ensure_all_started(prometheus),
    emqx_bcast_metrics:init(),
    %% Delivery goes through the async pool; start the plugin supervisor so
    %% pool workers exist, without starting the full application (no hooks).
    %% NOTE: start_link makes the caller the supervisor's parent, and a
    %% supervisor dies when its parent exits. The init_per_suite process
    %% exits right after setup, so a dedicated keeper process owns the
    %% supervisor for the whole suite lifetime.
    SupKeeper = spawn(fun() -> sup_keeper() end),
    [{apps, Apps}, {sup_keeper, SupKeeper} | Config].

end_per_suite(Config) ->
    ?config(sup_keeper, Config) ! stop,
    emqx_cth_suite:stop(?config(apps, Config)).

sup_keeper() ->
    {ok, _Pid} = emqx_bcast_sup:start_link(),
    receive
        stop -> ok
    end.

init_per_testcase(_Case, Config) ->
    %% Settle the async promoter before clearing anything: wait for the
    %% intake queue to drain and give an in-flight promotion batch time to
    %% finish, so the per-test clears cannot race it.
    wait_intake_idle(),
    [
        mnesia:clear_table(T)
     || T <- [
            bcast_msg,
            bcast_msg_meta,
            bcast_message,
            bcast_message_hash,
            bcast_message_api_id,
            bcast_msg_index,
            bcast_quota
        ]
    ],
    catch emqx_bcast:init_tables(),
    [
        catch ets:delete_all_objects(T)
     || T <- [
            bcast_device_registry,
            bcast_subscription
        ]
    ],
    %% Per-shard pull-pool tables (shard_count shards x 4 tables each).
    [
        catch ets:delete_all_objects(T)
     || S <- lists:seq(0, emqx_bcast_pull_pool:shard_count() - 1),
        T <- [
            emqx_bcast_pull_pool:tab(S, bcast_buffer_a),
            emqx_bcast_pull_pool:tab(S, bcast_buffer_b),
            emqx_bcast_pull_pool:tab(S, bcast_buffer3),
            emqx_bcast_pull_pool:tab(S, bcast_pull_inflight),
            emqx_bcast_pull_pool:tab(S, bcast_ack_pending)
        ]
    ],
    %% The owner ETS index/quota and the intake queue are not mnesia
    %% tables; reset them explicitly so no state leaks between tests.
    catch emqx_bcast_intake:reset(),
    catch emqx_bcast_index_owner:reset(),
    %% Full metric registry reset: per-test isolation so ledger/gauge
    %% assertions can compare absolute values, not just deltas.
    catch emqx_bcast_metrics:reset(),
    Config.

wait_intake_idle() ->
    _ = wait_until(fun() -> emqx_bcast_intake:depth() =:= 0 end, 50),
    timer:sleep(50).

end_per_testcase(_Case, _Config) ->
    ok.

init_test_config() ->
    Cfg = #{
        msg_ttl => 15 * 86400,
        cleanup_interval => 60,
        max_device_count => 10000,
        max_message_size_batch => 10240,
        max_message_size_broadcast => 65536,
        max_pending_deliveries => 10000000,
        max_pending_deliveries_per_device => 100,
        msg_warn_threshold => 100000,
        broadcast_topic => <<"/sys/broadcast/${productKey}">>,
        batch_topic => <<"/${productKey}/${deviceName}/user/get">>,
        delivery_pool_size => 2
    },
    persistent_term:put({?APP, config}, Cfg),
    ok.

%%--------------------------------------------------------------------
%% Config tests
%%--------------------------------------------------------------------

-doc "Plugin config defaults are populated with the documented values.".
t_config_defaults(_Config) ->
    Cfg = persistent_term:get({?APP, config}),
    ?assertEqual(10000, maps:get(max_device_count, Cfg)),
    ?assertEqual(15 * 86400, maps:get(msg_ttl, Cfg)),
    ?assertEqual(10240, maps:get(max_message_size_batch, Cfg)),
    ?assertEqual(65536, maps:get(max_message_size_broadcast, Cfg)),
    ?assert(is_binary(maps:get(broadcast_topic, Cfg))),
    ?assert(is_binary(maps:get(batch_topic, Cfg))).

%%--------------------------------------------------------------------
%% ID Mapping tests
%%--------------------------------------------------------------------

-doc "generate_message_id returns distinct API UUID and internal GUID.".
t_generate_message_id(_Config) ->
    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    ?assert(is_binary(ApiMsgId)),
    ?assert(is_binary(MsgGuid)),
    ?assertEqual(16, byte_size(MsgGuid)),
    ?assert(ApiMsgId =/= MsgGuid).

-doc "resolve_message_id returns not_found for an unknown API id.".
t_resolve_message_id_not_found(_Config) ->
    ?assertEqual({error, not_found}, emqx_bcast_id:resolve_message_id(<<"nonexistent">>)).

-doc "resolve_message_id maps a stored API id back to the GUID.".
t_resolve_message_id_found(_Config) ->
    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    Hash = crypto:hash(sha256, <<"test payload">>),
    emqx_bcast_storage:create_message(ApiMsgId, MsgGuid, Hash, <<"test payload">>),
    ?assertEqual({ok, MsgGuid}, emqx_bcast_id:resolve_message_id(ApiMsgId)).

%%--------------------------------------------------------------------
%% Storage / Mnesia tests
%%--------------------------------------------------------------------

-doc "create_message stores payload, hash and api id; lookup returns them.".
t_create_and_lookup_message(_Config) ->
    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    Payload = <<"hello world">>,
    Hash = crypto:hash(sha256, Payload),
    emqx_bcast_storage:create_message(ApiMsgId, MsgGuid, Hash, Payload),
    {ok, Msg} = emqx_bcast_storage:lookup_message(MsgGuid),
    ?assertEqual(Payload, Msg#bcast_message.payload),
    ?assertEqual(Hash, Msg#bcast_message.content_hash),
    ?assertEqual(ApiMsgId, Msg#bcast_message.api_msg_id).

-doc "lookup_message_by_hash finds a message by its content hash.".
t_lookup_by_hash(_Config) ->
    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    Payload = <<"dedup test">>,
    Hash = crypto:hash(sha256, Payload),
    emqx_bcast_storage:create_message(ApiMsgId, MsgGuid, Hash, Payload),
    {ok, Msg} = emqx_bcast_storage:lookup_message_by_hash(Hash),
    ?assertEqual(MsgGuid, Msg#bcast_message.msg_id).

-doc "refresh_message_ttl extends expires_at past the backdated expiry.".
t_refresh_message_ttl(_Config) ->
    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    Payload = <<"ttl test">>,
    Hash = crypto:hash(sha256, Payload),
    emqx_bcast_storage:create_message(ApiMsgId, MsgGuid, Hash, Payload),
    Now = emqx_bcast_utils:now_sec(),
    %% backdate expiry so a refresh is observable
    {atomic, ok} = mnesia:transaction(fun() ->
        [M] = mnesia:wread({bcast_message, MsgGuid}),
        mnesia:write(M#bcast_message{expires_at = Now - 100})
    end),
    {ok, Msg1} = emqx_bcast_storage:lookup_message(MsgGuid),
    emqx_bcast_storage:refresh_message_ttl(MsgGuid),
    {ok, Msg2} = emqx_bcast_storage:lookup_message(MsgGuid),
    ?assert(Msg2#bcast_message.expires_at > Msg1#bcast_message.expires_at).

-doc "create_delivery indexes the delivery for every target device.".
t_create_delivery(_Config) ->
    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    Payload = <<"delivery test">>,
    Hash = crypto:hash(sha256, Payload),
    emqx_bcast_storage:create_message(ApiMsgId, MsgGuid, Hash, Payload),
    DeliveryId = emqx_bcast_utils:gen_guid(),
    DNs = [<<"D1">>, <<"D2">>, <<"D3">>],
    PK = <<"P1">>,
    {ok, D} = emqx_bcast_storage:create_delivery(DeliveryId, MsgGuid, PK, <<"tpl">>, DNs, 3),
    ?assertEqual(0, D#bcast_msg.counter),
    ?assertEqual(3, D#bcast_msg.target_ack_count),
    {ok, Ids} = emqx_bcast_storage:get_device_deliveries({PK, <<"D1">>}),
    ?assertEqual([DeliveryId], Ids).

-doc "claim no_more on a fresh missing-row entry skips it (mria lag guard);\n"
"the orphan scan then removes the genuinely stale entry.".
t_claim_no_more_cleans_stale_index(_Config) ->
    {_ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    Payload = <<"stale claim">>,
    Hash = crypto:hash(sha256, Payload),
    emqx_bcast_storage:create_message(<<"stale-claim-api-id">>, MsgGuid, Hash, Payload),
    PK = <<"PSTALE">>,
    DN = <<"DSTALE">>,
    DeliveryId = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(
        DeliveryId, MsgGuid, PK, <<"tpl">>, [DN], 1
    ),
    ?assertEqual(1, emqx_bcast_storage:pending_delivery_count()),
    %% Simulate the two-phase delete crash window: the delivery record disappeared
    %% while its index entry and quota count survived. The drain hot path
    %% reads the small bcast_msg_meta row, so a vanished delivery = vanished meta.
    ok = mnesia:dirty_delete(bcast_msg_meta, DeliveryId),
    ?assertEqual(
        [{DN, no_more}],
        emqx_bcast_storage:claim_want_next_batch([
            #{clientid => DN, product_key => PK, topics => []}
        ])
    ),
    %% The entry is fresh (appended moments ago): a concurrent promotion on
    %% the peer core might still be replicating its rows, so the claim
    %% skips instead of dropping - the index entry and quota survive.
    ?assertEqual(1, emqx_bcast_storage:pending_delivery_count()),
    %% The bounded orphan scan is the designated repair path for genuinely
    %% stale entries (its per-entry validity check has no lag ambiguity).
    emqx_bcast_storage:cleanup_expired(),
    ?assertEqual(0, emqx_bcast_storage:pending_delivery_count()),
    ?assertEqual({ok, []}, emqx_bcast_storage:get_device_deliveries({PK, DN})).

-doc "A claim whose head entry is dropped (residual) must not deadlock\n"
"when the remaining entries only retry (topic mismatch) - the anchor\n"
"re-anchors to the new head (regression).".
t_claim_no_deadlock_on_dropped_head(_Config) ->
    {_ApiMsgId, MsgGuid} = create_test_msg(<<"deadlock guard">>),
    PK = <<"PDEADLOCK">>,
    DN = <<"DDEADLOCK">>,
    DeliveryA = emqx_bcast_utils:gen_guid(),
    DeliveryB = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(DeliveryA, MsgGuid, PK, <<"tpl">>, [DN], 1),
    {ok, _} = emqx_bcast_storage:create_delivery(DeliveryB, MsgGuid, PK, <<"tpl">>, [DN], 1),
    %% Drop the FIFO head entry from the index: the dids entry goes away but
    %% the queue residual stays (lazy removal), so the claim walk sees a
    %% dropped head followed by a topic-mismatch entry that would retry
    %% forever if the wrap anchor pointed at the vanished head.
    ok = emqx_bcast_storage:remove_index_entries(PK, [DN], DeliveryA),
    Parent = self(),
    {Pid, Ref} = spawn_monitor(fun() ->
        Parent !
            {
                claim_result,
                self(),
                emqx_bcast_storage:claim_want_next_batch([
                    #{clientid => DN, product_key => PK, topics => [{<<"nomatch">>, 1}]}
                ])
            }
    end),
    receive
        {claim_result, Pid, [{DN, no_more}]} ->
            ok;
        {claim_result, Pid, Other} ->
            ct:fail("unexpected claim result: ~p", [Other]);
        {'DOWN', Ref, process, Pid, Reason} ->
            ct:fail("claim process died: ~p", [Reason])
    after 5000 ->
        exit(Pid, kill),
        ct:fail("claim did not terminate (anchor deadlock)")
    end,
    %% DeliveryB must still be claimable with the matching topic.
    [{DN, {ok, _}}] = emqx_bcast_storage:claim_want_next_batch([
        #{clientid => DN, product_key => PK, topics => [{<<"tpl">>, 1}]}
    ]).

-doc "register_device / unregister_device use the keyed path and respect\n"
"the current channel pid.".
t_unregister_device_keyed(_Config) ->
    PK = <<"PREGKEY">>,
    DN = <<"DREGKEY">>,
    Pid1 = spawn(fun() ->
        receive
            stop -> ok
        end
    end),
    Pid2 = spawn(fun() ->
        receive
            stop -> ok
        end
    end),
    try
        emqx_bcast:register_device(PK, DN, Pid1),
        ?assertEqual({ok, Pid1}, emqx_bcast:lookup_device({PK, DN})),
        %% A stale pid (takeover) must not delete the current holder.
        emqx_bcast:unregister_device(PK, DN, Pid2),
        ?assertEqual({ok, Pid1}, emqx_bcast:lookup_device({PK, DN})),
        %% The current holder's disconnect does delete the entry.
        emqx_bcast:unregister_device(PK, DN, Pid1),
        ?assertEqual({error, not_found}, emqx_bcast:lookup_device({PK, DN}))
    after
        exit(Pid1, kill),
        exit(Pid2, kill)
    end.

-doc "process_ack_batch returns a per-ack list; the pull_server_pool ack_batch\n"
"worker must complete without crashing (regression).".
t_ack_batch_worker_no_crash(_Config) ->
    {_ApiMsgId, MsgGuid} = create_test_msg(<<"ack batch no crash">>),
    PK = <<"PACKB">>,
    DN = <<"DACKB">>,
    DeliveryId = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(DeliveryId, MsgGuid, PK, <<"tpl">>, [DN], 1),
    %% process_ack_batch returns a per-ack result LIST, never ok; the
    %% old ok = pattern in the spawned ack worker badmatched every batch.
    Results = emqx_bcast_storage:process_ack_batch([{PK, DN, DeliveryId}]),
    ?assertEqual([counted], Results),
    ?assertEqual({ok, []}, emqx_bcast_storage:get_device_deliveries({PK, DN})),
    %% Full pull_server_pool ack_batch path (cast + spawned worker) must
    %% complete the ack without crashing: fresh delivery, ack through the
    %% pool, index clears.
    DeliveryId2 = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(DeliveryId2, MsgGuid, PK, <<"tpl">>, [DN], 1),
    emqx_bcast_pull_server_pool:ack_batch([{PK, DN, DeliveryId2}]),
    ?assert(
        wait_until(
            fun() -> emqx_bcast_storage:get_device_deliveries({PK, DN}) =:= {ok, []} end,
            100
        )
    ).

-doc "begin_pools_restart snapshots only the called shard's inflight marks\n"
"(regression).".
t_begin_pools_restart_snapshot_own_shard(_Config) ->
    Shard0 = 0,
    Shard1 = 1,
    Now = erlang:system_time(millisecond),
    Inflight0 = emqx_bcast_pull_pool:tab(Shard0, bcast_pull_inflight),
    Inflight1 = emqx_bcast_pull_pool:tab(Shard1, bcast_pull_inflight),
    ets:insert(Inflight0, {<<"R5C0">>, 11, <<"R5P">>, Now}),
    ets:insert(Inflight1, {<<"R5C1">>, 22, <<"R5P">>, Now}),
    try
        {ok, Marks0} =
            gen_server:call(
                emqx_bcast_pull_pool:pool_name(Shard0), begin_pools_restart, infinity
            ),
        {ok, Marks1} =
            gen_server:call(
                emqx_bcast_pull_pool:pool_name(Shard1), begin_pools_restart, infinity
            ),
        %% Each shard returns ONLY its own marks (not the 4x aggregate).
        ?assertEqual([{<<"R5C0">>, 11, <<"R5P">>}], Marks0),
        ?assertEqual([{<<"R5C1">>, 22, <<"R5P">>}], Marks1)
    after
        gen_server:cast(emqx_bcast_pull_pool:pool_name(Shard0), {abort_pools_restart}),
        gen_server:cast(emqx_bcast_pull_pool:pool_name(Shard1), {abort_pools_restart}),
        ets:delete(Inflight0, <<"R5C0">>),
        ets:delete(Inflight1, <<"R5C1">>)
    end.

-doc "abort_pools_restart replays deferred deliver_results (regression): a\n"
"shard armed during a restart that is then aborted must not keep held\n"
"inflight marks forever (window=1 stall).".
t_abort_pools_restart_replays_deferred(_Config) ->
    DN = <<"N1DN">>,
    %% The mark lives on shard_of(DN): mark_current/clear_inflight_mark
    %% resolve the table by the client's shard, so the test must insert
    %% into that same shard's inflight table.
    Shard = emqx_bcast_pull_pool:shard_of(DN),
    Pool = emqx_bcast_pull_pool:pool_name(Shard),
    InflightTab = emqx_bcast_pull_pool:tab(Shard, bcast_pull_inflight),
    PK = <<"N1PK">>,
    Tag = 424242,
    ets:insert(InflightTab, {DN, Tag, PK, erlang:system_time(millisecond)}),
    try
        {ok, _Marks} = gen_server:call(Pool, begin_pools_restart, infinity),
        %% A deliver_results batch arrives while pools_restarting: deferred
        %% (kept, marks held).
        gen_server:cast(Pool, {deliver_results, [{DN, no_more}], [{DN, Tag, PK}]}),
        %% The restart is aborted (a sibling reported restart_in_progress):
        %% the deferred batch must be replayed so the held mark is cleared.
        gen_server:cast(Pool, {abort_pools_restart}),
        ?assert(
            wait_until(fun() -> ets:lookup(InflightTab, DN) =:= [] end, 100)
        )
    after
        gen_server:cast(Pool, {abort_pools_restart}),
        ets:delete(InflightTab, DN)
    end.

-doc "A delivery whose claim lease expires is redelivered; the client's ack\n"
"must decrement the pending quota exactly once, whatever the ack count\n"
"(delivery redelivery accounting - bug report).".
t_lease_expiry_redelivery_ack_accounting(_Config) ->
    {_ApiMsgId, MsgGuid} = create_test_msg(<<"lease accounting">>),
    PK = <<"PLEASEA">>,
    DN = <<"DLEASEA">>,
    DeliveryId = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(DeliveryId, MsgGuid, PK, <<"tpl">>, [DN], 1),
    ?assertEqual(1, emqx_bcast_storage:pending_delivery_count()),
    ?assertEqual(1, emqx_bcast_storage:pending_delivery_count_for({PK, DN})),
    %% First claim = first delivery.
    [{DN, {ok, M1}}] = emqx_bcast_storage:claim_want_next_batch([
        #{clientid => DN, product_key => PK, topics => [{<<"tpl">>, 1}]}
    ]),
    ?assertEqual(1, emqx_bcast_storage:pending_delivery_count()),
    %% Force the claim lease to expire: the inflight ts is rewritten so the
    %% next claim releases the entry back to the queue (redelivery).
    expire_inflight(PK, DN, DeliveryId),
    %% Second claim: lease expired -> the SAME delivery is claimed again.
    [{DN, {ok, M2}}] = emqx_bcast_storage:claim_want_next_batch([
        #{clientid => DN, product_key => PK, topics => [{<<"tpl">>, 1}]}
    ]),
    ?assertEqual(maps:get(delivery_id, M1), maps:get(delivery_id, M2)),
    ?assertEqual(1, emqx_bcast_storage:pending_delivery_count()),
    %% Client acks once: pending quota must go 1 -> 0 (never negative).
    ?assertEqual([counted], emqx_bcast_storage:process_ack_batch([{PK, DN, DeliveryId}])),
    ?assertEqual(0, emqx_bcast_storage:pending_delivery_count()),
    ?assertEqual(0, emqx_bcast_storage:pending_delivery_count_for({PK, DN})),
    ?assertEqual({ok, []}, emqx_bcast_storage:get_device_deliveries({PK, DN})),
    %% A duplicate PUBACK must be a no-op on the counters.
    ?assertEqual([not_found], emqx_bcast_storage:process_ack_batch([{PK, DN, DeliveryId}])),
    ?assertEqual(0, emqx_bcast_storage:pending_delivery_count()),
    ?assertEqual({ok, []}, emqx_bcast_storage:get_device_deliveries({PK, DN})).

%% Rewrite the shard's inflight timestamp for a delivery so the claim
%% lease appears expired (PENDING_TTL_MS backdated). The shard is the same
%% phash2 partition emqx_bcast_index_owner uses internally (shard_of is not
%% exported).
expire_inflight(PK, DN, Did) ->
    Shard = erlang:phash2({PK, DN}, emqx_bcast_index_owner:shard_count()),
    Name = list_to_atom("emqx_bcast_index_owner_" ++ integer_to_list(Shard)),
    Old = sys:get_state(Name),
    Infl = maps:get(inflights, Old),
    Key = {PK, DN},
    Key3 = {PK, DN, Did},
    DeviceInfl = maps:get(Key, Infl, #{}),
    case maps:get(Key3, DeviceInfl, undefined) of
        undefined ->
            ok;
        {_Ts, Tag} ->
            DeviceInfl1 = maps:put(Key3, {0, Tag}, DeviceInfl),
            New = Old#{inflights => maps:put(Key, DeviceInfl1, Infl)},
            sys:replace_state(Name, fun(_) -> New end),
            ok
    end.
-doc "cleanup_expired repairs orphaned index entries and quota counts.".
t_cleanup_expired_repairs_orphan_index(_Config) ->
    {_ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    Payload = <<"orphan cleanup">>,
    Hash = crypto:hash(sha256, Payload),
    emqx_bcast_storage:create_message(<<"orphan-api-id">>, MsgGuid, Hash, Payload),
    PK = <<"PORPHAN">>,
    DN = <<"DORPHAN">>,
    DeliveryId = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(
        DeliveryId, MsgGuid, PK, <<"tpl">>, [DN], 1
    ),
    ok = mnesia:dirty_delete(bcast_msg_meta, DeliveryId),
    ?assertEqual(1, emqx_bcast_storage:pending_delivery_count()),
    emqx_bcast_storage:cleanup_expired(),
    ?assertEqual(0, emqx_bcast_storage:pending_delivery_count()),
    ?assertEqual({ok, []}, emqx_bcast_storage:get_device_deliveries({PK, DN})).

-doc "A duplicate ack (redelivery after a claim-lease expiry) must not\n"
"complete the delivery early: the per-delivery meta counter is decremented\n"
"only for acks that actually removed an index entry (bug report).".
t_redelivery_duplicate_ack_no_early_complete(_Config) ->
    {_ApiMsgId, MsgGuid} = create_test_msg(<<"dup ack accounting">>),
    PK = <<"PDUPACK">>,
    DN1 = <<"DDUPACK1">>,
    DN2 = <<"DDUPACK2">>,
    DeliveryId = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(DeliveryId, MsgGuid, PK, <<"tpl">>, [DN1, DN2], 2),
    ?assertEqual(2, emqx_bcast_storage:pending_delivery_count()),
    %% DN1 is claimed (delivered) once, then redelivered after a lease
    %% expiry (the client was too slow to ack).
    [{DN1, {ok, _}}] = emqx_bcast_storage:claim_want_next_batch([
        #{clientid => DN1, product_key => PK, topics => [{<<"tpl">>, 1}]}
    ]),
    expire_inflight(PK, DN1, DeliveryId),
    [{DN1, {ok, _}}] = emqx_bcast_storage:claim_want_next_batch([
        #{clientid => DN1, product_key => PK, topics => [{<<"tpl">>, 1}]}
    ]),
    %% The client acks the delivery TWICE (first + redelivered PUBLISH).
    ?assertEqual([counted], emqx_bcast_storage:process_ack_batch([{PK, DN1, DeliveryId}])),
    ?assertEqual([not_found], emqx_bcast_storage:process_ack_batch([{PK, DN1, DeliveryId}])),
    %% The delivery must NOT be complete: DN2's entry is still pending and
    %% claimable (the meta counter still requires DN2's ack).
    ?assertEqual({ok, [DeliveryId]}, emqx_bcast_storage:get_device_deliveries({PK, DN2})),
    ?assertEqual(1, emqx_bcast_storage:pending_delivery_count()),
    [{DN2, {ok, _}}] = emqx_bcast_storage:claim_want_next_batch([
        #{clientid => DN2, product_key => PK, topics => [{<<"tpl">>, 1}]}
    ]),
    %% DN2's ack completes the delivery.
    ?assertEqual([counted], emqx_bcast_storage:process_ack_batch([{PK, DN2, DeliveryId}])),
    ?assertEqual(0, emqx_bcast_storage:pending_delivery_count()),
    ?assertEqual({ok, []}, emqx_bcast_storage:get_device_deliveries({PK, DN1})),
    ?assertEqual({ok, []}, emqx_bcast_storage:get_device_deliveries({PK, DN2})),
    ?assertEqual([], mnesia:dirty_match_object(#bcast_msg{_ = '_'})),
    ?assertEqual([], mnesia:dirty_match_object(#bcast_msg_meta{_ = '_'})).

-doc "qos0_fanout_nodes targets only the nodes hosting the listed devices\n"
"and falls back to all nodes when the global session registry is\n"
"disabled (enable_session_registry=false).".
t_qos0_fanout_nodes(_Config) ->
    Self = node(),
    %% undefined = every running node (PubBroadcast).
    Nodes = emqx_bcast_pull_server_pool:qos0_fanout_nodes(undefined),
    ?assert(lists:member(Self, Nodes)),
    %% An explicit DeviceNames list with no online channels falls back to
    %% the local node.
    ?assertEqual([Self], emqx_bcast_pull_server_pool:qos0_fanout_nodes([<<"Q0FN1">>])),
    %% With the global registry disabled, an explicit list must STILL
    %% fan out to every node (lookup_channels degrades to node-local and a
    %% targeted fanout would silently miss remote devices).
    Prev = emqx:get_config([broker, enable_session_registry]),
    try
        _ = emqx:update_config([broker, enable_session_registry], false),
        ?assertEqual(
            Nodes,
            emqx_bcast_pull_server_pool:qos0_fanout_nodes([<<"Q0FN1">>])
        )
    after
        _ = emqx:update_config([broker, enable_session_registry], Prev)
    end.

-doc "A claim over a mixed queue (a lazy-residual non-head entry among\n"
"topic-mismatch retries) must terminate and keep the claimable entry\n"
"claimable - a dropped non-head entry must not reset the wrap anchor\n"
"(keep_anchor).".
t_claim_mixed_queue_residual_nonhead_terminates(_Config) ->
    {_ApiMsgId, MsgGuid} = create_test_msg(<<"mixed queue">>),
    PK = <<"PMIXED">>,
    DN = <<"DMIXED">>,
    DA = emqx_bcast_utils:gen_guid(),
    DB = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(DA, MsgGuid, PK, <<"tpl">>, [DN], 1),
    {ok, _} = emqx_bcast_storage:create_delivery(DB, MsgGuid, PK, <<"tpl">>, [DN], 1),
    %% Remove B: its queue residual stays (dids gone) as a lazy residual.
    ok = emqx_bcast_storage:remove_index_entries(PK, [DN], DB),
    %% Claim with a mismatched topic: both entries are skipped; the claim
    %% must terminate (not wedge) with A left in the queue.
    [{DN, no_more}] = emqx_bcast_storage:claim_want_next_batch([
        #{clientid => DN, product_key => PK, topics => [{<<"nomatch">>, 1}]}
    ]),
    %% A is still claimable with the matching topic.
    [{DN, {ok, Map}}] = emqx_bcast_storage:claim_want_next_batch([
        #{clientid => DN, product_key => PK, topics => [{<<"tpl">>, 1}]}
    ]),
    ?assertEqual(DA, maps:get(delivery_id, Map)).

-doc "The global pending quota must never go negative under redelivery +\n"
"duplicate ack (bug report): create -> claim -> lease expiry -> claim again\n"
"-> ack -> duplicate ack, repeatedly; quota returns to 0 after each cycle.".
t_quota_never_negative_under_redelivery(_Config) ->
    PK = <<"PNEGQ">>,
    DN = <<"DNEGQ">>,
    lists:foreach(
        fun(I) ->
            {_ApiMsgId, MsgGuid} = create_test_msg(<<"neg q ", (integer_to_binary(I))/binary>>),
            DeliveryId = emqx_bcast_utils:gen_guid(),
            {ok, _} = emqx_bcast_storage:create_delivery(
                DeliveryId, MsgGuid, PK, <<"tpl">>, [DN], 1
            ),
            ?assertEqual(1, emqx_bcast_storage:pending_delivery_count()),
            %% Claim (deliver) -> lease expiry -> claim again (redelivery).
            [{DN, {ok, _}}] = emqx_bcast_storage:claim_want_next_batch([
                #{clientid => DN, product_key => PK, topics => [{<<"tpl">>, 1}]}
            ]),
            expire_inflight(PK, DN, DeliveryId),
            [{DN, {ok, _}}] = emqx_bcast_storage:claim_want_next_batch([
                #{clientid => DN, product_key => PK, topics => [{<<"tpl">>, 1}]}
            ]),
            %% The client acks both deliveries (duplicate PUBACKs).
            ?assertEqual([counted], emqx_bcast_storage:process_ack_batch([{PK, DN, DeliveryId}])),
            ?assertEqual([not_found], emqx_bcast_storage:process_ack_batch([{PK, DN, DeliveryId}])),
            Quota = emqx_bcast_storage:pending_delivery_count(),
            ?assert(Quota >= 0),
            ?assertEqual(0, Quota)
        end,
        lists:seq(1, 100)
    ).

-doc "backfill_meta_from_projection must not overwrite live meta rows when\n"
"the meta table exceeds the scan budget (regression): partially-acked\n"
"counters survive a takeover rebuild.".
t_backfill_preserves_live_meta_over_budget(_Config) ->
    PK = <<"PN2">>,
    DN = <<"DN2">>,
    {_ApiMsgId, MsgGuid} = create_test_msg(<<"n2 backfill">>),
    %% Write more than ?CLEANUP_BUDGET (10000) deliveries WITH existing meta
    %% rows. A bounded read-side scan (the bug) would drop the
    %% continuation and re-write the rows beyond the budget, resetting
    %% their counters from the legacy bcast_msg.counter (0) to Target.
    N = 10005,
    Now = emqx_bcast_utils:now_sec(),
    lists:foreach(
        fun(_I) ->
            Did = emqx_bcast_utils:gen_guid(),
            ok = mnesia:dirty_write(#bcast_msg{
                delivery_id = Did,
                msg_id = MsgGuid,
                product_key = PK,
                topic_template = <<"tpl">>,
                target_ack_count = 5,
                counter = 0,
                device_names = [DN],
                created_at = Now,
                expires_at = Now + 86400
            }),
            ok = mnesia:dirty_write(#bcast_msg_meta{
                delivery_id = Did,
                msg_id = MsgGuid,
                topic_template = <<"tpl">>,
                counter = 3
            })
        end,
        lists:seq(1, N)
    ),
    %% Trigger the takeover rebuild: drive_activation runs backfill on
    %% shard 0.
    ok = emqx_bcast_index_owner:rebuild_index(),
    %% Every existing meta row must keep its counter (3); a reset would
    %% write max(0, 5 - 0) = 5 for the rows the bounded scan missed.
    Counters = mnesia:dirty_select(
        bcast_msg_meta,
        [{#bcast_msg_meta{counter = '$1', _ = '_'}, [], ['$1']}]
    ),
    ?assertEqual(N, length(Counters)),
    ?assertEqual([], [C || C <- Counters, C =:= 5]),
    ?assertEqual(N, length([C || C <- Counters, C =:= 3])).
-doc "process_ack removes the delivery index entry for the acking device.".
t_process_ack(_Config) ->
    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    Payload = <<"ack test">>,
    Hash = crypto:hash(sha256, Payload),
    emqx_bcast_storage:create_message(ApiMsgId, MsgGuid, Hash, Payload),
    DeliveryId = emqx_bcast_utils:gen_guid(),
    DNs = [<<"DA">>, <<"DB">>],
    PK = <<"PA">>,
    {ok, _} = emqx_bcast_storage:create_delivery(DeliveryId, MsgGuid, PK, <<"tpl">>, DNs, 2),
    emqx_bcast_storage:process_ack(PK, <<"DA">>, DeliveryId),
    {ok, IdsA} = emqx_bcast_storage:get_device_deliveries({PK, <<"DA">>}),
    ?assertEqual([], IdsA),
    {ok, IdsB} = emqx_bcast_storage:get_device_deliveries({PK, <<"DB">>}),
    ?assertEqual([DeliveryId], IdsB).

-doc "the delivery record is deleted once all devices have acked.".
t_process_ack_all_devices(_Config) ->
    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    Payload = <<"ack all">>,
    Hash = crypto:hash(sha256, Payload),
    emqx_bcast_storage:create_message(ApiMsgId, MsgGuid, Hash, Payload),
    DeliveryId = emqx_bcast_utils:gen_guid(),
    DNs = [<<"DX">>],
    PK = <<"PX">>,
    {ok, _} = emqx_bcast_storage:create_delivery(DeliveryId, MsgGuid, PK, <<"tpl">>, DNs, 1),
    emqx_bcast_storage:process_ack(PK, <<"DX">>, DeliveryId),
    ?assertEqual([], mnesia:dirty_read(bcast_msg, DeliveryId)).

-doc "duplicate acks are idempotent and do not corrupt the index.".
t_process_ack_duplicate(_Config) ->
    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    Payload = <<"dup ack">>,
    Hash = crypto:hash(sha256, Payload),
    emqx_bcast_storage:create_message(ApiMsgId, MsgGuid, Hash, Payload),
    DeliveryId = emqx_bcast_utils:gen_guid(),
    DNs = [<<"DD">>, <<"DE">>],
    PK = <<"PD">>,
    {ok, _} = emqx_bcast_storage:create_delivery(DeliveryId, MsgGuid, PK, <<"tpl">>, DNs, 2),
    emqx_bcast_storage:process_ack(PK, <<"DD">>, DeliveryId),
    emqx_bcast_storage:process_ack(PK, <<"DD">>, DeliveryId),
    {ok, Ids} = emqx_bcast_storage:get_device_deliveries({PK, <<"DE">>}),
    ?assertEqual([DeliveryId], Ids).

-doc "cleanup_expired removes deliveries past their expiry.".
t_cleanup_expired_delivery(_Config) ->
    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    Payload = <<"expire test">>,
    Hash = crypto:hash(sha256, Payload),
    emqx_bcast_storage:create_message(ApiMsgId, MsgGuid, Hash, Payload),
    DeliveryId = emqx_bcast_utils:gen_guid(),
    DNs = [<<"DE">>],
    PK = <<"PE">>,
    {ok, D} = emqx_bcast_storage:create_delivery(DeliveryId, MsgGuid, PK, <<"tpl">>, DNs, 1),
    mnesia:dirty_write(D#bcast_msg{expires_at = 0}),
    emqx_bcast_storage:cleanup_expired(),
    ?assertEqual([], mnesia:dirty_read(bcast_msg, DeliveryId)).

%%--------------------------------------------------------------------
%% Utils tests
%%--------------------------------------------------------------------

-doc "expand_topic substitutes productKey and deviceName placeholders.".
t_topic_expansion(_Config) ->
    Result = emqx_bcast_utils:expand_topic(
        <<"/${productKey}/${deviceName}/user/get">>,
        <<"P1">>,
        <<"D1">>
    ),
    ?assertEqual(<<"/P1/D1/user/get">>, Result).

-doc "sha256 returns a 32-byte digest.".
t_sha256(_Config) ->
    Hash = emqx_bcast_utils:sha256(<<"test">>),
    ?assertEqual(32, byte_size(Hash)).

-doc "decode_base64 handles valid input and rejects invalid encoding.".
t_base64_decode(_Config) ->
    ?assertEqual({ok, <<"hello">>}, emqx_bcast_utils:decode_base64(<<"aGVsbG8=">>)),
    ?assertEqual({error, invalid_base64}, emqx_bcast_utils:decode_base64(<<"!!!">>)).

%%--------------------------------------------------------------------
%% Topic matching tests
%%--------------------------------------------------------------------

-doc "exact topic filters match the concrete topic.".
t_topic_match_exact(_Config) ->
    ?assert(emqx_topic:match(<<"/P1/D1/user/get">>, <<"/P1/D1/user/get">>)).

-doc "plus wildcards match a single topic level.".
t_topic_match_plus(_Config) ->
    ?assert(emqx_topic:match(<<"/P1/D1/user/get">>, <<"/P1/+/user/get">>)).

-doc "hash wildcards match any number of trailing levels.".
t_topic_match_hash(_Config) ->
    ?assert(emqx_topic:match(<<"/P1/D1/user/get">>, <<"/P1/#">>)).

-doc "unrelated filters do not match the topic.".
t_topic_match_no_match(_Config) ->
    ?assertNot(emqx_topic:match(<<"/P1/D1/user/get">>, <<"/P2/+/user/get">>)).

%% Subscription matching is covered by the e2e suite against real EMQX
%% subscription state; the plugin no longer maintains a subscription mirror.

-doc "message.acked removes the delivery; duplicate acks are idempotent.".
t_message_acked_hook(_Config) ->
    PK = <<"PC">>,
    DN = <<"DC1">>,
    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    Payload = <<"message acked test">>,
    Hash = crypto:hash(sha256, Payload),
    emqx_bcast_storage:create_message(ApiMsgId, MsgGuid, Hash, Payload),
    DeliveryId = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(DeliveryId, MsgGuid, PK, <<"tpl">>, [DN], 1),
    Msg = emqx_message:make(
        DeliveryId,
        DN,
        0,
        <<"/PC/DC1/user/get">>,
        Payload,
        #{},
        #{?BCAST_DELIVERY_ID => DeliveryId, ?BCAST_PRODUCT_KEY => PK}
    ),
    ok = emqx_bcast:on_message_acked(#{clientid => DN}, Msg),
    %% delivery record removed after the target ack count is reached
    ?assert(wait_until(fun() -> mnesia:dirty_read(bcast_msg, DeliveryId) =:= [] end, 100)),
    %% duplicate ack is idempotent and does not crash: the ack path is a cast
    %% into emqx_bcast_ack_pool, so sys:get_state guarantees it was processed
    ok = emqx_bcast:on_message_acked(#{clientid => DN}, Msg),
    _ = sys:get_state(emqx_bcast_ack_pool),
    ?assertEqual([], mnesia:dirty_read(bcast_msg, DeliveryId)),
    %% messages without plugin headers pass through untouched
    Plain = emqx_message:make(DN, 0, <<"/t">>, <<"p">>),
    ok = emqx_bcast:on_message_acked(#{clientid => DN}, Plain).

-doc "concurrent identical RegisterMessage calls yield one MessageId.".
t_register_message_concurrent_dedup(_Config) ->
    Content = base64:encode(crypto:strong_rand_bytes(16)),
    Body = #{
        <<"Action">> => <<"RegisterMessage">>,
        <<"MessageContent">> => Content
    },
    Parent = self(),
    N = 20,
    Pids = [
        spawn(fun() ->
            Res = emqx_bcast_api:handle(post, [<<"pub">>], #{body => Body}),
            Parent ! {reg_result, self(), Res}
        end)
     || _ <- lists:seq(1, N)
    ],
    Results = [
        receive
            {reg_result, P, R} -> R
        end
     || P <- Pids
    ],
    ?assertEqual(N, length(Results)),
    lists:foreach(fun(R) -> ?assertMatch({ok, 200, _, _}, R) end, Results),
    Ids = lists:usort([maps:get(<<"MessageId">>, Resp) || {ok, 200, _, Resp} <- Results]),
    ?assertEqual(1, length(Ids)).

-doc "re-registering content refreshes the message TTL.".
t_register_message_ttl_refresh(_Config) ->
    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    Payload = crypto:strong_rand_bytes(16),
    Hash = crypto:hash(sha256, Payload),
    emqx_bcast_storage:create_message(ApiMsgId, MsgGuid, Hash, Payload),
    TTL = emqx_bcast_utils:ttl(),
    Now = emqx_bcast_utils:now_sec(),
    %% backdate expiry so a refresh is observable
    {atomic, ok} = mnesia:transaction(fun() ->
        [M] = mnesia:wread({bcast_message, MsgGuid}),
        mnesia:write(M#bcast_message{expires_at = Now - 100})
    end),
    [#bcast_message{expires_at = OldExpiry}] = mnesia:dirty_read(bcast_message, MsgGuid),
    ?assertEqual(Now - 100, OldExpiry),
    Body = #{
        <<"Action">> => <<"RegisterMessage">>,
        <<"MessageContent">> => base64:encode(Payload)
    },
    {ok, 200, _, _} = emqx_bcast_api:handle(post, [<<"pub">>], #{body => Body}),
    [#bcast_message{expires_at = NewExpiry}] = mnesia:dirty_read(bcast_message, MsgGuid),
    ?assert(NewExpiry >= Now + TTL - 5).

%% Subscription-gated delivery behaviour is covered by the e2e suite with
%% real EMQX subscriptions; the plugin no longer maintains a mirror table
%% that unit tests could seed.

%% Poll until a prometheus counter reaches the expected value (async delivery
%% happens on pool workers, so metrics lag the API response).
wait_metric(Name, Expected) ->
    wait_until(fun() -> metric(Name) =:= Expected end, 100).

wait_until(_F, 0) ->
    false;
wait_until(F, N) ->
    case F() of
        true ->
            true;
        false ->
            timer:sleep(50),
            wait_until(F, N - 1)
    end.

flush_mailbox() ->
    receive
        #deliver{} -> flush_mailbox()
    after 0 -> ok
    end.

%%--------------------------------------------------------------------
%% Async delivery pool tests
%%--------------------------------------------------------------------

-doc "BatchPub by MessageId refreshes the message TTL asynchronously.".
t_async_ttl_refresh(_Config) ->
    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    Payload = crypto:strong_rand_bytes(16),
    Hash = crypto:hash(sha256, Payload),
    emqx_bcast_storage:create_message(ApiMsgId, MsgGuid, Hash, Payload),
    TTL = emqx_bcast_utils:ttl(),
    Now = emqx_bcast_utils:now_sec(),
    {atomic, ok} = mnesia:transaction(fun() ->
        [M] = mnesia:wread({bcast_message, MsgGuid}),
        mnesia:write(M#bcast_message{expires_at = Now - 100})
    end),
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"P1">>,
        <<"DeviceName">> => [<<"OFF1">>],
        <<"MessageId">> => ApiMsgId,
        <<"Qos">> => 1
    },
    {ok, 200, _, _} = emqx_bcast_api:handle(post, [<<"pub">>], #{body => Body}),
    %% TTL refresh is a fire-and-forget pool task; wait for it to land
    ?assert(
        wait_until(
            fun() ->
                [#bcast_message{expires_at = E}] = mnesia:dirty_read(bcast_message, MsgGuid),
                E >= Now + TTL - 5
            end,
            100
        )
    ).

%% QoS0 product-wide broadcast delivery is covered by the e2e suite with
%% real subscribed clients.

-doc "index add/remove are idempotent for repeated calls.".
t_index_add_remove_idempotent(_Config) ->
    PK = <<"PI">>,
    DNs = [<<"D1">>, <<"D2">>],
    Did = emqx_bcast_utils:gen_guid(),
    ok = emqx_bcast_storage:add_index_entries(PK, DNs, Did),
    ok = emqx_bcast_storage:add_index_entries(PK, DNs, Did),
    {ok, Ids} = emqx_bcast_storage:get_device_deliveries({PK, <<"D1">>}),
    ?assertEqual([Did], Ids),
    ok = emqx_bcast_storage:remove_index_entries(PK, DNs, Did),
    ok = emqx_bcast_storage:remove_index_entries(PK, DNs, Did),
    {ok, []} = emqx_bcast_storage:get_device_deliveries({PK, <<"D1">>}).

-doc "pull pool buffer tables exist after pool start.".
t_pull_pool_buffers_initialized(_Config) ->
    ?assertNotEqual(undefined, ets:info(emqx_bcast_pull_pool:tab(0, bcast_buffer_a))),
    ?assertNotEqual(undefined, ets:info(emqx_bcast_pull_pool:tab(0, bcast_buffer_b))),
    ?assertNotEqual(undefined, ets:info(emqx_bcast_pull_pool:tab(0, bcast_buffer3))).

-doc "missing Action returns 400 MissingAction.".
t_api_missing_action(_Config) ->
    Body = #{<<"ProductKey">> => <<"P1">>},
    Request = #{body => Body},
    {error, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(false, maps:get(<<"Success">>, Resp)),
    ?assertEqual(<<"MissingAction">>, maps:get(<<"Code">>, Resp)).

-doc "unknown Action returns 400 UnknownAction.".
t_api_unknown_action(_Config) ->
    Body = #{<<"Action">> => <<"BadAction">>},
    Request = #{body => Body},
    {error, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"UnknownAction">>, maps:get(<<"Code">>, Resp)).

-doc "unknown API path returns not_found.".
t_api_not_found(_Config) ->
    {error, not_found} = emqx_bcast_api:handle(get, [<<"pub">>], #{}).

%%--------------------------------------------------------------------
%% RegisterMessage API tests
%%--------------------------------------------------------------------

-doc "RegisterMessage creates a message and returns its MessageId.".
t_register_message_create(_Config) ->
    Body = #{
        <<"Action">> => <<"RegisterMessage">>,
        <<"MessageContent">> => <<"aGVsbG8=">>
    },
    Request = #{body => Body},
    {ok, 200, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assert(maps:get(<<"Success">>, Resp)),
    ?assert(is_binary(maps:get(<<"MessageId">>, Resp))),
    ?assert(is_binary(maps:get(<<"RequestId">>, Resp))).

-doc "identical content returns the same MessageId.".
t_register_message_dedup(_Config) ->
    Body = #{
        <<"Action">> => <<"RegisterMessage">>,
        <<"MessageContent">> => <<"aGVsbG8=">>
    },
    Request = #{body => Body},
    {ok, _, _, Resp1} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    {ok, _, _, Resp2} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(maps:get(<<"MessageId">>, Resp1), maps:get(<<"MessageId">>, Resp2)).

-doc "refreshing an unknown MessageId returns 400 MessageNotFound.".
t_register_message_refresh_not_found(_Config) ->
    Body = #{
        <<"Action">> => <<"RegisterMessage">>,
        <<"MessageId">> => <<"nonexistent-uuid">>
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"MessageNotFound">>, maps:get(<<"Code">>, Resp)).

-doc "MessageContent and MessageId together return 400.".
t_register_message_mutual_exclusion(_Config) ->
    Body = #{
        <<"Action">> => <<"RegisterMessage">>,
        <<"MessageContent">> => <<"aGVsbG8=">>,
        <<"MessageId">> => <<"some-id">>
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"MessageIdContentConflict">>, maps:get(<<"Code">>, Resp)).

-doc "invalid Base64 returns 400 InvalidBase64.".
t_register_message_invalid_base64(_Config) ->
    Body = #{
        <<"Action">> => <<"RegisterMessage">>,
        <<"MessageContent">> => <<"!!!">>
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"InvalidBase64">>, maps:get(<<"Code">>, Resp)).

-doc "payloads over max_message_size_batch return 400 MessageTooLarge.".
t_register_message_too_large(_Config) ->
    Cfg = persistent_term:get({?APP, config}),
    MaxSize = maps:get(max_message_size_batch, Cfg, 10240),
    Payload = crypto:strong_rand_bytes(MaxSize + 1),
    Body = #{
        <<"Action">> => <<"RegisterMessage">>,
        <<"MessageContent">> => base64:encode(Payload)
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"MessageTooLarge">>, maps:get(<<"Code">>, Resp)).

-doc "RegisterMessage with no content or id returns 400.".
t_register_message_empty(_Config) ->
    Body = #{<<"Action">> => <<"RegisterMessage">>},
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"MessageIdContentConflict">>, maps:get(<<"Code">>, Resp)).

%%--------------------------------------------------------------------
%% BatchPub API tests
%%--------------------------------------------------------------------

-doc "QoS=0 inline BatchPub is accepted.".
t_batch_pub_qos0_inline(_Config) ->
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"P1">>,
        <<"DeviceName">> => [<<"D1">>, <<"D2">>],
        <<"MessageContent">> => <<"aGVsbG8=">>,
        <<"Qos">> => 0
    },
    Request = #{body => Body},
    {ok, 200, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assert(maps:get(<<"Success">>, Resp)),
    ?assert(is_binary(maps:get(<<"MessageId">>, Resp))).

-doc "QoS=1 inline BatchPub is accepted.".
t_batch_pub_qos1_inline(_Config) ->
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"P1">>,
        <<"DeviceName">> => [<<"D1">>],
        <<"MessageContent">> => <<"aGVsbG8=">>,
        <<"Qos">> => 1
    },
    Request = #{body => Body},
    {ok, 200, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assert(maps:get(<<"Success">>, Resp)).

-doc "BatchPub by MessageId reuses the stored payload and returns the id.".
t_batch_pub_messageid_reuse(_Config) ->
    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    Hash = crypto:hash(sha256, <<"reuse">>),
    emqx_bcast_storage:create_message(ApiMsgId, MsgGuid, Hash, <<"reuse">>),
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"P1">>,
        <<"DeviceName">> => [<<"D1">>],
        <<"MessageId">> => ApiMsgId,
        <<"Qos">> => 1
    },
    Request = #{body => Body},
    {ok, 200, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assert(maps:get(<<"Success">>, Resp)),
    ?assertEqual(ApiMsgId, maps:get(<<"MessageId">>, Resp)).

-doc "BatchPub with an unknown MessageId returns 400 MessageNotFound.".
t_batch_pub_messageid_not_found(_Config) ->
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"P1">>,
        <<"DeviceName">> => [<<"D1">>],
        <<"MessageId">> => <<"no-such-id">>,
        <<"Qos">> => 0
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"MessageNotFound">>, maps:get(<<"Code">>, Resp)).

-doc "TopicTemplateName overrides the delivery topic.".
t_batch_pub_topic_template_name(_Config) ->
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"PX">>,
        <<"DeviceName">> => [<<"D1">>],
        <<"MessageContent">> => <<"aGVsbG8=">>,
        <<"Qos">> => 0,
        <<"TopicTemplateName">> => <<"/custom/${deviceName}/topic">>
    },
    Request = #{body => Body},
    {ok, 200, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assert(maps:get(<<"Success">>, Resp)).

-doc "TopicShortName builds the delivery topic suffix.".
t_batch_pub_topic_short_name(_Config) ->
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"PX">>,
        <<"DeviceName">> => [<<"D1">>],
        <<"MessageContent">> => <<"aGVsbG8=">>,
        <<"Qos">> => 0,
        <<"TopicShortName">> => <<"custom">>
    },
    Request = #{body => Body},
    {ok, 200, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assert(maps:get(<<"Success">>, Resp)).

-doc "BatchPub without topic params uses the configured default topic.".
t_batch_pub_default_topic(_Config) ->
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"P1">>,
        <<"DeviceName">> => [<<"D1">>],
        <<"MessageContent">> => <<"aGVsbG8=">>,
        <<"Qos">> => 0
    },
    Request = #{body => Body},
    {ok, 200, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assert(maps:get(<<"Success">>, Resp)).

-doc "duplicate DeviceName entries return 400 DuplicateDeviceName.".
t_batch_pub_duplicate_devices(_Config) ->
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"P1">>,
        <<"DeviceName">> => [<<"D1">>, <<"D1">>],
        <<"MessageContent">> => <<"aGVsbG8=">>,
        <<"Qos">> => 0
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"DuplicateDeviceName">>, maps:get(<<"Code">>, Resp)).

-doc "missing DeviceName returns 400 InvalidDeviceName.".
t_batch_pub_missing_devices(_Config) ->
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"P1">>,
        <<"MessageContent">> => <<"aGVsbG8=">>,
        <<"Qos">> => 0
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"InvalidDeviceName">>, maps:get(<<"Code">>, Resp)).

-doc "MessageContent and MessageId together return 400.".
t_batch_pub_content_id_conflict(_Config) ->
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"P1">>,
        <<"DeviceName">> => [<<"D1">>],
        <<"MessageContent">> => <<"aGVsbG8=">>,
        <<"MessageId">> => <<"some-id">>,
        <<"Qos">> => 0
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"MessageIdContentConflict">>, maps:get(<<"Code">>, Resp)).

-doc "BatchPub with neither content nor id returns 400.".
t_batch_pub_neither_content_nor_id(_Config) ->
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"P1">>,
        <<"DeviceName">> => [<<"D1">>],
        <<"Qos">> => 0
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"MessageIdContentConflict">>, maps:get(<<"Code">>, Resp)).

-doc "BatchPub with an empty DeviceName list returns 400.".
t_batch_pub_empty_device_names(_Config) ->
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"P1">>,
        <<"DeviceName">> => [],
        <<"MessageContent">> => <<"aGVsbG8=">>,
        <<"Qos">> => 1
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"InvalidDeviceName">>, maps:get(<<"Code">>, Resp)).

-doc "BatchPub rejects DeviceName entries with wildcard or separator characters.".
t_batch_pub_device_name_special_chars(_Config) ->
    lists:foreach(
        fun(DN) ->
            Body = #{
                <<"Action">> => <<"BatchPub">>,
                <<"ProductKey">> => <<"P1">>,
                <<"DeviceName">> => [DN],
                <<"MessageContent">> => <<"aGVsbG8=">>,
                <<"Qos">> => 0
            },
            Request = #{body => Body},
            {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
            ?assertEqual(<<"InvalidDeviceName">>, maps:get(<<"Code">>, Resp))
        end,
        [<<"D+1">>, <<"D#1">>, <<"D/1">>, <<"D$1">>]
    ).

-doc "BatchPub rejects a ProductKey with wildcard or separator characters.".
t_batch_pub_product_key_special_chars(_Config) ->
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"P+1">>,
        <<"DeviceName">> => [<<"D1">>],
        <<"MessageContent">> => <<"aGVsbG8=">>,
        <<"Qos">> => 0
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"InvalidProductKey">>, maps:get(<<"Code">>, Resp)).

-doc "BatchPub rejects an invalid TopicShortName with 400 InvalidTopicTemplate.".
t_batch_pub_invalid_short_name(_Config) ->
    lists:foreach(
        fun(ShortName) ->
            Body = #{
                <<"Action">> => <<"BatchPub">>,
                <<"ProductKey">> => <<"P1">>,
                <<"DeviceName">> => [<<"D1">>],
                <<"MessageContent">> => <<"aGVsbG8=">>,
                <<"Qos">> => 0,
                <<"TopicShortName">> => ShortName
            },
            Request = #{body => Body},
            {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
            ?assertEqual(<<"InvalidTopicTemplate">>, maps:get(<<"Code">>, Resp))
        end,
        [<<"a/b">>, <<"a+b">>, <<"a#b">>, <<"a$b">>, <<"a${b}">>, 123]
    ).

-doc "BatchPub rejects a TopicTemplateName with wildcards or unknown placeholders.".
t_batch_pub_invalid_template_name(_Config) ->
    lists:foreach(
        fun(TemplateName) ->
            Body = #{
                <<"Action">> => <<"BatchPub">>,
                <<"ProductKey">> => <<"P1">>,
                <<"DeviceName">> => [<<"D1">>],
                <<"MessageContent">> => <<"aGVsbG8=">>,
                <<"Qos">> => 0,
                <<"TopicTemplateName">> => TemplateName
            },
            Request = #{body => Body},
            {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
            ?assertEqual(<<"InvalidTopicTemplate">>, maps:get(<<"Code">>, Resp))
        end,
        [<<"/a/+/b">>, <<"/a/#/b">>, <<"/a/${unknown}/b">>, 123]
    ).

-doc "BatchPub accepts a TopicTemplateName with the supported ${productKey} and ${deviceName} placeholders.".
t_batch_pub_template_supported_placeholders(_Config) ->
    lists:foreach(
        fun(TemplateName) ->
            Body = #{
                <<"Action">> => <<"BatchPub">>,
                <<"ProductKey">> => <<"P1">>,
                <<"DeviceName">> => [<<"D1">>],
                <<"MessageContent">> => <<"aGVsbG8=">>,
                <<"Qos">> => 0,
                <<"TopicTemplateName">> => TemplateName
            },
            Request = #{body => Body},
            {ok, 200, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
            ?assert(maps:get(<<"Success">>, Resp))
        end,
        [
            <<"/${productKey}/${deviceName}/user/get">>,
            <<"/sys/${productKey}/thing/service">>,
            <<"/${deviceName}/user/update">>
        ]
    ).

-doc "BatchPub with a non-binary MessageId returns 400 MessageNotFound.".
t_batch_pub_message_id_wrong_type(_Config) ->
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"P1">>,
        <<"DeviceName">> => [<<"D1">>],
        <<"MessageId">> => 123,
        <<"Qos">> => 0
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"MessageNotFound">>, maps:get(<<"Code">>, Resp)).

-doc "RegisterMessage with a non-binary MessageId returns 400 MessageNotFound.".
t_register_message_id_wrong_type(_Config) ->
    Body = #{
        <<"Action">> => <<"RegisterMessage">>,
        <<"MessageId">> => 123
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"MessageNotFound">>, maps:get(<<"Code">>, Resp)).

%%--------------------------------------------------------------------
%% Pending delivery quota tests
%%--------------------------------------------------------------------

-doc "BatchPub QoS=1 rejects devices that would exceed the per-device pending quota.".
t_quota_per_device_exceeded(_Config) ->
    Cfg = persistent_term:get({?APP, config}),
    persistent_term:put(
        {?APP, config}, Cfg#{max_pending_deliveries_per_device => 10}
    ),
    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    emqx_bcast_storage:create_message(ApiMsgId, MsgGuid, <<"h">>, <<"p">>),
    %% Pre-fill 10 pending deliveries for D1 so a new one would exceed 10.
    lists:foreach(
        fun(_) ->
            {ok, _} = emqx_bcast_storage:create_delivery(
                emqx_bcast_utils:gen_guid(), MsgGuid, <<"PQ">>, <<"tpl">>, [<<"D1">>], 1
            )
        end,
        lists:seq(1, 10)
    ),
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"PQ">>,
        <<"DeviceName">> => [<<"D1">>, <<"D2">>],
        <<"MessageContent">> => <<"aGVsbG8=">>,
        <<"Qos">> => 1
    },
    {ok, 429, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], #{body => Body}),
    ?assertEqual(<<"QuotaExceeded">>, maps:get(<<"Code">>, Resp)),
    ?assertEqual([<<"D1">>], maps:get(<<"Devices">>, Resp)),
    persistent_term:put({?APP, config}, Cfg).

-doc "BatchPub QoS=1 passes when the per-device pending count is within quota.".
t_quota_per_device_within(_Config) ->
    Cfg = persistent_term:get({?APP, config}),
    persistent_term:put(
        {?APP, config}, Cfg#{max_pending_deliveries_per_device => 10}
    ),
    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    emqx_bcast_storage:create_message(ApiMsgId, MsgGuid, <<"h">>, <<"p">>),
    lists:foreach(
        fun(_) ->
            {ok, _} = emqx_bcast_storage:create_delivery(
                emqx_bcast_utils:gen_guid(), MsgGuid, <<"PQ">>, <<"tpl">>, [<<"D1">>], 1
            )
        end,
        lists:seq(1, 9)
    ),
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"PQ">>,
        <<"DeviceName">> => [<<"D1">>],
        <<"MessageContent">> => <<"aGVsbG8=">>,
        <<"Qos">> => 1
    },
    {ok, 200, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], #{body => Body}),
    ?assert(maps:get(<<"Success">>, Resp)),
    persistent_term:put({?APP, config}, Cfg).

-doc "BatchPub QoS=1 rejects when the global pending delivery quota would be exceeded.".
t_quota_global_exceeded(_Config) ->
    Cfg = persistent_term:get({?APP, config}),
    persistent_term:put({?APP, config}, Cfg#{max_pending_deliveries => 2}),
    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    emqx_bcast_storage:create_message(ApiMsgId, MsgGuid, <<"h">>, <<"p">>),
    lists:foreach(
        fun(DN) ->
            {ok, _} = emqx_bcast_storage:create_delivery(
                emqx_bcast_utils:gen_guid(), MsgGuid, <<"PQ">>, <<"tpl">>, [DN], 1
            )
        end,
        [<<"D1">>, <<"D2">>]
    ),
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"PQ">>,
        <<"DeviceName">> => [<<"D3">>],
        <<"MessageContent">> => <<"aGVsbG8=">>,
        <<"Qos">> => 1
    },
    {ok, 429, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], #{body => Body}),
    ?assertEqual(<<"QuotaExceeded">>, maps:get(<<"Code">>, Resp)),
    persistent_term:put({?APP, config}, Cfg).

%%--------------------------------------------------------------------
%% PubBroadcast API tests
%%--------------------------------------------------------------------

-doc "PubBroadcast accepts a custom TopicFullName.".
t_broadcast_with_topic_full_name(_Config) ->
    Body = #{
        <<"Action">> => <<"PubBroadcast">>,
        <<"ProductKey">> => <<"P1">>,
        <<"MessageContent">> => <<"aGVsbG8=">>,
        <<"TopicFullName">> => <<"/custom/broadcast/topic">>
    },
    Request = #{body => Body},
    {ok, 200, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assert(maps:get(<<"Success">>, Resp)).

-doc "PubBroadcast rejects an invalid TopicFullName with 400 InvalidTopicTemplate.".
t_broadcast_invalid_topic_full_name(_Config) ->
    lists:foreach(
        fun(TopicFullName) ->
            Body = #{
                <<"Action">> => <<"PubBroadcast">>,
                <<"ProductKey">> => <<"P1">>,
                <<"MessageContent">> => <<"aGVsbG8=">>,
                <<"TopicFullName">> => TopicFullName
            },
            Request = #{body => Body},
            {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
            ?assertEqual(<<"InvalidTopicTemplate">>, maps:get(<<"Code">>, Resp))
        end,
        [<<"/a/+/b">>, <<"/a/#/b">>, <<"/a/${b}">>, 123]
    ).

-doc "PubBroadcast rejects a ProductKey with wildcard or separator characters.".
t_broadcast_product_key_special_chars(_Config) ->
    Body = #{
        <<"Action">> => <<"PubBroadcast">>,
        <<"ProductKey">> => <<"P/1">>,
        <<"MessageContent">> => <<"aGVsbG8=">>
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"InvalidProductKey">>, maps:get(<<"Code">>, Resp)).

-doc "PubBroadcast without ProductKey returns 400 InvalidProductKey.".
t_broadcast_missing_product_key(_Config) ->
    Body = #{
        <<"Action">> => <<"PubBroadcast">>,
        <<"MessageContent">> => <<"aGVsbG8=">>
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"InvalidProductKey">>, maps:get(<<"Code">>, Resp)).

-doc "PubBroadcast without content returns 400.".
t_broadcast_missing_content(_Config) ->
    Body = #{
        <<"Action">> => <<"PubBroadcast">>,
        <<"ProductKey">> => <<"P1">>
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"InvalidBase64">>, maps:get(<<"Code">>, Resp)).

-doc "PubBroadcast with invalid Base64 returns 400 InvalidBase64.".
t_broadcast_invalid_base64(_Config) ->
    Body = #{
        <<"Action">> => <<"PubBroadcast">>,
        <<"ProductKey">> => <<"P1">>,
        <<"MessageContent">> => <<"!!!">>
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"InvalidBase64">>, maps:get(<<"Code">>, Resp)).

%%--------------------------------------------------------------------
%% Metric verification tests
%%--------------------------------------------------------------------

metric(Name) ->
    try
        prometheus_counter:value(?BCAST_REGISTRY, mname(Name), [])
    catch
        _:_ -> 0
    end.

mname(Suffix) -> <<"bcast_", Suffix/binary>>.

-doc "Duplicate PUBACKs do not increment the acked metric twice.".
t_duplicate_puback_metric_counted_once(_Config) ->
    PK = <<"PMETRIC_ACK">>,
    DN = <<"DMETRIC_ACK">>,
    {_ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    Payload = <<"metric ack payload">>,
    Hash = crypto:hash(sha256, Payload),
    emqx_bcast_storage:create_message(<<"metric-ack-api">>, MsgGuid, Hash, Payload),
    DeliveryId = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(
        DeliveryId, MsgGuid, PK, <<"tpl">>, [DN], 1
    ),
    Msg = emqx_message:make(
        DeliveryId,
        DN,
        1,
        <<"tpl">>,
        Payload,
        #{},
        #{?BCAST_DELIVERY_ID => DeliveryId, ?BCAST_PRODUCT_KEY => PK}
    ),
    %% The metric is emitted only when take_pending matches an active buffer
    %% entry, so seed the current buffer exactly like the claim path. The
    %% buffer is a single fixed public table (the AB flip is gone).
    ActiveTab = emqx_bcast_pull_pool:tab(emqx_bcast_pull_pool:shard_of(DN), bcast_buffer_a),
    ets:insert(ActiveTab, #bcast_buffer_entry{
        clientid = DN,
        delivery_id = DeliveryId,
        product_key = PK,
        topic_template = <<"tpl">>,
        payload = Payload,
        pid = self()
    }),
    Before = metric(<<"batch_pub_qos1_acked">>),
    ok = emqx_bcast:on_message_acked(#{clientid => DN}, Msg),
    ?assert(wait_metric(<<"batch_pub_qos1_acked">>, Before + 1)),
    ok = emqx_bcast:on_message_acked(#{clientid => DN}, Msg),
    _ = sys:get_state(emqx_bcast_ack_pool),
    _ = sys:get_state(emqx_bcast_pull_pool:pool_name(emqx_bcast_pull_pool:shard_of(DN))),
    ?assertEqual(Before + 1, metric(<<"batch_pub_qos1_acked">>)).

-doc "Repro: a duplicate PUBACK arriving after a redelivery (reconnect)\n"
"generation re-creates the client buffer for the SAME delivery id, and\n"
"take_pending matches it - so the acked metric counts the logical delivery\n"
"twice. Observed online as acked == wanted + redelivered. take_pending keys\n"
"only on (clientid, delivery_id); it cannot tell the old generation's late\n"
"PUBACK from the new generation's own PUBACK. Until acks are counted\n"
"authoritatively once per logical delivery (e.g. at the core index removal),\n"
"this case locks the CURRENT behavior (2) and must be flipped to 1 after the\n"
"fix.".
t_metrics_acked_redelivery_generation_overcount(_Config) ->
    PK = <<"PMETRIC_GEN">>,
    DN = <<"DMETRIC_GEN">>,
    {_ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    Payload = <<"metric gen payload">>,
    Hash = crypto:hash(sha256, Payload),
    emqx_bcast_storage:create_message(<<"metric-gen-api">>, MsgGuid, Hash, Payload),
    DeliveryId = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(
        DeliveryId, MsgGuid, PK, <<"tpl">>, [DN], 1
    ),
    Msg = emqx_message:make(
        DeliveryId,
        DN,
        1,
        <<"tpl">>,
        Payload,
        #{},
        #{?BCAST_DELIVERY_ID => DeliveryId, ?BCAST_PRODUCT_KEY => PK}
    ),
    ActiveTab = emqx_bcast_pull_pool:tab(emqx_bcast_pull_pool:shard_of(DN), bcast_buffer_a),
    seed_ack_buffer(ActiveTab, DN, DeliveryId, PK, Payload),
    Before = metric(<<"batch_pub_qos1_acked">>),
    %% generation 1: first PUBLISH acked (counted once, buffer consumed).
    ok = emqx_bcast:on_message_acked(#{clientid => DN}, Msg),
    ?assert(wait_metric(<<"batch_pub_qos1_acked">>, Before + 1)),
    %% redelivery generation (reconnect re-claim): buffer re-created for the
    %% same delivery id.
    seed_ack_buffer(ActiveTab, DN, DeliveryId, PK, Payload),
    %% the old generation's late duplicate PUBACK would once have matched the
    %% new buffer and counted twice; with core-applied confirmation counting
    %% (ack_in_flight marker + ack_applied), the logical delivery is counted
    %% exactly once and the late duplicate is ignored.
    ok = emqx_bcast:on_message_acked(#{clientid => DN}, Msg),
    _ = sys:get_state(emqx_bcast_ack_pool),
    _ = sys:get_state(emqx_bcast_pull_pool:pool_name(emqx_bcast_pull_pool:shard_of(DN))),
    ?assert(wait_metric(<<"batch_pub_qos1_acked">>, Before + 1)),
    timer:sleep(50),
    ?assertEqual(Before + 1, metric(<<"batch_pub_qos1_acked">>)).

seed_ack_buffer(Tab, DN, DeliveryId, PK, Payload) ->
    ets:insert(Tab, #bcast_buffer_entry{
        clientid = DN,
        delivery_id = DeliveryId,
        product_key = PK,
        topic_template = <<"tpl">>,
        topic = <<"tpl">>,
        payload = Payload,
        pid = self(),
        attempts = 1
    }),
    ok.

-doc "Ack micro-storm: measure real-entry ack batch cost and the pull_server\n"
"pool dispatch queueing under a burst. Records: real ack batch(500) cost,\n"
"schedulers, theoretical per-worker caps (current ACK_WORKER_MAX vs\n"
"schedulers), and max in_flight/pending observed during a parallel burst.\n"
"Loose sanity only (no crash, drains); the numbers are compared before/\n"
"after the ack-path fixes.".
t_ack_micro_storm_throughput(_Config) ->
    PK = <<"PMICRO">>,
    Count = 2000,
    DNs = [<<"DM_", (integer_to_binary(N))/binary>> || N <- lists:seq(1, Count)],
    {_ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    Payload = <<"micro storm">>,
    Hash = crypto:hash(sha256, Payload),
    emqx_bcast_storage:create_message(<<"micro-storm-api">>, MsgGuid, Hash, Payload),
    Did = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(Did, MsgGuid, PK, <<"tpl">>, DNs, Count),
    Batch = fun(S, L) -> [{PK, DN, Did} || DN <- lists:sublist(DNs, S, L)] end,
    %% (a) real ack batch cost (first pass removes entries -> one-time cost)
    Costs = [
        element(1, timer:tc(fun() -> emqx_bcast_storage:process_ack_batch(Batch(S, 500)) end))
     || S <- [1, 501, 1001, 1501]
    ],
    Avg = lists:sum(Costs) div max(1, length(Costs)),
    Sched = erlang:system_info(schedulers_online),
    ct:pal(
        "ack real batch(500) avg=~p us | schedulers=~p | cap16~p/s | sched-cap~p/s",
        [Avg, Sched, 16 * 1000000 div max(1, Avg), Sched * 1000000 div max(1, Avg)]
    ),
    %% (b) dispatch queueing under a parallel burst (duplicate acks after the
    %% first real pass are cheap, but exercise the ack_batch dispatch + worker
    %% cap path and the pending_acks queueing)
    Stats = ets:new(ack_storm_stats, [public, set]),
    ets:insert(Stats, {max_i, 0}),
    ets:insert(Stats, {max_p, 0}),
    Sampler = spawn(fun() -> storm_sample(Stats, 10000) end),
    [
        spawn_link(fun() ->
            lists:foreach(
                fun(_) -> emqx_bcast_pull_server_pool:ack_batch(Batch(1, 100)) end,
                lists:seq(1, 300)
            )
        end)
     || _ <- lists:seq(1, 8)
    ],
    ?assert(
        wait_until(
            fun() ->
                S = sys:get_state(emqx_bcast_pull_server_pool),
                maps:get(in_flight, S) =:= 0 andalso maps:get(pending_acks, S) =:= []
            end,
            400
        )
    ),
    exit(Sampler, kill),
    timer:sleep(10),
    [{_, MaxInflight}] = ets:lookup(Stats, max_i),
    [{_, MaxPending}] = ets:lookup(Stats, max_p),
    ets:delete(Stats),
    ct:pal("ack storm done: max_in_flight=~p max_pending_batches=~p", [MaxInflight, MaxPending]),
    ?assert(MaxInflight >= 0),
    ok.

storm_sample(Stats, 0) ->
    ok;
storm_sample(Stats, N) ->
    S = catch sys:get_state(emqx_bcast_pull_server_pool),
    case S of
        #{} ->
            In = maps:get(in_flight, S),
            Pending = length(maps:get(pending_acks, S)),
            [{_, MaxI}] = ets:lookup(Stats, max_i),
            [{_, MaxP}] = ets:lookup(Stats, max_p),
            ets:insert(Stats, {max_i, max(MaxI, In)}),
            ets:insert(Stats, {max_p, max(MaxP, Pending)});
        _ ->
            ok
    end,
    timer:sleep(2),
    storm_sample(Stats, N - 1).

-doc "A real PUBACK sets the ack-in-flight marker until the core-applied\n"
"confirmation arrives, which also counts acked exactly once and clears it.".
t_metrics_ack_in_flight_marker_lifecycle(_Config) ->
    PK = <<"PMARK">>,
    DN = <<"DMARK">>,
    {_ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    Payload = <<"marker payload">>,
    Hash = crypto:hash(sha256, Payload),
    emqx_bcast_storage:create_message(<<"marker-api">>, MsgGuid, Hash, Payload),
    DeliveryId = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(DeliveryId, MsgGuid, PK, <<"tpl">>, [DN], 1),
    Msg = emqx_message:make(
        DeliveryId,
        DN,
        1,
        <<"tpl">>,
        Payload,
        #{},
        #{?BCAST_DELIVERY_ID => DeliveryId, ?BCAST_PRODUCT_KEY => PK}
    ),
    Shard = emqx_bcast_pull_pool:shard_of(DN),
    ActiveTab = emqx_bcast_pull_pool:tab(Shard, bcast_buffer_a),
    AckTab = emqx_bcast_pull_pool:tab(Shard, bcast_ack_pending),
    ets:insert(ActiveTab, #bcast_buffer_entry{
        clientid = DN,
        delivery_id = DeliveryId,
        product_key = PK,
        topic_template = <<"tpl">>,
        topic = <<"tpl">>,
        payload = Payload,
        pid = self(),
        attempts = 1
    }),
    Before = metric(<<"batch_pub_qos1_acked">>),
    ok = emqx_bcast:on_message_acked(#{clientid => DN}, Msg),
    %% core-applied confirmation arrives asynchronously and counts acked
    %% exactly once (the marker lives in the pull shard only for the brief
    %% ack-in-flight window; on a single node it is set and cleared faster
    %% than a poll can observe, so we assert the observable contract)
    ?assert(wait_metric(<<"batch_pub_qos1_acked">>, Before + 1)),
    %% a duplicate PUBACK (buffer already consumed) must not count again
    ok = emqx_bcast:on_message_acked(#{clientid => DN}, Msg),
    timer:sleep(100),
    ?assertEqual(Before + 1, metric(<<"batch_pub_qos1_acked">>)),
    %% no residual marker after the confirmation
    ?assert(wait_until(fun() -> not ets:member(AckTab, DN) end, 100)).

-doc "When the claim holder node is down, the shard requeues its in-flight\n"
"claims so other nodes can deliver (cleanup_local reclaim).".
t_metrics_claim_holder_node_down_reclaim(_Config) ->
    PK = <<"PHOLDER">>,
    DN = <<"DHOLDER">>,
    {_ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    Payload = <<"holder payload">>,
    Hash = crypto:hash(sha256, Payload),
    emqx_bcast_storage:create_message(<<"holder-api">>, MsgGuid, Hash, Payload),
    DeliveryId = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(DeliveryId, MsgGuid, PK, <<"tpl">>, [DN], 1),
    [{DN, {ok, _}}] = emqx_bcast_storage:claim_want_next_batch([
        #{clientid => DN, product_key => PK, topics => [{<<"tpl">>, 1}]}
    ]),
    %% fake a dead holder on the owning index shard, then run the reclaim pass
    Key = {PK, DN},
    Shard = erlang:phash2(Key, emqx_bcast_index_owner:shard_count()),
    Name = list_to_atom("emqx_bcast_index_owner_" ++ integer_to_list(Shard)),
    Old = sys:get_state(Name),
    Holders = maps:put(Key, 'down_fake@node', maps:get(holders, Old, #{})),
    sys:replace_state(Name, fun(_) -> Old#{holders => Holders} end),
    emqx_bcast_storage:cleanup_expired(),
    %% the claim is reclaimed: inflight is 0 and the delivery is claimable again
    ?assert(
        wait_until(
            fun() ->
                S = sys:get_state(Name),
                case maps:get(Key, maps:get(inflights, S), #{}) of
                    I when map_size(I) =:= 0 -> true;
                    _ -> false
                end
            end,
            100
        )
    ),
    [{DN, {ok, M}}] = emqx_bcast_storage:claim_want_next_batch([
        #{clientid => DN, product_key => PK, topics => [{<<"tpl">>, 1}]}
    ]),
    ?assertEqual(DeliveryId, maps:get(delivery_id, M)).

-doc "While an ack is in flight (ack-in-flight marker set), a subscribe\n"
"trigger for that client must not stage a want_next (pull returns no_more).".
t_pull_ack_in_flight_gates_subscribe_trigger(_Config) ->
    PK = <<"PGATE_S">>,
    DN = <<"DGATE_S">>,
    Shard = emqx_bcast_pull_pool:shard_of(DN),
    AckTab = emqx_bcast_pull_pool:tab(Shard, bcast_ack_pending),
    Buf3 = emqx_bcast_pull_pool:tab(Shard, bcast_buffer3),
    ets:insert(AckTab, {DN, emqx_bcast_utils:gen_guid()}),
    emqx_bcast_pull_pool:cast_client(DN, {subscribe, DN, self(), PK}),
    timer:sleep(100),
    %% nothing staged while the marker is present
    ?assertEqual([], ets:lookup(Buf3, DN)),
    ets:delete(AckTab, DN).

-doc "While an ack is in flight, a ping keepalive trigger must also be\n"
"suppressed (no want_next staged).".
t_pull_ack_in_flight_gates_ping_trigger(_Config) ->
    PK = <<"PGATE_P">>,
    DN = <<"DGATE_P">>,
    Shard = emqx_bcast_pull_pool:shard_of(DN),
    AckTab = emqx_bcast_pull_pool:tab(Shard, bcast_ack_pending),
    Buf3 = emqx_bcast_pull_pool:tab(Shard, bcast_buffer3),
    ets:insert(AckTab, {DN, emqx_bcast_utils:gen_guid()}),
    emqx_bcast_pull_pool:cast_client(DN, {ping, DN, self(), PK}),
    timer:sleep(100),
    ?assertEqual([], ets:lookup(Buf3, DN)),
    ets:delete(AckTab, DN).

-doc "The QoS0 auto-ack path counts delivered/auto_acked locally and never\n"
"touches acked (no client PUBACK); the pull ack entry point forwards it and\n"
"the core-applied confirmation (or its absence) decides advancement.".
t_metrics_auto_ack_path_counts_local(_Config) ->
    PK = <<"PAUTO_LOCAL">>,
    DN = <<"DAUTO_LOCAL">>,
    Did = emqx_bcast_utils:gen_guid(),
    D0 = metric(<<"batch_pub_qos1_delivered">>),
    A0 = metric(<<"batch_pub_qos1_auto_acked">>),
    ACK0 = metric(<<"batch_pub_qos1_acked">>),
    R0 = metric(<<"batch_pub_qos1_redelivered">>),
    ok = emqx_bcast_pull_pool:do_deliver_qos0_and_ack(
        DN, self(), <<"tpl">>, <<"auto payload">>, Did, PK, 1
    ),
    ?assert(wait_metric(<<"batch_pub_qos1_delivered">>, D0 + 1)),
    ?assert(wait_metric(<<"batch_pub_qos1_auto_acked">>, A0 + 1)),
    ?assertEqual(ACK0, metric(<<"batch_pub_qos1_acked">>)),
    ?assertEqual(R0, metric(<<"batch_pub_qos1_redelivered">>)).

-doc "A second PUBLISH copy of an already-confirmed delivery must not\n"
"re-count acked (the core no longer holds the entry).".
t_metrics_acked_second_copy_after_confirm_not_counted(_Config) ->
    PK = <<"P2ND">>,
    DN = <<"D2ND">>,
    {_ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    Payload = <<"second copy payload">>,
    Hash = crypto:hash(sha256, Payload),
    emqx_bcast_storage:create_message(<<"2nd-api">>, MsgGuid, Hash, Payload),
    DeliveryId = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(DeliveryId, MsgGuid, PK, <<"tpl">>, [DN], 1),
    Msg = emqx_message:make(
        DeliveryId,
        DN,
        1,
        <<"tpl">>,
        Payload,
        #{},
        #{?BCAST_DELIVERY_ID => DeliveryId, ?BCAST_PRODUCT_KEY => PK}
    ),
    Shard = emqx_bcast_pull_pool:shard_of(DN),
    ActiveTab = emqx_bcast_pull_pool:tab(Shard, bcast_buffer_a),
    seed_ack_buffer(ActiveTab, DN, DeliveryId, PK, Payload),
    Before = metric(<<"batch_pub_qos1_acked">>),
    ok = emqx_bcast:on_message_acked(#{clientid => DN}, Msg),
    ?assert(wait_metric(<<"batch_pub_qos1_acked">>, Before + 1)),
    %% a later copy (would-be redelivery) is acknowledged: the buffer was
    %% re-seeded for the test, but the core entry is gone, so no
    %% confirmation arrives and acked stays at one
    seed_ack_buffer(ActiveTab, DN, DeliveryId, PK, Payload),
    ok = emqx_bcast:on_message_acked(#{clientid => DN}, Msg),
    timer:sleep(200),
    ?assertEqual(Before + 1, metric(<<"batch_pub_qos1_acked">>)).

-doc "Concurrent BatchPub calls cannot pass the global quota through races.".
t_quota_concurrent_atomic(_Config) ->
    Cfg = persistent_term:get({?APP, config}),
    persistent_term:put({?APP, config}, Cfg#{max_pending_deliveries => 5}),
    try
        Parent = self(),
        Pids = [
            spawn(fun() ->
                Body = #{
                    <<"Action">> => <<"BatchPub">>,
                    <<"ProductKey">> => <<"PCONCURRENT">>,
                    <<"DeviceName">> => [<<"DCONCURRENT_", (integer_to_binary(N))/binary>>],
                    <<"MessageContent">> => base64:encode(crypto:strong_rand_bytes(8)),
                    <<"Qos">> => 1
                },
                Result = emqx_bcast_api:handle(post, [<<"pub">>], #{body => Body}),
                Parent ! {quota_result, self(), Result}
            end)
         || N <- lists:seq(1, 12)
        ],
        Results = [
            receive
                {quota_result, P, R} -> R
            end
         || P <- Pids
        ],
        OkCount = length([ok || {ok, 200, _, _} <- Results]),
        QuotaCount = length([ok || {ok, 429, _, _} <- Results]),
        ?assertEqual(5, OkCount),
        ?assertEqual(7, QuotaCount),
        ?assertEqual(5, emqx_bcast_storage:pending_delivery_count())
    after
        persistent_term:put({?APP, config}, Cfg)
    end.

-doc "QoS=0 BatchPub increments the targeted counter by device count.".
t_metrics_qos0_targeted(_Config) ->
    Before = metric(<<"batch_pub_qos0_targeted">>),
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"P1">>,
        <<"DeviceName">> => [<<"D1">>, <<"D2">>, <<"D3">>],
        <<"MessageContent">> => <<"aGVsbG8=">>,
        <<"Qos">> => 0
    },
    {ok, 200, _, _} = emqx_bcast_api:handle(post, [<<"pub">>], #{body => Body}),
    After = metric(<<"batch_pub_qos0_targeted">>),
    ?assertEqual(3, After - Before).

-doc "PubBroadcast increments the broadcast_pub_in counter.".
t_metrics_broadcast_in(_Config) ->
    Before = metric(<<"broadcast_pub_in">>),
    Body = #{
        <<"Action">> => <<"PubBroadcast">>,
        <<"ProductKey">> => <<"P1">>,
        <<"MessageContent">> => <<"aGVsbG8=">>
    },
    {ok, 200, _, _} = emqx_bcast_api:handle(post, [<<"pub">>], #{body => Body}),
    After = metric(<<"broadcast_pub_in">>),
    ?assertEqual(1, After - Before).

-doc "failed PubBroadcast increments the broadcast error counter.".
t_metrics_broadcast_error(_Config) ->
    Before = metric(<<"broadcast_pub_error">>),
    Body = #{<<"Action">> => <<"PubBroadcast">>, <<"MessageContent">> => <<"!!!">>},
    {ok, 400, _, _} = emqx_bcast_api:handle(post, [<<"pub">>], #{body => Body}),
    After = metric(<<"broadcast_pub_error">>),
    ?assertEqual(1, After - Before).

-doc "QoS=1 BatchPub increments the wanted counter by device count, counted\n"
"at the durable mria commit (promoter), not at API acceptance.".
t_metrics_qos1_wanted(_Config) ->
    Before = metric(<<"batch_pub_qos1_wanted">>),
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"P1">>,
        <<"DeviceName">> => [<<"DA">>, <<"DB">>],
        <<"MessageContent">> => <<"aGVsbG8=">>,
        <<"Qos">> => 1
    },
    {ok, 200, _, _} = emqx_bcast_api:handle(post, [<<"pub">>], #{body => Body}),
    %% wanted is counted asynchronously once the promoter commits both
    %% devices, so wait instead of asserting immediately.
    ?assert(wait_metric(<<"batch_pub_qos1_wanted">>, Before + 2)).

-doc "RegisterMessage increments the register_message_in counter.".
t_metrics_register_message_in(_Config) ->
    Before = metric(<<"register_message_in">>),
    Body = #{<<"Action">> => <<"RegisterMessage">>, <<"MessageContent">> => <<"dGVzdA==">>},
    {ok, 200, _, _} = emqx_bcast_api:handle(post, [<<"pub">>], #{body => Body}),
    After = metric(<<"register_message_in">>),
    ?assertEqual(1, After - Before).

%%--------------------------------------------------------------------
%% Delivery-ledger metric tests
%%--------------------------------------------------------------------

gauge(Name) ->
    try
        prometheus_gauge:value(?BCAST_REGISTRY, mname(Name), [])
    catch
        _:_ -> 0
    end.

refresh_gauges() ->
    %% gauges are sampled at scrape time; emulate a scrape before reading.
    _ = emqx_bcast_metrics:collect(),
    ok.

make_msg(PayloadBin) ->
    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    Hash = crypto:hash(sha256, PayloadBin),
    emqx_bcast_storage:create_message(ApiMsgId, MsgGuid, Hash, PayloadBin),
    MsgGuid.

-doc "A claim carries the attempt number; a lease-expiry redelivery claim\n"
"carries attempt 2, and the redelivered bookkeeping matches the claim.".
t_metrics_claim_attempt_number(_Config) ->
    MsgGuid = make_msg(<<"attempt">>),
    PK = <<"PATT">>,
    DN = <<"DATT">>,
    DeliveryId = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(DeliveryId, MsgGuid, PK, <<"tpl">>, [DN], 1),
    [{DN, {ok, M1}}] = emqx_bcast_storage:claim_want_next_batch([
        #{clientid => DN, product_key => PK, topics => [{<<"tpl">>, 1}]}
    ]),
    ?assertEqual(1, maps:get(attempt, M1)),
    expire_inflight(PK, DN, DeliveryId),
    [{DN, {ok, M2}}] = emqx_bcast_storage:claim_want_next_batch([
        #{clientid => DN, product_key => PK, topics => [{<<"tpl">>, 1}]}
    ]),
    ?assertEqual(2, maps:get(attempt, M2)),
    %% Ack removes the entry entirely (attempt state cleaned with it).
    emqx_bcast_storage:process_ack(PK, DN, DeliveryId),
    ?assertEqual({ok, []}, emqx_bcast_storage:get_device_deliveries({PK, DN})).

-doc "queued/inflight gauges track the per-shard live state (queued, then\n"
"in-flight while claimed, then empty after the ack).".
t_metrics_gauge_sample(_Config) ->
    MsgGuid = make_msg(<<"gauges">>),
    PK = <<"PGAU">>,
    DN = <<"DGAU">>,
    DeliveryId = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(DeliveryId, MsgGuid, PK, <<"tpl">>, [DN], 1),
    refresh_gauges(),
    ?assertEqual(1, gauge(<<"batch_pub_qos1_queued">>)),
    ?assertEqual(0, gauge(<<"batch_pub_qos1_inflight">>)),
    [{DN, {ok, _}}] = emqx_bcast_storage:claim_want_next_batch([
        #{clientid => DN, product_key => PK, topics => [{<<"tpl">>, 1}]}
    ]),
    refresh_gauges(),
    ?assertEqual(0, gauge(<<"batch_pub_qos1_queued">>)),
    ?assertEqual(1, gauge(<<"batch_pub_qos1_inflight">>)),
    emqx_bcast_storage:process_ack(PK, DN, DeliveryId),
    refresh_gauges(),
    ?assertEqual(0, gauge(<<"batch_pub_qos1_queued">>)),
    ?assertEqual(0, gauge(<<"batch_pub_qos1_inflight">>)).

-doc "TTL expiry of a partially-acked delivery counts the remaining unacked\n"
"logical deliveries into ttl_expired (acked ones are not recounted).".
t_metrics_ttl_expired(_Config) ->
    MsgGuid = make_msg(<<"ttl metric">>),
    PK = <<"PTTL">>,
    DeliveryId = emqx_bcast_utils:gen_guid(),
    DNs = [<<"D1">>, <<"D2">>],
    {ok, D} = emqx_bcast_storage:create_delivery(DeliveryId, MsgGuid, PK, <<"tpl">>, DNs, 2),
    %% one device acks, the other stays unacked until TTL
    emqx_bcast_storage:process_ack(PK, <<"D1">>, DeliveryId),
    mnesia:dirty_write(D#bcast_msg{expires_at = 0}),
    emqx_bcast_storage:cleanup_expired(),
    ?assertEqual(1, metric(<<"batch_pub_qos1_ttl_expired">>)),
    ?assertEqual([], mnesia:dirty_read(bcast_msg, DeliveryId)).

-doc "Management delete of a partially-acked delivery counts the removed\n"
"unacked logical deliveries into canceled.".
t_metrics_canceled_mgmt_delete(_Config) ->
    MsgGuid = make_msg(<<"cancel metric">>),
    PK = <<"PCAN">>,
    DeliveryId = emqx_bcast_utils:gen_guid(),
    DNs = [<<"D1">>, <<"D2">>],
    {ok, _} = emqx_bcast_storage:create_delivery(DeliveryId, MsgGuid, PK, <<"tpl">>, DNs, 2),
    emqx_bcast_storage:process_ack(PK, <<"D1">>, DeliveryId),
    ok = emqx_bcast_storage:delete_delivery(DeliveryId),
    ?assertEqual(1, metric(<<"batch_pub_qos1_canceled">>)),
    ?assertEqual([], mnesia:dirty_read(bcast_msg, DeliveryId)).

-doc "The guarded metric reset refuses while queued/in-flight deliveries\n"
"exist, and after they drain it resets every counter to zero.".
t_metrics_reset_guarded(_Config) ->
    MsgGuid = make_msg(<<"reset metric">>),
    PK = <<"PRST">>,
    DeliveryId = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(
        DeliveryId, MsgGuid, PK, <<"tpl">>, [<<"DRST">>], 1
    ),
    ?assertMatch({error, {pending_deliveries, 1, 0}}, emqx_bcast_metrics:reset_guarded()),
    %% drain: delete the pending entry (counted into canceled), then reset ok
    ok = emqx_bcast_storage:delete_delivery(DeliveryId),
    ?assertEqual(ok, emqx_bcast_metrics:reset_guarded()),
    ?assertEqual(0, metric(<<"batch_pub_qos1_canceled">>)),
    ?assertEqual(0, metric(<<"batch_pub_qos1_wanted">>)).

%%--------------------------------------------------------------------
%% Management API tests
%%--------------------------------------------------------------------

-doc "list messages paginates with a cursor and no payload leak.".
t_mgmt_list_messages_pagination(_Config) ->
    [create_test_msg(<<"mgmt-list-", (integer_to_binary(N))/binary>>) || N <- [1, 2, 3]],
    {ok, 200, _, Page1} = emqx_bcast_api:handle(get, [<<"messages">>], #{
        query_string => #{<<"limit">> => <<"2">>}
    }),
    Items1 = maps:get(<<"Messages">>, Page1),
    ?assertEqual(2, length(Items1)),
    [
        begin
            ?assert(maps:is_key(<<"MessageId">>, Item)),
            ?assert(maps:is_key(<<"CreatedAt">>, Item)),
            ?assert(maps:is_key(<<"ExpiresAt">>, Item)),
            ?assert(maps:is_key(<<"PayloadSize">>, Item)),
            ?assertNot(maps:is_key(<<"Payload">>, Item))
        end
     || Item <- Items1
    ],
    %% A cursor is returned when there are more pages.
    Cursor = maps:get(<<"Cursor">>, Page1),
    ?assert(is_binary(Cursor)),
    {ok, 200, _, Page2} = emqx_bcast_api:handle(get, [<<"messages">>], #{
        query_string => #{<<"limit">> => <<"2">>, <<"cursor">> => Cursor}
    }),
    Items2 = maps:get(<<"Messages">>, Page2),
    ?assert(length(Items2) >= 1),
    Ids1 = [maps:get(<<"MessageId">>, I) || I <- Items1],
    Ids2 = [maps:get(<<"MessageId">>, I) || I <- Items2],
    ?assertEqual([], [I || I <- Ids1, lists:member(I, Ids2)]).

-doc "the last page carries no cursor; a malformed cursor is a 400.".
t_mgmt_list_messages_cursor_end(_Config) ->
    [create_test_msg(<<"mgmt-off-", (integer_to_binary(N))/binary>>) || N <- [1, 2, 3]],
    {ok, 200, _, Page1} = emqx_bcast_api:handle(get, [<<"messages">>], #{
        query_string => #{<<"limit">> => <<"10">>}
    }),
    %% All messages fit on one page: no cursor.
    ?assertNot(maps:is_key(<<"Cursor">>, Page1)),
    %% An invalid cursor is a client error rather than a silent restart from
    %% the first page.
    {error, 400, _, Resp} = emqx_bcast_api:handle(get, [<<"messages">>], #{
        query_string => #{<<"limit">> => <<"10">>, <<"cursor">> => <<"garbage">>}
    }),
    ?assertEqual(<<"InvalidParams">>, maps:get(<<"Code">>, Resp)).

-doc "a limit above the maximum returns 400 InvalidParams.".
t_mgmt_list_messages_limit_too_high(_Config) ->
    {error, 400, _, Resp} = emqx_bcast_api:handle(get, [<<"messages">>], #{
        query_string => #{<<"limit">> => <<"1001">>}
    }),
    ?assertEqual(<<"InvalidParams">>, maps:get(<<"Code">>, Resp)).

-doc "get message returns metadata and delivery count; unknown id 404s.".
t_mgmt_get_message(_Config) ->
    {ApiMsgId, MsgGuid} = create_test_msg(<<"mgmt-get">>),
    DeliveryId = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(
        DeliveryId, MsgGuid, <<"PMGMT">>, <<"tpl">>, [<<"DM1">>], 1
    ),
    {ok, 200, _, Resp} = emqx_bcast_api:handle(get, [<<"messages">>, ApiMsgId], #{}),
    ?assertEqual(ApiMsgId, maps:get(<<"MessageId">>, Resp)),
    ?assertEqual(1, maps:get(<<"DeliveryCount">>, Resp)),
    ?assertEqual(8, maps:get(<<"PayloadSize">>, Resp)),
    ?assertNot(maps:is_key(<<"Payload">>, Resp)),
    {error, 404, _, NotFound} = emqx_bcast_api:handle(
        get, [<<"messages">>, <<"no-such-id">>], #{}
    ),
    ?assertEqual(<<"MessageNotFound">>, maps:get(<<"Code">>, NotFound)).

-doc "deleting a message cascades to its deliveries and index entries.".
t_mgmt_delete_message_cascade(_Config) ->
    {ApiMsgId, MsgGuid} = create_test_msg(<<"mgmt-del">>),
    DeliveryId = emqx_bcast_utils:gen_guid(),
    DNs = [<<"DD1">>, <<"DD2">>],
    {ok, _} = emqx_bcast_storage:create_delivery(
        DeliveryId, MsgGuid, <<"PMGMT">>, <<"tpl">>, DNs, 2
    ),
    {ok, [_]} = emqx_bcast_storage:get_device_deliveries({<<"PMGMT">>, <<"DD1">>}),
    {ok, 200, _, Resp} = emqx_bcast_api:handle(delete, [<<"messages">>, ApiMsgId], #{}),
    ?assert(maps:get(<<"Success">>, Resp)),
    {error, 404, _, _} = emqx_bcast_api:handle(get, [<<"messages">>, ApiMsgId], #{}),
    ?assertEqual({error, not_found}, emqx_bcast_storage:lookup_message(MsgGuid)),
    {error, 404, _, _} = emqx_bcast_api:handle(
        get, [<<"deliveries">>, emqx_bcast_utils:guid_to_uuid(DeliveryId)], #{}
    ),
    {ok, []} = emqx_bcast_storage:get_device_deliveries({<<"PMGMT">>, <<"DD1">>}),
    {ok, []} = emqx_bcast_storage:get_device_deliveries({<<"PMGMT">>, <<"DD2">>}),
    {error, 404, _, Again} = emqx_bcast_api:handle(delete, [<<"messages">>, ApiMsgId], #{}),
    ?assertEqual(<<"MessageNotFound">>, maps:get(<<"Code">>, Again)).

-doc "deliveries for a device list UUIDs and metadata; missing params 400.".
t_mgmt_deliveries_for_device(_Config) ->
    {ApiMsgId, MsgGuid} = create_test_msg(<<"mgmt-dev">>),
    D1 = emqx_bcast_utils:gen_guid(),
    D2 = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(
        D1, MsgGuid, <<"PMGMT">>, <<"tpl">>, [<<"DEV1">>], 1
    ),
    {ok, _} = emqx_bcast_storage:create_delivery(
        D2, MsgGuid, <<"PMGMT">>, <<"tpl">>, [<<"DEV1">>], 1
    ),
    {ok, 200, _, Resp} = emqx_bcast_api:handle(get, [<<"deliveries">>], #{
        query_string => #{<<"product_key">> => <<"PMGMT">>, <<"device_name">> => <<"DEV1">>}
    }),
    Deliveries = maps:get(<<"Deliveries">>, Resp),
    ?assertEqual(2, length(Deliveries)),
    Ids = lists:sort([maps:get(<<"DeliveryId">>, D) || D <- Deliveries]),
    ?assertEqual(
        lists:sort([emqx_bcast_utils:guid_to_uuid(D1), emqx_bcast_utils:guid_to_uuid(D2)]),
        Ids
    ),
    [
        ?assertMatch(
            <<_:8/binary, $-, _:4/binary, $-, _:4/binary, $-, _:4/binary, $-, _:12/binary>>, Id
        )
     || Id <- Ids
    ],
    [
        begin
            ?assertEqual(ApiMsgId, maps:get(<<"MessageId">>, D)),
            ?assertEqual(1, maps:get(<<"TargetCount">>, D)),
            ?assertEqual(1, maps:get(<<"PendingCount">>, D)),
            ?assertEqual(<<"PMGMT">>, maps:get(<<"ProductKey">>, D))
        end
     || D <- Deliveries
    ],
    {error, 400, _, BadReq} = emqx_bcast_api:handle(get, [<<"deliveries">>], #{
        query_string => #{<<"product_key">> => <<"PMGMT">>}
    }),
    ?assertEqual(<<"InvalidParams">>, maps:get(<<"Code">>, BadReq)).

-doc "deleting a delivery removes it; unknown or malformed ids 404.".
t_mgmt_delete_delivery(_Config) ->
    {_ApiMsgId, MsgGuid} = create_test_msg(<<"mgmt-ddel">>),
    DeliveryId = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(
        DeliveryId, MsgGuid, <<"PMGMT">>, <<"tpl">>, [<<"DE1">>], 1
    ),
    {ok, [_]} = emqx_bcast_storage:get_device_deliveries({<<"PMGMT">>, <<"DE1">>}),
    IdStr = emqx_bcast_utils:guid_to_uuid(DeliveryId),
    {ok, 200, _, _} = emqx_bcast_api:handle(delete, [<<"deliveries">>, IdStr], #{}),
    {error, 404, _, NotFound} = emqx_bcast_api:handle(
        get, [<<"deliveries">>, IdStr], #{}
    ),
    ?assertEqual(<<"DeliveryNotFound">>, maps:get(<<"Code">>, NotFound)),
    {ok, []} = emqx_bcast_storage:get_device_deliveries({<<"PMGMT">>, <<"DE1">>}),
    {error, 404, _, _} = emqx_bcast_api:handle(delete, [<<"deliveries">>, IdStr], #{}),
    {error, 404, _, BadId} = emqx_bcast_api:handle(
        get, [<<"deliveries">>, <<"not-a-uuid">>], #{}
    ),
    ?assertEqual(<<"DeliveryNotFound">>, maps:get(<<"Code">>, BadId)).

%%--------------------------------------------------------------------
%% Regression tests for review bugs and user-reported upgrade/duplicate issues
%%--------------------------------------------------------------------

-doc "QoS=1 200 is returned on intake acceptance; the delivery row and index\n"
"are promoted into mria shortly after (async persistence by design).".
t_qos1_response_means_stored(_Config) ->
    PK = <<"PSTORED">>,
    DN = <<"DSTORED">>,
    {ok, 200, _, _} = emqx_bcast_api:handle(post, [<<"pub">>], #{
        body => #{
            <<"Action">> => <<"BatchPub">>,
            <<"ProductKey">> => PK,
            <<"DeviceName">> => [DN],
            <<"MessageContent">> => base64:encode(<<"stored before 200">>),
            <<"Qos">> => 1
        }
    }),
    %% The promoter commits the delivery and appends the index asynchronously.
    ?assert(
        wait_until(
            fun() ->
                length(mnesia:dirty_match_object(#bcast_msg{_ = '_'})) =:= 1 andalso
                    emqx_bcast_storage:pending_delivery_count() =:= 1
            end,
            100
        )
    ).

-doc "Legacy 0.1.x table layouts are migrated in place on startup.".
t_migrate_legacy_mnesia_layout(_Config) ->
    [catch mnesia:delete_table(T) || T <- [bcast_message, bcast_msg, bcast_msg_index]],
    MsgId = emqx_bcast_utils:gen_guid(),
    ApiMsgId = emqx_bcast_utils:gen_api_uuid(),
    Hash = crypto:hash(sha256, <<"legacy payload">>),
    DeliveryId = emqx_bcast_utils:gen_guid(),
    PK = <<"PLEGACY">>,
    DN = <<"DLEGACY">>,
    {atomic, ok} = mnesia:create_table(bcast_message, [
        {disc_copies, [node()]},
        {type, set},
        {record_name, bcast_message},
        {attributes, [msg_id, api_msg_id, content_hash, payload, created_at, expires_at]}
    ]),
    {atomic, ok} = mnesia:create_table(bcast_msg, [
        {disc_copies, [node()]},
        {type, set},
        {record_name, bcast_msg},
        {attributes, [
            delivery_id,
            msg_id,
            product_key,
            topic_template,
            target_ack_count,
            counter,
            device_names,
            created_at,
            expires_at,
            response_topic_template
        ]}
    ]),
    {atomic, ok} = mnesia:create_table(bcast_msg_index, [
        {disc_copies, [node()]},
        {type, set},
        {record_name, bcast_msg_index},
        {attributes, [key, deliveries]}
    ]),
    ok = mnesia:dirty_write({bcast_message, MsgId, ApiMsgId, Hash, <<"legacy payload">>, 111, 222}),
    ok = mnesia:dirty_write(
        {bcast_msg, DeliveryId, MsgId, PK, <<"tpl">>, 1, 0, [DN], 111, 222,
            <<"legacy response topic">>}
    ),
    ok = mnesia:dirty_write({bcast_msg_index, {PK, DN}, [DeliveryId]}),
    ok = emqx_bcast:init_tables(),
    %% The per-device index is a derived ETS cache on the owner core; after
    %% an in-place legacy migration the owner rebuilds it from the migrated
    %% bcast_msg rows (the same path used at owner takeover).
    ok = emqx_bcast_index_owner:rebuild_index(),
    ?assertEqual(
        [msg_id, api_msg_id, content_hash, payload, delivery_count, created_at, expires_at],
        mnesia:table_info(bcast_message, attributes)
    ),
    {ok, Msg} = emqx_bcast_storage:lookup_message(MsgId),
    ?assertEqual(0, Msg#bcast_message.delivery_count),
    ?assertEqual({ok, [DeliveryId]}, emqx_bcast_storage:get_device_deliveries({PK, DN})),
    ?assertEqual(
        {ok, [{DeliveryId, stored}]},
        emqx_bcast_storage:get_device_delivery_entries({PK, DN})
    ),
    ?assertEqual(1, emqx_bcast_storage:pending_delivery_count()).

-doc "Concurrent create and ack transactions complete without lock-order failures.".
t_concurrent_create_ack_lock_order(_Config) ->
    PK = <<"PLOCK">>,
    DN = <<"DLOCK">>,
    {_ApiMsgId, MsgGuid} = create_test_msg(<<"lock payload">>),
    DeliveryIds = [emqx_bcast_utils:gen_guid() || _ <- lists:seq(1, 16)],
    lists:foreach(
        fun(Did) ->
            {ok, _} = emqx_bcast_storage:create_delivery(Did, MsgGuid, PK, <<"tpl">>, [DN], 1)
        end,
        DeliveryIds
    ),
    Parent = self(),
    AckPids = [
        spawn(fun() ->
            Result = emqx_bcast_storage:process_ack(PK, DN, Did),
            Parent ! {ack_result, self(), Result}
        end)
     || Did <- DeliveryIds
    ],
    CreatePids = [
        spawn(fun() ->
            Payload = crypto:strong_rand_bytes(8),
            Hash = crypto:hash(sha256, Payload),
            {NewApiId, NewMsgId} = emqx_bcast_id:generate_message_id(),
            NewDid = emqx_bcast_utils:gen_guid(),
            Result = emqx_bcast_storage:create_message_and_delivery(
                Payload, Hash, NewApiId, NewMsgId, NewDid, PK, <<"tpl">>, [DN]
            ),
            Parent ! {create_result, self(), Result}
        end)
     || _ <- lists:seq(1, 16)
    ],
    AckResults = [
        receive
            {ack_result, P, R} -> R
        end
     || P <- AckPids
    ],
    CreateResults = [
        receive
            {create_result, P, R} -> R
        end
     || P <- CreatePids
    ],
    lists:foreach(fun(R) -> ?assertEqual(counted, R) end, AckResults),
    lists:foreach(fun(R) -> ?assertMatch({ok, _, _}, R) end, CreateResults),
    ?assertEqual(16, emqx_bcast_storage:pending_delivery_count()).

-doc "Restarting worker pools releases tagged inflight claims.".
t_worker_pool_restart_recovers_inflight(_Config) ->
    PK = <<"PRESTART">>,
    DN = <<"DRESTART">>,
    Tag = 888888,
    InflightTab = emqx_bcast_pull_pool:tab(emqx_bcast_pull_pool:shard_of(DN), bcast_pull_inflight),
    _ = create_tagged_claim(PK, DN, Tag),
    ets:insert(InflightTab, {DN, Tag, PK, erlang:system_time(millisecond)}),
    ok = emqx_bcast_sup:restart_pools(2),
    ?assert(
        wait_until(
            fun() -> ets:lookup(InflightTab, DN) =:= [] end,
            100
        )
    ),
    ?assert(
        wait_until(
            fun() ->
                case emqx_bcast_storage:get_device_delivery_entries({PK, DN}) of
                    {ok, [{_, stored}]} -> true;
                    _ -> false
                end
            end,
            100
        )
    ).

-doc "A stale deliver_results generation cannot clear the current inflight mark.".
t_stale_deliver_results_keep_current_generation(_Config) ->
    PK = <<"PSTALE">>,
    DN = <<"DSTALE">>,
    OldTag = 777777,
    NewTag = 777778,
    Shard = emqx_bcast_pull_pool:shard_of(DN),
    InflightTab = emqx_bcast_pull_pool:tab(Shard, bcast_pull_inflight),
    Map = create_tagged_claim(PK, DN, OldTag),
    ets:insert(InflightTab, {DN, OldTag, PK, erlang:system_time(millisecond)}),
    ets:insert(InflightTab, {DN, NewTag, PK, erlang:system_time(millisecond)}),
    gen_server:cast(
        emqx_bcast_pull_pool:pool_name(Shard),
        {deliver_results, [{DN, {ok, Map}}], [{DN, OldTag, PK}]}
    ),
    _ = sys:get_state(emqx_bcast_pull_pool:pool_name(Shard)),
    ?assertMatch([{DN, NewTag, PK, _}], ets:lookup(InflightTab, DN)),
    ?assertEqual([], ets:tab2list(emqx_bcast_pull_pool:tab(Shard, bcast_buffer_a))),
    ?assertEqual([], ets:tab2list(emqx_bcast_pull_pool:tab(Shard, bcast_buffer_b))),
    ?assert(
        wait_until(
            fun() ->
                case emqx_bcast_storage:get_device_delivery_entries({PK, DN}) of
                    {ok, [{_, stored}]} -> true;
                    _ -> false
                end
            end,
            100
        )
    ).

-doc "An empty claim result after an RPC timeout releases the tagged pending entry.".
t_failed_claim_result_releases_pending_generation(_Config) ->
    PK = <<"PTIMEOUT">>,
    DN = <<"DTIMEOUT">>,
    Tag = 999999,
    Shard = emqx_bcast_pull_pool:shard_of(DN),
    InflightTab = emqx_bcast_pull_pool:tab(Shard, bcast_pull_inflight),
    _ = create_tagged_claim(PK, DN, Tag),
    ets:insert(InflightTab, {DN, Tag, PK, erlang:system_time(millisecond)}),
    gen_server:cast(
        emqx_bcast_pull_pool:pool_name(Shard),
        {deliver_results, [], [{DN, Tag, PK}]}
    ),
    _ = sys:get_state(emqx_bcast_pull_pool:pool_name(Shard)),
    ?assertEqual([], ets:lookup(InflightTab, DN)),
    ?assert(
        wait_until(
            fun() ->
                case emqx_bcast_storage:get_device_delivery_entries({PK, DN}) of
                    {ok, [{_, stored}]} -> true;
                    _ -> false
                end
            end,
            100
        )
    ).

create_tagged_claim(PK, DN, Tag) ->
    {_ApiMsgId, MsgGuid} = create_test_msg(<<"tagged claim">>),
    DeliveryId = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(
        DeliveryId, MsgGuid, PK, <<"tpl">>, [DN], 1
    ),
    [{DN, {ok, Map}}] = emqx_bcast_storage:claim_want_next_batch([
        #{
            clientid => DN,
            product_key => PK,
            topics => [{<<"tpl">>, 1}],
            claim_tag => Tag
        }
    ]),
    Map.

create_test_msg(Payload) ->
    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    Hash = crypto:hash(sha256, Payload),
    emqx_bcast_storage:create_message(ApiMsgId, MsgGuid, Hash, Payload),
    {ApiMsgId, MsgGuid}.

%% Single-shard op-capacity probe: all devices are forced onto ONE shard
%% (phash2 {PK, DN} -> target), so the numbers measure that single shard
%% gen_server's real serial capacity for append (create_delivery), claim
%% (2x mnesia dirty reads + topic match) and ack (index remove + meta dec),
%% without EMQX channel/bench interference.
-doc "Single-shard append/claim/ack op capacity probe.".
t_shard_op_capacity_probe(_Config) ->
    PK = <<"P1SHARD">>,
    Target = 0,
    Schedulers = erlang:system_info(schedulers_online),
    true = wait_until(fun() -> shard_active(Target) end, 200),
    NDev = 2000,
    Depth = 20,
    DNs = same_shard_dns(PK, Target, NDev),
    ct:pal(
        "probe: schedulers=~p target_shard=~p devices=~p depth=~p entries=~p",
        [Schedulers, Target, NDev, Depth, NDev * Depth]
    ),
    {_ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    Payload = binary:copy(<<"0123456789abcdef">>, 16),
    Hash = crypto:hash(sha256, Payload),
    emqx_bcast_storage:create_message(<<"probe-api">>, MsgGuid, Hash, Payload),
    %% ---- create + append (promoter-side cost incl. mnesia tx) ----
    {CreateUs, _Dids} = timer:tc(fun() ->
        [
            begin
                Did = emqx_bcast_utils:gen_guid(),
                {ok, _} = emqx_bcast_storage:create_delivery(
                    Did, MsgGuid, PK, <<"tpl">>, DNs, NDev
                ),
                Did
            end
         || _ <- lists:seq(1, Depth)
        ]
    end),
    ct:pal(
        "create+append: ~p entries in ~p us -> ~.1f entries/s (~.1f us/entry)",
        [
            NDev * Depth,
            CreateUs,
            NDev * Depth * 1.0e6 / max(1, CreateUs),
            CreateUs / max(1, NDev * Depth)
        ]
    ),
    Entries = [
        #{clientid => DN, product_key => PK, topics => [{<<"tpl">>, 1}]}
     || DN <- DNs
    ],
    %% ---- drain: Depth rounds of (claim NDev) -> (ack NDev), window=1 ----
    {ClaimUs, AckUs, TotalOk} =
        lists:foldl(
            fun(_, {CAcc, AAcc, OkAcc}) ->
                {CUs, Claimed} = timer:tc(fun() ->
                    emqx_bcast_storage:claim_want_next_batch(Entries)
                end),
                Acks = [
                    {PK, DN, maps:get(delivery_id, M)}
                 || {DN, {ok, M}} <- Claimed
                ],
                {AUs, AckRes} = timer:tc(fun() -> emqx_bcast_storage:process_ack_batch(Acks) end),
                Counted = length([1 || counted <- AckRes]),
                {CAcc + CUs, AAcc + AUs, OkAcc + Counted}
            end,
            {0, 0, 0},
            lists:seq(1, Depth)
        ),
    ct:pal(
        "claim: ~p entries in ~p us -> ~.1f entries/s (~.1f us/entry)",
        [TotalOk, ClaimUs, TotalOk * 1.0e6 / max(1, ClaimUs), ClaimUs / max(1, TotalOk)]
    ),
    ct:pal(
        "ack  : ~p entries in ~p us -> ~.1f entries/s (~.1f us/entry)",
        [TotalOk, AckUs, TotalOk * 1.0e6 / max(1, AckUs), AckUs / max(1, TotalOk)]
    ),
    Total = NDev * Depth,
    ct:pal(
        "drain(claim+ack): ~p entries in ~p us -> ~.1f entries/s combined",
        [TotalOk, ClaimUs + AckUs, TotalOk * 1.0e6 / max(1, ClaimUs + AckUs)]
    ),
    ct:pal(
        "full cycle (create+claim+ack): ~p us -> ~.1f entries/s",
        [CreateUs + ClaimUs + AckUs, Total * 1.0e6 / max(1, CreateUs + ClaimUs + AckUs)]
    ),
    %% ---- residual / ledger check ----
    {Queued, Inflight} = emqx_bcast_index_owner:gauge_sample(),
    Pending = emqx_bcast_storage:pending_delivery_count(),
    HeapBefore = shard_heap(Target),
    _ = wait_until(fun() -> emqx_bcast_intake:depth() =:= 0 end, 50),
    HeapAfter = shard_heap(Target),
    ct:pal(
        "residual: queued=~p inflight=~p pending=~p shard_heap_before=~p after=~p words",
        [Queued, Inflight, Pending, HeapBefore, HeapAfter]
    ),
    ?assertEqual(Total, TotalOk),
    ?assertEqual(0, Queued + Inflight),
    ?assertEqual(0, Pending),
    ok.

%% All device names that phash2 into the target shard.
same_shard_dns(PK, Target, N) ->
    same_shard_dns(PK, Target, N, 1, []).
same_shard_dns(_PK, _Target, 0, _I, Acc) ->
    lists:reverse(Acc);
same_shard_dns(PK, Target, Need, I, Acc) ->
    DN = <<"DN_", (integer_to_binary(I))/binary>>,
    case erlang:phash2({PK, DN}, emqx_bcast_index_owner:shard_count()) of
        Target ->
            same_shard_dns(PK, Target, Need - 1, I + 1, [DN | Acc]);
        _ ->
            same_shard_dns(PK, Target, Need, I + 1, Acc)
    end.

shard_active(S) ->
    try
        maps:get(
            active, sys:get_state(list_to_atom("emqx_bcast_index_owner_" ++ integer_to_list(S)))
        )
    of
        A -> A =:= true
    catch
        _:_ -> false
    end.

shard_heap(S) ->
    Name = list_to_atom("emqx_bcast_index_owner_" ++ integer_to_list(S)),
    case erlang:process_info(whereis(Name), total_heap_size) of
        {total_heap_size, H} -> H;
        _ -> -1
    end.
