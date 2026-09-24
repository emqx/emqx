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
            bcast_msg_meta_counter,
            bcast_msg_acked,
            bcast_message,
            bcast_message_order,
            bcast_message_hash,
            bcast_message_api_id,
            bcast_msg_index,
            bcast_quota
        ]
    ],
    catch emqx_bcast:init_tables(),
    [catch ets:delete_all_objects(T) || T <- [bcast_device_registry]],
    %% Per-shard pull state tables (one per-client state row per shard).
    [
        catch ets:delete_all_objects(emqx_bcast_pull_shard:tab(S, bcast_client_state))
     || S <- lists:seq(0, emqx_bcast_pull_shard:shard_count() - 1)
    ],
    %% Per-shard flush counters (claim). Re-seed after clearing:
    %% ets:update_counter is a no-op on a missing key, so an emptied table
    %% would silently freeze the flush bookkeeping.
    [
        catch begin
            T = emqx_bcast_pull_shard:tab(S, bcast_pull_counters),
            ets:delete_all_objects(T),
            ets:insert(T, [{claim, 0}])
        end
     || S <- lists:seq(0, emqx_bcast_pull_shard:shard_count() - 1)
    ],
    %% The owner ETS index/quota and the intake queue are not mnesia
    %% tables; reset them explicitly so no state leaks between tests.
    catch emqx_bcast_intake:reset(),
    catch emqx_bcast_index_owner:reset(),
    %% The activation leader's record of the peers whose probe timed out is
    %% about the outside world, not about this node's index, so the index reset
    %% above leaves it alone. A case that hangs shards would therefore hold them
    %% back in every following case's drive; start each case with no peer held
    %% back.
    catch forget_probe_backoff(),
    %% Full metric registry reset: per-test isolation so ledger/gauge
    %% assertions can compare absolute values, not just deltas.
    catch emqx_bcast_metrics:reset(),
    Config.

wait_intake_idle() ->
    _ = wait_until(
        fun() ->
            emqx_bcast_intake:depth() =:= 0 andalso emqx_bcast_intake:deferred_depth() =:= 0
        end,
        50
    ),
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

-doc "Durations accept the forms the rest of EMQX accepts for config\n"
"durations - compound units and sub-second values - rounded up to whole\n"
"seconds here, and a bare number still means seconds. An unparseable value\n"
"falls back to the field default with a warning at config time instead of\n"
"silently behaving differently at request time.".
t_config_duration_formats(_Config) ->
    try
        ok = emqx_bcast_config:update(#{<<"msg_ttl">> => <<"1h30m">>}),
        ?assertEqual(5400, emqx_bcast_config:get(msg_ttl)),
        ok = emqx_bcast_config:update(#{<<"cleanup_interval">> => <<"2m30s">>}),
        ?assertEqual(150, emqx_bcast_config:get(cleanup_interval)),
        %% Sub-second rounds up: 500ms must not become a TTL of 0.
        ok = emqx_bcast_config:update(#{<<"msg_ttl">> => <<"500ms">>}),
        ?assertEqual(1, emqx_bcast_config:get(msg_ttl)),
        %% A bare number keeps meaning seconds.
        ok = emqx_bcast_config:update(#{<<"msg_ttl">> => <<"60">>}),
        ?assertEqual(60, emqx_bcast_config:get(msg_ttl)),
        Reports = emqx_cth_log_capture:capture(fun() ->
            ok = emqx_bcast_config:update(#{<<"msg_ttl">> => <<"1h30">>})
        end),
        ?assertEqual(15 * 86400, emqx_bcast_config:get(msg_ttl)),
        ?assert(
            lists:any(
                fun(R) ->
                    maps:get(field, R, undefined) =:= msg_ttl andalso
                        maps:get(default, R, undefined) =:= 15 * 86400
                end,
                Reports
            )
        )
    after
        init_test_config()
    end.

-doc "The runtime config surface matches the avro schema: keys the schema "
"does not declare (e.g. the hardcoded intake queue depth) must not leak "
"into the normalized runtime config - they could never be set through "
"the plugin config API anyway.".
t_config_surface_matches_schema(_Config) ->
    try
        ok = emqx_bcast_config:update(#{<<"intake_queue_depth">> => 5}),
        ?assertEqual(undefined, emqx_bcast_config:get(intake_queue_depth, undefined)),
        RuntimeKeys = lists:sort(maps:keys(persistent_term:get({?APP, config}))),
        SchemaPath = filename:join(code:priv_dir(emqx_bcast), "config_schema.avsc"),
        {ok, SchemaBin} = file:read_file(SchemaPath),
        Schema = emqx_utils_json:decode(SchemaBin),
        SchemaFields = lists:sort([
            binary_to_atom(maps:get(<<"name">>, F))
         || F <- maps:get(<<"fields">>, Schema)
        ]),
        ?assert(lists:all(fun(K) -> lists:member(K, SchemaFields) end, RuntimeKeys))
    after
        init_test_config()
    end.

-doc "Every field declared in config_schema.avsc must also be present in the "
"shipped default config.hocon. A fresh install serves config.hocon as the "
"plugin config, and the dashboard's avro conversion requires every schema "
"field to be present (it does not fall back to the schema default for a "
"missing field), so an omitted field breaks the plugin config page.".
t_default_config_covers_schema(_Config) ->
    Priv = code:priv_dir(emqx_bcast),
    {ok, SchemaBin} = file:read_file(filename:join(Priv, "config_schema.avsc")),
    Schema = emqx_utils_json:decode(SchemaBin),
    SchemaFields = [maps:get(<<"name">>, F) || F <- maps:get(<<"fields">>, Schema)],
    {ok, HoconBin} = file:read_file(filename:join(Priv, "config.hocon")),
    {ok, DefaultConfig} = hocon:binary(HoconBin),
    Missing = [Field || Field <- SchemaFields, not maps:is_key(Field, DefaultConfig)],
    ?assertEqual([], Missing),
    %% Retired settings that must stay declared. A config stored by an older
    %% plugin version still carries these names (delivery_queue_max is the one
    %% an 0.1.1 config sets), and decoding it against a schema that dropped the
    %% declaration is rejected, which would leave the plugin unable to load
    %% after an upgrade. They are declared without $ui, so the dashboard hides
    %% them, and they have no runtime effect.
    lists:foreach(
        fun(Field) -> ?assert(lists:member(Field, SchemaFields)) end,
        [<<"msg_warn_threshold">>, <<"force_upgrade_qos">>, <<"delivery_queue_max">>]
    ).

-doc "The schema has to declare the numeric bounds the plugin actually applies.\n"
"Declaring them is what keeps a dashboard form from offering a value the\n"
"plugin would silently override (or ignore, for a negative limit), but the\n"
"decode path does not enforce them - the attributes are inert to the Avro\n"
"decoder - so the runtime check stays the authority. The retired settings stay\n"
"declared without bounds: a config stored by an older version may carry any\n"
"value for them, and a bound could only reject that config.".
t_config_schema_declares_int_bounds(_Config) ->
    Priv = code:priv_dir(emqx_bcast),
    {ok, SchemaBin} = file:read_file(filename:join(Priv, "config_schema.avsc")),
    Schema = emqx_utils_json:decode(SchemaBin),
    Fields = maps:from_list([
        {maps:get(<<"name">>, F), F}
     || F <- maps:get(<<"fields">>, Schema)
    ]),
    %% Zero is a documented "allow nothing" bound for these limits, so zero is
    %% the declared floor; a negative value is a typo the runtime ignores.
    lists:foreach(
        fun(Name) ->
            ?assertEqual(0, maps:get(<<"minimum">>, maps:get(Name, Fields), undefined))
        end,
        [
            <<"max_device_count">>,
            <<"max_message_size_broadcast">>,
            <<"max_message_size_batch">>,
            <<"max_pending_deliveries">>,
            <<"delivery_pool_size">>
        ]
    ),
    %% The per-device cap is clamped to this window at runtime.
    PerDevice = maps:get(<<"max_pending_deliveries_per_device">>, Fields),
    ?assertEqual(10, maps:get(<<"minimum">>, PerDevice, undefined)),
    ?assertEqual(200, maps:get(<<"maximum">>, PerDevice, undefined)),
    lists:foreach(
        fun(Name) ->
            Retired = maps:get(Name, Fields),
            ?assertEqual(undefined, maps:get(<<"minimum">>, Retired, undefined)),
            ?assertEqual(undefined, maps:get(<<"maximum">>, Retired, undefined))
        end,
        [<<"msg_warn_threshold">>, <<"delivery_queue_max">>]
    ),
    %% The declared window is the one the plugin applies: a value below the
    %% floor is raised to it, a value above the ceiling is brought down to it.
    try
        ok = emqx_bcast_config:update(#{<<"max_pending_deliveries_per_device">> => 0}),
        ?assertEqual(10, emqx_bcast_config:get(max_pending_deliveries_per_device)),
        ok = emqx_bcast_config:update(#{<<"max_pending_deliveries_per_device">> => 1000000}),
        ?assertEqual(200, emqx_bcast_config:get(max_pending_deliveries_per_device))
    after
        init_test_config()
    end.

-doc "Out-of-range per-device quota values are clamped to [10, 200]; a "
"warning tells the operator the configured value was overridden "
"instead of silently rewriting it.".
t_config_per_device_quota_clamped_with_warning(_Config) ->
    try
        Reports = emqx_cth_log_capture:capture(fun() ->
            ok = emqx_bcast_config:update(#{<<"max_pending_deliveries_per_device">> => 500})
        end),
        ?assertEqual(200, emqx_bcast_config:get(max_pending_deliveries_per_device)),
        ?assert(
            lists:any(
                fun(R) -> maps:get(msg, R, undefined) =:= per_device_quota_clamped end, Reports
            )
        ),
        ok = emqx_bcast_config:update(#{<<"max_pending_deliveries_per_device">> => 1}),
        ?assertEqual(10, emqx_bcast_config:get(max_pending_deliveries_per_device)),
        ok = emqx_bcast_config:update(#{<<"max_pending_deliveries_per_device">> => 50}),
        ?assertEqual(50, emqx_bcast_config:get(max_pending_deliveries_per_device))
    after
        init_test_config()
    end.

-doc "A negative integer limit is a configuration typo with a silent and total\n"
"effect (max_message_size_batch = -1 rejects every publish). It falls back to\n"
"the default with a warning, like the per-device quota clamp and a bad\n"
"duration: the schema declares the bounds but does not enforce them, because\n"
"the Avro decoder that reads a stored config ignores them. Zero is a\n"
"legitimate \"allow nothing\" bound and is kept.".
t_config_negative_limits_fall_back_to_default(_Config) ->
    Fields = [
        max_device_count,
        max_message_size_broadcast,
        max_message_size_batch,
        max_pending_deliveries
    ],
    Defaults = [{Field, emqx_bcast_config:get(Field)} || Field <- Fields],
    try
        Reports = emqx_cth_log_capture:capture(fun() ->
            ok = emqx_bcast_config:update(#{
                <<"max_device_count">> => -1,
                <<"max_message_size_broadcast">> => -1,
                <<"max_message_size_batch">> => -1,
                <<"max_pending_deliveries">> => -5
            })
        end),
        lists:foreach(
            fun({Field, Expected}) ->
                ?assertEqual(Expected, emqx_bcast_config:get(Field))
            end,
            Defaults
        ),
        ?assert(
            lists:any(
                fun(R) ->
                    maps:get(field, R, undefined) =:= max_message_size_batch andalso
                        maps:get(configured, R, undefined) =:= -1
                end,
                Reports
            )
        ),
        %% Zero stays live: it is a cap of zero deliveries, not a typo.
        ok = emqx_bcast_config:update(#{<<"max_pending_deliveries">> => 0}),
        ?assertEqual(0, emqx_bcast_config:get(max_pending_deliveries))
    after
        init_test_config()
    end.

-doc "Configured topic templates are validated like per-request templates: "
"wildcards and unknown placeholders fall back to the default with a "
"warning instead of failing at publish time; valid templates are kept.".
t_config_topic_template_validated(_Config) ->
    DefaultBroadcast = <<"/sys/broadcast/${productKey}">>,
    DefaultBatch = <<"/${productKey}/${deviceName}/user/get">>,
    try
        Reports = emqx_cth_log_capture:capture(fun() ->
            ok = emqx_bcast_config:update(#{<<"broadcast_topic">> => <<"/sys/broadcast/+">>}),
            ok = emqx_bcast_config:update(#{<<"batch_topic">> => <<>>})
        end),
        ?assertEqual(DefaultBroadcast, emqx_bcast_config:get(broadcast_topic)),
        ?assertEqual(DefaultBatch, emqx_bcast_config:get(batch_topic)),
        ?assertEqual(
            2,
            length([
                R
             || R <- Reports,
                maps:get(msg, R, undefined) =:= invalid_plugin_config_topic_template
            ])
        ),
        ok = emqx_bcast_config:update(#{
            <<"broadcast_topic">> => <<"/sys/b/${productKey}/${deviceName}">>,
            <<"batch_topic">> => <<"/${productKey}/${deviceName}/custom">>
        }),
        ?assertEqual(
            <<"/sys/b/${productKey}/${deviceName}">>, emqx_bcast_config:get(broadcast_topic)
        ),
        ?assertEqual(
            <<"/${productKey}/${deviceName}/custom">>, emqx_bcast_config:get(batch_topic)
        )
    after
        init_test_config()
    end.

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
    ?assertMatch(
        [{DN, {no_more, _}}],
        emqx_bcast_storage:claim_want_next_batch([
            #{residual => true, clientid => DN, product_key => PK, topics => []}
        ])
    ),
    %% The entry is fresh (appended moments ago): a concurrent promotion on
    %% the peer core might still be replicating its rows, so the claim
    %% skips instead of dropping - the index entry and quota survive.
    ?assertEqual(1, emqx_bcast_storage:pending_delivery_count()),
    %% The bounded orphan scan is the designated repair path for genuinely
    %% stale entries. It applies the same replication-lag rule as the claim
    %% path, so the repair happens once the entry is past that window: age the
    %% entry instead of sleeping through it.
    backdate_index_entry(PK, DN, DeliveryId),
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
                    #{
                        residual => true,
                        clientid => DN,
                        product_key => PK,
                        topics => [{<<"nomatch">>, 1}]
                    }
                ])
            }
    end),
    receive
        {claim_result, Pid, [{DN, {no_more, _}}]} ->
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
    [{DN, {ok, [_]}}] = emqx_bcast_storage:claim_want_next_batch([
        #{residual => true, clientid => DN, product_key => PK, topics => [{<<"tpl">>, 1}]}
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
    seed_claim_row(<<"R5P">>, <<"R5C0">>, 11, Now, Shard0),
    seed_claim_row(<<"R5P">>, <<"R5C1">>, 22, Now, Shard1),
    try
        {ok, Marks0} =
            gen_server:call(
                emqx_bcast_pull_shard:shard_name(Shard0), begin_pools_restart, infinity
            ),
        {ok, Marks1} =
            gen_server:call(
                emqx_bcast_pull_shard:shard_name(Shard1), begin_pools_restart, infinity
            ),
        %% Each shard returns ONLY its own marks (not the 4x aggregate).
        ?assertEqual([{<<"R5C0">>, 11, <<"R5P">>}], Marks0),
        ?assertEqual([{<<"R5C1">>, 22, <<"R5P">>}], Marks1)
    after
        gen_server:cast(emqx_bcast_pull_shard:shard_name(Shard0), {abort_pools_restart}),
        gen_server:cast(emqx_bcast_pull_shard:shard_name(Shard1), {abort_pools_restart}),
        cleanup_row(<<"R5P">>, <<"R5C0">>),
        cleanup_row(<<"R5P">>, <<"R5C1">>)
    end.

-doc "abort_pools_restart replays deferred deliver_results (regression): a\n"
"shard armed during a restart that is then aborted must not keep held\n"
"inflight marks forever (window=1 stall).".
t_abort_pools_restart_replays_deferred(_Config) ->
    DN = <<"N1DN">>,
    PK = <<"N1PK">>,
    %% The mark lives on shard_of(PK, DN): mark_current/clear_inflight_mark
    %% resolve the table by the client's shard, so the test must insert
    %% into that same shard's inflight table.
    Shard = emqx_bcast_pull_shard:shard_of(PK, DN),
    Pool = emqx_bcast_pull_shard:shard_name(Shard),
    Tag = 424242,
    seed_claim_row(PK, DN, Tag, erlang:system_time(millisecond), Shard),
    try
        {ok, _Marks} = gen_server:call(Pool, begin_pools_restart, infinity),
        %% A deliver_results batch arrives while pools_restarting: deferred
        %% (kept, marks held).
        gen_server:cast(Pool, {deliver_results, [{DN, no_more}], [{DN, Tag, PK}]}),
        %% The restart is aborted (a sibling reported restart_in_progress):
        %% the deferred batch must be replayed so the held claim is cleared.
        gen_server:cast(Pool, {abort_pools_restart}),
        ?assert(
            wait_until(fun() -> row_claim_of(PK, DN) =:= undefined end, 100)
        )
    after
        gen_server:cast(Pool, {abort_pools_restart}),
        cleanup_row(PK, DN)
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
    [{DN, {ok, [M1]}}] = emqx_bcast_storage:claim_want_next_batch([
        #{residual => true, clientid => DN, product_key => PK, topics => [{<<"tpl">>, 1}]}
    ]),
    ?assertEqual(1, emqx_bcast_storage:pending_delivery_count()),
    %% Force the claim lease to expire: the inflight ts is rewritten so the
    %% next claim releases the entry back to the queue (redelivery).
    expire_inflight(PK, DN, DeliveryId),
    %% Second claim: lease expired -> the SAME delivery is claimed again.
    [{DN, {ok, [M2]}}] = emqx_bcast_storage:claim_want_next_batch([
        #{residual => true, clientid => DN, product_key => PK, topics => [{<<"tpl">>, 1}]}
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
    %% The scan shares the claim path's replication-lag rule, so age the entry
    %% past that window instead of sleeping through it.
    backdate_index_entry(PK, DN, DeliveryId),
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
    [{DN1, {ok, [_]}}] = emqx_bcast_storage:claim_want_next_batch([
        #{residual => true, clientid => DN1, product_key => PK, topics => [{<<"tpl">>, 1}]}
    ]),
    expire_inflight(PK, DN1, DeliveryId),
    [{DN1, {ok, [_]}}] = emqx_bcast_storage:claim_want_next_batch([
        #{residual => true, clientid => DN1, product_key => PK, topics => [{<<"tpl">>, 1}]}
    ]),
    %% The client acks the delivery TWICE (first + redelivered PUBLISH).
    ?assertEqual([counted], emqx_bcast_storage:process_ack_batch([{PK, DN1, DeliveryId}])),
    ?assertEqual([not_found], emqx_bcast_storage:process_ack_batch([{PK, DN1, DeliveryId}])),
    %% The delivery must NOT be complete: DN2's entry is still pending and
    %% claimable (the meta counter still requires DN2's ack).
    ?assertEqual({ok, [DeliveryId]}, emqx_bcast_storage:get_device_deliveries({PK, DN2})),
    ?assertEqual(1, emqx_bcast_storage:pending_delivery_count()),
    [{DN2, {ok, [_]}}] = emqx_bcast_storage:claim_want_next_batch([
        #{residual => true, clientid => DN2, product_key => PK, topics => [{<<"tpl">>, 1}]}
    ]),
    %% DN2's ack completes the delivery.
    ?assertEqual([counted], emqx_bcast_storage:process_ack_batch([{PK, DN2, DeliveryId}])),
    ?assertEqual(0, emqx_bcast_storage:pending_delivery_count()),
    ?assertEqual({ok, []}, emqx_bcast_storage:get_device_deliveries({PK, DN1})),
    ?assertEqual({ok, []}, emqx_bcast_storage:get_device_deliveries({PK, DN2})),
    ?assert(
        wait_until(
            fun() ->
                mnesia:dirty_match_object(#bcast_msg{_ = '_'}) =:= [] andalso
                    mnesia:dirty_match_object(#bcast_msg_meta{_ = '_'}) =:= [] andalso
                    mnesia:dirty_match_object(#bcast_msg_meta_counter{_ = '_'}) =:= []
            end,
            100
        )
    ).

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
    %% must terminate (not wedge) with A left in the queue. A carries the
    %% residual, which is what lets the pull side tell "not claimable yet"
    %% from "drained" and re-arm the client instead of stranding it.
    [{DN, {no_more, 1}}] = emqx_bcast_storage:claim_want_next_batch([
        #{residual => true, clientid => DN, product_key => PK, topics => [{<<"nomatch">>, 1}]}
    ]),
    %% A is still claimable with the matching topic.
    [{DN, {ok, [Map]}}] = emqx_bcast_storage:claim_want_next_batch([
        #{residual => true, clientid => DN, product_key => PK, topics => [{<<"tpl">>, 1}]}
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
            [{DN, {ok, [_]}}] = emqx_bcast_storage:claim_want_next_batch([
                #{residual => true, clientid => DN, product_key => PK, topics => [{<<"tpl">>, 1}]}
            ]),
            expire_inflight(PK, DN, DeliveryId),
            [{DN, {ok, [_]}}] = emqx_bcast_storage:claim_want_next_batch([
                #{residual => true, clientid => DN, product_key => PK, topics => [{<<"tpl">>, 1}]}
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
    ?assert(wait_until(fun() -> mnesia:dirty_read(bcast_msg, DeliveryId) =:= [] end, 100)).

-doc "Once every device of a delivery has acked, the per-delivery storage\n"
"rows (bcast_msg + bcast_msg_meta + bcast_msg_meta_counter) are removed so\n"
"the batch tables do not accumulate (regression: the ack counter decrement\n"
"saturated the rlog and aborted complete_delivery, leaking all three\n"
"tables). The ack counter is flushed asynchronously, so the assertion\n"
"polls until the flush lands.".
t_ack_completion_clears_msg_tables(_Config) ->
    PK = <<"PACTC">>,
    {_ApiMsgId, MsgGuid} = create_test_msg(<<"ack clears tables">>),
    %% Several devices ack the SAME delivery; the counter is decremented
    %% once per device (possibly from different shards) and the delivery
    %% rows must vanish after the last ack.
    DNs = [iolist_to_binary([<<"DACTC">>, integer_to_binary(I)]) || I <- lists:seq(1, 8)],
    DeliveryId = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(
        DeliveryId, MsgGuid, PK, <<"tpl">>, DNs, length(DNs)
    ),
    ?assertEqual(length(DNs), emqx_bcast_storage:pending_delivery_count()),
    Results = emqx_bcast_storage:process_ack_batch([{PK, DN, DeliveryId} || DN <- DNs]),
    ?assertEqual([counted || _ <- DNs], Results),
    ?assertEqual(0, emqx_bcast_storage:pending_delivery_count()),
    ?assert(
        wait_until(
            fun() ->
                mnesia:dirty_match_object(#bcast_msg{_ = '_'}) =:= [] andalso
                    mnesia:dirty_match_object(#bcast_msg_meta{_ = '_'}) =:= [] andalso
                    mnesia:dirty_match_object(#bcast_msg_meta_counter{_ = '_'}) =:= []
            end,
            100
        )
    ).

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

-doc "The ack flush persists the per-device acked marker and decrements the\n"
"completion counter with two lock-free dirty writes, and it writes the\n"
"marker FIRST. The pair is ordered, not atomic. The ordering is what keeps\n"
"the crash window safe in the direction that matters: a rebuilt index skips\n"
"a device whose marker is persisted, so a replayed ack of an already-counted\n"
"device is rejected instead of decrementing the counter a second time and\n"
"deleting the delivery before the remaining devices acked. The reverse order\n"
"would allow exactly that. The residual of this order - a counter left too\n"
"high when the shard dies between the two writes - is covered by\n"
"t_counted_ack_crash_window_between_marker_and_decrement.".
t_counted_ack_counter_and_marker_atomic(_Config) ->
    PK = <<"PATOM">>,
    {_ApiMsgId, MsgGuid} = create_test_msg(<<"atomic ack">>),
    A = <<"DATOM_A">>,
    B = <<"DATOM_B">>,
    DeliveryId = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(DeliveryId, MsgGuid, PK, <<"tpl">>, [A, B], 2),
    counted = emqx_bcast_storage:process_ack(PK, A, DeliveryId),
    %% The async flush lands the marker first and the counter second. Wait on
    %% the counter: the two writes are ordered, not atomic, so seeing the
    %% marker does not imply the counter has moved yet.
    ?assert(wait_until(fun() -> ack_counter(DeliveryId) =:= 1 end, 100)),
    ?assert(mnesia:dirty_read(bcast_msg_acked, DeliveryId) =/= []),
    %% a rebuild keeps A acked and the counter untouched; B keeps its entry
    ok = emqx_bcast_index_owner:rebuild_index(),
    {ok, []} = emqx_bcast_storage:get_device_deliveries({PK, A}),
    {ok, [DeliveryId]} = emqx_bcast_storage:get_device_deliveries({PK, B}),
    [#bcast_msg_meta_counter{counter = 1}] = mnesia:dirty_read(
        bcast_msg_meta_counter, DeliveryId
    ),
    %% a duplicate ack of A is not counted again
    not_found = emqx_bcast_storage:process_ack(PK, A, DeliveryId),
    [#bcast_msg_meta_counter{counter = 1}] = mnesia:dirty_read(
        bcast_msg_meta_counter, DeliveryId
    ),
    %% B's ack completes the delivery exactly once; all rows go away
    counted = emqx_bcast_storage:process_ack(PK, B, DeliveryId),
    ?assert(wait_until(fun() -> mnesia:dirty_read(bcast_msg, DeliveryId) =:= [] end, 100)),
    [] = mnesia:dirty_read(bcast_msg_acked, DeliveryId),
    [] = mnesia:dirty_read(bcast_msg_meta_counter, DeliveryId).

-doc "Crash window between the acked-marker write and the counter decrement:\n"
"mock mnesia:dirty_update_counter so that the index shard is killed right\n"
"after the decrement lands (both dirty writes of the flush are then done).\n"
"The restart re-drives a rebuild from the projection plus the markers. The\n"
"marker is written BEFORE the counter precisely so that this crash can never\n"
"resurrect the already-acked device nor let its replayed ack decrement the\n"
"counter a second time: the counter can only be left too high, which delays\n"
"completion instead of completing the delivery early.".
t_counted_ack_crash_window_between_decrement_and_marker(_Config) ->
    PK = <<"PWIN">>,
    {_ApiMsgId, MsgGuid} = create_test_msg(<<"crash window">>),
    A = <<"DWIN_A">>,
    B = <<"DWIN_B">>,
    DeliveryId = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(
        DeliveryId, MsgGuid, PK, <<"tpl">>, [A, B], 2
    ),
    TestProc = self(),
    meck:new(mnesia, [passthrough, no_link]),
    try
        meck:expect(
            mnesia,
            dirty_update_counter,
            fun
                (Tab, Key, Incr) when Tab =:= bcast_msg_meta_counter, Key =:= DeliveryId ->
                    Res = meck:passthrough([Tab, Key, Incr]),
                    TestProc ! {decrement_done, self()},
                    receive
                        kill_me -> exit(kill)
                    after 3000 -> ok
                    end,
                    Res;
                (Tab, Key, Incr) ->
                    meck:passthrough([Tab, Key, Incr])
            end
        ),
        counted = emqx_bcast_storage:process_ack(PK, A, DeliveryId),
        receive
            {decrement_done, ShardPid} ->
                %% Kill it for real: the flush guard retries anything raised
                %% inside the flush, and only a process death models the crash
                %% this window is about (an untrappable kill that leaves the
                %% decrement applied and the rest of the flush un-run).
                exit(ShardPid, kill),
                put(shard_died, true)
        after 3000 ->
            put(shard_died, false)
        end
    after
        meck:unload(mnesia)
    end,
    ShardDied = erase(shard_died),
    case ShardDied of
        true ->
            %% The shard died after the decrement. The marker for A was
            %% written first, so the rebuild must not bring A back: it has no
            %% pending entry, its replayed ack is rejected, and the counter
            %% still owes exactly B's ack.
            ok = emqx_bcast_index_owner:rebuild_index(),
            ?assertEqual({ok, []}, emqx_bcast_storage:get_device_deliveries({PK, A})),
            not_found = emqx_bcast_storage:process_ack(PK, A, DeliveryId),
            timer:sleep(300),
            ?assertMatch([#bcast_msg{}], mnesia:dirty_read(bcast_msg, DeliveryId)),
            [#bcast_msg_meta_counter{counter = 1}] = mnesia:dirty_read(
                bcast_msg_meta_counter, DeliveryId
            ),
            ?assertEqual(
                {ok, [DeliveryId]},
                emqx_bcast_storage:get_device_deliveries({PK, B})
            );
        false ->
            %% The hook never fired (the shard was already gone when the ack
            %% was buffered). The accounting contract is the same.
            ?assert(wait_until(fun() -> ack_counter(DeliveryId) =:= 1 end, 100)),
            ?assert(mnesia:dirty_read(bcast_msg_acked, DeliveryId) =/= []),
            ok = emqx_bcast_index_owner:rebuild_index(),
            {ok, []} = emqx_bcast_storage:get_device_deliveries({PK, A}),
            not_found = emqx_bcast_storage:process_ack(PK, A, DeliveryId),
            ?assertEqual(
                {ok, [DeliveryId]},
                emqx_bcast_storage:get_device_deliveries({PK, B})
            )
    end,
    %% B completes the delivery exactly once.
    counted = emqx_bcast_storage:process_ack(PK, B, DeliveryId),
    ?assert(
        wait_until(fun() -> mnesia:dirty_read(bcast_msg, DeliveryId) =:= [] end, 100)
    ).

-doc "Reproduction of the crash window the part report leaves open: mock\n"
"mnesia:dirty_write so the index shard is killed right after the acked marker\n"
"lands and BEFORE the counter is decremented. The marker is durable, so the\n"
"restart + rebuild must not resurrect the already-acked device and its\n"
"replayed ack must not be counted a second time; the marker-based reconcile\n"
"brings the counter back in step, so the remaining device's ack completes the\n"
"delivery exactly once - nothing lost, nothing delivered twice.".
t_counted_ack_crash_window_between_marker_and_decrement(_Config) ->
    PK = <<"PWIN2">>,
    {_ApiMsgId, MsgGuid} = create_test_msg(<<"crash between marker and counter">>),
    %% One device per part: A's own ack finishes its part (the marker write),
    %% B's is the one still owed.
    {A, B} = two_different_shards(PK),
    DeliveryId = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(
        DeliveryId, MsgGuid, PK, <<"tpl">>, [A, B], 2
    ),
    TestProc = self(),
    meck:new(mnesia, [passthrough, no_link]),
    try
        meck:expect(
            mnesia,
            dirty_write,
            fun
                (#bcast_msg_acked{delivery_id = Did} = Rec) when Did =:= DeliveryId ->
                    %% The marker lands first; die before the counter moves.
                    Res = meck:passthrough([Rec]),
                    TestProc ! {marker_written, self()},
                    receive
                        kill_me -> exit(kill)
                    after 3000 -> ok
                    end,
                    Res;
                (Rec) ->
                    meck:passthrough([Rec])
            end
        ),
        counted = emqx_bcast_storage:process_ack(PK, A, DeliveryId),
        receive
            {marker_written, ShardPid} ->
                %% Untrappable kill: the flush guard retries a raise from
                %% inside the flush, so the crash window has to be a real
                %% process death (marker written, counter not decremented).
                exit(ShardPid, kill),
                put(marker_window_hit, true)
        after 3000 ->
            put(marker_window_hit, false)
        end
    after
        meck:unload(mnesia)
    end,
    %% The window is hit deterministically: the flush always writes the
    %% marker before the counter, so this hook always fires for this ack.
    ?assert(erase(marker_window_hit)),
    Shard = emqx_bcast_index_owner:shard_of({PK, A}),
    Name = list_to_atom("emqx_bcast_index_owner_" ++ integer_to_list(Shard)),
    ?assert(
        wait_until(
            fun() -> whereis(Name) =/= undefined andalso shard_active(Shard) end, 200
        )
    ),
    ok = emqx_bcast_index_owner:rebuild_index(),
    %% The durable marker keeps A out of the rebuilt index, and A's replayed
    %% ack is rejected: no second decrement, no early completion.
    ?assertEqual({ok, []}, emqx_bcast_storage:get_device_deliveries({PK, A})),
    not_found = emqx_bcast_storage:process_ack(PK, A, DeliveryId),
    ?assertEqual({ok, [DeliveryId]}, emqx_bcast_storage:get_device_deliveries({PK, B})),
    %% The rebuild reconciled the counter from the durable marker: exactly B's
    %% acknowledgement is still owed.
    ?assert(wait_until(fun() -> ack_counter(DeliveryId) =:= 1 end, 100)),
    counted = emqx_bcast_storage:process_ack(PK, B, DeliveryId),
    %% Both devices are now accounted for, so the delivery completes exactly
    %% once - and A was never re-indexed, so it was not delivered twice.
    ?assert(wait_until(fun() -> mnesia:dirty_read(bcast_msg, DeliveryId) =:= [] end, 100)),
    ?assertEqual({ok, []}, emqx_bcast_storage:get_device_deliveries({PK, A})).

-doc "A delivery committed by a build without ack markers leaves none behind, so\n"
"a rebuild re-indexes every target device and its legacy remaining-ack counter\n"
"must be restored to the full target first (regression). Left at the legacy\n"
"value, the first duplicate ack from a re-delivered already-acked device drove\n"
"that counter to zero and completed the delivery while the never-acked devices\n"
"had no index entry left to deliver from - their payload row was gone.".
t_legacy_delivery_counter_restored_on_rebuild(_Config) ->
    PK = <<"PLEGACY">>,
    A = <<"DLEGACY_A">>,
    B = <<"DLEGACY_B">>,
    {_ApiMsgId, MsgGuid} = create_test_msg(<<"legacy delivery">>),
    DeliveryId = emqx_bcast_utils:gen_guid(),
    Now = emqx_bcast_utils:now_sec(),
    %% Exactly what a pre-marker build leaves behind for a delivery whose
    %% device A acked: bcast_msg.counter counts the acks received, the atomic
    %% counter row holds the remaining acks, bcast_msg_meta holds the full
    %% target, and there is no bcast_msg_acked row at all.
    ok = mnesia:dirty_write(#bcast_msg{
        delivery_id = DeliveryId,
        msg_id = MsgGuid,
        product_key = PK,
        topic_template = <<"tpl">>,
        target_ack_count = 2,
        counter = 1,
        device_names = [A, B],
        created_at = Now,
        expires_at = Now + 86400
    }),
    ok = mnesia:dirty_write(#bcast_msg_meta{
        delivery_id = DeliveryId,
        msg_id = MsgGuid,
        topic_template = <<"tpl">>,
        counter = 2
    }),
    ok = mnesia:dirty_write(#bcast_msg_meta_counter{delivery_id = DeliveryId, counter = 1}),
    ?assertEqual([], mnesia:dirty_read(bcast_msg_acked, DeliveryId)),
    ok = emqx_bcast_index_owner:rebuild_index(),
    %% Both devices are back in the index, so the counter has to expect an ack
    %% from both of them again.
    ?assertEqual({ok, [DeliveryId]}, emqx_bcast_storage:get_device_deliveries({PK, A})),
    ?assertEqual({ok, [DeliveryId]}, emqx_bcast_storage:get_device_deliveries({PK, B})),
    ?assertMatch(
        [#bcast_msg_meta_counter{counter = 2}],
        mnesia:dirty_read(bcast_msg_meta_counter, DeliveryId)
    ),
    %% A's re-delivered duplicate ack must not complete the delivery: B has
    %% not acked yet, and completing here would drop the payload row.
    counted = emqx_bcast_storage:process_ack(PK, A, DeliveryId),
    timer:sleep(200),
    ?assertMatch([#bcast_msg{}], mnesia:dirty_read(bcast_msg, DeliveryId)),
    ?assertEqual({ok, [DeliveryId]}, emqx_bcast_storage:get_device_deliveries({PK, B})),
    %% B's ack is the one that completes it.
    counted = emqx_bcast_storage:process_ack(PK, B, DeliveryId),
    ?assert(
        wait_until(fun() -> mnesia:dirty_read(bcast_msg, DeliveryId) =:= [] end, 100)
    ).

-doc "The ack flush is lock-free: one dirty marker write plus one dirty counter\n"
"decrement per delivery, and no mnesia transaction. A transaction there would\n"
"make every device shard take an exclusive write lock on the single\n"
"per-delivery counter row, and mnesia_tm restarts the whole shard with a\n"
"timer:sleep/1 backoff on each conflict - the fanout collapse this pins down.\n"
"The hook only fails when the transaction is issued by an index shard, so\n"
"unrelated mnesia traffic on the node cannot make this test flaky.".
t_ack_flush_uses_no_transaction(_Config) ->
    PK = <<"PNOTX">>,
    {_ApiMsgId, MsgGuid} = create_test_msg(<<"no transaction">>),
    A = <<"DNOTX_A">>,
    B = <<"DNOTX_B">>,
    DeliveryId = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(
        DeliveryId, MsgGuid, PK, <<"tpl">>, [A, B], 2
    ),
    TestProc = self(),
    meck:new(mnesia, [passthrough, no_link]),
    try
        %% Only one of the two devices is acked, so no completion runs and an
        %% index shard has no legitimate transaction in the window.
        meck:expect(mnesia, transaction, fun(Fun, Retries) ->
            case in_index_shard() of
                true -> TestProc ! ack_flush_used_transaction;
                false -> ok
            end,
            meck:passthrough([Fun, Retries])
        end),
        counted = emqx_bcast_storage:process_ack(PK, A, DeliveryId),
        %% The flush lands the marker first and the counter second, so wait on
        %% the counter: seeing the marker does not mean the decrement happened.
        ?assert(wait_until(fun() -> ack_counter(DeliveryId) =:= 1 end, 100))
    after
        meck:unload(mnesia)
    end,
    receive
        ack_flush_used_transaction -> ?assert(false)
    after 0 -> ok
    end,
    %% The flush still landed: marker plus a counter that only owes B.
    ?assert(mnesia:dirty_read(bcast_msg_acked, DeliveryId) =/= []),
    [#bcast_msg_meta_counter{counter = 1}] = mnesia:dirty_read(
        bcast_msg_meta_counter, DeliveryId
    ),
    counted = emqx_bcast_storage:process_ack(PK, B, DeliveryId),
    ?assert(
        wait_until(fun() -> mnesia:dirty_read(bcast_msg, DeliveryId) =:= [] end, 100)
    ).

in_index_shard() ->
    case erlang:process_info(self(), registered_name) of
        {registered_name, Name} ->
            lists:prefix("emqx_bcast_index_owner", atom_to_list(Name));
        _ ->
            false
    end.

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

-doc "The periodic cleanup fallback reclaims a delivery whose ack counter\n"
"reached zero but whose completion never deleted the rows (the counter is\n"
"deleted dirty and meta/rec transactionally).".
t_cleanup_completed_deliveries_fallback(_Config) ->
    PK = <<"PCMP">>,
    DN = <<"DCMP">>,
    {_ApiMsgId, MsgGuid} = create_test_msg(<<"completed fallback">>),
    DeliveryId = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(DeliveryId, MsgGuid, PK, <<"tpl">>, [DN], 1),
    ?assertEqual(1, emqx_bcast_storage:pending_delivery_count()),
    %% Simulate a completion whose meta/rec transaction aborted: the counter
    %% reached zero but the three rows were never deleted.
    ok = mnesia:dirty_write(#bcast_msg_meta_counter{delivery_id = DeliveryId, counter = 0}),
    ?assertNotEqual([], mnesia:dirty_read(bcast_msg, DeliveryId)),
    ?assertNotEqual([], mnesia:dirty_read(bcast_msg_meta, DeliveryId)),
    %% The cleanup tick's fallback must reclaim them.
    emqx_bcast_storage:cleanup_expired(),
    ?assertEqual([], mnesia:dirty_read(bcast_msg, DeliveryId)),
    ?assertEqual([], mnesia:dirty_read(bcast_msg_meta, DeliveryId)),
    ?assertEqual([], mnesia:dirty_read(bcast_msg_meta_counter, DeliveryId)).

-doc "An index rebuild must not resurrect a per-device index entry whose\n"
"ack was already counted: acked-device markers are persisted on the ack\n"
"flush path, so a rebuilt delivery only queues the devices still missing\n"
"their ack. Without the persistent markers the rebuild re-created entries\n"
"for every device in the bcast_msg row, a redelivered duplicate ack\n"
"decremented the completion counter a second time, the delivery completed\n"
"early, and the never-acked devices were stranded with no index entry.".
t_rebuild_skips_counted_acked_devices(_Config) ->
    PK = <<"PACKED">>,
    [DN1] = same_shard_dns(PK, 7, 1),
    [DN2] = same_shard_dns(PK, 8, 1),
    {_ApiMsgId, MsgGuid} = create_test_msg(<<"acked persist">>),
    DeliveryId = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(
        DeliveryId, MsgGuid, PK, <<"tpl">>, [DN1, DN2], 2
    ),
    ?assertEqual(2, emqx_bcast_storage:pending_delivery_count()),
    counted = emqx_bcast_storage:process_ack(PK, DN1, DeliveryId),
    %% The acked marker lands with the shard's ack flush tick.
    ?assert(wait_until(fun() -> acked_devices(DeliveryId) =:= [DN1] end, 100)),
    %% Rebuild the index (forced, as on an activation drive).
    ok = emqx_bcast_index_owner:rebuild_index(),
    %% DN1's entry is not resurrected; DN2's is still pending.
    {ok, []} = emqx_bcast_storage:get_device_delivery_entries({PK, DN1}),
    {ok, [{DeliveryId, stored}]} = emqx_bcast_storage:get_device_delivery_entries({PK, DN2}),
    %% The completion counter still tracks exactly DN2's missing ack.
    [#bcast_msg_meta_counter{counter = 1}] = mnesia:dirty_read(
        bcast_msg_meta_counter, DeliveryId
    ),
    %% DN2's ack still completes the delivery, clearing the acked markers.
    counted = emqx_bcast_storage:process_ack(PK, DN2, DeliveryId),
    ?assert(wait_until(fun() -> mnesia:dirty_read(bcast_msg, DeliveryId) =:= [] end, 100)),
    ?assertEqual([], mnesia:dirty_read(bcast_msg_acked, DeliveryId)).

-doc "Deliveries committed within the same second must rebuild in commit\n"
"order: created_at has second granularity, so the rebuild sort\n"
"tie-breaks on the millisecond-ordered msg_id prefix, or the rebuilt\n"
"per-device FIFO scrambles (a later delivery could be claimed first).".
t_rebuild_preserves_same_second_fifo_order(_Config) ->
    PK = <<"PFIFO">>,
    [DN] = same_shard_dns(PK, 9, 1),
    Now = emqx_bcast_utils:now_sec(),
    Dids = lists:map(
        fun(I) ->
            {_ApiMsgId, MsgGuid} = create_test_msg(
                <<"fifo ", (integer_to_binary(I))/binary>>
            ),
            DeliveryId = emqx_bcast_utils:gen_guid(),
            {ok, D} = emqx_bcast_storage:create_delivery(
                DeliveryId, MsgGuid, PK, <<"tpl">>, [DN], 1
            ),
            %% force the same-second collision deterministically
            mnesia:dirty_write(D#bcast_msg{created_at = Now}),
            DeliveryId
        end,
        lists:seq(1, 8)
    ),
    ok = emqx_bcast_index_owner:rebuild_index(),
    {ok, Entries} = emqx_bcast_storage:get_device_delivery_entries({PK, DN}),
    ?assertEqual([{Did, stored} || Did <- Dids], Entries).

-doc "A management delivery delete also removes the persisted acked-device\n"
"markers, so they cannot leak into a later delivery that would reuse the\n"
"table key space.".
t_delete_delivery_clears_acked_markers(_Config) ->
    PK = <<"PDELACK">>,
    [DN1] = same_shard_dns(PK, 7, 1),
    [DN2] = same_shard_dns(PK, 8, 1),
    {_ApiMsgId, MsgGuid} = create_test_msg(<<"acked delete">>),
    DeliveryId = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(
        DeliveryId, MsgGuid, PK, <<"tpl">>, [DN1, DN2], 2
    ),
    counted = emqx_bcast_storage:process_ack(PK, DN1, DeliveryId),
    ?assert(wait_until(fun() -> acked_devices(DeliveryId) =:= [DN1] end, 100)),
    ok = emqx_bcast_storage:delete_delivery(DeliveryId),
    ?assertEqual([], mnesia:dirty_read(bcast_msg_acked, DeliveryId)).

-doc "A crash inside promoter batch processing must be contained: the\n"
"intake take is atomic (dequeue-then-process), so an uncaught worker\n"
"crash would silently lose the whole dequeued batch and kill the linked\n"
"promoter gen_server with it. The guard turns a crash into a retry of\n"
"the same batch; once the fault clears the batch still promotes.".
t_promoter_retries_after_internal_crash(_Config) ->
    Promoter = whereis(emqx_bcast_promoter),
    true = is_pid(Promoter),
    MRef = monitor(process, Promoter),
    PK = <<"PPCRASH">>,
    DN = <<"DPCRASH">>,
    Now = emqx_bcast_utils:now_sec(),
    Payload = <<"crash-guard payload">>,
    {_ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    Entry = #{
        payload => Payload,
        hash => crypto:hash(sha256, Payload),
        api_msg_id => <<"crash-api">>,
        msg_id => MsgGuid,
        delivery_id => emqx_bcast_utils:gen_guid(),
        product_key => PK,
        topic_template => <<"tpl/crash">>,
        devices => [DN],
        created_at => Now,
        expires_at => Now + 3600
    },
    meck:new(emqx_bcast_index_owner, [passthrough, no_link]),
    %% Crash the first few appends (below the consecutive-failure budget),
    %% then let the real append through.
    Ctr = atomics:new(1, []),
    atomics:put(Ctr, 1, 6),
    meck:expect(emqx_bcast_index_owner, append_batch, fun(Entries) ->
        case atomics:sub_get(Ctr, 1, 1) > 0 of
            true -> error(injected_append_crash);
            false -> meck:passthrough([Entries])
        end
    end),
    try
        {ok, _Seq} = emqx_bcast_intake:enqueue(Entry),
        %% the same batch promotes once the fault clears (no silent loss)
        ?assert(
            wait_until(
                fun() ->
                    case catch emqx_bcast_index_owner:device_deliveries({PK, DN}) of
                        {ok, [_ | _]} -> true;
                        _ -> false
                    end
                end,
                5000
            )
        ),
        %% the promoter and its linked workers survived the crashes
        ?assertEqual(Promoter, whereis(emqx_bcast_promoter)),
        receive
            {'DOWN', MRef, process, Promoter, Reason} ->
                ct:fail({promoter_died, Reason})
        after 0 ->
            ok
        end
    after
        meck:unload(emqx_bcast_index_owner),
        demonitor(MRef, [flush])
    end,
    ok.

%% One intake entry for the promoter retry tests, built by hand so the test
%% does not depend on the API path.
retry_entry(PK, DN) ->
    Now = emqx_bcast_utils:now_sec(),
    Payload = <<"promoter-retry payload">>,
    #{
        payload => Payload,
        hash => crypto:hash(sha256, Payload),
        api_msg_id => emqx_bcast_utils:gen_guid(),
        msg_id => emqx_bcast_utils:gen_guid(),
        delivery_id => emqx_bcast_utils:gen_guid(),
        product_key => PK,
        topic_template => <<"tpl/retry">>,
        devices => [DN],
        created_at => Now,
        expires_at => Now + 3600
    }.

indexed_deliveries(PK, DN) ->
    case catch emqx_bcast_index_owner:device_deliveries({PK, DN}) of
        {ok, Deliveries} -> Deliveries;
        _ -> []
    end.

-doc "A batch whose index append keeps failing must not pin a promoter\n"
"worker: after the in-worker retry budget the batch goes back to the\n"
"intake queue with a backoff (bcast_promoter_append_deferred) and the\n"
"worker returns to the queue. The batch is retried until it succeeds -\n"
"a committed batch is never dropped, and the drain path keeps serving\n"
"other batches while it waits.".
t_append_failure_beyond_budget_defers_the_batch(_Config) ->
    PK = <<"PDEFER">>,
    DNFail = <<"DDEFER-FAIL">>,
    DNOk = <<"DDEFER-OK">>,
    DeferredBefore = metric(<<"batch_pub_qos1_deferred">>),
    Gate = atomics:new(1, []),
    atomics:put(Gate, 1, 1),
    meck:new(emqx_bcast_index_owner, [passthrough, no_link]),
    meck:expect(emqx_bcast_index_owner, append_batch, fun(Entries) ->
        case lists:keymember(DNFail, 2, Entries) andalso atomics:get(Gate, 1) =:= 1 of
            true -> {error, append_failed};
            false -> meck:passthrough([Entries])
        end
    end),
    try
        {ok, _Seq} = emqx_bcast_intake:enqueue(retry_entry(PK, DNFail)),
        %% The in-worker budget runs out and the batch is handed to the
        %% deferred queue instead of holding its worker forever.
        ?assert(wait_until(fun() -> emqx_bcast_intake:deferred_depth() >= 1 end, 100)),
        ?assert(metric(<<"batch_pub_qos1_deferred">>) > DeferredBefore),
        %% The drain path is not wedged: another batch is promoted and
        %% indexed while the failing one waits out its backoff.
        {ok, _Seq2} = emqx_bcast_intake:enqueue(retry_entry(PK, DNOk)),
        ?assert(wait_until(fun() -> indexed_deliveries(PK, DNOk) =/= [] end, 100)),
        ?assertEqual([], indexed_deliveries(PK, DNFail)),
        %% The fault clears: the deferred batch is swept back and appended,
        %% so nothing was lost.
        atomics:put(Gate, 1, 0),
        ?assert(wait_until(fun() -> indexed_deliveries(PK, DNFail) =/= [] end, 600)),
        ?assert(
            wait_until(fun() -> emqx_bcast_intake:deferred_depth() =:= 0 end, 100)
        )
    after
        meck:unload(emqx_bcast_index_owner)
    end,
    ok.

-doc "A batch handed back to the intake queue must not be takeable before\n"
"its backoff elapsed, must come back at the tail (fresh Seq), and must\n"
"back off further each time it is deferred.".
t_deferred_batch_waits_out_its_backoff(_Config) ->
    %% The policy itself: doubling from the base, capped (a permanently
    %% unreachable shard must not turn into a hot retry loop).
    ?assertEqual(100, emqx_bcast_intake:backoff_ms(1)),
    ?assertEqual(200, emqx_bcast_intake:backoff_ms(2)),
    ?assertEqual(400, emqx_bcast_intake:backoff_ms(3)),
    ?assertEqual(30000, emqx_bcast_intake:backoff_ms(30)),
    PK = <<"PBACKOFF">>,
    DN = <<"DBACKOFF">>,
    {ok, Seq0} = emqx_bcast_intake:enqueue(retry_entry(PK, DN)),
    [{Seq0, Entry}] = emqx_bcast_intake:take_batch(10, 1000),
    ?assertEqual(1, emqx_bcast_intake:requeue([Entry])),
    ?assertEqual(1, emqx_bcast_intake:deferred_depth()),
    ?assertEqual(0, emqx_bcast_intake:depth()),
    %% Out of the promoter's reach until the backoff elapses: not takeable,
    %% not swept, and the promoter's own tick cannot pull it forward either.
    ?assertEqual([], emqx_bcast_intake:take_batch(10, 1000)),
    ?assertEqual(0, emqx_bcast_intake:sweep_deferred()),
    timer:sleep(60),
    ?assertEqual([], emqx_bcast_intake:take_batch(10, 1000)),
    ?assertEqual(1, emqx_bcast_intake:deferred_depth()),
    %% Once it elapses the entry comes back (swept by the promoter's tick or
    %% by this call) and leaves the deferred table.
    ?assert(
        wait_until(
            fun() ->
                _ = emqx_bcast_intake:sweep_deferred(),
                emqx_bcast_intake:deferred_depth() =:= 0
            end,
            100
        )
    ),
    ok.

-doc "The delivery worker must commit only the fields it owns (claim,\n"
"inflight): committing a full row copy read earlier would clobber a\n"
"topics update that landed between the read and the write (subscription\n"
"cache lost-update, no self-heal until the next hook event). The guarded\n"
"update preserves the concurrently written fields.".
t_commit_preserves_concurrent_topics_update(_Config) ->
    PK = <<"PLU">>,
    DN = <<"DLU">>,
    _ = emqx_bcast:register_device(PK, DN, self()),
    {_ApiMsgId, MsgGuid} = create_test_msg(<<"lost update">>),
    DeliveryId = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(DeliveryId, MsgGuid, PK, <<"tpl/lu">>, [DN], 1),
    Shard = emqx_bcast_pull_shard:shard_of(PK, DN),
    Tab = emqx_bcast_pull_shard:tab(Shard, bcast_client_state),
    Now = emqx_bcast_utils:now_sec(),
    Tag = emqx_bcast_utils:gen_guid(),
    ets:insert(Tab, #bcast_client_state{
        key = {PK, DN},
        product_key = PK,
        clientid = DN,
        pid = self(),
        claim = {Tag, Now},
        inflight = [],
        topics = [{<<"old/filter">>, 1}]
    }),
    NewTopics = [{<<"new/filter">>, 1}],
    %% Land the topics update between the worker's row read and its write:
    %% the interposer runs inside prepare_and_commit (after the lookup,
    %% before the commit) exactly where the race window lives.
    meck:new(emqx_bcast, [passthrough, no_link]),
    meck:expect(emqx_bcast, lookup_device, fun(Key) ->
        [R] = ets:lookup(Tab, {PK, DN}),
        ets:insert(Tab, R#bcast_client_state{topics = NewTopics}),
        meck:passthrough([Key])
    end),
    ClaimMap = #{
        delivery_id => DeliveryId,
        msg_id => MsgGuid,
        product_key => PK,
        topic_template => <<"tpl/lu">>,
        claim_tag => Tag,
        sub_qos => 0,
        attempt => 1
    },
    try
        ok = emqx_bcast_pull_shard:do_commit_deliveries(
            Shard, [{DN, {ok, [ClaimMap]}, {DN, Tag, PK}}]
        )
    after
        meck:unload(emqx_bcast)
    end,
    [Row] = ets:lookup(Tab, {PK, DN}),
    ?assertEqual(NewTopics, Row#bcast_client_state.topics),
    ?assertEqual(undefined, Row#bcast_client_state.claim),
    cleanup_row(PK, DN).

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
    %% into the client's emqx_bcast_ack_shard, so sys:get_state guarantees it was processed
    ok = emqx_bcast:on_message_acked(#{clientid => DN}, Msg),
    _ = sys:get_state(
        emqx_bcast_ack_shard:shard_name(emqx_bcast_ack_shard:shard_of(PK, DN))
    ),
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
t_pull_shard_buffers_initialized(_Config) ->
    %% One per-client state table per pull shard partition, created at
    %% shard init (the former unacked/stage/claim-inflight tables are
    %% merged into bcast_client_state).
    ?assertNotEqual(undefined, ets:info(emqx_bcast_pull_shard:tab(0, bcast_client_state))).

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
        %% Well-formed but unknown: the shape has to be a UUID for this case,
        %% otherwise it is an input error (see the case below).
        <<"MessageId">> => <<"550e8400-e29b-41d4-a716-446655440000">>
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"MessageNotFound">>, maps:get(<<"Code">>, Resp)).

-doc "A MessageId that is not a UUID cannot name a stored message, so it is\n"
"rejected as input (400 InvalidMessageId) instead of being reported as an\n"
"unknown one, which the caller cannot act on.".
t_register_message_invalid_message_id(_Config) ->
    Body = #{
        <<"Action">> => <<"RegisterMessage">>,
        <<"MessageId">> => <<"nonexistent-uuid">>
    },
    {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], #{body => Body}),
    ?assertEqual(<<"InvalidMessageId">>, maps:get(<<"Code">>, Resp)).

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

-doc "BatchPub rejects a TopicTemplateName that is empty, carries wildcards or\n"
"carries unknown placeholders: an empty template is not 'use the configured\n"
"default' (that is an absent field) but an explicit empty MQTT topic, which\n"
"would be accepted and never deliverable.".
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
        [<<>>, <<"/a/+/b">>, <<"/a/#/b">>, <<"/a/${unknown}/b">>, 123]
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

-doc "RegisterMessage with a non-binary MessageId is an input error (400\n"
"InvalidMessageId): no value of the wrong type can name a stored message.".
t_register_message_id_wrong_type(_Config) ->
    Body = #{
        <<"Action">> => <<"RegisterMessage">>,
        <<"MessageId">> => 123
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"InvalidMessageId">>, maps:get(<<"Code">>, Resp)).

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

-doc "PubBroadcast without content is an input error with its own code (400\n"
"MessageContentRequired): a broadcast has no MessageId to fall back on, and\n"
"InvalidBase64 is reserved for a payload that failed to decode.".
t_broadcast_missing_content(_Config) ->
    Body = #{
        <<"Action">> => <<"PubBroadcast">>,
        <<"ProductKey">> => <<"P1">>
    },
    Request = #{body => Body},
    {ok, 400, _, Resp} = emqx_bcast_api:handle(post, [<<"pub">>], Request),
    ?assertEqual(<<"MessageContentRequired">>, maps:get(<<"Code">>, Resp)).

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
    seed_window(PK, DN, DeliveryId, false),
    Before = metric(<<"batch_pub_qos1_acked">>),
    ok = emqx_bcast:on_message_acked(#{clientid => DN}, Msg),
    ?assert(wait_metric(<<"batch_pub_qos1_acked">>, Before + 1)),
    ok = emqx_bcast:on_message_acked(#{clientid => DN}, Msg),
    _ = sys:get_state(
        emqx_bcast_ack_shard:shard_name(emqx_bcast_ack_shard:shard_of(PK, DN))
    ),
    _ = sys:get_state(emqx_bcast_pull_shard:shard_name(emqx_bcast_pull_shard:shard_of(PK, DN))),
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
    seed_window(PK, DN, DeliveryId, false),
    Before = metric(<<"batch_pub_qos1_acked">>),
    %% generation 1: first PUBLISH acked (counted once, buffer consumed).
    ok = emqx_bcast:on_message_acked(#{clientid => DN}, Msg),
    ?assert(wait_metric(<<"batch_pub_qos1_acked">>, Before + 1)),
    %% redelivery generation (reconnect re-claim): window re-created for the
    %% same delivery id.
    seed_window(PK, DN, DeliveryId, false),
    %% the old generation's late duplicate PUBACK would once have matched the
    %% new buffer and counted twice; with core-applied confirmation counting
    %% (ack_in_flight marker + ack_applied), the logical delivery is counted
    %% exactly once and the late duplicate is ignored.
    ok = emqx_bcast:on_message_acked(#{clientid => DN}, Msg),
    _ = sys:get_state(
        emqx_bcast_ack_shard:shard_name(emqx_bcast_ack_shard:shard_of(PK, DN))
    ),
    _ = sys:get_state(emqx_bcast_pull_shard:shard_name(emqx_bcast_pull_shard:shard_of(PK, DN))),
    ?assert(wait_metric(<<"batch_pub_qos1_acked">>, Before + 1)),
    timer:sleep(50),
    ?assertEqual(Before + 1, metric(<<"batch_pub_qos1_acked">>)).

%%--- pull state helpers (single per-client state row per shard) ---
acked_devices(Did) ->
    lists:sort(
        lists:append([
            DNs
         || #bcast_msg_acked{device_names = DNs} <- mnesia:dirty_read(bcast_msg_acked, Did)
        ])
    ).

%% Remaining-ack counter of a delivery, or undefined when its row is gone.
%% The ack flush writes the marker before the counter, so tests must wait on
%% the counter to know the flush finished.
ack_counter(Did) ->
    case mnesia:dirty_read(bcast_msg_meta_counter, Did) of
        [#bcast_msg_meta_counter{counter = N}] -> N;
        [] -> undefined
    end.

state_tab(PK, DN) ->
    emqx_bcast_pull_shard:tab(emqx_bcast_pull_shard:shard_of(PK, DN), bcast_client_state).

%% The filters cached on a client row (empty when there is no row).
row_topics_of(PK, DN) ->
    case catch ets:lookup(state_tab(PK, DN), {PK, DN}) of
        [Row] -> element(8, Row);
        _ -> []
    end.

seed_window(PK, DN, DeliveryId, true) ->
    seed_window(PK, DN, DeliveryId, {true, erlang:system_time(millisecond)});
seed_window(PK, DN, DeliveryId, AckInFlight) ->
    Row = #bcast_client_state{
        key = {PK, DN},
        product_key = PK,
        clientid = DN,
        pid = self(),
        inflight = [{DeliveryId, AckInFlight}]
    },
    ets:insert(state_tab(PK, DN), Row),
    ok.

seed_claim_row(PK, DN, Tag, Ts, Shard) ->
    Row = #bcast_client_state{
        key = {PK, DN},
        product_key = PK,
        clientid = DN,
        pid = self(),
        claim = {Tag, Ts}
    },
    ets:insert(emqx_bcast_pull_shard:tab(Shard, bcast_client_state), Row),
    ok.

row_lookup(PK, DN) ->
    case ets:lookup(state_tab(PK, DN), {PK, DN}) of
        [Row] -> Row;
        [] -> undefined
    end.

row_claim_of(PK, DN) ->
    case row_lookup(PK, DN) of
        #bcast_client_state{claim = C} -> C;
        undefined -> undefined
    end.

%% Deferred-claim mark of a client row: an integer while the sweep still owes
%% the client a retry, undefined once it retried (or was never marked).
rearm_at_of(PK, DN) ->
    case row_lookup(PK, DN) of
        #bcast_client_state{rearm_at = {T, _Origin}} -> T;
        #bcast_client_state{rearm_at = T} -> T;
        undefined -> undefined
    end.

row_window_empty(PK, DN) ->
    case row_lookup(PK, DN) of
        #bcast_client_state{inflight = []} -> true;
        _ -> false
    end.

cleanup_row(PK, DN) ->
    catch ets:delete(state_tab(PK, DN), {PK, DN}),
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

storm_sample(_Stats, 0) ->
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
    seed_window(PK, DN, DeliveryId, false),
    Before = metric(<<"batch_pub_qos1_acked">>),
    ok = emqx_bcast:on_message_acked(#{clientid => DN}, Msg),
    %% core-applied confirmation arrives asynchronously and counts acked
    %% exactly once (the window entry lives in the pull shard only for the
    %% brief ack-in-flight window; on a single node it is set and cleared
    %% faster than a poll can observe, so we assert the observable
    %% contract)
    ?assert(wait_metric(<<"batch_pub_qos1_acked">>, Before + 1)),
    %% a duplicate PUBACK (window entry already consumed) must not count again
    ok = emqx_bcast:on_message_acked(#{clientid => DN}, Msg),
    timer:sleep(100),
    ?assertEqual(Before + 1, metric(<<"batch_pub_qos1_acked">>)),
    %% no residual window entry after the confirmation
    ?assert(wait_until(fun() -> row_window_empty(PK, DN) end, 100)).

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
    [{DN, {ok, [_]}}] = emqx_bcast_storage:claim_want_next_batch([
        #{residual => true, clientid => DN, product_key => PK, topics => [{<<"tpl">>, 1}]}
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
    [{DN, {ok, [M]}}] = emqx_bcast_storage:claim_want_next_batch([
        #{residual => true, clientid => DN, product_key => PK, topics => [{<<"tpl">>, 1}]}
    ]),
    ?assertEqual(DeliveryId, maps:get(delivery_id, M)).

-doc "A subscription whose options the caller did not provide is not a QoS 0\n"
"subscription. EMQX records a subscription in two steps (the subscriber's topic\n"
"list first, the options second), so a read can see a topic with empty options;\n"
"recording that as QoS 0 sends the device's QoS=1 deliveries out as QoS 0, with\n"
"no PUBACK and no retransmission. The cached filter must keep what it knew.".
t_session_subscribed_without_qos_is_not_qos0(_Config) ->
    PK = <<"PUNKQOS">>,
    DN = <<"DUNKQOS">>,
    Filter = <<"/PUNKQOS/DUNKQOS/user/get">>,
    ClientInfo = #{clientid => DN, client_attrs => #{<<"tns">> => PK}},
    _ = emqx_bcast:register_device(PK, DN, self()),
    %% The row as a QoS=1 subscription leaves it.
    ets:insert(state_tab(PK, DN), #bcast_client_state{
        key = {PK, DN},
        product_key = PK,
        clientid = DN,
        pid = self(),
        topics = [{Filter, 1}]
    }),
    ok = emqx_bcast:on_session_subscribed(ClientInfo, Filter, #{}),
    ok = emqx_bcast:on_session_subscribed(ClientInfo, Filter, #{nl => 0}),
    _ = sys:get_state(emqx_bcast_pull_shard:shard_name(emqx_bcast_pull_shard:shard_of(PK, DN))),
    ?assertEqual([{Filter, 1}], row_topics_of(PK, DN)),
    %% A well-formed subscribe still updates the cache.
    ok = emqx_bcast:on_session_subscribed(ClientInfo, Filter, #{qos => 0}),
    _ = sys:get_state(emqx_bcast_pull_shard:shard_name(emqx_bcast_pull_shard:shard_of(PK, DN))),
    ?assertEqual([{Filter, 0}], row_topics_of(PK, DN)),
    cleanup_row(PK, DN),
    emqx_bcast:unregister_device(PK, DN, self()).

-doc "A resumed session re-syncs its cached filters from EMQX's tables. A read\n"
"that catches the broker between its two writes (the topic is listed, the\n"
"options are not) must not drop the client's known-good filters: the last QoS\n"
"the plugin knew is the best answer, and an empty cache would stall the device's\n"
"backlog until its next subscribe.".
t_resume_resync_keeps_filters_when_qos_is_unknown(_Config) ->
    PK = <<"PRESYNC">>,
    DN = <<"DRESYNC">>,
    Filter = <<"/PRESYNC/DRESYNC/user/get">>,
    Name = emqx_bcast_pull_shard:shard_name(emqx_bcast_pull_shard:shard_of(PK, DN)),
    _ = emqx_bcast:register_device(PK, DN, self()),
    ets:insert(state_tab(PK, DN), #bcast_client_state{
        key = {PK, DN},
        product_key = PK,
        clientid = DN,
        pid = self(),
        topics = [{Filter, 1}]
    }),
    meck:new(emqx_broker, [passthrough, no_link]),
    try
        %% The tearing shape: the topic is there, its options are not.
        meck:expect(emqx_broker, subscriptions, fun(_Pid) -> [{Filter, #{}}] end),
        gen_server:cast(Name, {resume, DN, self(), PK}),
        _ = sys:get_state(Name),
        ?assertEqual([{Filter, 1}], row_topics_of(PK, DN)),
        %% When the options are visible again the resync takes over.
        meck:expect(emqx_broker, subscriptions, fun(_Pid) -> [{Filter, #{qos => 0}}] end),
        gen_server:cast(Name, {resume, DN, self(), PK}),
        _ = sys:get_state(Name),
        ?assertEqual([{Filter, 0}], row_topics_of(PK, DN))
    after
        meck:unload(emqx_broker)
    end,
    cleanup_row(PK, DN),
    emqx_bcast:unregister_device(PK, DN, self()).

-doc "While an ack is in flight (ack-in-flight marker set), a subscribe\n"
"trigger for that client must not stage a want_next (pull returns no_more).".
t_pull_ack_in_flight_gates_subscribe_trigger(_Config) ->
    PK = <<"PGATE_S">>,
    DN = <<"DGATE_S">>,
    seed_window(PK, DN, emqx_bcast_utils:gen_guid(), true),
    %% A matching filter now exists (production path: session.subscribed), so
    %% only the ack-in-flight gate can keep the round from being opened.
    emqx_bcast_pull_shard:cast_client(PK, DN, {topic_added, DN, self(), PK, <<"#">>, 1}),
    timer:sleep(100),
    %% nothing claimed while the window is full (window=1) and the
    %% delivery is ack-in-flight
    ?assertEqual(undefined, row_claim_of(PK, DN)),
    cleanup_row(PK, DN).

-doc "A stale client_disconnected cast (old channel pid) must not clear the\n"
"state row of a client that already reconnected with a new pid (takeover\n"
"race): the row's claim round and window belong to the new session, and\n"
"clearing them under the old pid's event would lose exactly-once ack\n"
"accounting and claim bookkeeping.".
t_cleanup_client_ignores_stale_pid(_Config) ->
    PK = <<"PSTALEPID">>,
    DN = <<"DSTALEPID">>,
    Shard = emqx_bcast_pull_shard:shard_of(PK, DN),
    Name = emqx_bcast_pull_shard:shard_name(Shard),
    OldPid = spawn(fun() ->
        receive
            stop -> ok
        end
    end),
    NewPid = spawn(fun() ->
        receive
            stop -> ok
        end
    end),
    Tag = 424242,
    seed_claim_row(PK, DN, Tag, erlang:system_time(millisecond), Shard),
    %% The row now belongs to the new channel pid (reconnect/takeover).
    Tab = emqx_bcast_pull_shard:tab(Shard, bcast_client_state),
    [Row0] = ets:lookup(Tab, {PK, DN}),
    ets:insert(Tab, Row0#bcast_client_state{pid = NewPid}),
    %% A stale disconnect event for the OLD pid arrives late: the row must
    %% survive with its claim round and pid untouched.
    gen_server:cast(Name, {client_disconnected, DN, OldPid, PK}),
    _ = sys:get_state(Name),
    ?assertMatch(#bcast_client_state{claim = {Tag, _}, pid = NewPid}, row_lookup(PK, DN)),
    %% A disconnect for the CURRENT pid still cleans the row up.
    gen_server:cast(Name, {client_disconnected, DN, NewPid, PK}),
    _ = sys:get_state(Name),
    ?assertEqual(undefined, row_lookup(PK, DN)),
    exit(OldPid, kill),
    exit(NewPid, kill).

-doc "An ack-in-flight marker whose core-applied confirmation never\n"
"arrives (dropped cast, core crash between the ack application and the\n"
"confirmation) must not hold the window closed forever: the periodic\n"
"sweep expires markers older than the TTL, releases the core claim and\n"
"re-claims the client so the delivery is redriven (at-least-once).\n"
"A fresh marker is not expired.".
t_pull_ack_in_flight_ttl_reclaims_window(_Config) ->
    PK = <<"PACKTTL">>,
    DN = <<"DACKTTL">>,
    Shard = emqx_bcast_pull_shard:shard_of(PK, DN),
    Tab = emqx_bcast_pull_shard:tab(Shard, bcast_client_state),
    Name = emqx_bcast_pull_shard:shard_name(Shard),
    Did = emqx_bcast_utils:gen_guid(),
    Old = erlang:system_time(millisecond) - 60_000,
    Row = #bcast_client_state{
        key = {PK, DN},
        product_key = PK,
        clientid = DN,
        pid = self(),
        inflight = [{Did, {true, Old}}]
    },
    ets:insert(Tab, Row),
    %% trigger the sweep directly instead of waiting for the periodic tick
    _ = whereis(Name) ! sweep_stale_claims,
    _ = sys:get_state(Name),
    ?assertEqual([], (row_lookup(PK, DN))#bcast_client_state.inflight),
    %% a fresh ack-in-flight marker survives the sweep
    Fresh = {emqx_bcast_utils:gen_guid(), {true, erlang:system_time(millisecond)}},
    ets:insert(Tab, Row#bcast_client_state{inflight = [Fresh]}),
    _ = whereis(Name) ! sweep_stale_claims,
    _ = sys:get_state(Name),
    ?assertEqual([Fresh], (row_lookup(PK, DN))#bcast_client_state.inflight),
    cleanup_row(PK, DN).

-doc "While an ack is in flight, a ping keepalive trigger must also be\n"
"suppressed (no want_next claimed).".
t_pull_ack_in_flight_gates_ping_trigger(_Config) ->
    PK = <<"PGATE_P">>,
    DN = <<"DGATE_P">>,
    seed_window(PK, DN, emqx_bcast_utils:gen_guid(), true),
    emqx_bcast_pull_shard:cast_client(PK, DN, {ping, DN, self(), PK}),
    timer:sleep(100),
    ?assertEqual(undefined, row_claim_of(PK, DN)),
    cleanup_row(PK, DN).

-doc "A keepalive ping only refreshes the device registry: it must not\n"
"claim a want_next for an idle client (regression: at 800k online devices\n"
"every PINGREQ used to drive an empty-queue claim probe cycle that grew\n"
"the pull shard mailboxes with zero deliveries in flight).".
t_ping_does_not_claim_idle_client(_Config) ->
    PK = <<"PPING_N">>,
    DN = <<"DPING_N">>,
    Shard = emqx_bcast_pull_shard:shard_of(PK, DN),
    Cnt = emqx_bcast_pull_shard:tab(Shard, bcast_pull_counters),
    Claim0 = ets:lookup_element(Cnt, claim, 2),
    emqx_bcast_pull_shard:cast_client(PK, DN, {ping, DN, self(), PK}),
    timer:sleep(100),
    ?assertEqual(Claim0, ets:lookup_element(Cnt, claim, 2)),
    %% no state row is created by a bare keepalive ping
    ?assertEqual([], ets:lookup(state_tab(PK, DN), {PK, DN})),
    emqx_bcast:unregister_device(PK, DN, self()).

-doc "A claim refused by the per-shard round cap must not become a lost\n"
"wakeup: the promoter trigger is one-shot and the stale-claim sweep only\n"
"revisits rows that still hold a claim, so a refused round that is dropped\n"
"leaves the client idle (claim = undefined, empty window) with its backlog\n"
"stranded until TTL. The refused client is marked on its own row instead, so\n"
"the mark cannot be dropped when the backpressure episode is large, and the\n"
"periodic sweep retries it.".
t_pull_claim_refused_at_cap_is_retried_by_sweep(_Config) ->
    PK = <<"PCAPRETRY">>,
    DN = <<"DCAPRETRY">>,
    Shard = emqx_bcast_pull_shard:shard_of(PK, DN),
    Cnt = emqx_bcast_pull_shard:tab(Shard, bcast_pull_counters),
    %% The pid the retry has to survive: a sweep that dies on the mark drops
    %% the shard's whole row table, which clears the mark as well, so the
    %% assertion below would pass for the wrong reason without this.
    PrePid = whereis(emqx_bcast_pull_shard:shard_name(Shard)),
    _ = emqx_bcast:register_device(PK, DN, self()),
    %% Saturate the cap so the subscribe claim has to be refused.
    true = ets:insert(Cnt, {claim, 2000}),
    emqx_bcast_pull_shard:cast_client(PK, DN, {topic_added, DN, self(), PK, <<"#">>, 1}),
    _ = sys:get_state(emqx_bcast_pull_shard:shard_name(Shard)),
    %% Refused, but remembered on the row itself: no round was opened, and the
    %% client carries the deferred-claim mark the sweep will revisit. The row
    %% is created here on purpose - a refused round writes none, so a mark in a
    %% separate table would be the only trace of the client, and that table
    %% could fill up and drop it.
    ?assert(is_integer(rearm_at_of(PK, DN))),
    ?assertEqual(undefined, row_claim_of(PK, DN)),
    ?assert(row_window_empty(PK, DN)),
    %% Headroom returns; the next sweep retries the marked claim, which opens a
    %% round and clears the mark. With the refusal dropped instead, the mark
    %% would never appear and the client would stay idle until TTL.
    true = ets:insert(Cnt, {claim, 0}),
    ?assert(wait_until(fun() -> rearm_at_of(PK, DN) =:= undefined end, 240)),
    _ = sys:get_state(emqx_bcast_pull_shard:shard_name(Shard)),
    ?assertEqual(PrePid, whereis(emqx_bcast_pull_shard:shard_name(Shard))),
    cleanup_row(PK, DN),
    emqx_bcast:unregister_device(PK, DN, self()).

-doc "The pre-commit subscribe hook arms the client before its filter is\n"
"committed, so a claim round that reaches the core inside that window sees no\n"
"matching subscription and comes back no_more, clearing the round. The\n"
"post-commit topic_added hook is the first point where the filter is visible,\n"
"so it must re-arm an idle client - otherwise that client's queued backlog\n"
"waits for TTL.".
t_pull_topic_added_rearms_idle_client(_Config) ->
    PK = <<"PTOPICARM">>,
    DN = <<"DTOPICARM">>,
    Shard = emqx_bcast_pull_shard:shard_of(PK, DN),
    _ = emqx_bcast:register_device(PK, DN, self()),
    %% The stranded state a no_more round leaves behind: a row with an empty
    %% window, no claim round and no cached filter.
    ets:insert(state_tab(PK, DN), #bcast_client_state{
        key = {PK, DN},
        product_key = PK,
        clientid = DN,
        pid = self(),
        topics = []
    }),
    emqx_bcast_pull_shard:cast_client(PK, DN, {topic_added, DN, self(), PK, <<"tpl">>, 1}),
    _ = sys:get_state(emqx_bcast_pull_shard:shard_name(Shard)),
    %% The round is opened again now that the filter is cached.
    ?assertNotEqual(undefined, row_claim_of(PK, DN)),
    cleanup_row(PK, DN),
    emqx_bcast:unregister_device(PK, DN, self()).

-doc "A claim round answered no_more must re-arm the client when the core\n"
"reports that entries are still waiting: no_more is not only the drained\n"
"answer, it is also what the core says while the entries are there and\n"
"merely not claimable yet (subscription not visible to the core when the\n"
"round ran, replication lag, a blocked head cycling). A drained answer\n"
"(residual 0) must not queue anything, or every device that finishes its\n"
"backlog would add a pointless retry.".
t_pull_no_more_residual_rearms_only_when_entries_remain(_Config) ->
    PK = <<"PNOMORER">>,
    DN1 = <<"DNOMORER1">>,
    DN2 = <<"DNOMORER2">>,
    Shard = emqx_bcast_pull_shard:shard_of(PK, DN1),
    Name = emqx_bcast_pull_shard:shard_name(Shard),
    %% Residual > 0: the round is cleared and the client row marked for the
    %% bounded sweep retry.
    Tag1 = 424242,
    seed_claim_row(PK, DN1, Tag1, erlang:system_time(millisecond), Shard),
    gen_server:cast(Name, {deliver_results, [{DN1, {no_more, 1}}], [{DN1, Tag1, PK}]}),
    _ = sys:get_state(Name),
    ?assertEqual(undefined, row_claim_of(PK, DN1)),
    ?assert(is_integer(rearm_at_of(PK, DN1))),
    %% Residual 0: the drained answer, which must not mark the client.
    Tag2 = 424243,
    seed_claim_row(PK, DN2, Tag2, erlang:system_time(millisecond), Shard),
    gen_server:cast(Name, {deliver_results, [{DN2, {no_more, 0}}], [{DN2, Tag2, PK}]}),
    _ = sys:get_state(Name),
    ?assertEqual(undefined, row_claim_of(PK, DN2)),
    ?assertEqual(undefined, rearm_at_of(PK, DN2)),
    cleanup_row(PK, DN1),
    cleanup_row(PK, DN2).

-doc "The periodic sweep must survive the deferred-claim mark it is draining:\n"
"the deferred round is refused or refused-empty on the ordinary paths, so this\n"
"mark is what a backlog relies on after a lost wakeup, and the sweep is the\n"
"only thing that revisits it. A sweep that raises on the mark kills the shard\n"
"and drops every row it holds - the client state, its window and the mark - so\n"
"the retry must leave the same shard process running.".
t_deferred_claim_sweep_survives_the_mark_it_drains(_Config) ->
    PK = <<"PSWEEPR">>,
    DN = <<"DSWEEPR">>,
    Shard = emqx_bcast_pull_shard:shard_of(PK, DN),
    Name = emqx_bcast_pull_shard:shard_name(Shard),
    PrePid = whereis(Name),
    %% A round answered no_more with a residual: entries are still queued at the
    %% core but were not claimable when the round ran.
    Tag = 424244,
    seed_claim_row(PK, DN, Tag, erlang:system_time(millisecond), Shard),
    gen_server:cast(Name, {deliver_results, [{DN, {no_more, 1}}], [{DN, Tag, PK}]}),
    _ = sys:get_state(Name),
    ?assertEqual(undefined, row_claim_of(PK, DN)),
    ?assert(is_integer(rearm_at_of(PK, DN))),
    %% The sweep retries the marked client and clears the mark. The device is
    %% not registered here, so reclaim_online/4 drops it without core traffic;
    %% the mark is cleared either way, and only a shard that stayed up can
    %% still hold the row afterwards.
    ?assert(wait_until(fun() -> rearm_at_of(PK, DN) =:= undefined end, 240)),
    _ = sys:get_state(Name),
    ?assertEqual(PrePid, whereis(Name)),
    cleanup_row(PK, DN).

-doc "The periodic sweep guards only error:badarg, so anything else it can run\n"
"into - a term in the row table that is not a client state, a bug in the\n"
"deferred-claim retry it ends with - takes the shard down together with the\n"
"client state of its whole partition, and every mark in it. The sweep has to\n"
"report the failure and leave its shard serving: the state it already applied\n"
"stays, and the next pass (with the bad row gone) drains what this one could\n"
"not.".
t_sweep_survives_an_unexpected_error(_Config) ->
    PK = <<"PSWEEPGUARD">>,
    DN = <<"DSWEEPGUARD">>,
    Shard = emqx_bcast_pull_shard:shard_of(PK, DN),
    Name = emqx_bcast_pull_shard:shard_name(Shard),
    Tab = emqx_bcast_pull_shard:tab(Shard, bcast_client_state),
    PrePid = whereis(Name),
    %% The fold matches client states, so any other term in this table reaches
    %% the sweep as a function_clause. Keyed apart from the client below, so it
    %% is only the fold that trips over it.
    BogusKey = {<<"PSWEEPGUARDBOGUS">>, <<"DSWEEPGUARDBOGUS">>},
    true = ets:insert(Tab, {bcast_client_state, BogusKey}),
    %% A deferred-claim mark for the sweep to drain once the bad row is gone.
    Tag = 424245,
    seed_claim_row(PK, DN, Tag, erlang:system_time(millisecond), Shard),
    gen_server:cast(Name, {deliver_results, [{DN, {no_more, 1}}], [{DN, Tag, PK}]}),
    _ = sys:get_state(Name),
    ?assertEqual(undefined, row_claim_of(PK, DN)),
    ?assert(is_integer(rearm_at_of(PK, DN))),
    Reports = emqx_cth_log_capture:capture(fun() ->
        Name ! sweep_stale_claims,
        %% Barrier: the sweep has finished when the shard answers this.
        _ = catch sys:get_state(Name)
    end),
    ?assertEqual(PrePid, whereis(Name)),
    ?assert(
        lists:any(
            fun(R) -> maps:get(msg, R, undefined) =:= "bcast_sweep_stale_marks_failed" end,
            Reports
        )
    ),
    %% Alive is not enough: with the bad row gone the mark this pass could not
    %% drain must still be drained by a later one, which only a working sweep
    %% and an intact row table can do.
    true = ets:delete(Tab, BogusKey),
    ?assert(wait_until(fun() -> rearm_at_of(PK, DN) =:= undefined end, 240)),
    _ = sys:get_state(Name),
    ?assertEqual(PrePid, whereis(Name)),
    cleanup_row(PK, DN).

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
    %% The send path only fires for the channel that currently holds the client
    %% (see t_qos0_auto_ack_skips_a_stale_channel); this case is about the
    %% counters, so make this process the holder.
    ok = meck:new(emqx_cm, [passthrough, no_link]),
    try
        ok = meck:expect(emqx_cm, lookup_channels, fun(_ClientId) -> [self()] end),
        ok = emqx_bcast_pull_shard:do_deliver_qos0_and_ack(
            DN, self(), <<"tpl">>, <<"auto payload">>, Did, PK, 1
        )
    after
        meck:unload(emqx_cm)
    end,
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
    seed_window(PK, DN, DeliveryId, false),
    Before = metric(<<"batch_pub_qos1_acked">>),
    ok = emqx_bcast:on_message_acked(#{clientid => DN}, Msg),
    ?assert(wait_metric(<<"batch_pub_qos1_acked">>, Before + 1)),
    %% a later copy (would-be redelivery) is acknowledged: the window was
    %% re-seeded for the test, but the core entry is gone, so no
    %% confirmation arrives and acked stays at one
    seed_window(PK, DN, DeliveryId, false),
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
    [{DN, {ok, [M1]}}] = emqx_bcast_storage:claim_want_next_batch([
        #{residual => true, clientid => DN, product_key => PK, topics => [{<<"tpl">>, 1}]}
    ]),
    ?assertEqual(1, maps:get(attempt, M1)),
    expire_inflight(PK, DN, DeliveryId),
    [{DN, {ok, [M2]}}] = emqx_bcast_storage:claim_want_next_batch([
        #{residual => true, clientid => DN, product_key => PK, topics => [{<<"tpl">>, 1}]}
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
    [{DN, {ok, [_]}}] = emqx_bcast_storage:claim_want_next_batch([
        #{residual => true, clientid => DN, product_key => PK, topics => [{<<"tpl">>, 1}]}
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

-doc "Management reads must not scan a whole table per request: the message\n"
"detail GET counts one message's deliveries, and the message list orders all\n"
"messages. Both grow with usage and, left as scans, eventually outlive the API\n"
"budget and answer 503. The count reads through a secondary index on the\n"
"delivery table's msg_id, and the list pages through an ordered index of\n"
"{created_at, msg_id} that follows the message rows.".
t_management_reads_use_indexes(_Config) ->
    {ApiMsgId, MsgGuid} = create_test_msg(<<"indexed management read">>),
    DeliveryId = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(
        DeliveryId, MsgGuid, <<"PIDX">>, <<"tpl">>, [<<"DIDX">>], 1
    ),
    %% One message's deliveries are counted through the index, not by walking
    %% the whole delivery table.
    ?assert(lists:member(3, mnesia:table_info(bcast_msg, index))),
    ?assertEqual(
        [DeliveryId],
        [
            D#bcast_msg.delivery_id
         || D <- mnesia:dirty_index_read(bcast_msg, MsgGuid, #bcast_msg.msg_id)
        ]
    ),
    {ok, _Msg, 1} = emqx_bcast_storage:get_message_by_api_id(ApiMsgId),
    %% The list is ordered by an index kept in step with the message rows: it
    %% is written in the same transaction that creates a message ...
    {ok, Stored} = emqx_bcast_storage:lookup_message(MsgGuid),
    CreatedAt = Stored#bcast_message.created_at,
    ?assertEqual(
        [#bcast_message_order{key = {CreatedAt, MsgGuid}}],
        mnesia:dirty_read(bcast_message_order, {CreatedAt, MsgGuid})
    ),
    %% ... and removed with it.
    ok = emqx_bcast_index_owner:delete_message(ApiMsgId),
    ?assertEqual([], mnesia:dirty_read(bcast_message_order, {CreatedAt, MsgGuid})).

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

-doc "An index shard that crashes and is restarted by the supervisor must\n"
"not stay dormant: it asks the activation leader for a targeted rebuild of\n"
"its own partition and resumes serving claims, while sibling partitions and\n"
"the global pending quota are untouched (the quota row never stopped\n"
"counting the crashed shard's entries, so the rebuild must not re-count).".
t_shard_crash_reactivates_partition(_Config) ->
    PK = <<"PSELFIX">>,
    Target = 1,
    [DN1] = same_shard_dns(PK, Target, 1),
    %% A sibling partition whose live heap state must survive the restart.
    Sibling = 2,
    [DN2] = same_shard_dns(PK, Sibling, 1),
    {_ApiMsgId, MsgGuid} = create_test_msg(<<"shard self heal">>),
    Did = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(Did, MsgGuid, PK, <<"tpl">>, [DN1, DN2], 2),
    true = wait_until(fun() -> shard_active(Target) end, 100),
    ?assertEqual(2, emqx_bcast_storage:pending_delivery_count()),
    %% Crash the shard; the supervisor restarts it with an empty heap.
    Name = list_to_atom("emqx_bcast_index_owner_" ++ integer_to_list(Target)),
    Pid0 = whereis(Name),
    exit(Pid0, kill),
    ?assert(
        wait_until(fun() -> whereis(Name) =/= undefined andalso whereis(Name) =/= Pid0 end, 100)
    ),
    %% The restarted shard re-activates its partition from the durable log
    %% and serves claims again.
    ?assert(
        wait_until(
            fun() ->
                case
                    emqx_bcast_storage:claim_want_next_batch([
                        #{
                            residual => true,
                            clientid => DN1,
                            product_key => PK,
                            topics => [{<<"tpl">>, 1}]
                        }
                    ])
                of
                    [{DN1, {ok, [_]}}] -> true;
                    _ -> false
                end
            end,
            200
        )
    ),
    %% The sibling partition was not reset: its entry is still claimable.
    [{DN2, {ok, [M2]}}] = emqx_bcast_storage:claim_want_next_batch([
        #{residual => true, clientid => DN2, product_key => PK, topics => [{<<"tpl">>, 1}]}
    ]),
    ?assertEqual(Did, maps:get(delivery_id, M2)),
    %% Targeted re-activation did not re-count the rebuilt entry into the
    %% global pending quota.
    ?assertEqual(2, emqx_bcast_storage:pending_delivery_count()).

-doc "A drive that cannot reach a sibling must still serve the partition it\n"
"rebuilt. Shard ownership and the activation leader both follow the live core\n"
"set, so the leader is the first partition the drive rebuilds - and a peer that\n"
"still runs a build without the shard-status call answers nothing at all.\n"
"Waiting for that peer before activating anything withholds the leader's own\n"
"partition for as long as the peer stays un-upgraded, so the drive keeps what\n"
"it did rebuild and leaves the siblings to ask for a targeted activation.\n"
"This is the rolling-upgrade window: what answers the probe here is a shard\n"
"that has stopped answering, like a peer whose plugin is uninstalled.".
t_index_activation_keeps_its_partition_when_a_sibling_is_unreachable(_Config) ->
    Leader = list_to_atom("emqx_bcast_index_owner_0"),
    Unreachable = list_to_atom("emqx_bcast_index_owner_7"),
    ?assert(is_pid(whereis(Leader))),
    ?assert(is_pid(whereis(Unreachable))),
    ?assert(shard_active(0)),
    %% Boot state of the leader: dormant, so the next poll runs a drive.
    _ = sys:replace_state(Leader, fun(St) -> St#{active => false} end),
    ?assertNot(shard_active(0)),
    ok = sys:suspend(Unreachable),
    try
        Leader ! maybe_activate,
        %% The drive rebuilds shard 0's own partition and then stops on the
        %% sibling that cannot answer. Shard 0 must end up active anyway; the
        %% probe of the silent sibling costs one ?SYNC_TIMEOUT_MS, so the wait
        %% is longer than a drive over reachable shards only.
        ?assert(wait_until(fun() -> shard_active(0) end, 400))
    after
        ok = sys:resume(Unreachable)
    end.

-doc "Restarting the activation leader (shard 0) must not reset sibling\n"
"shards: their heaps (in-flight claims included) survive the leader's\n"
"re-drive, and the global pending quota is recounted from live shard state\n"
"(the quota ETS table dies with shard 0) instead of double-counting\n"
"rebuilt entries.".
t_leader_restart_preserves_sibling_shards(_Config) ->
    PK = <<"PLEADRST">>,
    Sibling = 3,
    [DN] = same_shard_dns(PK, Sibling, 1),
    {_ApiMsgId, MsgGuid} = create_test_msg(<<"leader restart">>),
    Did = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(Did, MsgGuid, PK, <<"tpl">>, [DN], 1),
    [{DN, {ok, [_]}}] = emqx_bcast_storage:claim_want_next_batch([
        #{residual => true, clientid => DN, product_key => PK, topics => [{<<"tpl">>, 1}]}
    ]),
    ?assertEqual(1, emqx_bcast_storage:pending_delivery_count()),
    Name = list_to_atom("emqx_bcast_index_owner_0"),
    Pid0 = whereis(Name),
    exit(Pid0, kill),
    ?assert(
        wait_until(
            fun() -> whereis(Name) =/= undefined andalso whereis(Name) =/= Pid0 end, 100
        )
    ),
    ?assert(wait_until(fun() -> shard_active(0) end, 200)),
    %% The sibling heap was preserved: the entry is still in-flight, not
    %% rebuilt back to stored (a reset would also lose the inflight mark).
    {ok, Entries} = emqx_bcast_storage:get_device_delivery_entries({PK, DN}),
    ?assertMatch([{Did, {pending, _}}], Entries),
    %% The quota row was recounted from live shard state after the leader
    %% restart destroyed the quota table: neither zeroed nor doubled.
    ?assertEqual(1, emqx_bcast_storage:pending_delivery_count()).

-doc "The activation leader is the core that owns partition 0, and only its own\n"
"shard 0 drives activation. A dormant shard 0 that is not the owner has to keep\n"
"polling: nothing else can wake it (a dormant shard 0 ignores the\n"
"{activate_shard, ...} casts the other partitions send it), so without the poll\n"
"the node that becomes the new owner never activates its own partition, and no\n"
"partition whose ownership moved can be rebuilt anywhere.".
t_leader_poll_survives_not_being_the_owner(_Config) ->
    Leader = list_to_atom("emqx_bcast_index_owner_0"),
    LeaderPid = whereis(Leader),
    ?assert(shard_active(0)),
    Polls = atomics:new(1, []),
    meck:new(emqx_bcast, [passthrough, no_link]),
    try
        %% Another core owns partition 0 for the length of this block, and every
        %% ownership question the leader itself asks is counted: that is how its
        %% poll is observed.
        meck:expect(
            emqx_bcast,
            core_nodes,
            fun() ->
                case self() =:= LeaderPid of
                    true ->
                        _ = atomics:add(Polls, 1, 1),
                        ['aaa@nohost' | meck:passthrough([])];
                    false ->
                        meck:passthrough([])
                end
            end
        ),
        %% Wake the leader while it is not the owner: it cannot drive this time,
        %% and it has to leave that attempt with a poll still armed.
        _ = sys:replace_state(Leader, fun(St) -> St#{active => false} end),
        Leader ! maybe_activate,
        _ = sys:get_state(Leader, 120000),
        ?assertNot(shard_active(0)),
        %% Let every poll that was armed while this node still was the owner
        %% fire (they are ?ACTIVATE_POLL_MS apart), then measure the polls that
        %% come after: only a poll armed during this not-owner period keeps
        %% arriving, and without it the leader falls silent for good.
        timer:sleep(800),
        Before = atomics:get(Polls, 1),
        ?assert(wait_until(fun() -> atomics:get(Polls, 1) - Before >= 5 end, 40))
    after
        meck:unload(emqx_bcast)
    end,
    %% Ownership is back (that core left). The poll armed while this node was
    %% not the owner is the only thing that can bring partition 0 back, so this
    %% is the assertion a missing re-arm fails.
    ?assert(wait_until(fun() -> shard_active(0) end, 400)),
    ?assert(shard_active(0)),
    %% And the partition serves again.
    PK = <<"PLEADERPOLL">>,
    [DN] = same_shard_dns(PK, 0, 1),
    {_ApiMsgId, MsgGuid} = create_test_msg(<<"leader poll">>),
    Did = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(Did, MsgGuid, PK, <<"tpl">>, [DN], 1),
    ?assertEqual({ok, [Did]}, emqx_bcast_storage:get_device_deliveries({PK, DN})),
    cleanup_row(PK, DN).

-doc "A forced full rebuild (rebuild_index) flushes buffered ack\n"
"decrements before resetting shard state: a decrement dropped with the\n"
"reset would strand the delivery counter above zero and leak the\n"
"delivery rows until TTL expiry.".
t_rebuild_flushes_buffered_acks(_Config) ->
    PK = <<"PACKBUF">>,
    Sibling = 4,
    [DN] = same_shard_dns(PK, Sibling, 1),
    {_ApiMsgId, MsgGuid} = create_test_msg(<<"ack buf flush">>),
    Did = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(Did, MsgGuid, PK, <<"tpl">>, [DN], 1),
    %% The ack decrement sits buffered in the device shard's ack_buf; the
    %% flush timer (50ms) has not fired yet.
    counted = emqx_bcast_storage:process_ack(PK, DN, Did),
    %% Immediately force a full rebuild, racing the pending flush timer.
    ok = emqx_bcast_index_owner:rebuild_index(),
    %% The delivery completes anyway: the buffered decrement was flushed
    %% before the reset, not dropped with it.
    ?assert(
        wait_until(
            fun() -> mnesia:dirty_read(bcast_msg, Did) =:= [] end,
            200
        )
    ).

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

-doc "Concurrent create + ack must not deadlock on the lock order and must\n"
"still clean up every acked delivery's storage rows (regression: the\n"
"rec-first create order deadlocked with the meta-first ack completion).".
t_concurrent_create_ack_no_deadlock_clears(_Config) ->
    PK = <<"PCNCL">>,
    DN = <<"DCNCL">>,
    {_ApiMsgId, MsgGuid} = create_test_msg(<<"concurrent lock">>),
    N = 20,
    DeliveryIds = [emqx_bcast_utils:gen_guid() || _ <- lists:seq(1, N)],
    lists:foreach(
        fun(Did) ->
            {ok, _} = emqx_bcast_storage:create_delivery(Did, MsgGuid, PK, <<"tpl">>, [DN], 1)
        end,
        DeliveryIds
    ),
    Parent = self(),
    AckPids = [
        spawn(fun() ->
            Parent ! {ack_result, self(), emqx_bcast_storage:process_ack(PK, DN, Did)}
        end)
     || Did <- DeliveryIds
    ],
    %% Interleave fresh creates to exercise the create vs complete lock order.
    CreatePids = [
        spawn(fun() ->
            Payload = crypto:strong_rand_bytes(8),
            Hash = crypto:hash(sha256, Payload),
            {NewApiId, NewMsgId} = emqx_bcast_id:generate_message_id(),
            NewDid = emqx_bcast_utils:gen_guid(),
            Parent !
                {create_result, self(),
                    emqx_bcast_storage:create_message_and_delivery(
                        Payload, Hash, NewApiId, NewMsgId, NewDid, PK, <<"tpl">>, [DN]
                    )}
        end)
     || _ <- lists:seq(1, N)
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
    %% Every acked delivery (the original N) must be fully cleaned up.
    ?assert(
        wait_until(
            fun() ->
                lists:all(
                    fun(Did) ->
                        mnesia:dirty_read(bcast_msg, Did) =:= [] andalso
                            mnesia:dirty_read(bcast_msg_meta, Did) =:= [] andalso
                            mnesia:dirty_read(bcast_msg_meta_counter, Did) =:= []
                    end,
                    DeliveryIds
                )
            end,
            100
        )
    ).

-doc "Restarting worker pools releases tagged inflight claims.".
t_worker_pool_restart_recovers_inflight(_Config) ->
    PK = <<"PRESTART">>,
    DN = <<"DRESTART">>,
    Tag = 888888,
    Shard = emqx_bcast_pull_shard:shard_of(PK, DN),
    _ = create_tagged_claim(PK, DN, Tag),
    seed_claim_row(PK, DN, Tag, erlang:system_time(millisecond), Shard),
    ok = emqx_bcast_sup:restart_pools(2),
    ?assert(
        wait_until(
            fun() -> row_claim_of(PK, DN) =:= undefined end,
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

-doc "A pools restart with no claim rounds in flight must still release\n"
"the per-shard restart guard: the completion cast must reach every shard,\n"
"otherwise pools_restarting stays true until the watchdog, stalling\n"
"flush_buffer3 submissions and rejecting any subsequent restart.".
t_pools_restart_without_inflight_marks(_Config) ->
    Baseline = bcast_pool_worker_count(bcast_pull_worker_pool_sup),
    ok = emqx_bcast_sup:restart_pools(Baseline),
    ?assert(
        wait_until(
            fun() -> bcast_pool_worker_count(bcast_pull_worker_pool_sup) =:= Baseline end,
            100
        )
    ),
    %% A second restart immediately after must not be rejected by the
    %% reentry guard even though the first restart had no marks to replay.
    ok = emqx_bcast_sup:restart_pools(Baseline + 1),
    ?assert(
        wait_until(
            fun() -> bcast_pool_worker_count(bcast_pull_worker_pool_sup) =:= Baseline + 1 end,
            100
        )
    ),
    ok = emqx_bcast_sup:restart_pools(Baseline),
    ?assert(
        wait_until(
            fun() -> bcast_pool_worker_count(bcast_pull_worker_pool_sup) =:= Baseline end,
            100
        )
    ).

bcast_pool_worker_count(PoolSupId) ->
    {_, Pid, _, _} = lists:keyfind(PoolSupId, 1, supervisor:which_children(emqx_bcast_sup)),
    length(supervisor:which_children(Pid)).

-doc "Disconnecting a client whose window holds a timestamped\n"
"ack-in-flight entry must not crash the pull shard: the keep/release\n"
"partition of the window matches the two inflight states explicitly\n"
"(a {true, Ts} entry fed to lists:partition/2 as a predicate result\n"
"raises case_clause and kills the shard mid-cleanup).".
t_cleanup_client_with_timestamped_ack_in_flight(_Config) ->
    PK = <<"PTCIF">>,
    DN = <<"DTCIF">>,
    Shard = emqx_bcast_pull_shard:shard_of(PK, DN),
    Name = emqx_bcast_pull_shard:shard_name(Shard),
    ShardPid = whereis(Name),
    MRef = monitor(process, ShardPid),
    Client = spawn(fun() -> timer:sleep(60_000) end),
    gen_server:cast(Name, {client_connected, DN, Client, PK}),
    %% sync barrier: the cast created the state row
    _ = sys:get_state(Name),
    Tab = emqx_bcast_pull_shard:tab(Shard, bcast_client_state),
    [Row] = ets:lookup(Tab, {PK, DN}),
    Did = emqx_bcast_utils:gen_guid(),
    Ts = erlang:system_time(millisecond),
    ets:insert(Tab, Row#bcast_client_state{inflight = [{Did, {true, Ts}}]}),
    %% the client disconnects while its ack is still awaiting core
    %% confirmation; the hook casts client_disconnected to the shard
    gen_server:cast(Name, {client_disconnected, DN, Client, PK}),
    %% sync barrier: the cast is ordered before this call (same sender),
    %% so a reply guarantees the cleanup ran (or crashed the shard)
    _ = sys:get_state(Name),
    %% the ack-in-flight entry kept its row; nothing was released
    [#bcast_client_state{inflight = [{Did, {true, Ts}}]}] = ets:lookup(Tab, {PK, DN}),
    %% the shard survived the cleanup
    ?assertEqual(ShardPid, whereis(Name)),
    receive
        {'DOWN', MRef, process, ShardPid, Reason} -> ct:fail({shard_died, Reason})
    after 0 ->
        ok
    end,
    demonitor(MRef, [flush]).

-doc "A stale deliver_results generation cannot clear the current inflight mark.".
t_stale_deliver_results_keep_current_generation(_Config) ->
    PK = <<"PSTALE">>,
    DN = <<"DSTALE">>,
    OldTag = 777777,
    NewTag = 777778,
    Shard = emqx_bcast_pull_shard:shard_of(PK, DN),
    _Map = create_tagged_claim(PK, DN, OldTag),
    %% The current row claim round is NewTag; a deliver_results batch for
    %% the stale OldTag round must not clear it (only release OldTag).
    seed_claim_row(PK, DN, NewTag, erlang:system_time(millisecond), Shard),
    gen_server:cast(
        emqx_bcast_pull_shard:shard_name(Shard),
        {deliver_results, [{DN, no_more}], [{DN, OldTag, PK}]}
    ),
    _ = sys:get_state(emqx_bcast_pull_shard:shard_name(Shard)),
    ?assertMatch({NewTag, _}, row_claim_of(PK, DN)),
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
    ),
    cleanup_row(PK, DN).

-doc "An empty claim result after an RPC timeout releases the tagged pending entry.".
t_failed_claim_result_releases_pending_generation(_Config) ->
    PK = <<"PTIMEOUT">>,
    DN = <<"DTIMEOUT">>,
    Tag = 999999,
    Shard = emqx_bcast_pull_shard:shard_of(PK, DN),
    _ = create_tagged_claim(PK, DN, Tag),
    seed_claim_row(PK, DN, Tag, erlang:system_time(millisecond), Shard),
    gen_server:cast(
        emqx_bcast_pull_shard:shard_name(Shard),
        {deliver_results, [], [{DN, Tag, PK}]}
    ),
    _ = sys:get_state(emqx_bcast_pull_shard:shard_name(Shard)),
    ?assert(wait_until(fun() -> row_claim_of(PK, DN) =:= undefined end, 100)),
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
    ),
    cleanup_row(PK, DN).

-doc "A malformed deliver_results batch must not crash the pull shard\n"
"(regression: a timed-out claim leg used to leak an {'EXIT', _} tuple as\n"
"the improper tail of the result list, maps:from_list raised badarg, and\n"
"the shard died up to max_restart_intensity at 800k devices).".
t_malformed_deliver_results_do_not_crash_shard(_Config) ->
    PK = <<"PMALFORM">>,
    DN = <<"DMALFORM">>,
    Shard = emqx_bcast_pull_shard:shard_of(PK, DN),
    Name = emqx_bcast_pull_shard:shard_name(Shard),
    Pid = whereis(Name),
    ?assert(is_pid(Pid)),
    %% Improper tail exactly as the crash logs showed: proper prefix then
    %% {'EXIT', {timeout, {gen_server, call, ...}}} consed as the tail.
    Bad = [
        {DN, no_more}
        | {'EXIT',
            {timeout, {gen_server, call, [emqx_bcast_index_owner_0, {claim, [], node()}, 5000]}}}
    ],
    gen_server:cast(Name, {deliver_results, Bad, [{DN, 424242, PK}]}),
    %% A fully non-list batch (worker crashed before assembling pairs).
    gen_server:cast(
        Name,
        {deliver_results, {'EXIT', noproc}, [{DN, 424243, PK}]}
    ),
    %% Barrier on the shard: it must be alive and unchanged after both.
    _ = sys:get_state(Name),
    ?assertEqual(Pid, whereis(Name)),
    cleanup_row(PK, DN).

create_tagged_claim(PK, DN, Tag) ->
    {_ApiMsgId, MsgGuid} = create_test_msg(<<"tagged claim">>),
    DeliveryId = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(
        DeliveryId, MsgGuid, PK, <<"tpl">>, [DN], 1
    ),
    [{DN, {ok, [Map]}}] = emqx_bcast_storage:claim_want_next_batch([
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
        #{residual => true, clientid => DN, product_key => PK, topics => [{<<"tpl">>, 1}]}
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
                 || {DN, {ok, [M]}} <- Claimed
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

%% Two device names whose index shards differ, so each device forms a delivery
%% part of its own (the ack path books per shard, per delivery).
two_different_shards(PK) ->
    {hd(same_shard_dns(PK, 0, 1)), hd(same_shard_dns(PK, 1, 1))}.
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

-doc "An active index shard must deactivate once it stops being the owner:\n"
"ownership follows the live core set, so a core joining or leaving remaps most\n"
"shards; a former owner left active would serve a stale index - entries\n"
"appended while it was not the owner are missing, and acknowledgements it\n"
"already applied are counted twice - as soon as ownership returns.".
t_index_shard_deactivates_when_ownership_moves(_Config) ->
    PK = <<"POWNERMOVE">>,
    DN = <<"DOWNERMOVE">>,
    Shard = emqx_bcast_index_owner:shard_of({PK, DN}),
    Name = list_to_atom("emqx_bcast_index_owner_" ++ integer_to_list(Shard)),
    ok = emqx_bcast_index_owner:rebuild_index(),
    ?assert(wait_until(fun() -> shard_active(Shard) end, 200)),
    %% Another node becomes the owner of this shard.
    meck:new(emqx_bcast, [passthrough, no_link]),
    try
        meck:expect(emqx_bcast, core_nodes, fun() -> ['other@nohost'] end),
        Name ! maybe_activate,
        ?assert(wait_until(fun() -> shard_active(Shard) =:= false end, 100)),
        %% The stale heap index is dropped, not kept for a later takeover.
        State = sys:get_state(Name),
        ?assertEqual(#{}, maps:get(counts, State)),
        ?assertEqual(#{}, maps:get(dids, State)),
        ?assertEqual(#{}, maps:get(queues, State))
    after
        meck:unload(emqx_bcast)
    end,
    %% Ownership comes back: the dormant shard rebuilds itself.
    ?assert(wait_until(fun() -> shard_active(Shard) end, 200)).

-doc "A flush that dies between the marker write and the counter decrement must\n"
"not resurrect an already-acked device. The marker is the durable record a\n"
"rebuild consults, so the remaining-ack count may stay too high (the delivery\n"
"finishes late, or is reclaimed by the TTL) but never too low - a low count\n"
"would re-deliver the message and decrement a second time. Both tables live in\n"
"one mria shard (mria_config:shard_rlookup/1), so the decrement cannot reach a\n"
"replica ahead of the marker, and this pins the write order the design relies\n"
"on.".
t_ack_marker_survives_lost_counter_decrement(_Config) ->
    PK = <<"PMARKERWIN">>,
    A = <<"DMARKERWIN_A">>,
    B = <<"DMARKERWIN_B">>,
    {_ApiMsgId, MsgGuid} = create_test_msg(<<"marker window">>),
    DeliveryId = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(
        DeliveryId, MsgGuid, PK, <<"tpl">>, [A, B], 2
    ),
    ok = emqx_bcast_index_owner:rebuild_index(),
    ?assertEqual({ok, [DeliveryId]}, emqx_bcast_storage:get_device_deliveries({PK, A})),
    %% The crash window: the ack flush persisted A's marker and died before
    %% decrementing the remaining-ack count, which therefore still says 2.
    ok = mnesia:dirty_write(#bcast_msg_acked{delivery_id = DeliveryId, device_names = [A]}),
    ?assertMatch(
        [#bcast_msg_meta_counter{counter = 2}],
        mnesia:dirty_read(bcast_msg_meta_counter, DeliveryId)
    ),
    ok = emqx_bcast_index_owner:rebuild_index(),
    %% A stays out of the rebuilt index: no duplicate delivery, and its
    %% replayed ack finds nothing to count a second time.
    ?assertEqual({ok, []}, emqx_bcast_storage:get_device_deliveries({PK, A})),
    not_found = emqx_bcast_storage:process_ack(PK, A, DeliveryId),
    ?assertEqual({ok, [DeliveryId]}, emqx_bcast_storage:get_device_deliveries({PK, B})),
    %% B's ack is durable and its decrement applied; the count is still above
    %% zero because A's decrement was lost, so the delivery is not finished
    %% here - the next rebuild reconciles it from the markers.
    counted = emqx_bcast_storage:process_ack(PK, B, DeliveryId),
    timer:sleep(200),
    ok = emqx_bcast_index_owner:rebuild_index(),
    ?assertEqual([], mnesia:dirty_read(bcast_msg, DeliveryId)).

-doc "The ack flush must persist the device marker before it decrements the\n"
"remaining-ack count: the marker is what a rebuild consults, so a crash\n"
"between the two writes may leave the count too high but must never leave it\n"
"decremented with the marker missing. This pins the order itself, which the\n"
"same-shard mria replication then preserves for every core.".
t_ack_flush_writes_marker_before_counter(_Config) ->
    PK = <<"PORDER">>,
    A = <<"DORDER_A">>,
    B = <<"DORDER_B">>,
    {_ApiMsgId, MsgGuid} = create_test_msg(<<"write order">>),
    DeliveryId = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(
        DeliveryId, MsgGuid, PK, <<"tpl">>, [A, B], 2
    ),
    ok = emqx_bcast_index_owner:rebuild_index(),
    TestProc = self(),
    meck:new(mnesia, [passthrough, no_link]),
    try
        meck:expect(mnesia, dirty_update_counter, fun(Tab, Key, Incr) ->
            case {Tab, Key} of
                {bcast_msg_meta_counter, DeliveryId} ->
                    TestProc !
                        {marker_durable_before_decrement,
                            mnesia:dirty_read(bcast_msg_acked, DeliveryId) =/= []};
                _ ->
                    ok
            end,
            meck:passthrough([Tab, Key, Incr])
        end),
        counted = emqx_bcast_storage:process_ack(PK, A, DeliveryId),
        receive
            {marker_durable_before_decrement, Durable} -> ?assert(Durable)
        after 5000 ->
            ?assert(false)
        end
    after
        meck:unload(mnesia)
    end.
-doc "A peer that has not been upgraded yet releases claims as\n"
"{claim, {PK, DN, Did}} - the nested shape that crashed the sender's own index\n"
"shards. The receiver must release it instead of crashing, and must not crash\n"
"on any shape it does not know.".
t_release_batch_accepts_legacy_entry_shape(_Config) ->
    PK = <<"PLEGACYREL">>,
    DN = <<"DLEGACYREL">>,
    Did = emqx_bcast_utils:gen_guid(),
    Shard = emqx_bcast_index_owner:shard_of({PK, DN}),
    Name = list_to_atom("emqx_bcast_index_owner_" ++ integer_to_list(Shard)),
    ?assert(wait_until(fun() -> shard_active(Shard) end, 200)),
    Pid0 = whereis(Name),
    {_ApiMsgId, MsgGuid} = create_test_msg(<<"legacy release">>),
    {ok, _} = emqx_bcast_storage:create_delivery(Did, MsgGuid, PK, <<"tpl">>, [DN], 1),
    ok = emqx_bcast_index_owner:rebuild_index(),
    Claim = fun(Tag) ->
        [
            #{
                residual => true,
                clientid => DN,
                product_key => PK,
                topics => [{<<"tpl">>, 1}],
                claim_tag => Tag
            }
        ]
    end,
    Claimable = fun(Tag) ->
        ?assertMatch([{DN, {ok, [_]}}], emqx_bcast_storage:claim_want_next_batch(Claim(Tag)))
    end,
    %% Only the nested claim shape: it must release the entry it names. Each
    %% shape gets its own phase, because sending both at once would let the
    %% claim release free the entry and leave the tag clause unguarded.
    Claimable(4242),
    gen_server:cast(Name, {release_batch, [{claim, {PK, DN, Did}}]}),
    _ = sys:get_state(Name),
    ?assertEqual(Pid0, whereis(Name)),
    Claimable(4243),
    %% Only the nested tag shape: it must release the claim currently held
    %% under that tag.
    gen_server:cast(Name, {release_batch, [{tag, {PK, DN, 4243}}]}),
    _ = sys:get_state(Name),
    ?assertEqual(Pid0, whereis(Name)),
    Claimable(4244),
    %% A shape this build does not know is logged, never fatal.
    gen_server:cast(Name, {release_batch, [{unknown_shape, PK, DN}]}),
    _ = sys:get_state(Name),
    ?assertEqual(Pid0, whereis(Name)).

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

-doc "A rebuild reconciliation whose completion transaction aborts must not\n"
"re-index the already-acked devices: that would leave a ghost pending entry\n"
"for a delivery another shard completes, and the closing quota recount would\n"
"count it. The next shard in the same drive retries the completion instead.".
t_rebuild_completion_abort_leaves_no_ghost(_Config) ->
    PK = <<"PGHOST">>,
    [DN1] = same_shard_dns(PK, 7, 1),
    [DN2] = same_shard_dns(PK, 8, 1),
    ?assertNotEqual(
        emqx_bcast_index_owner:shard_of({PK, DN1}),
        emqx_bcast_index_owner:shard_of({PK, DN2})
    ),
    {_ApiMsgId, MsgGuid} = create_test_msg(<<"ghost completion">>),
    DeliveryId = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(
        DeliveryId, MsgGuid, PK, <<"tpl">>, [DN1, DN2], 2
    ),
    %% The crash-window state: every device has a persisted ack marker while
    %% the remaining-ack counter is still positive.
    ok = mnesia:dirty_write(#bcast_msg_acked{delivery_id = DeliveryId, device_names = [DN1, DN2]}),
    ok = mnesia:dirty_write(#bcast_msg_meta_counter{delivery_id = DeliveryId, counter = 2}),
    TestProc = self(),
    Cnt = atomics:new(1, []),
    meck:new(mnesia, [passthrough, no_link]),
    try
        meck:expect(mnesia, transaction, fun(Fun, Retries) ->
            case in_index_shard() of
                true ->
                    case atomics:add_get(Cnt, 1, 1) of
                        1 ->
                            TestProc ! completion_aborted,
                            {aborted, injected};
                        _ ->
                            meck:passthrough([Fun, Retries])
                    end;
                false ->
                    meck:passthrough([Fun, Retries])
            end
        end),
        ok = emqx_bcast_index_owner:rebuild_index()
    after
        meck:unload(mnesia)
    end,
    receive
        completion_aborted -> ok
    after 2000 ->
        ?assert(false)
    end,
    %% A later shard completed the delivery, and the aborted shard left no
    %% ghost entry behind.
    ?assert(wait_until(fun() -> mnesia:dirty_read(bcast_msg, DeliveryId) =:= [] end, 200)),
    ?assertEqual({ok, []}, emqx_bcast_storage:get_device_deliveries({PK, DN1})),
    ?assertEqual({ok, []}, emqx_bcast_storage:get_device_deliveries({PK, DN2})).

-doc "The last-resort orphan scan must not drop an index entry whose delivery\n"
"row is merely not replicated to this node yet: a fresh entry with an invisible\n"
"row is the mria-lag case (the claim path keeps it for the same reason), and\n"
"dropping it removes a committed delivery from the index, so the device never\n"
"hears about that message again and nothing logs the loss. The same scan still\n"
"drops a genuine orphan once the entry is older than the replication window.".
t_orphan_scan_keeps_fresh_entry_with_invisible_row(_Config) ->
    PK = <<"PORPHANLAG">>,
    DN = <<"DORPHANLAG">>,
    Shard = emqx_bcast_index_owner:shard_of({PK, DN}),
    Name = list_to_atom("emqx_bcast_index_owner_" ++ integer_to_list(Shard)),
    %% An entry with no delivery/message row behind it, which is what a claim
    %% sees while a peer core's promotion is still replicating.
    Did = emqx_bcast_utils:gen_guid(),
    ok = gen_server:call(Name, {append_batch, [{PK, DN, Did}]}, 5000),
    ?assertEqual(1, orphan_queue_len(Name, PK, DN)),
    ok = gen_server:call(Name, {cleanup_local}, 5000),
    ?assertEqual(1, orphan_queue_len(Name, PK, DN)),
    %% Older than ?REPLICATION_LAG_MS: the same scan drops it as a real orphan.
    backdate_index_entry(PK, DN, Did),
    ok = gen_server:call(Name, {cleanup_local}, 5000),
    ?assertEqual(0, orphan_queue_len(Name, PK, DN)).

%% Age one index entry past the replication window (and the claim path's and the
%% orphan scan's tolerance for it), so a test can exercise the "genuinely stale"
%% branch without sleeping for seconds.
backdate_index_entry(PK, DN, Did) ->
    Shard = emqx_bcast_index_owner:shard_of({PK, DN}),
    Name = list_to_atom("emqx_bcast_index_owner_" ++ integer_to_list(Shard)),
    Old = erlang:system_time(millisecond) - 60000,
    _ = sys:replace_state(Name, fun(St) ->
        St#{dids => maps:update({PK, DN, Did}, Old, maps:get(dids, St))}
    end),
    ok.

orphan_queue_len(Name, PK, DN) ->
    St = sys:get_state(Name),
    case maps:get({PK, DN}, maps:get(queues, St), undefined) of
        undefined -> 0;
        Q -> queue:len(Q)
    end.

-doc "MEASUREMENT, not a correctness test: how much a full index rebuild (the\n"
"activation leader's 'drive') blocks the index shard it runs in, and what that\n"
"costs a claim aimed at the same shard.\n"
"\n"
"Background: rebuild_index/0 is a synchronous gen_server call to index shard 0,\n"
"which then - inside its own process - scans the delivery table once and walks\n"
"all 48 shards to have them load their slice. While that runs, shard 0 cannot\n"
"answer anything else, so a claim for a device whose index lives in shard 0\n"
"waits for it. This case measures that wait and the rebuild's own duration for\n"
"a configurable backlog size.\n"
"\n"
"Numbers are printed with ct:pal as one BCAST_REBUILD_MEASUREMENT term. Backlog\n"
"size comes from BCAST_REBUILD_MEASURE_DELIVERIES (default 1000, 0 = no rows).".
t_rebuild_blocking_measurement(_Config) ->
    K = list_to_integer(os:getenv("BCAST_REBUILD_MEASURE_DELIVERIES", "1000")),
    TargetPK = <<"PMEASURET">>,
    ProbePK = <<"PMEASUREP">>,
    %% The probe device must hash to shard 0 (the shard the drive runs in) and
    %% must have no queued entries, so a probe measures queueing only.
    [ProbeDN] = same_shard_dns(ProbePK, 0, 1),
    Targets = same_shard_dns(TargetPK, 0, 20),
    {_ApiMsgId, MsgGuid} = create_test_msg(<<"rebuild measurement">>),
    Setup0 = mono_us(),
    lists:foreach(
        fun(I) ->
            DN = lists:nth((I rem length(Targets)) + 1, Targets),
            Did = emqx_bcast_utils:gen_guid(),
            {ok, _} = emqx_bcast_storage:create_delivery(
                Did, MsgGuid, TargetPK, <<"tpl">>, [DN], 1
            )
        end,
        lists:seq(1, K)
    ),
    SetupUs = mono_us() - Setup0,
    %% Warm up, then measure the idle baseline.
    _ = probe_status(0),
    _ = probe_claim(ProbePK, ProbeDN),
    Baseline = [probe_claim(ProbePK, ProbeDN) || _ <- lists:seq(1, 50)],
    %% One rebuild while probing from this process.
    {During, DriveUs} = measure_drive(fun() -> rebuild_once() end),
    %% Same, with one sibling shard unable to answer: every unreachable shard
    %% costs the drive one ?SYNC_TIMEOUT_MS wait.
    Unreachable = index_shard_name(7),
    ok = sys:suspend(Unreachable),
    {_During2, DriveUnreachableUs} =
        try
            measure_drive(fun() -> rebuild_once() end)
        after
            ok = sys:resume(Unreachable)
        end,
    ct:pal(
        "BCAST_REBUILD_MEASUREMENT ~p",
        [
            #{
                deliveries => K,
                setup_ms => SetupUs div 1000,
                baseline_us_sorted => lists:sort(Baseline),
                during_us_sorted => lists:sort(During),
                during_probe_count => length(During),
                drive_ms => DriveUs div 1000,
                drive_with_one_unreachable_peer_ms => DriveUnreachableUs div 1000
            }
        ]
    ),
    %% A drive this short can finish before a throttled probe lands; only a
    %% drive long enough to be measured has to have been probed.
    ?assert(length(During) > 0 orelse DriveUs < 5000),
    ?assert(DriveUs > 0).

-doc "A rebuild must not hold the index shard that coordinates it. The whole drive\n"
"runs inside shard 0's process today: it probes, scans the message table and\n"
"drives all 48 partitions there, so a claim aimed at shard 0 waits for the whole\n"
"drive. The orchestration belongs in a process of its own; shard 0 should only\n"
"wait for its own 1/48 of the load and the closing bookkeeping. (Relative\n"
"assertion: the probe samples the very shard the drive owns, in the same run, so\n"
"a loaded host cannot make it flaky.)".
t_rebuild_does_not_block_the_coordinating_shard(_Config) ->
    K = 3000,
    %% Spread the backlog over every partition, so the coordinator's own slice is
    %% the same 1/48 as any other partition loads.
    Pairs = lists:append([
        begin
            PK = <<"PCOORD", (integer_to_binary(S))/binary>>,
            [{PK, DN} || DN <- same_shard_dns(PK, S, 2)]
        end
     || S <- lists:seq(0, 47)
    ]),
    {_ApiMsgId, MsgGuid} = create_test_msg(<<"coordinated rebuild">>),
    lists:foreach(
        fun(I) ->
            {PK, DN} = lists:nth((I rem length(Pairs)) + 1, Pairs),
            Did = emqx_bcast_utils:gen_guid(),
            {ok, _} = emqx_bcast_storage:create_delivery(
                Did, MsgGuid, PK, <<"tpl">>, [DN], 1
            )
        end,
        lists:seq(1, K)
    ),
    %% Warm up: the first call into the shard pays for its own table lookups.
    _ = probe_status(0),
    {Latencies, DriveUs} = measure_drive(fun() -> rebuild_once() end),
    MaxProbeUs = lists:max(Latencies),
    ct:pal(
        "BCAST_COORDINATOR_BLOCKING ~p",
        [#{deliveries => K, drive_ms => DriveUs div 1000, max_probe_us => MaxProbeUs}]
    ),
    ?assert(length(Latencies) > 0),
    ?assert(DriveUs > 0),
    %% Before: the probe waits for the whole drive. After: only for the
    %% coordinator's own slice, so half the drive is a generous bound.
    ?assert(MaxProbeUs < DriveUs div 2).

-doc "Only one drive may run at a time. Two drives would scan the table twice,\n"
"load the same partitions twice and re-base the authoritative quota row twice,\n"
"and the second one would race the first one's bookkeeping. A rebuild that\n"
"arrives while a drive is in flight joins that drive instead: it answers when\n"
"the drive reports, and the leader logs that it coalesced.".
t_rebuild_is_single_flight(_Config) ->
    Unreachable = index_shard_name(7),
    ok = sys:suspend(Unreachable),
    try
        Parent = self(),
        _ = spawn(fun() ->
            ok = emqx_bcast_index_owner:rebuild_index(),
            Parent ! first_rebuild_done
        end),
        %% Barrier: the leader has recorded the drive it started.
        ?assert(wait_until(fun() -> drive_in_flight() =/= undefined end, 200)),
        Flight = drive_in_flight(),
        ?assert(is_map(Flight)),
        %% A rebuild that arrives while that drive runs joins it instead of
        %% starting a drive of its own: the leader waits for the same drive, and
        %% both callers are answered when it reports.
        _ = spawn(fun() ->
            ok = emqx_bcast_index_owner:rebuild_index(),
            Parent ! second_rebuild_done
        end),
        ?assert(wait_until(fun() -> drive_waiters() >= 2 end, 100)),
        ?assertEqual(Flight, drive_in_flight()),
        receive
            first_rebuild_done -> ok
        after 10000 -> ?assert(false)
        end,
        receive
            second_rebuild_done -> ok
        after 10000 -> ?assert(false)
        end,
        %% The drive those three calls shared is over, and the leader is idle.
        ?assertEqual(undefined, drive_in_flight())
    after
        ok = sys:resume(Unreachable)
    end.

-doc "The drive coordinator is a process of its own, so it can die: the leader\n"
"has to survive that (it holds the partition's clients, and the index of every\n"
"device on it), stop waiting for that drive, and come back on the next poll\n"
"instead of leaving the cluster without its partition for good.".
t_rebuild_survives_its_coordinator_dying(_Config) ->
    PK = <<"PCOORDKILL">>,
    DN = <<"DCOORDKILL">>,
    Unreachable = index_shard_name(7),
    Leader = index_shard_name(0),
    PrePid = whereis(Leader),
    ok = sys:suspend(Unreachable),
    try
        _ = sys:replace_state(Leader, fun(St) -> St#{active => false} end),
        Leader ! maybe_activate,
        ?assert(wait_until(fun() -> drive_in_flight() =/= undefined end, 200)),
        #{pid := Coordinator} = drive_in_flight(),
        exit(Coordinator, kill),
        %% The leader survives (it holds the partition's clients and their
        %% index) and stops waiting for the dead coordinator.
        ?assertEqual(PrePid, whereis(Leader)),
        ?assert(wait_until(fun() -> drive_in_flight() =:= undefined end, 200)),
        %% With the peer reachable again the next drive finishes: the partition
        %% is serving and its devices are indexed.
        ok = sys:resume(Unreachable),
        ?assert(wait_until(fun() -> shard_active(0) end, 400)),
        ?assertEqual(PrePid, whereis(Leader)),
        ?assert(shard_active(7)),
        {_ApiMsgId, MsgGuid} = create_test_msg(<<"coordinator died">>),
        DeliveryId = emqx_bcast_utils:gen_guid(),
        {ok, _} = emqx_bcast_storage:create_delivery(DeliveryId, MsgGuid, PK, <<"tpl">>, [DN], 1),
        ?assertEqual({ok, [DeliveryId]}, emqx_bcast_storage:get_device_deliveries({PK, DN})),
        cleanup_row(PK, DN)
    after
        ok = sys:resume(Unreachable)
    end.

-doc "A manual rebuild is one shot: when a peer cannot be reached it reports the\n"
"failure, but it must not leave the automatic retry armed - shard 0 would then\n"
"keep re-driving and re-probing the unreachable peer with its call timeout for\n"
"as long as that peer stays unreachable, which is exactly the cost the retry\n"
"bound exists to avoid. Found by the measurement case above.".
t_manual_rebuild_does_not_arm_the_retry(_Config) ->
    Unreachable = index_shard_name(7),
    ok = sys:suspend(Unreachable),
    try
        %% The rebuild call itself times out while the drive waits on the
        %% unreachable peer; the point is the state it leaves behind.
        _ =
            try
                emqx_bcast_index_owner:rebuild_index()
            catch
                _:_ -> timeout
            end,
        %% sys:get_state/1 is the barrier: shard 0 answers it only once the
        %% drive has finished.
        State = sys:get_state(index_shard_name(0)),
        ?assertNot(is_map_key(activation_missing, State))
    after
        ok = sys:resume(Unreachable)
    end.

-doc "A drive must not spend one call timeout per unreachable peer. The probes\n"
"belong together and with a probe-sized timeout, so N unreachable partitions\n"
"cost about one wait instead of N, and index shard 0 stays free to answer the\n"
"claims of its own partition. (Regression: the probes ran one at a time with the\n"
"5s activation timeout, so 8 hung peers held shard 0 for ~40s.)".
t_rebuild_with_unreachable_peers_is_bounded(_Config) ->
    Peers = [index_shard_name(S) || S <- [1, 2, 3, 4, 5, 6, 7, 8]],
    [ok = sys:suspend(P) || P <- Peers],
    try
        T0 = mono_us(),
        _ =
            try
                emqx_bcast_index_owner:rebuild_index()
            catch
                _:_ -> timeout
            end,
        %% Barrier: shard 0 answers this only once the drive has finished.
        _ = sys:get_state(index_shard_name(0), 120000),
        ElapsedMs = (mono_us() - T0) div 1000,
        ct:pal("BCAST_DRIVE_UNREACHABLE_MS ~p", [ElapsedMs]),
        %% 8 x 5s before the fix, about one probe timeout after it; the bound is
        %% generous so a loaded CI host cannot make it flaky.
        ?assert(ElapsedMs < 5000)
    after
        [ok = sys:resume(P) || P <- Peers]
    end.

-doc "An unreachable peer must not be waited on again in the round after the one\n"
"that discovered it: the timeouts are what make a drive expensive, and a "
"recovered peer asks for its own activation anyway. (Relative assertion: both\n"
"rounds rebuild the same reachable partitions, only the first pays the peer.)".
t_rebuild_does_not_wait_for_a_known_peer_again(_Config) ->
    Unreachable = index_shard_name(7),
    %% The assertion below is a difference between two rounds, so the peer must
    %% not already be held back when the first one starts.
    ok = forget_probe_backoff(),
    ok = sys:suspend(Unreachable),
    try
        FirstMs = rebuild_round_ms(),
        SecondMs = rebuild_round_ms(),
        ct:pal("BCAST_DRIVE_ROUNDS_MS ~p", [{FirstMs, SecondMs}]),
        ?assert(FirstMs - SecondMs > 500)
    after
        ok = sys:resume(Unreachable)
    end.

forget_probe_backoff() ->
    %% sys:replace_state/2 answers the new state, not ok.
    _ = sys:replace_state(index_shard_name(0), fun(St) -> maps:remove(bad_peers, St) end),
    ok.

rebuild_round_ms() ->
    T0 = mono_us(),
    _ =
        try
            emqx_bcast_index_owner:rebuild_index()
        catch
            _:_ -> timeout
        end,
    _ = sys:get_state(index_shard_name(0), 120000),
    (mono_us() - T0) div 1000.

-doc "A drive that could not reach a peer must not keep re-driving until that\n"
"peer answers. Recovered partitions are driven by their own activation request,\n"
"and the authoritative quota row is re-based by the periodic recount - not by a\n"
"drive that happens to close at some arbitrary later moment. (Mechanism behind\n"
"t_migrate_legacy_mnesia_layout failing: the leader kept retrying a suspended\n"
"peer and completed a whole drive the moment it came back.).".
t_drive_does_not_rebase_quota_when_a_peer_recovers(_Config) ->
    Unreachable = index_shard_name(7),
    ok = sys:suspend(Unreachable),
    try
        %% Automatic drive (as after a restart) with a peer that cannot answer.
        _ = sys:replace_state(index_shard_name(0), fun(St) -> St#{active => false} end),
        index_shard_name(0) ! maybe_activate,
        %% The leader runs the drive in a coordinator process of its own now, so
        %% the barrier is "no drive in flight" rather than "the shard answered
        %% the message": the peer has to stay unreachable until the drive that
        %% found it is over.
        _ = sys:get_state(index_shard_name(0), 120000),
        ?assert(wait_until(fun() -> drive_in_flight() =:= undefined end, 400)),
        %% The drive could not close, so put a sentinel in the authoritative row
        %% and let the peer come back.
        true = ets:insert(bcast_quota_ets, {global, 4242}),
        ok = sys:resume(Unreachable),
        timer:sleep(2000),
        ?assertEqual(4242, emqx_bcast_storage:pending_delivery_count())
    after
        ok = sys:resume(Unreachable)
    end.

rebuild_once() ->
    _ = emqx_bcast_index_owner:rebuild_index(),
    ok.

%% The drive the activation leader is running, if any: the leader starts it in a
%% coordinator process and records it in its state, and it answers its mailbox
%% long before the drive finishes. A case that has to know the drive is over
%% (rather than "the shard answered") waits on this.
drive_in_flight() ->
    maps:get(drive_in_flight, sys:get_state(index_shard_name(0)), undefined).

%% How many callers are waiting for the drive in flight.
drive_waiters() ->
    length(maps:get(drive_waiters, sys:get_state(index_shard_name(0)), [])).

%% Rebuild in another process; probe from this one at a fixed rate until it
%% finishes. The drive call itself may time out (it is a 5s gen_server call and
%% a drive can take longer), so the outcome is caught: a failed call must still
%% end the probing, or the harness spins forever.
measure_drive(DriveFun) ->
    Parent = self(),
    Start = mono_us(),
    _ = spawn(fun() ->
        _ =
            try
                DriveFun()
            catch
                _:_ -> timeout
            end,
        Parent ! {drive_done, mono_us()}
    end),
    {Latencies, Done} = probe_until_done([]),
    {lists:reverse(Latencies), Done - Start}.

%% ~1000 probes/second: fast enough to see the queueing, slow enough that the
%% probe itself is not the load being measured. The first probe is sent
%% immediately so a short drive still gets sampled.
probe_until_done(Acc) ->
    probe_until_done(Acc, true).

probe_until_done(Acc, true) ->
    %% First probe goes out immediately, then the loop throttles.
    case drive_done() of
        {done, T} -> {Acc, T};
        no -> probe_until_done([probe_status(0) | Acc], false)
    end;
probe_until_done(Acc, false) ->
    receive
        {drive_done, T} -> {Acc, T}
    after 1 -> probe_until_done([probe_status(0) | Acc], false)
    end.

drive_done() ->
    receive
        {drive_done, T} -> {done, T}
    after 0 -> no
    end.

%% A real claim through the storage API for a device with no backlog: it answers
%% {no_more, 0} and touches no state, so it measures the shard's availability.
probe_claim(PK, DN) ->
    T0 = mono_us(),
    [{_DN, _Result}] = emqx_bcast_storage:claim_want_next_batch([
        #{residual => true, clientid => DN, product_key => PK, topics => []}
    ]),
    mono_us() - T0.

%% The cheapest call into a shard: a state read with no side effects.
probe_status(Shard) ->
    T0 = mono_us(),
    _ = gen_server:call(index_shard_name(Shard), {shard_status}, 30000),
    mono_us() - T0.

mono_us() -> erlang:monotonic_time(microsecond).

-doc "The part report writes Mnesia rows from inside the shard process, so a\n"
"failure while reporting must not take the shard - and every partition it\n"
"holds - down: the part stays in the accounting and the periodic cleanup tick\n"
"retries it (and drops it once the delivery is gone).".
t_ack_part_report_failure_retries_instead_of_killing_shard(_Config) ->
    PK = <<"PACKPART">>,
    DN = <<"DACKPART">>,
    Shard = emqx_bcast_index_owner:shard_of({PK, DN}),
    Name = index_shard_name(Shard),
    ?assert(wait_until(fun() -> shard_active(Shard) end, 200)),
    PrePid = whereis(Name),
    %% A finished part whose report cannot read the delivery row.
    PoisonDid = <<"poison-delivery">>,
    _ = sys:replace_state(
        Name,
        fun(St) ->
            St#{part_pending => #{PoisonDid => 0}, part_applied => #{PoisonDid => 1}}
        end
    ),
    meck:new(mnesia, [passthrough, no_link]),
    Reports =
        try
            meck:expect(mnesia, dirty_read, fun
                (bcast_msg, Did) when Did =:= PoisonDid -> error(injected_report_failure);
                (Tab, Key) -> meck:passthrough([Tab, Key])
            end),
            emqx_cth_log_capture:capture(fun() ->
                ok = gen_server:call(Name, {cleanup_local}, 30000)
            end)
        after
            meck:unload(mnesia)
        end,
    %% The failure was contained (the shard is the same process) and logged
    %% with the delivery it belonged to.
    ?assertEqual(PrePid, whereis(Name)),
    ?assert(
        lists:any(
            fun(R) -> maps:get(delivery_id, R, undefined) =:= PoisonDid end,
            Reports
        )
    ),
    %% The part is still owed a report: the failure kept the accounting.
    State1 = sys:get_state(Name),
    ?assertEqual(0, maps:get(PoisonDid, maps:get(part_pending, State1))),
    ?assertEqual(1, maps:get(PoisonDid, maps:get(part_applied, State1))),
    %% The next cleanup tick retries it; the delivery does not exist, so the
    %% part is dropped instead of leaking.
    ok = gen_server:call(Name, {cleanup_local}, 30000),
    State2 = sys:get_state(Name),
    ?assertNot(maps:is_key(PoisonDid, maps:get(part_pending, State2))),
    ?assertNot(maps:is_key(PoisonDid, maps:get(part_applied, State2))),
    ?assertEqual(PrePid, whereis(Name)).

-doc "Deferred batches count towards the intake bound: a node whose promoter\n"
"keeps handing batches back (an index shard that cannot be appended to) must\n"
"answer 429 to new work instead of growing the deferred queue without limit,\n"
"since every deferred batch holds its payload and was already accepted.".
t_intake_admission_counts_deferred(_Config) ->
    %% Stop the promoter so the queue under test is the one this process fills
    %% (its workers would otherwise drain it).
    ok = supervisor:terminate_child(emqx_bcast_sup, emqx_bcast_promoter),
    try
        Depth = ?INTAKE_QUEUE_DEPTH,
        Enqueued = [
            emqx_bcast_intake:enqueue(retry_entry(<<"PCAP">>, <<"DCAP">>))
         || _ <- lists:seq(1, Depth)
        ],
        ?assert(
            lists:all(
                fun
                    ({ok, _}) -> true;
                    (_) -> false
                end,
                Enqueued
            )
        ),
        %% The queue is at its bound: new work is rejected (429).
        ?assertEqual(full, emqx_bcast_intake:enqueue(retry_entry(<<"PCAP">>, <<"DCAP">>))),
        %% A worker takes a batch and hands it back for a later retry.
        Taken = emqx_bcast_intake:take_batch(10, 1000),
        ?assertNotEqual([], Taken),
        ?assertEqual(length(Taken), emqx_bcast_intake:requeue([E || {_Seq, E} <- Taken])),
        ?assert(emqx_bcast_intake:deferred_depth() > 0),
        ?assertEqual(Depth, emqx_bcast_intake:admission_depth()),
        %% ... and the bound still holds: deferred entries are accepted work
        %% that no device has been offered yet, so accepting more of it would
        %% let an unavailable index shard grow the node without limit.
        ?assertEqual(full, emqx_bcast_intake:enqueue(retry_entry(<<"PCAP">>, <<"DCAP">>))),
        ?assert(emqx_bcast_intake:depth() < Depth)
    after
        catch emqx_bcast_intake:reset(),
        {ok, _} = supervisor:restart_child(emqx_bcast_sup, emqx_bcast_promoter)
    end.

-doc "The TTL reap must not strip a message whose expiry a concurrent refresh\n"
"extended. The reap reads the expired row, removes the hash / API-id / order /\n"
"reg rows, then deletes the body with the record it read - so a refresh landing\n"
"in between keeps the body (its delete no longer matches) while reusing the very\n"
"same MessageId, and removing the index rows makes the next registration of the\n"
"same content hand out a DIFFERENT MessageId.".
t_expiry_reap_keeps_index_of_refreshed_message(_Config) ->
    Payload = <<"refresh during reap">>,
    Hash = crypto:hash(sha256, Payload),
    ApiMsgId = emqx_bcast_utils:gen_api_uuid(),
    MsgId = emqx_bcast_utils:gen_guid(),
    ok = emqx_bcast_storage:create_message(ApiMsgId, MsgId, Hash, Payload),
    Now = emqx_bcast_utils:now_sec(),
    {atomic, ok} = mnesia:transaction(fun() ->
        [M] = mnesia:wread({bcast_message, MsgId}),
        mnesia:write(M#bcast_message{expires_at = Now - 100})
    end),
    meck:new(mnesia, [passthrough, no_link]),
    try
        meck:expect(mnesia, dirty_delete, fun
            ({bcast_message_hash, H}) when H =:= Hash ->
                %% A RegisterMessage commits right here: it extends this very
                %% row in place (same MessageId, same hash and API-id rows) and
                %% rewrites the index rows, which the reap then overtakes.
                [Msg] = mnesia:dirty_read(bcast_message, MsgId),
                ok = mnesia:dirty_write(Msg#bcast_message{expires_at = Now + 3600}),
                meck:passthrough([{bcast_message_hash, H}]);
            (Key) ->
                meck:passthrough([Key])
        end),
        emqx_bcast_storage:cleanup_expired()
    after
        meck:unload(mnesia)
    end,
    %% The body survived the reap (that part already worked) ...
    {ok, #bcast_message{msg_id = MsgId, api_msg_id = ApiMsgId} = Live} =
        emqx_bcast_storage:lookup_message(MsgId),
    %% ... and so must everything that makes it findable.
    ?assertMatch(
        [#bcast_message_hash{msg_id = MsgId}], mnesia:dirty_read(bcast_message_hash, Hash)
    ),
    ?assertMatch(
        [#bcast_message_api_id{msg_id = MsgId}], mnesia:dirty_read(bcast_message_api_id, ApiMsgId)
    ),
    ?assertMatch(
        [#bcast_message_order{}],
        mnesia:dirty_read(bcast_message_order, {Live#bcast_message.created_at, MsgId})
    ),
    ?assertMatch([#bcast_message_reg{msg_id = MsgId}], mnesia:dirty_read(bcast_message_reg, MsgId)),
    %% Which is what keeps the de-duplication promise: the same content resolves
    %% to the same MessageId.
    {existing, ReusedApiMsgId, ReusedMsgId} = emqx_bcast_storage:lookup_or_create_message(
        Payload, Hash, emqx_bcast_utils:gen_api_uuid(), emqx_bcast_utils:gen_guid()
    ),
    ?assertEqual(ApiMsgId, ReusedApiMsgId),
    ?assertEqual(MsgId, ReusedMsgId).

-doc "A promotion that keeps aborting must hand the batch back for a later\n"
"retry instead of dropping it: the request was already acknowledged to the\n"
"caller, and a crash path can reach the same state after the mria commit, so\n"
"dropping leaves committed deliveries with no index entry and nothing to\n"
"retry them.".
t_promote_failure_defers_instead_of_dropping(_Config) ->
    PK = <<"PPROMDEFER">>,
    DN = <<"DPROMDEFER">>,
    Gate = atomics:new(1, []),
    atomics:put(Gate, 1, 1),
    DeferredBefore = metric(<<"batch_pub_qos1_deferred">>),
    meck:new(emqx_bcast_storage, [passthrough, no_link]),
    meck:expect(emqx_bcast_storage, promote_batch, fun(Entries) ->
        case atomics:get(Gate, 1) of
            0 -> meck:passthrough([Entries]);
            _ -> {error, injected_promote_failure}
        end
    end),
    try
        {ok, _Seq} = emqx_bcast_intake:enqueue(retry_entry(PK, DN)),
        %% Out of the in-worker budget the batch goes back to the intake queue
        %% (deferred) - it is not dropped.
        ?assert(wait_until(fun() -> emqx_bcast_intake:deferred_depth() >= 1 end, 200)),
        ?assert(metric(<<"batch_pub_qos1_deferred">>) > DeferredBefore),
        ?assertEqual([], indexed_deliveries(PK, DN)),
        %% Nothing is lost: once promotion works again the delivery is indexed.
        atomics:put(Gate, 1, 0),
        ?assert(wait_until(fun() -> indexed_deliveries(PK, DN) =/= [] end, 600))
    after
        meck:unload(emqx_bcast_storage)
    end,
    ok.

-doc "The promotion count is taken exactly once per committed batch, even when\n"
"the trigger broadcast raises after the append succeeded: the crash guard then\n"
"retries the same batch, so counting before the trigger counted it twice.".
t_wanted_counted_once_when_the_trigger_raises(_Config) ->
    PK = <<"PTRIGCNT">>,
    DN = <<"DTRIGCNT">>,
    WantedBefore = metric(<<"batch_pub_qos1_wanted">>),
    meck:new(emqx_bcast_pull_server_pool, [passthrough, no_link]),
    meck:expect(emqx_bcast_pull_server_pool, qos1_trigger, fun
        (PK0, [DN0 | _] = DNs, Tpl) when PK0 =:= PK, DN0 =:= DN ->
            %% The first trigger raises - the batch is already committed and
            %% indexed at that point, so the crash guard retries it; the real
            %% trigger goes through afterwards.
            case get(trigger_raised) of
                undefined ->
                    put(trigger_raised, true),
                    error(injected_trigger_failure);
                _ ->
                    meck:passthrough([PK0, DNs, Tpl])
            end;
        (PK0, DNs, Tpl) ->
            meck:passthrough([PK0, DNs, Tpl])
    end),
    try
        {ok, _Seq} = emqx_bcast_intake:enqueue(retry_entry(PK, DN)),
        ?assert(wait_until(fun() -> indexed_deliveries(PK, DN) =/= [] end, 500)),
        %% One device, one committed batch: exactly one wanted, never two.
        ?assert(
            wait_until(
                fun() -> metric(<<"batch_pub_qos1_wanted">>) - WantedBefore =:= 1 end, 100
            )
        ),
        timer:sleep(200),
        ?assertEqual(1, metric(<<"batch_pub_qos1_wanted">>) - WantedBefore)
    after
        meck:unload(emqx_bcast_pull_server_pool)
    end,
    ok.

-doc "The TTL reap must really remove the registration row. Deleting it with a\n"
"record built from the default `registered_at` (undefined) matches nothing -\n"
"dirty_delete_object/1 compares the whole record - so every message that\n"
"expired kept its bcast_message_reg row on every core, for good.".
t_expiry_reap_removes_registration_row(_Config) ->
    Payload = <<"registration row reap">>,
    Hash = crypto:hash(sha256, Payload),
    ApiMsgId = emqx_bcast_utils:gen_api_uuid(),
    MsgId = emqx_bcast_utils:gen_guid(),
    ok = emqx_bcast_storage:create_message(ApiMsgId, MsgId, Hash, Payload),
    ok = emqx_bcast_storage:mark_message_registered(MsgId),
    ?assertMatch([#bcast_message_reg{msg_id = MsgId}], mnesia:dirty_read(bcast_message_reg, MsgId)),
    Now = emqx_bcast_utils:now_sec(),
    {atomic, ok} = mnesia:transaction(fun() ->
        [M] = mnesia:wread({bcast_message, MsgId}),
        mnesia:write(M#bcast_message{expires_at = Now - 100})
    end),
    emqx_bcast_storage:cleanup_expired(),
    ?assertEqual([], mnesia:dirty_read(bcast_message, MsgId)),
    ?assertEqual([], mnesia:dirty_read(bcast_message_reg, MsgId)).

-doc "The per-part ack accounting has to go away with the delivery. A delivery\n"
"that ends with an unacked device - the ordinary offline-device case reaped by\n"
"TTL, or one removed by management - used to keep one part_pending (and\n"
"part_applied) entry per (delivery, shard) for the life of the node: unbounded\n"
"heap growth in the 48 shard processes, plus a cleanup tick that walks all of\n"
"it.".
t_part_accounting_pruned_when_delivery_leaves(_Config) ->
    PK = <<"PPARTPRUNE">>,
    %% Two devices on one shard (so one part applies an ack while still owing
    %% another) and one on a second shard that never acks.
    [A1, A2] = same_shard_dns(PK, 0, 2),
    [B1] = same_shard_dns(PK, 1, 1),
    {_ApiMsgId, MsgGuid} = create_test_msg(<<"part accounting prune">>),
    DeliveryId = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(
        DeliveryId, MsgGuid, PK, <<"tpl">>, [A1, A2, B1], 3
    ),
    ShardA = emqx_bcast_index_owner:shard_of({PK, A1}),
    ShardB = emqx_bcast_index_owner:shard_of({PK, B1}),
    ?assert(
        wait_until(fun() -> shard_active(ShardA) andalso shard_active(ShardB) end, 200)
    ),
    counted = emqx_bcast_storage:process_ack(PK, A1, DeliveryId),
    %% A1's ack is applied in A's part, but that part is not finished (A2 still
    %% owes one), so the durable remaining-ack counter has not moved: the
    %% accounting below is what has to be pruned, not the counter.
    ?assertEqual(3, ack_counter(DeliveryId)),
    %% A1's shard holds the delivery with an applied ack and one device still
    %% owed; B1's shard holds it with nothing applied.
    ?assertEqual(1, part_entry(ShardA, part_applied, DeliveryId)),
    ?assertEqual(1, part_entry(ShardA, part_pending, DeliveryId)),
    ?assertEqual(1, part_entry(ShardA, part_entries, DeliveryId)),
    ?assertEqual(1, part_entry(ShardB, part_entries, DeliveryId)),
    %% The delivery leaves both shards (management delete path).
    ok = emqx_bcast_storage:delete_delivery(DeliveryId),
    ?assert(
        wait_until(
            fun() ->
                part_held(ShardA, DeliveryId) =:= false andalso
                    part_held(ShardB, DeliveryId) =:= false
            end,
            100
        )
    ).

part_entry(Shard, Key, Did) ->
    maps:get(Did, maps:get(Key, sys:get_state(index_shard_name(Shard))), 0).

%% Does this shard still hold anything for the delivery: index entries or
%% per-part accounting?
part_held(Shard, Did) ->
    State = sys:get_state(index_shard_name(Shard)),
    HasEntry = lists:any(
        fun({{_PK, _DN, D}, _Ts}) -> D =:= Did end,
        maps:to_list(maps:get(dids, State))
    ),
    HasAccounting = lists:any(
        fun(Key) -> maps:is_key(Did, maps:get(Key, State, #{})) end,
        [part_pending, part_applied, part_entries]
    ),
    HasEntry orelse HasAccounting.

-doc "A part report whose counter write fails after the marker landed must be\n"
"repaired by its retry. The retry used to stop as soon as it saw the marker,\n"
"so the remaining-ack counter stayed at 1 and the delivery leaked until TTL\n"
"(only an activation rebuild recomputed it). The marker is the durable record\n"
"of which devices acked, so the retry recomputes the remaining from it.".
t_ack_counter_repaired_when_decrement_is_lost(_Config) ->
    PK = <<"PCNTREC">>,
    %% One device per part, on different shards: A's ack finishes A's part.
    {A, B} = two_different_shards(PK),
    {_ApiMsgId, MsgGuid} = create_test_msg(<<"counter reconcile">>),
    DeliveryId = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(DeliveryId, MsgGuid, PK, <<"tpl">>, [A, B], 2),
    ShardA = emqx_bcast_index_owner:shard_of({PK, A}),
    Name = index_shard_name(ShardA),
    ?assert(wait_until(fun() -> shard_active(ShardA) end, 200)),
    counted = emqx_bcast_storage:process_ack(PK, A, DeliveryId),
    ?assert(wait_until(fun() -> mnesia:dirty_read(bcast_msg_acked, DeliveryId) =/= [] end, 100)),
    ?assertEqual(1, ack_counter(DeliveryId)),
    %% Now land the window this test is about, without mocking Mnesia (its
    %% reload races every other Mnesia user in the suite): the durable marker
    %% says A's part reported, but its decrement was lost - the counter still
    %% counts A's ack, and the part is still in the accounting (as it is when
    %% the report itself fails).
    ok = mnesia:dirty_write(#bcast_msg_meta_counter{
        delivery_id = DeliveryId, counter = 2
    }),
    _ = sys:replace_state(Name, fun(St) ->
        St#{
            part_pending => (maps:get(part_pending, St))#{DeliveryId => 0},
            part_applied => (maps:get(part_applied, St))#{DeliveryId => 1}
        }
    end),
    ?assertEqual(2, ack_counter(DeliveryId)),
    ?assertNotEqual([], mnesia:dirty_read(bcast_msg, DeliveryId)),
    %% The periodic cleanup tick retries the unreported part and recomputes the
    %% counter from the durable marker - without it the delivery would sit at 2
    %% until TTL, and B's ack alone could never complete it.
    ok = gen_server:call(Name, {cleanup_local}, 30000),
    ?assert(wait_until(fun() -> ack_counter(DeliveryId) =:= 1 end, 100)),
    %% B's ack then completes the delivery, exactly once.
    counted = emqx_bcast_storage:process_ack(PK, B, DeliveryId),
    ?assert(wait_until(fun() -> mnesia:dirty_read(bcast_msg, DeliveryId) =:= [] end, 100)),
    ?assertEqual([], mnesia:dirty_read(bcast_msg_acked, DeliveryId)),
    ?assertEqual([], mnesia:dirty_read(bcast_msg_meta_counter, DeliveryId)).

-doc "Deleting one delivery must fail - and keep the durable rows - when a\n"
"device shard cannot remove its index entry: deleting the rows first would\n"
"strand an index entry no sweep can attribute (the sweep matches on the\n"
"delivery row) and count it as canceled while the device is still claimed.".
t_delete_delivery_fails_when_index_removal_fails(_Config) ->
    PK = <<"PDELFAIL">>,
    DN = <<"DDELFAIL">>,
    Shard = emqx_bcast_index_owner:shard_of({PK, DN}),
    Name = index_shard_name(Shard),
    {_ApiMsgId, MsgGuid} = create_test_msg(<<"delete with a dead shard">>),
    DeliveryId = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(DeliveryId, MsgGuid, PK, <<"tpl">>, [DN], 1),
    ?assert(wait_until(fun() -> shard_active(Shard) end, 200)),
    ok = sys:suspend(Name),
    Result =
        try
            emqx_bcast_storage:delete_delivery(DeliveryId)
        after
            ok = sys:resume(Name)
        end,
    ?assertMatch({error, {index_remove_failed, [Shard]}}, Result),
    ?assertNotEqual([], mnesia:dirty_read(bcast_msg, DeliveryId)),
    %% (The index entry itself may already be gone: resuming the shard lets it
    %% run the request that timed out - a removal is idempotent - but the
    %% durable rows are what the delete must not commit to while a leg is
    %% unconfirmed.)
    %% Once the shard answers again the delete completes and the index is gone.
    ok = emqx_bcast_storage:delete_delivery(DeliveryId),
    ?assertEqual([], mnesia:dirty_read(bcast_msg, DeliveryId)),
    ?assertEqual({ok, []}, emqx_bcast_storage:get_device_deliveries({PK, DN})).

-doc "Every epoch-bump leg must fit the API request budget: this fan-out runs\n"
"on the synchronous management-delete path, and the framework kills a callback\n"
"that outlives its budget (answering 503) - after some cores may already have\n"
"advanced their epoch for the hash.".
t_epoch_bump_timeout_stays_inside_api_budget(_Config) ->
    meck:new(emqx_bcast_utils, [passthrough, no_link]),
    try
        meck:expect(emqx_bcast_utils, api_rpc_timeout_ms, fun() -> 5000 end),
        ?assertEqual(5000, emqx_bcast:epoch_bump_timeout_ms()),
        %% Clamped by the generic RPC timeout as well.
        meck:expect(emqx_bcast_utils, api_rpc_timeout_ms, fun() -> 60000 end),
        ?assertEqual(?BCAST_RPC_CALL_TIMEOUT_MS, emqx_bcast:epoch_bump_timeout_ms()),
        %% The real budget is tighter than the generic timeout, which is the
        %% point: 5s - 1.5s margin.
        meck:expect(emqx_bcast_utils, api_rpc_timeout_ms, fun() -> 3500 end),
        ?assertEqual(3500, emqx_bcast:epoch_bump_timeout_ms()),
        %% And the local leg still bumps (no regression in the happy path).
        Hash = crypto:hash(sha256, <<"epoch-bump budget">>),
        Before = emqx_bcast:msg_epoch(Hash),
        ?assertEqual(ok, emqx_bcast:bump_msg_epoch_everywhere(Hash)),
        ?assertEqual(Before + 1, emqx_bcast:msg_epoch(Hash))
    after
        meck:unload(emqx_bcast_utils)
    end.

-doc "Completed-delivery cleanup must keep the <=0 counter row when its delete\n"
"transaction aborts, so the next cleanup tick re-discovers the rows instead\n"
"of leaking them to TTL.".
t_cleanup_completed_abort_keeps_retry_marker(_Config) ->
    PK = <<"PCABT">>,
    [DN] = same_shard_dns(PK, 7, 1),
    {_ApiMsgId, MsgGuid} = create_test_msg(<<"cleanup abort">>),
    DeliveryId = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(DeliveryId, MsgGuid, PK, <<"tpl">>, [DN], 1),
    %% A completion whose counter reached zero but whose meta/rec transaction
    %% never landed: the counter row is the retry marker.
    ok = mnesia:dirty_write(#bcast_msg_meta_counter{delivery_id = DeliveryId, counter = 0}),
    Cnt = atomics:new(1, []),
    meck:new(mnesia, [passthrough, no_link]),
    try
        meck:expect(mnesia, transaction, fun(Fun, Retries) ->
            case atomics:add_get(Cnt, 1, 1) of
                1 -> {aborted, injected};
                _ -> meck:passthrough([Fun, Retries])
            end
        end),
        ok = emqx_bcast_index_owner:cleanup_completed_deliveries()
    after
        meck:unload(mnesia)
    end,
    %% The marker survived the abort, so the rows are still discoverable.
    ?assertMatch(
        [#bcast_msg_meta_counter{}], mnesia:dirty_read(bcast_msg_meta_counter, DeliveryId)
    ),
    ?assertNotEqual([], mnesia:dirty_read(bcast_msg, DeliveryId)),
    %% The next tick (no injection) reclaims everything.
    ok = emqx_bcast_index_owner:cleanup_completed_deliveries(),
    ?assertEqual([], mnesia:dirty_read(bcast_msg, DeliveryId)),
    ?assertEqual([], mnesia:dirty_read(bcast_msg_meta, DeliveryId)),
    ?assertEqual([], mnesia:dirty_read(bcast_msg_meta_counter, DeliveryId)).

-doc "The periodic self-healing recount corrects a drifted authoritative\n"
"pending row from the live shard pendings, so quota can never stay\n"
"permanently high or low.".
t_quota_recount_self_heals(_Config) ->
    ?assert(emqx_bcast_index_owner:is_owner()),
    PK = <<"PQH">>,
    [DN] = same_shard_dns(PK, 7, 1),
    {_ApiMsgId, MsgGuid} = create_test_msg(<<"quota heal">>),
    DeliveryId = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(DeliveryId, MsgGuid, PK, <<"tpl">>, [DN], 1),
    Before = emqx_bcast_index_owner:pending_count(),
    ?assert(Before >= 1),
    true = ets:insert(bcast_quota_ets, {global, Before + 12345}),
    ?assertEqual(Before + 12345, emqx_bcast_index_owner:pending_count()),
    list_to_atom("emqx_bcast_index_owner_0") ! quota_recount,
    ?assert(
        wait_until(
            fun() -> emqx_bcast_index_owner:pending_count() =:= Before end, 200
        )
    ).

-doc "Delete Message closes the enumerate/delete window: a delivery committed\n"
"after the enumeration but before the delete transaction is caught by the\n"
"post-delete re-enumeration instead of being orphaned.".
t_delete_message_catches_straggler(_Config) ->
    PK = <<"PSTRAG">>,
    [DN] = same_shard_dns(PK, 7, 1),
    {ApiMsgId, MsgGuid} = create_test_msg(<<"straggler">>),
    DeliveryId = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(DeliveryId, MsgGuid, PK, <<"tpl">>, [DN], 1),
    Cnt = atomics:new(1, []),
    meck:new(mnesia, [passthrough, no_link]),
    try
        meck:expect(mnesia, dirty_match_object, fun
            (bcast_msg, Pat) ->
                case atomics:add_get(Cnt, 1, 1) of
                    1 -> [];
                    _ -> meck:passthrough([bcast_msg, Pat])
                end;
            (Tab, Pat) ->
                meck:passthrough([Tab, Pat])
        end),
        ok = emqx_bcast_storage:delete_message(ApiMsgId)
    after
        meck:unload(mnesia)
    end,
    ?assertEqual([], mnesia:dirty_read(bcast_msg, DeliveryId)),
    ?assertEqual([], mnesia:dirty_read(bcast_message, MsgGuid)),
    ?assertEqual([], mnesia:dirty_read(bcast_message_api_id, ApiMsgId)),
    ?assertEqual({ok, []}, emqx_bcast_storage:get_device_deliveries({PK, DN})).

-doc "The periodic recount must keep the admission reservations of requests\n"
"still waiting in an intake queue; recomputing from index pendings only\n"
"would erase them from the global cap permanently.".
t_quota_recount_keeps_unpromoted_reservations(_Config) ->
    ?assert(emqx_bcast_index_owner:is_owner()),
    PK = <<"PQRES">>,
    [DN] = same_shard_dns(PK, 7, 1),
    Before = emqx_bcast_index_owner:pending_count(),
    %% Reserve without promoting: exactly the state of an accepted request
    %% that is still sitting in an intake queue.
    ok = emqx_bcast_index_owner:admit(PK, [DN]),
    try
        %% Drift the authoritative row, so the assertion below cannot pass
        %% unless the recount really ran.
        true = ets:insert(bcast_quota_ets, {global, Before + 999}),
        list_to_atom("emqx_bcast_index_owner_0") ! quota_recount,
        ?assert(
            wait_until(
                fun() -> emqx_bcast_index_owner:pending_count() =:= Before + 1 end, 200
            )
        )
    after
        _ = emqx_bcast_index_owner:release_admit(PK, [DN])
    end,
    ?assert(
        wait_until(fun() -> emqx_bcast_index_owner:pending_count() =:= Before end, 200)
    ).

-doc "A recount round must be abandoned when any shard cannot be probed: a\n"
"partial sum would understate the global cap until the next good round.".
t_quota_recount_skips_when_shard_unavailable(_Config) ->
    ?assert(emqx_bcast_index_owner:is_owner()),
    PK = <<"PQSKIP">>,
    [DN] = same_shard_dns(PK, 7, 1),
    {_ApiMsgId, MsgGuid} = create_test_msg(<<"quota skip">>),
    DeliveryId = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(DeliveryId, MsgGuid, PK, <<"tpl">>, [DN], 1),
    Before = emqx_bcast_index_owner:pending_count(),
    Bogus = Before + 4242,
    true = ets:insert(bcast_quota_ets, {global, Bogus}),
    Suspended = list_to_atom("emqx_bcast_index_owner_1"),
    ?assert(is_pid(whereis(Suspended))),
    ok = sys:suspend(Suspended),
    try
        list_to_atom("emqx_bcast_index_owner_0") ! quota_recount,
        %% Longer than the shard probe timeout: the round must give up
        %% without touching the authoritative row.
        timer:sleep(6000),
        ?assertEqual(Bogus, emqx_bcast_index_owner:pending_count())
    after
        ok = sys:resume(Suspended)
    end,
    %% With every shard reachable again the next round heals the row.
    list_to_atom("emqx_bcast_index_owner_0") ! quota_recount,
    ?assert(
        wait_until(fun() -> emqx_bcast_index_owner:pending_count() =:= Before end, 200)
    ).

-doc "A Delete Message that already returned success must not be undone by an\n"
"intake entry admitted before it: the entry's delete epoch is older than the\n"
"current one, so promotion drops it instead of re-creating the message.".
t_promote_rejects_entry_from_before_delete(_Config) ->
    PK = <<"PDELREUSE">>,
    [DN] = same_shard_dns(PK, 7, 1),
    Payload = <<"reuse after delete">>,
    Hash = crypto:hash(sha256, Payload),
    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    ok = emqx_bcast_storage:create_message(ApiMsgId, MsgGuid, Hash, Payload),
    Now = emqx_bcast_utils:now_sec(),
    DeliveryId = emqx_bcast_utils:gen_guid(),
    %% The entry shape of a request admitted while the message still existed:
    %% it carries the epoch that was current at admission.
    Entry = #{
        payload => Payload,
        hash => Hash,
        api_msg_id => ApiMsgId,
        msg_id => MsgGuid,
        epoch => emqx_bcast:msg_epoch(Hash),
        delivery_id => DeliveryId,
        product_key => PK,
        topic_template => <<"tpl">>,
        devices => [DN],
        created_at => Now,
        expires_at => Now + 3600
    },
    ok = emqx_bcast_storage:delete_message(ApiMsgId),
    ?assertEqual([], mnesia:dirty_read(bcast_message, MsgGuid)),
    ?assertEqual({ok, [deleted]}, emqx_bcast_storage:promote_batch([Entry])),
    %% The delete stands: nothing was re-created.
    ?assertEqual([], mnesia:dirty_read(bcast_message, MsgGuid)),
    ?assertEqual([], mnesia:dirty_read(bcast_message_hash, Hash)),
    ?assertEqual([], mnesia:dirty_read(bcast_message_api_id, ApiMsgId)),
    ?assertEqual([], mnesia:dirty_read(bcast_msg, DeliveryId)),
    ?assertEqual({ok, []}, emqx_bcast_storage:get_device_deliveries({PK, DN})).

-doc "A hash group that mixes an entry admitted before a Delete Message with a\n"
"legitimate new request for the same content must decide per entry: drop the\n"
"stale one and promote the new one, in either grouping order.".
t_promote_mixed_group_decides_per_entry(_Config) ->
    PK = <<"PMIXED">>,
    [DN] = same_shard_dns(PK, 7, 1),
    Now = emqx_bcast_utils:now_sec(),
    Promote = fun(Idx, Reverse) ->
        Payload = <<"mixed group ", (integer_to_binary(Idx))/binary>>,
        Hash = crypto:hash(sha256, Payload),
        {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
        ok = emqx_bcast_storage:create_message(ApiMsgId, MsgGuid, Hash, Payload),
        MakeEntry = fun() ->
            #{
                payload => Payload,
                hash => Hash,
                api_msg_id => ApiMsgId,
                msg_id => MsgGuid,
                epoch => emqx_bcast:msg_epoch(Hash),
                delivery_id => emqx_bcast_utils:gen_guid(),
                product_key => PK,
                topic_template => <<"tpl">>,
                devices => [DN],
                created_at => Now,
                expires_at => Now + 3600
            }
        end,
        %% Admitted before the delete ...
        Stale = MakeEntry(),
        ok = emqx_bcast_storage:delete_message(ApiMsgId),
        ?assertEqual([], mnesia:dirty_read(bcast_message, MsgGuid)),
        %% ... and a legitimate new request for the same content after it.
        Fresh = MakeEntry(),
        StaleId = maps:get(delivery_id, Stale),
        FreshId = maps:get(delivery_id, Fresh),
        Entries =
            case Reverse of
                true -> [Fresh, Stale];
                false -> [Stale, Fresh]
            end,
        {ok, Results} = emqx_bcast_storage:promote_batch(Entries),
        ByDid = maps:from_list(
            lists:zip([maps:get(delivery_id, E) || E <- Entries], Results)
        ),
        ?assertEqual(deleted, maps:get(StaleId, ByDid)),
        ?assertEqual(ok, maps:get(FreshId, ByDid)),
        %% The new request created the message and its delivery row; the stale
        %% entry did neither, so the delete was not undone.
        ?assertNotEqual([], mnesia:dirty_read(bcast_message, MsgGuid)),
        ?assertEqual([], mnesia:dirty_read(bcast_msg, StaleId)),
        ?assertNotEqual([], mnesia:dirty_read(bcast_msg, FreshId))
    end,
    Promote(1, false),
    Promote(2, true).

-doc "When the message row is gone but no delete epoch was bumped (a TTL\n"
"reclaim), only an entry that may create the message is promoted: an\n"
"explicit MessageId entry is dropped even though a sibling inline entry\n"
"re-creates the message in the same group.".
t_promote_missing_message_only_creates_for_inline(_Config) ->
    PK = <<"PCANCREATE">>,
    [DN] = same_shard_dns(PK, 7, 1),
    Payload = <<"can create">>,
    Hash = crypto:hash(sha256, Payload),
    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    ok = emqx_bcast_storage:create_message(ApiMsgId, MsgGuid, Hash, Payload),
    Now = emqx_bcast_utils:now_sec(),
    MakeEntry = fun(CanCreate) ->
        #{
            payload => Payload,
            hash => Hash,
            api_msg_id => ApiMsgId,
            msg_id => MsgGuid,
            epoch => emqx_bcast:msg_epoch(Hash),
            can_create => CanCreate,
            delivery_id => emqx_bcast_utils:gen_guid(),
            product_key => PK,
            topic_template => <<"tpl">>,
            devices => [DN],
            created_at => Now,
            expires_at => Now + 3600
        }
    end,
    Inline = MakeEntry(true),
    Reuse = MakeEntry(false),
    InlineId = maps:get(delivery_id, Inline),
    ReuseId = maps:get(delivery_id, Reuse),
    %% Simulate a TTL reclaim: the message rows are gone and no delete epoch
    %% was bumped, so both entries still carry the current epoch.
    ok = mnesia:dirty_delete({bcast_message, MsgGuid}),
    ok = mnesia:dirty_delete({bcast_message_hash, Hash}),
    ok = mnesia:dirty_delete({bcast_message_api_id, ApiMsgId}),
    {ok, Results} = emqx_bcast_storage:promote_batch([Reuse, Inline]),
    ByDid = maps:from_list(lists:zip([ReuseId, InlineId], Results)),
    ?assertEqual(deleted, maps:get(ReuseId, ByDid)),
    ?assertEqual(ok, maps:get(InlineId, ByDid)),
    %% The inline entry re-created the message; the explicit MessageId entry
    %% was not written back alongside it.
    ?assertNotEqual([], mnesia:dirty_read(bcast_message, MsgGuid)),
    ?assertEqual([], mnesia:dirty_read(bcast_msg, ReuseId)),
    ?assertNotEqual([], mnesia:dirty_read(bcast_msg, InlineId)).

-doc "A reset that fails on a node during the reset phase is reported as a\n"
"partial reset, not as a success: the nodes reset before it are already\n"
"zeroed.".
t_metrics_reset_reports_partial_failure(_Config) ->
    meck:new(emqx_bcast_index_owner, [passthrough, no_link]),
    meck:new(prometheus_registry, [passthrough, no_link]),
    try
        %% Phase one passes on every node; phase two fails on this one.
        meck:expect(emqx_bcast_index_owner, gauge_sample, fun() -> {0, 0} end),
        meck:expect(prometheus_registry, clear, fun(_) -> error(injected_reset_failure) end),
        ?assertMatch(
            {error, {partial_reset, [{_, {error, _}}]}},
            emqx_bcast_metrics:reset_cluster()
        ),
        %% The API reports the partial reset instead of a success that omits
        %% the failed node.
        {ok, Status, _Headers, Body} = emqx_bcast_api:handle_local(
            post, [<<"metrics">>, <<"reset">>], #{}
        ),
        ?assertEqual(500, Status),
        ?assertEqual(false, maps:get(<<"Success">>, Body)),
        ?assertEqual(<<"PartialReset">>, maps:get(<<"Code">>, Body)),
        ?assertEqual([], maps:get(<<"ResetNodes">>, Body)),
        ?assertMatch([#{<<"Node">> := _, <<"Reason">> := _}], maps:get(<<"FailedNodes">>, Body))
    after
        meck:unload(prometheus_registry),
        meck:unload(emqx_bcast_index_owner)
    end.

-doc "A cluster-wide metrics reset has to finish inside the plugin framework's\n"
"own budget too: the endpoint kills the callback after\n"
"plugins.api_endpoint.timeout (5s by default) and answers 503, and a reset that\n"
"went on to touch half the cluster while the caller was told nothing is worse\n"
"than refusing. Two phases over N nodes used to cost N x 15s.".
t_metrics_reset_fits_the_endpoint_budget(_Config) ->
    meck:new(emqx, [passthrough, no_link]),
    meck:new(emqx_rpc, [passthrough, no_link]),
    try
        meck:expect(emqx, running_nodes, fun() -> [node(), 'stalled@nohost'] end),
        %% A node that never answers, but honours the timeout it was given.
        meck:expect(
            emqx_rpc,
            call,
            fun(Mod, Node, M, F, A, Timeout) ->
                case Node of
                    'stalled@nohost' ->
                        timer:sleep(Timeout + 50),
                        {badrpc, timeout};
                    _ ->
                        meck:passthrough([Mod, Node, M, F, A, Timeout])
                end
            end
        ),
        T0 = mono_us(),
        Result = emqx_bcast_metrics:reset_cluster(),
        ElapsedMs = (mono_us() - T0) div 1000,
        ct:pal("BCAST_RESET_MS ~p", [ElapsedMs]),
        %% The stalled node fails the check phase, so nothing is reset - and
        %% that verdict arrives inside the budget.
        ?assertMatch({error, {pending_deliveries, _}}, Result),
        ?assert(ElapsedMs < emqx_bcast_utils:api_budget_ms())
    after
        meck:unload(emqx_rpc),
        meck:unload(emqx)
    end.

-doc "The cascade delete of a message shared by many deliveries runs in\n"
"bounded chunks with the message rows removed last, so no single transaction\n"
"covers an unbounded number of deliveries.".
t_delete_message_cascade_is_chunked(_Config) ->
    PK = <<"PCHUNK">>,
    DNS = same_shard_dns(PK, 7, 3),
    Payload = <<"chunked cascade">>,
    Hash = crypto:hash(sha256, Payload),
    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    ok = emqx_bcast_storage:create_message(ApiMsgId, MsgGuid, Hash, Payload),
    DeliveryIds = [
        begin
            Did = emqx_bcast_utils:gen_guid(),
            {ok, _} = emqx_bcast_storage:create_delivery(Did, MsgGuid, PK, <<"tpl">>, [DN], 1),
            Did
        end
     || DN <- DNS
    ],
    %% Force one delivery per chunk so the multi-chunk path is exercised.
    application:set_env(emqx_bcast, mgmt_delete_chunk, 1),
    try
        ok = emqx_bcast_storage:delete_message(ApiMsgId)
    after
        application:unset_env(emqx_bcast, mgmt_delete_chunk)
    end,
    lists:foreach(
        fun(Did) -> ?assertEqual([], mnesia:dirty_read(bcast_msg, Did)) end,
        DeliveryIds
    ),
    ?assertEqual([], mnesia:dirty_read(bcast_message, MsgGuid)),
    ?assertEqual([], mnesia:dirty_read(bcast_message_hash, Hash)),
    ?assertEqual([], mnesia:dirty_read(bcast_message_api_id, ApiMsgId)),
    lists:foreach(
        fun(DN) -> ?assertEqual({ok, []}, emqx_bcast_storage:get_device_deliveries({PK, DN})) end,
        DNS
    ).

-doc "A failure in the middle of the chunked cascade delete leaves every\n"
"already-deleted chunk reconciled - rows, index entries and the canceled\n"
"counter - so a retry only has to finish the chunks whose rows are still\n"
"there.".
t_delete_message_chunk_failure_is_resumable(_Config) ->
    PK = <<"PCHUNKFAIL">>,
    DNS = same_shard_dns(PK, 7, 3),
    Payload = <<"chunk failure">>,
    Hash = crypto:hash(sha256, Payload),
    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    ok = emqx_bcast_storage:create_message(ApiMsgId, MsgGuid, Hash, Payload),
    DeliveryIds = [
        begin
            Did = emqx_bcast_utils:gen_guid(),
            {ok, _} = emqx_bcast_storage:create_delivery(Did, MsgGuid, PK, <<"tpl">>, [DN], 1),
            Did
        end
     || DN <- DNS
    ],
    CanceledBefore = metric(<<"batch_pub_qos1_canceled">>),
    %% One delivery per chunk, so the second chunk is the one that fails.
    application:set_env(emqx_bcast, mgmt_delete_chunk, 1),
    try
        Cnt = atomics:new(1, []),
        meck:new(mnesia, [passthrough, no_link]),
        try
            %% The chunk deletes run in shard 0, so only transactions issued
            %% from an index shard process are counted.
            %% Fail BOTH attempts of the second chunk, so the chunk stays
            %% unreconciled instead of being absorbed by the retry.
            meck:expect(mnesia, transaction, fun(Fun, Retries) ->
                case
                    in_index_shard() andalso
                        lists:member(atomics:add_get(Cnt, 1, 1), [2, 3])
                of
                    true -> {aborted, injected};
                    false -> meck:passthrough([Fun, Retries])
                end
            end),
            ?assertMatch({error, _}, emqx_bcast_storage:delete_message(ApiMsgId))
        after
            meck:unload(mnesia)
        end,
        %% Exactly one chunk landed: its row is gone and its index entry was
        %% removed and counted, while the other rows and the message rows are
        %% still there so the delete can be retried.
        Applied = [Did || Did <- DeliveryIds, mnesia:dirty_read(bcast_msg, Did) =:= []],
        ?assertEqual(1, length(Applied)),
        ?assertEqual(CanceledBefore + 1, metric(<<"batch_pub_qos1_canceled">>)),
        ?assertEqual(
            2, length([Did || Did <- DeliveryIds, mnesia:dirty_read(bcast_msg, Did) =/= []])
        ),
        ?assertNotEqual([], mnesia:dirty_read(bcast_message, MsgGuid)),
        %% The retry finishes the remaining chunks.
        ok = emqx_bcast_storage:delete_message(ApiMsgId),
        lists:foreach(
            fun(Did) -> ?assertEqual([], mnesia:dirty_read(bcast_msg, Did)) end,
            DeliveryIds
        ),
        ?assertEqual([], mnesia:dirty_read(bcast_message, MsgGuid)),
        ?assertEqual([], mnesia:dirty_read(bcast_message_api_id, ApiMsgId)),
        ?assertEqual(CanceledBefore + 3, metric(<<"batch_pub_qos1_canceled">>)),
        lists:foreach(
            fun(DN) ->
                ?assertEqual({ok, []}, emqx_bcast_storage:get_device_deliveries({PK, DN}))
            end,
            DNS
        )
    after
        application:unset_env(emqx_bcast, mgmt_delete_chunk)
    end.

-doc "A transient failure of one chunk's row delete is absorbed by the single\n"
"idempotent retry, so a delete whose reply was lost (or that hit a conflict)\n"
"still completes.".
t_delete_message_chunk_retry_absorbs_transient_abort(_Config) ->
    PK = <<"PCHUNKRETRY">>,
    DNS = same_shard_dns(PK, 7, 3),
    Payload = <<"chunk retry">>,
    Hash = crypto:hash(sha256, Payload),
    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    ok = emqx_bcast_storage:create_message(ApiMsgId, MsgGuid, Hash, Payload),
    DeliveryIds = [
        begin
            Did = emqx_bcast_utils:gen_guid(),
            {ok, _} = emqx_bcast_storage:create_delivery(Did, MsgGuid, PK, <<"tpl">>, [DN], 1),
            Did
        end
     || DN <- DNS
    ],
    CanceledBefore = metric(<<"batch_pub_qos1_canceled">>),
    application:set_env(emqx_bcast, mgmt_delete_chunk, 1),
    try
        Cnt = atomics:new(1, []),
        meck:new(mnesia, [passthrough, no_link]),
        try
            %% Fail only the FIRST attempt of the second chunk.
            meck:expect(mnesia, transaction, fun(Fun, Retries) ->
                case in_index_shard() andalso atomics:add_get(Cnt, 1, 1) =:= 2 of
                    true -> {aborted, injected};
                    false -> meck:passthrough([Fun, Retries])
                end
            end),
            ?assertEqual(ok, emqx_bcast_storage:delete_message(ApiMsgId))
        after
            meck:unload(mnesia)
        end,
        lists:foreach(
            fun(Did) -> ?assertEqual([], mnesia:dirty_read(bcast_msg, Did)) end,
            DeliveryIds
        ),
        ?assertEqual([], mnesia:dirty_read(bcast_message, MsgGuid)),
        ?assertEqual(CanceledBefore + 3, metric(<<"batch_pub_qos1_canceled">>)),
        lists:foreach(
            fun(DN) ->
                ?assertEqual({ok, []}, emqx_bcast_storage:get_device_deliveries({PK, DN}))
            end,
            DNS
        )
    after
        application:unset_env(emqx_bcast, mgmt_delete_chunk)
    end.

-doc "Concurrent admission of the same device cannot exceed the per-device cap:\n"
"an active shard checks and reserves in one call, so the requests cannot all\n"
"pass a stale check.".
t_admit_same_device_concurrent_respects_cap(_Config) ->
    PK = <<"PADMITSAME">>,
    [DN] = same_shard_dns(PK, 7, 1),
    Max = emqx_bcast_config:get(max_pending_deliveries_per_device),
    Attempts = Max + 20,
    Parent = self(),
    [
        spawn(fun() -> Parent ! {admitted, emqx_bcast_index_owner:admit(PK, [DN])} end)
     || _ <- lists:seq(1, Attempts)
    ],
    Outcomes = [
        receive
            {admitted, R} -> R
        end
     || _ <- lists:seq(1, Attempts)
    ],
    Accepted = length([ok || ok <- Outcomes]),
    ?assertEqual(Max, Accepted),
    lists:foreach(
        fun(_) -> _ = emqx_bcast_index_owner:release_admit(PK, [DN]) end,
        lists:seq(1, Accepted)
    ).

-doc "An unavailable shard keeps the degrade-to-accept behaviour: the request is\n"
"admitted without per-device accounting on that shard instead of failing.".
t_admit_unavailable_shard_still_accepts(_Config) ->
    PK = <<"PADMITDEGRADE">>,
    [DN] = same_shard_dns(PK, 8, 1),
    Suspended = list_to_atom("emqx_bcast_index_owner_8"),
    ?assert(is_pid(whereis(Suspended))),
    ok = sys:suspend(Suspended),
    try
        ?assertEqual(ok, emqx_bcast_index_owner:admit(PK, [DN]))
    after
        ok = sys:resume(Suspended)
    end,
    %% The shard answers again after the resume.
    ?assertEqual({ok, []}, emqx_bcast_storage:get_device_deliveries({PK, DN})).

-doc "Admission has to answer before the plugin framework's own budget expires\n"
"(plugins.api_endpoint.timeout, 5s by default): past it the framework kills the\n"
"callback and answers 503, while a reservation the plugin already took is only\n"
"reclaimed by the 60s stale-reservation sweep. The per-shard legs have to fit\n"
"that budget, which is a lot less than the 5s a hot-path leg may use.".
t_admission_fits_the_endpoint_budget(_Config) ->
    PK = <<"PADMITBUDGET">>,
    [DN] = same_shard_dns(PK, 5, 1),
    ok = sys:suspend(index_shard_name(5)),
    try
        T0 = mono_us(),
        Result = emqx_bcast_index_owner:admit(PK, [DN]),
        ElapsedMs = (mono_us() - T0) div 1000,
        ct:pal("BCAST_ADMIT_MS ~p", [ElapsedMs]),
        %% An unreachable shard still degrades to acceptance, as before.
        ?assertEqual(ok, Result),
        ?assert(ElapsedMs < emqx_bcast_utils:api_budget_ms())
    after
        ok = sys:resume(index_shard_name(5))
    end.

-doc "A definite over-limit leg is reported as a quota error and the reservation\n"
"made by the other, confirmed leg is rolled back.".
t_admit_over_limit_rolls_back_other_legs(_Config) ->
    PK = <<"PADMITRB">>,
    [Full] = same_shard_dns(PK, 7, 1),
    [Free] = same_shard_dns(PK, 8, 1),
    Max = emqx_bcast_config:get(max_pending_deliveries_per_device),
    lists:foreach(
        fun(_) -> ok = emqx_bcast_index_owner:admit(PK, [Full]) end, lists:seq(1, Max)
    ),
    %% Free is on another shard, which confirms its reservation before the
    %% over-limit leg on Full's shard rejects the request.
    ?assertEqual(
        {error, {quota_exceeded, [Full]}}, emqx_bcast_index_owner:admit(PK, [Free, Full])
    ),
    %% Free's capacity is intact: the rollback dropped the reservation the
    %% failed request had already taken. A leaked reservation would leave only
    %% Max - 1 slots, so the last of these admits would be rejected.
    lists:foreach(
        fun(_) -> ?assertEqual(ok, emqx_bcast_index_owner:admit(PK, [Free])) end, lists:seq(1, Max)
    ),
    lists:foreach(
        fun(_) ->
            _ = emqx_bcast_index_owner:release_admit(PK, [Full]),
            _ = emqx_bcast_index_owner:release_admit(PK, [Free])
        end,
        lists:seq(1, Max)
    ).

-doc "An index shard that cannot be reached in a delete round must not discard\n"
"what the reachable shard removed in the same round: the entries removed before\n"
"the failure are counted, while the unreachable shard is left unresolved.".
t_delete_message_cross_shard_retry_counts_each_round(_Config) ->
    PK = <<"PCHUNKXSHARD">>,
    [DN7] = same_shard_dns(PK, 7, 1),
    [DN8] = same_shard_dns(PK, 8, 1),
    Payload = <<"cross shard retry">>,
    Hash = crypto:hash(sha256, Payload),
    {ApiMsgId, MsgGuid} = emqx_bcast_id:generate_message_id(),
    ok = emqx_bcast_storage:create_message(ApiMsgId, MsgGuid, Hash, Payload),
    %% One delivery, two devices on two different index shards: the chunk's
    %% index removal fans out to both shards in the same round.
    Did = emqx_bcast_utils:gen_guid(),
    {ok, _} = emqx_bcast_storage:create_delivery(Did, MsgGuid, PK, <<"tpl">>, [DN7, DN8], 1),
    ?assertEqual(7, emqx_bcast_index_owner:shard_of({PK, DN7})),
    ?assertEqual(8, emqx_bcast_index_owner:shard_of({PK, DN8})),
    CanceledBefore = metric(<<"batch_pub_qos1_canceled">>),
    application:set_env(emqx_bcast, mgmt_delete_chunk, 10),
    Suspended = list_to_atom("emqx_bcast_index_owner_7"),
    try
        %% Shard 7 never answers, so every round of its leg fails while shard 8
        %% removes its entry in the first round.
        ok = sys:suspend(Suspended),
        ?assertEqual(ok, emqx_bcast_storage:delete_message(ApiMsgId)),
        %% Shard 8's removal is counted even though the same round failed on
        %% shard 7. Shard 7 remains unresolved at assertion time: the suspended
        %% call is still queued and may run on resume, so its entry is not
        %% asserted either way here.
        ?assertEqual(CanceledBefore + 1, metric(<<"batch_pub_qos1_canceled">>)),
        ?assertEqual([], mnesia:dirty_read(bcast_msg, Did)),
        ?assertEqual([], mnesia:dirty_read(bcast_message, MsgGuid)),
        ?assertEqual({ok, []}, emqx_bcast_storage:get_device_deliveries({PK, DN8}))
    after
        ok = sys:resume(Suspended),
        application:unset_env(emqx_bcast, mgmt_delete_chunk)
    end.

-doc "The public async claim release must reach the index shard in the shape its\n"
"handle_cast/2 expects. A nested {claim, {PK, DN, Did}} crashed the shard with\n"
"function_clause, so shard survival and a real release are both asserted.".
t_release_claims_async_releases_and_keeps_shard_alive(_Config) ->
    PK = <<"PRELASYNC">>,
    DN = <<"DRELASYNC">>,
    Tag = 424242,
    _ = create_tagged_claim(PK, DN, Tag),
    {ok, [{Did, _}]} = emqx_bcast_storage:get_device_delivery_entries({PK, DN}),
    %% Capture the shard identity BEFORE the release. Reading it afterwards
    %% would hide a crash-and-restart: the supervisor replaces the pid, and a
    %% rebuild can also put the entry back to `stored` on its own.
    PrePid = index_shard_pid(PK, DN),
    ?assertEqual(ok, emqx_bcast_index_owner:release_claims_async([{PK, DN, Did}])),
    assert_same_index_shard(PK, DN, PrePid),
    ?assertEqual(
        true,
        wait_until(
            fun() ->
                case emqx_bcast_storage:get_device_delivery_entries({PK, DN}) of
                    {ok, [{_, stored}]} -> true;
                    _ -> false
                end
            end,
            100
        )
    ),
    cleanup_row(PK, DN).

-doc "The public async tag release must reach the index shard in the shape its\n"
"handle_cast/2 expects, releasing the tagged claim instead of crashing.".
t_release_client_claims_async_releases_and_keeps_shard_alive(_Config) ->
    PK = <<"PRELASYNCTAG">>,
    DN = <<"DRELASYNCTAG">>,
    Tag = 434343,
    _ = create_tagged_claim(PK, DN, Tag),
    PrePid = index_shard_pid(PK, DN),
    ?assertEqual(ok, emqx_bcast_index_owner:release_client_claims_async([{PK, DN, Tag}])),
    assert_same_index_shard(PK, DN, PrePid),
    ?assertEqual(
        true,
        wait_until(
            fun() ->
                case emqx_bcast_storage:get_device_delivery_entries({PK, DN}) of
                    {ok, [{_, stored}]} -> true;
                    _ -> false
                end
            end,
            100
        )
    ),
    cleanup_row(PK, DN).

index_shard_pid(PK, DN) ->
    Shard = emqx_bcast_index_owner:shard_of({PK, DN}),
    Pid = whereis(index_shard_name(Shard)),
    ?assert(is_pid(Pid)),
    Pid.

%% Both public releases are asynchronous casts, so the shard is polled: the
%% sys:get_state/1 barrier makes it process the cast first, and a different
%% pid afterwards means it crashed and was restarted by the supervisor.
assert_same_index_shard(PK, DN, PrePid) ->
    Shard = emqx_bcast_index_owner:shard_of({PK, DN}),
    Name = index_shard_name(Shard),
    _ = sys:get_state(Name),
    ?assertEqual(PrePid, whereis(Name)).

index_shard_name(Shard) ->
    list_to_atom("emqx_bcast_index_owner_" ++ integer_to_list(Shard)).

%%--------------------------------------------------------------------
%% Review follow-ups: storage-table list, QoS0 self-ack holder check,
%% expiry delete re-check, role probe retry.
%%--------------------------------------------------------------------

-doc "Every storage table must be in the list that repairs a core's local\n"
"copy. A core that starts (or restarts its plugin) after the table already\n"
"exists in the cluster schema gets no copy from `mria:create_table` -\n"
"`already_exists` - and the repair pass is what gives it one; the management\n"
"message list reads the order index from that local copy.".
t_storage_tables_cover_the_copy_repair_list(_Config) ->
    Tables = emqx_bcast:storage_tables(),
    ?assert(lists:member(bcast_message_order, Tables)),
    ?assert(lists:member(bcast_msg, Tables)),
    [
        ?assert(lists:member(node(), mnesia:table_info(Tab, ram_copies)))
     || Tab <- Tables
    ],
    %% The management list pages through the local order table.
    ?assertNotEqual(undefined, ets:info(bcast_message_order)).

-doc "A QoS0-subscription delivery is self-confirmed (auto-acked), so it must\n"
"not be sent to a channel the client no longer owns: a takeover between the\n"
"claim and the send would count the delivery as delivered and take it out of\n"
"the index, and the current session would never receive it. The claim is\n"
"released instead.".
t_qos0_auto_ack_skips_a_stale_channel(_Config) ->
    PK = <<"PQOS0STALE">>,
    DN = <<"DQOS0STALE">>,
    Shard = emqx_bcast_pull_shard:shard_of(PK, DN),
    Name = emqx_bcast_pull_shard:shard_name(Shard),
    Did = emqx_bcast_utils:gen_guid(),
    Stale = spawn(fun() ->
        receive
            stop -> ok
        end
    end),
    %% The window entry the auto-ack would complete.
    ets:insert(emqx_bcast_pull_shard:tab(Shard, bcast_client_state), #bcast_client_state{
        key = {PK, DN},
        product_key = PK,
        clientid = DN,
        pid = Stale,
        claim = {erlang:unique_integer([positive]), erlang:system_time(millisecond)},
        inflight = [{Did, false}]
    }),
    %% The client's channel moved to another pid: emqx_cm no longer lists this
    %% one as the holder.
    ok = meck:new(emqx_cm, [passthrough, no_link]),
    try
        ok = meck:expect(emqx_cm, lookup_channels, fun(_ClientId) -> [] end),
        ok = emqx_bcast_pull_shard:do_deliver_qos0_and_ack(
            DN, Stale, <<"t/stale">>, <<"payload">>, Did, PK, 1
        ),
        ?assertEqual([], element(2, erlang:process_info(Stale, messages))),
        %% The shard released the window entry instead of self-confirming it,
        %% so the delivery stays claimable for the current session.
        _ = sys:get_state(Name),
        ?assertEqual([], (row_lookup(PK, DN))#bcast_client_state.inflight)
    after
        meck:unload(emqx_cm),
        exit(Stale, kill)
    end.

-doc "The expiry scan is a snapshot: a create or re-send that extended the\n"
"same message between the scan and the delete must not be deleted with it,\n"
"or the delivery of the refreshed message is left without a payload. A row\n"
"that is still the scanned expired row is deleted with all of its derived\n"
"rows.".
t_cleanup_expired_message_keeps_a_refreshed_row(_Config) ->
    Payload = <<"expired then refreshed">>,
    Hash = crypto:hash(sha256, Payload),
    {ApiMsgId, MsgId} = emqx_bcast_id:generate_message_id(),
    ok = emqx_bcast_storage:create_message(ApiMsgId, MsgId, Hash, Payload),
    [Row] = mnesia:dirty_read(bcast_message, MsgId),
    CreatedAt = Row#bcast_message.created_at,
    Now = emqx_bcast_utils:now_sec(),
    %% What a scan that ran just before the refresh collected: expiry 0.
    Scanned = {MsgId, Hash, ApiMsgId, 0, CreatedAt},
    %% A concurrent create/re-send refreshed the row (its new TTL) in between.
    ok = mnesia:dirty_write(Row#bcast_message{expires_at = Now + 3600}),
    ?assertEqual({[], 1}, emqx_bcast_index_owner:delete_expired_messages([Scanned], Now)),
    ?assertNotEqual([], mnesia:dirty_read(bcast_message, MsgId)),
    ?assertNotEqual([], mnesia:dirty_read(bcast_message_hash, Hash)),
    ?assertNotEqual([], mnesia:dirty_read(bcast_message_api_id, ApiMsgId)),
    %% A row that is still exactly the scanned expired row is deleted whole.
    ok = mnesia:dirty_write(Row#bcast_message{expires_at = 0}),
    ?assertEqual({[Scanned], 0}, emqx_bcast_index_owner:delete_expired_messages([Scanned], Now)),
    ?assertEqual([], mnesia:dirty_read(bcast_message, MsgId)),
    ?assertEqual([], mnesia:dirty_read(bcast_message_hash, Hash)),
    ?assertEqual([], mnesia:dirty_read(bcast_message_api_id, ApiMsgId)),
    ?assertEqual([], mnesia:dirty_read(bcast_message_order, {CreatedAt, MsgId})).

-doc "The role lookup can miss while mria is still publishing it. The startup\n"
"probe (`init_role/0`) retries before it commits to the non-core layout, and\n"
"a role that never resolves still falls back to replicant - the safe side.\n"
"Per-request `is_core/0` keeps the immediate fallback (it does not sleep).".
t_role_probe_retries_before_giving_up(_Config) ->
    ok = meck:new(mria_config, [passthrough, no_link]),
    try
        Calls = counters:new(1, [atomics]),
        ok = meck:expect(mria_config, whoami, fun() ->
            _ = counters:add(Calls, 1, 1),
            case counters:get(Calls, 1) of
                1 -> error(mria_not_ready);
                _ -> core
            end
        end),
        ?assert(emqx_bcast:init_role()),
        %% A permanent miss still answers replicant, so the node does not
        %% create tables or serve writes it cannot serve.
        ok = meck:expect(mria_config, whoami, fun() -> error(mria_not_ready) end),
        ?assertNot(emqx_bcast:init_role())
    after
        meck:unload(mria_config)
    end.
