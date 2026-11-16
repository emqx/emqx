%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
%% Cluster topology e2e and small performance evaluation:
%%   * 1 core + 2 replicants
%%   * 3 cores
-module(emqx_bcast_topo_perf_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("emqx_bcast.hrl").

-define(PAYLOAD, <<"topo_perf_payload">>).
-define(PERF_N, 20).
-define(MNESIA_CLIENTS, 12000).
-define(MNESIA_CONNECT_BATCH, 1000).
-define(MNESIA_REQUEST_SIZES, [500, 800, 1000, 1200, 1500, 1800, 2200, 3000]).
%% 0.4.3 baselines from repeated runs:
-define(MNESIA_BURST_DELIVERIES, 25).
%% calls=38096, in_bytes=2542914, out_bytes=19569727..19569778.
%% 0.4.5 adds a fixed 400 calls for the per-part accounting watermark.
%% Partial marker replacement may add a write and delete, but must not add a
%% full bcast_msg read proportional to the number of flushes.
-define(MNESIA_FIXED_CALLS_MAX, 38496).
-define(MNESIA_TOTAL_CALLS_MAX, 41000).
-define(MNESIA_IN_BYTES_MAX, 2700000).
-define(MNESIA_OUT_BYTES_MAX, 20250000).
-define(MNESIA_BATCH_MSG_READ_CALLS_MAX, 384).
-define(MNESIA_BATCH_MSG_READ_BYTES_MAX, 13920288).
-define(MNESIA_BURST_CALLS_MAX, 6225).
-define(MNESIA_BURST_IN_BYTES_MAX, 379300).
-define(MNESIA_BURST_OUT_BYTES_MAX, 1350000).
-define(MNESIA_BURST_REDUCTIONS_PER_PART_MAX, 550).
-define(MNESIA_BURST_ELAPSED_US_MAX, 10000000).
-define(MNESIA_BURST_RESTARTS_MAX, 25).
-define(MNESIA_OP_CALLS_MAX, #{
    dirty_read => 40000,
    dirty_write => 1024,
    dirty_delete_object => 1024,
    dirty_match_object => 384,
    dirty_update_counter => 384,
    transaction => 100,
    lock => 100,
    delete => 40,
    read => 1200,
    write => 900,
    wread => 40,
    dirty_delete => 8
}).

all() ->
    [
        t_1c2r_e2e,
        t_3c_e2e,
        t_perf_1c2r,
        t_perf_3c,
        t_mnesia_batchpub_qos1_smoke,
        t_mnesia_batchpub_qos1_online_12k,
        t_mnesia_batchpub_qos1_offline_replay_12k,
        t_mnesia_qos1_part_completion_burst_scaled
    ].

suite() -> [{timetrap, {minutes, 30}}].

init_per_testcase(TC, Config) ->
    Cluster = emqx_cth_cluster:mk_nodespecs(
        nodespecs(TC),
        #{work_dir => emqx_cth_suite:work_dir(TC, Config)}
    ),
    [{cluster, Cluster} | Config].

end_per_testcase(_, Config) ->
    emqx_cth_cluster:stop(?config(cluster, Config)).

%%--------------------------------------------------------------------
%% Topology specs
%%--------------------------------------------------------------------

nodespecs(t_1c2r_e2e) ->
    [
        {topo1c2r_core, #{role => core, apps => apps()}},
        {topo1c2r_rep1, #{role => replicant, apps => apps()}},
        {topo1c2r_rep2, #{role => replicant, apps => apps()}}
    ];
nodespecs(t_perf_1c2r) ->
    [
        {perf1c2r_core, #{role => core, apps => apps()}},
        {perf1c2r_rep1, #{role => replicant, apps => apps()}},
        {perf1c2r_rep2, #{role => replicant, apps => apps()}}
    ];
nodespecs(t_3c_e2e) ->
    [
        {topo3c_core1, #{role => core, apps => apps()}},
        {topo3c_core2, #{role => core, apps => apps()}},
        {topo3c_core3, #{role => core, apps => apps()}}
    ];
nodespecs(t_perf_3c) ->
    [
        {perf3c_core1, #{role => core, apps => apps()}},
        {perf3c_core2, #{role => core, apps => apps()}},
        {perf3c_core3, #{role => core, apps => apps()}}
    ];
nodespecs(TC) when
    TC =:= t_mnesia_batchpub_qos1_online_12k;
    TC =:= t_mnesia_batchpub_qos1_offline_replay_12k;
    TC =:= t_mnesia_qos1_part_completion_burst_scaled
->
    [
        {perf_mnesia_core, #{
            role => core,
            apps => apps(),
            start_opts => #{args => perf_mnesia_erl_flags()}
        }}
    ];
nodespecs(t_mnesia_batchpub_qos1_smoke) ->
    [
        {perf_mnesia_smoke_core, #{
            role => core,
            apps => apps(),
            start_opts => #{args => perf_mnesia_erl_flags()}
        }}
    ].

perf_mnesia_erl_flags() ->
    ["+S", "4:4" | perf_code_path_args()].

perf_code_path_args() ->
    [
        "-pa"
        | [
            Path
         || Path <- code:get_path(),
            string:prefix(Path, code:lib_dir()) =:= nomatch,
            string:str(Path, "_build/default/plugins") =:= 0
        ]
    ].

apps() ->
    [
        {emqx, #{config => #{<<"authorization">> => #{<<"no_match">> => <<"allow">>}}}},
        emqx_bcast
    ].

%%--------------------------------------------------------------------
%% e2e tests
%%--------------------------------------------------------------------

-doc "QoS=0, QoS=1 and cross-node replay work in a 1-core 2-replicant cluster.".
t_1c2r_e2e(Config) ->
    [Core, Rep1, Rep2] = emqx_cth_cluster:start(?config(cluster, Config)),
    settle_cluster([Core, Rep1, Rep2], [Core]),
    DN1 = <<"topo_1c2r_dn1">>,
    DN2 = <<"topo_1c2r_dn2">>,
    C1 = connect(Rep1, DN1),
    C2 = connect(Rep2, DN2),
    try
        sub(C1, topic(DN1)),
        sub(C2, topic(DN2)),
        ?assert(wait_sub(Rep1, DN1, topic(DN1))),
        ?assert(wait_sub(Rep2, DN2, topic(DN2))),
        wait_ready(Core, [{Rep1, DN1}, {Rep2, DN2}]),

        %% QoS0 broadcast reaches clients on both replicants.
        {ok, 200, _, _} = api_call(Core, #{
            <<"Action">> => <<"BatchPub">>,
            <<"ProductKey">> => <<"default">>,
            <<"DeviceName">> => [DN1, DN2],
            <<"MessageContent">> => b64(?PAYLOAD),
            <<"Qos">> => 0
        }),
        [M1] = recv(1),
        [M2] = recv(1),
        ?assertEqual(?PAYLOAD, maps:get(payload, M1)),
        ?assertEqual(?PAYLOAD, maps:get(payload, M2)),

        %% QoS1 is pulled through the core and delivered on both replicants.
        {ok, 200, _, _} = api_call(Core, #{
            <<"Action">> => <<"BatchPub">>,
            <<"ProductKey">> => <<"default">>,
            <<"DeviceName">> => [DN1, DN2],
            <<"MessageContent">> => b64(?PAYLOAD),
            <<"Qos">> => 1
        }),

        [M1b] = recv(1),
        [M2b] = recv(1),
        ?assertEqual(?PAYLOAD, maps:get(payload, M1b)),
        ?assertEqual(?PAYLOAD, maps:get(payload, M2b)),
        %% Drain the online rounds before publishing the offline one, so
        %% wait_queued/2 below can only observe the new delivery.
        ?assert(wait_device_ack(Core, <<"default">>, DN1)),
        ?assert(wait_device_ack(Core, <<"default">>, DN2)),

        %% Offline device on Rep1, then reconnect on Rep2 and replay.
        emqtt:stop(C1),
        ?assert(wait_channel_gone(Rep1, DN1)),
        {ok, 200, _, _} = api_call(Core, #{
            <<"Action">> => <<"BatchPub">>,
            <<"ProductKey">> => <<"default">>,
            <<"DeviceName">> => [DN1],
            <<"MessageContent">> => b64(?PAYLOAD),
            <<"Qos">> => 1
        }),
        ?assert(wait_queued(Core, DN1)),
        C1b = connect(Rep2, DN1),
        sub(C1b, topic(DN1)),
        ?assert(wait_sub(Rep2, DN1, topic(DN1))),
        wait_ready(Core, [{Rep2, DN1}]),
        [Replayed] = recv(1),
        ?assertEqual(?PAYLOAD, maps:get(payload, Replayed)),
        emqtt:stop(C1b)
    after
        catch emqtt:stop(C1),
        catch emqtt:stop(C2)
    end.

-doc "QoS=0, QoS=1 and cross-node replay work in a 3-core cluster.".
t_3c_e2e(Config) ->
    [C1, C2, C3] = emqx_cth_cluster:start(?config(cluster, Config)),
    settle_cluster([C1, C2, C3], [C1, C2, C3]),
    DN1 = <<"topo_3c_dn1">>,
    DN2 = <<"topo_3c_dn2">>,
    Client1 = connect(C1, DN1),
    Client2 = connect(C2, DN2),
    try
        sub(Client1, topic(DN1)),
        sub(Client2, topic(DN2)),
        ?assert(wait_sub(C1, DN1, topic(DN1))),
        ?assert(wait_sub(C2, DN2, topic(DN2))),
        wait_ready(C3, [{C1, DN1}, {C2, DN2}]),

        %% API lands on C3, deliveries go to clients on C1/C2.
        {ok, 200, _, _} = api_call(C3, #{
            <<"Action">> => <<"BatchPub">>,
            <<"ProductKey">> => <<"default">>,
            <<"DeviceName">> => [DN1, DN2],
            <<"MessageContent">> => b64(?PAYLOAD),
            <<"Qos">> => 0
        }),
        [M1] = recv(1),
        [M2] = recv(1),
        ?assertEqual(?PAYLOAD, maps:get(payload, M1)),
        ?assertEqual(?PAYLOAD, maps:get(payload, M2)),

        {ok, 200, _, _} = api_call(C3, #{
            <<"Action">> => <<"BatchPub">>,
            <<"ProductKey">> => <<"default">>,
            <<"DeviceName">> => [DN1, DN2],
            <<"MessageContent">> => b64(?PAYLOAD),
            <<"Qos">> => 1
        }),
        [M1b] = recv(1, 10_000),
        [M2b] = recv(1, 10_000),
        ?assertEqual(?PAYLOAD, maps:get(payload, M1b)),
        ?assertEqual(?PAYLOAD, maps:get(payload, M2b)),
        %% Drain the online rounds before publishing the offline one, so
        %% wait_queued/2 below can only observe the new delivery.
        ?assert(wait_device_ack(C3, <<"default">>, DN1)),
        ?assert(wait_device_ack(C3, <<"default">>, DN2)),

        %% Offline device on C1, then reconnect on C3 and replay.
        emqtt:stop(Client1),
        ?assert(wait_channel_gone(C1, DN1)),
        {ok, 200, _, _} = api_call(C3, #{
            <<"Action">> => <<"BatchPub">>,
            <<"ProductKey">> => <<"default">>,
            <<"DeviceName">> => [DN1],
            <<"MessageContent">> => b64(?PAYLOAD),
            <<"Qos">> => 1
        }),
        ?assert(wait_queued(C3, DN1)),
        Client1b = connect(C3, DN1),
        sub(Client1b, topic(DN1)),
        ?assert(wait_sub(C3, DN1, topic(DN1))),
        wait_ready(C3, [{C3, DN1}]),
        [Replayed] = recv(1),
        ?assertEqual(?PAYLOAD, maps:get(payload, Replayed)),
        emqtt:stop(Client1b)
    after
        catch emqtt:stop(Client1),
        catch emqtt:stop(Client2)
    end.

%%--------------------------------------------------------------------
%% Performance evaluation
%%--------------------------------------------------------------------

-doc "Latency probe: QoS=0 and QoS=1 round trips in a 1-core 2-replicant cluster.".
t_perf_1c2r(Config) ->
    [Core, Rep1, Rep2] = emqx_cth_cluster:start(?config(cluster, Config)),
    settle_cluster([Core, Rep1, Rep2], [Core]),
    DN = <<"perf_1c2r_dn">>,
    C = connect(Rep1, DN),
    try
        sub(C, topic(DN)),
        ?assert(wait_sub(Rep1, DN, topic(DN))),
        wait_ready(Core, [{Rep1, DN}]),
        run_perf(Core, Core, DN, <<"1core-2replicant">>)
    after
        catch emqtt:stop(C)
    end.

-doc "Latency probe: QoS=0 and QoS=1 round trips in a 3-core cluster.".
t_perf_3c(Config) ->
    [C1, C2, C3] = emqx_cth_cluster:start(?config(cluster, Config)),
    settle_cluster([C1, C2, C3], [C1, C2, C3]),
    DN = <<"perf_3c_dn">>,
    C = connect(C1, DN),
    try
        sub(C, topic(DN)),
        ?assert(wait_sub(C1, DN, topic(DN))),
        wait_ready(C3, [{C1, DN}]),
        run_perf(C3, C1, DN, <<"3core">>)
    after
        catch emqtt:stop(C)
    end.

settle_cluster(Nodes, CoreNodes) ->
    emqx_bcast_test_cluster:wait_ready(Nodes, CoreNodes).

-doc "One-client probe for the bcast-attributed Mnesia instrumentation.".
t_mnesia_batchpub_qos1_smoke(Config) ->
    OldTrapExit = process_flag(trap_exit, true),
    [Node] = emqx_cth_cluster:start(?config(cluster, Config)),
    settle_cluster([Node], [Node]),
    [DN] = device_names(1),
    [Client] = connect_in_batches(Node, [DN], 1),
    try
        ?assert(wait_registry_size(Node, 1)),
        disable_cleanup(Node),
        ok = erpc:call(Node, emqx_bcast_mnesia_perf, start, []),
        publish_request(Node, 1, [DN]),
        ?assertEqual(1, length(mnesia_recv(1, 30_000))),
        ?assert(wait_pipeline_drained(Node, 600)),
        Stats = erpc:call(Node, emqx_bcast_mnesia_perf, stop, []),
        emqx_bcast_mnesia_perf:report(<<"smoke">>, Stats),
        ?assert(maps:get(calls, Stats) > 0)
    after
        stop_clients([Client]),
        process_flag(trap_exit, OldTrapExit)
    end.

-doc "Mnesia hot-path budget: 12k clients come online in batches, then varied\n"
"BatchPub QoS1 requests cover every client exactly once and drain every ACK.".
t_mnesia_batchpub_qos1_online_12k(Config) ->
    OldTrapExit = process_flag(trap_exit, true),
    [Node] = emqx_cth_cluster:start(?config(cluster, Config)),
    settle_cluster([Node], [Node]),
    DNs = device_names(?MNESIA_CLIENTS),
    Clients = connect_in_batches(Node, DNs, ?MNESIA_CONNECT_BATCH),
    try
        wait_registry_size(Node, length(DNs)),
        timer:sleep(2000),
        ?assertEqual(0, pending_delivery_count(Node)),
        disable_cleanup(Node),
        ok = maybe_start_mnesia_perf(Node),
        try
            publish_varied_requests(Node, DNs),
            Messages = mnesia_recv(length(DNs), 120_000),
            case length(Messages) of
                Expected when Expected =:= length(DNs) ->
                    ok;
                Got ->
                    FailedStats = stop_mnesia_perf(Node),
                    FailedVersion = plugin_version(Node),
                    FailedLabel = <<
                        FailedVersion/binary, "-batchpub-qos1-online-12k-incomplete"
                    >>,
                    emqx_bcast_mnesia_perf:report(FailedLabel, FailedStats),
                    ct:fail({delivery_incomplete, Got, length(DNs)})
            end,
            ?assertEqual(length(DNs), length(Messages)),
            ?assert(wait_pipeline_drained(Node, 1200)),
            Stats = stop_mnesia_perf(Node),
            Version = plugin_version(Node),
            Label = <<Version/binary, "-batchpub-qos1-online-12k">>,
            emqx_bcast_mnesia_perf:report(Label, Stats),
            assert_mnesia_baseline(online, Stats)
        catch
            Class:Reason:Stacktrace ->
                _ = stop_mnesia_perf(Node),
                erlang:raise(Class, Reason, Stacktrace)
        end
    after
        stop_clients(Clients),
        process_flag(trap_exit, OldTrapExit)
    end.

-doc "Mnesia recovery hot-path budget: 6k clients are online while another 6k\n"
"are absent, then the absent clients reconnect in batches and replay their ACKs.".
t_mnesia_batchpub_qos1_offline_replay_12k(Config) ->
    OldTrapExit = process_flag(trap_exit, true),
    [Node] = emqx_cth_cluster:start(?config(cluster, Config)),
    settle_cluster([Node], [Node]),
    DNs = device_names(?MNESIA_CLIENTS),
    {OnlineDNs, OfflineDNs} = lists:split(?MNESIA_CLIENTS div 2, DNs),
    OnlineClients = connect_in_batches(Node, OnlineDNs, ?MNESIA_CONNECT_BATCH),
    try
        ?assert(wait_registry_size(Node, length(OnlineDNs))),
        timer:sleep(1000),
        disable_cleanup(Node),
        ok = maybe_start_mnesia_perf(Node),
        try
            publish_varied_requests(Node, DNs),
            ?assertEqual(length(OnlineDNs), length(mnesia_recv(length(OnlineDNs), 120_000))),
            ?assert(wait_pending_count(Node, length(OfflineDNs), 1200)),
            OfflineClients = connect_in_batches(
                Node, OfflineDNs, ?MNESIA_CONNECT_BATCH, length(OnlineDNs)
            ),
            try
                ?assertEqual(
                    length(OfflineDNs),
                    length(mnesia_recv(length(OfflineDNs), 120_000))
                ),
                ?assert(wait_pipeline_drained(Node, 1200)),
                Stats = stop_mnesia_perf(Node),
                Version = plugin_version(Node),
                Label = <<Version/binary, "-batchpub-qos1-offline-replay-12k">>,
                emqx_bcast_mnesia_perf:report(Label, Stats),
                assert_mnesia_baseline(offline_replay, Stats)
            after
                stop_clients(OfflineClients)
            end
        catch
            Class:Reason:Stacktrace ->
                _ = stop_mnesia_perf(Node),
                erlang:raise(Class, Reason, Stacktrace)
        end
    after
        stop_clients(OnlineClients),
        process_flag(trap_exit, OldTrapExit)
    end.

-doc "Scaled QoS1 part-completion contention probe. One production second at\n"
"100 requests/s with 1000 devices creates about 100 * 48 = 4800 part reports.\n"
"Run 25 deliveries with one device in every index shard, then submit all ACKs\n"
"in one batch so the 48 parts of each delivery start concurrently. This is\n"
"25% of one production second by part-report count while keeping the workload\n"
"small enough for CI.".
t_mnesia_qos1_part_completion_burst_scaled(Config) ->
    OldTrapExit = process_flag(trap_exit, true),
    [Node] = emqx_cth_cluster:start(?config(cluster, Config)),
    settle_cluster([Node], [Node]),
    PK = <<"p1_scaled_burst">>,
    ShardCount = erpc:call(Node, emqx_bcast_index_owner, shard_count, []),
    DNs = erpc:call(Node, fun() -> burst_devices(PK, ShardCount) end),
    {ApiMsgId, MsgGuid} = erpc:call(Node, emqx_bcast_id, generate_message_id, []),
    Payload = <<"p1 scaled part completion burst">>,
    Hash = crypto:hash(sha256, Payload),
    ok = erpc:call(Node, emqx_bcast_storage, create_message, [
        ApiMsgId, MsgGuid, Hash, Payload
    ]),
    DeliveryIds = [
        erpc:call(Node, emqx_bcast_utils, gen_guid, [])
     || _ <- lists:seq(1, ?MNESIA_BURST_DELIVERIES)
    ],
    [
        {ok, _} = erpc:call(Node, emqx_bcast_storage, create_delivery, [
            DeliveryId, MsgGuid, PK, <<"tpl">>, DNs, ShardCount
        ])
     || DeliveryId <- DeliveryIds
    ],
    PartReports = ?MNESIA_BURST_DELIVERIES * ShardCount,
    ?assert(wait_pending_count(Node, PartReports, 600)),
    disable_cleanup(Node),
    ok = erpc:call(Node, emqx_bcast_mnesia_perf, start, []),
    try
        Reductions0 = index_owner_reductions(Node),
        Restarts0 = erpc:call(Node, mnesia, system_info, [transaction_restarts]),
        Acks = [
            {PK, DN, DeliveryId}
         || DeliveryId <- DeliveryIds,
            DN <- DNs
        ],
        Started = erlang:monotonic_time(microsecond),
        Results = erpc:call(Node, emqx_bcast_storage, process_ack_batch, [Acks]),
        ElapsedUs = erlang:monotonic_time(microsecond) - Started,
        ?assertEqual([counted || _ <- Acks], Results),
        ?assert(wait_pipeline_drained(Node, 600)),
        Stats = stop_mnesia_perf(Node),
        Reductions1 = index_owner_reductions(Node),
        Restarts1 = erpc:call(Node, mnesia, system_info, [transaction_restarts]),
        ReductionsPerPart = (Reductions1 - Reductions0) div PartReports,
        ct:pal(
            "P1_SCALED_BURST deliveries=~p shards=~p part_reports=~p "
            "elapsed_us=~p reductions_per_part=~p transaction_restarts=~p",
            [
                ?MNESIA_BURST_DELIVERIES,
                ShardCount,
                PartReports,
                ElapsedUs,
                ReductionsPerPart,
                Restarts1 - Restarts0
            ]
        ),
        emqx_bcast_mnesia_perf:report(<<"p1-scaled-burst">>, Stats),
        assert_p1_scaled_baseline(
            ?MNESIA_BURST_DELIVERIES,
            PartReports,
            ReductionsPerPart,
            Restarts1 - Restarts0,
            ElapsedUs,
            Stats
        )
    catch
        Class:Reason:Stacktrace ->
            _ = stop_mnesia_perf(Node),
            erlang:raise(Class, Reason, Stacktrace)
    end,
    process_flag(trap_exit, OldTrapExit).

run_perf(ApiNode, AckNode, DN, Label) ->
    Qos0 = perf_qos0(ApiNode, DN),
    Qos1 = perf_qos1(ApiNode, AckNode, DN),
    report(Label, Qos0, Qos1),
    ok.

perf_qos0(ApiNode, DN) ->
    lists:map(
        fun(_) ->
            Start = mono_us(),
            {ok, 200, _, _} = api_call(ApiNode, qos_body(DN, 0)),
            ApiUs = mono_us() - Start,
            [Msg] = recv(1),
            E2eUs = mono_us() - Start,
            ?assertEqual(?PAYLOAD, maps:get(payload, Msg)),
            {ApiUs, E2eUs}
        end,
        lists:seq(1, ?PERF_N)
    ).

perf_qos1(ApiNode, AckNode, DN) ->
    lists:map(
        fun(_) ->
            Start = mono_us(),
            {ok, 200, _, _} = api_call(ApiNode, qos_body(DN, 1)),
            ApiUs = mono_us() - Start,
            [Msg] = recv(1),
            E2eUs = mono_us() - Start,
            ?assertEqual(?PAYLOAD, maps:get(payload, Msg)),
            %% Wait until core has accounted the ack so the next iteration
            %% starts from a clean device index (no duplicate replay).
            ?assert(wait_device_ack(AckNode, <<"default">>, DN)),
            {ApiUs, E2eUs}
        end,
        lists:seq(1, ?PERF_N)
    ).

report(Label, Qos0, Qos1) ->
    {Q0ApiAvg, Q0E2eAvg, Q0E2eMax} = summarize(Qos0),
    {Q1ApiAvg, Q1E2eAvg, Q1E2eMax} = summarize(Qos1),
    ct:pal(
        "PERF ~s qos0_api_avg_us=~p qos0_e2e_avg_us=~p qos0_e2e_max_us=~p "
        "qos1_api_avg_us=~p qos1_e2e_avg_us=~p qos1_e2e_max_us=~p",
        [Label, Q0ApiAvg, Q0E2eAvg, Q0E2eMax, Q1ApiAvg, Q1E2eAvg, Q1E2eMax]
    ),
    ok.

summarize(Items) ->
    Api = [A || {A, _} <- Items],
    E2e = [E || {_, E} <- Items],
    {avg(Api), avg(E2e), lists:max(E2e)}.

avg([]) -> 0;
avg(List) -> lists:sum(List) div length(List).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

device_names(Count) ->
    [
        <<"mnesia_perf_dn_", (integer_to_binary(I))/binary>>
     || I <- lists:seq(1, Count)
    ].

connect_in_batches(Node, DNs, BatchSize) ->
    connect_in_batches(Node, DNs, BatchSize, 0).

connect_in_batches(Node, DNs, BatchSize, InitialCount) ->
    Port = emqx_cth_cluster:get_tcp_mqtt_port(Node),
    Connected =
        lists:foldl(
            fun(Batch, Acc) ->
                New = [connect_on_port(Port, DN) || DN <- Batch],
                Expected = InitialCount + length(Acc) + length(New),
                ?assert(wait_registry_size(Node, Expected)),
                timer:sleep(250),
                New ++ Acc
            end,
            [],
            chunks(DNs, BatchSize)
        ),
    lists:reverse(Connected).

connect_on_port(Port, ClientId) ->
    connect_on_port(Port, ClientId, 5).

connect_on_port(Port, ClientId, Attempts) ->
    Result =
        try
            {ok, Client} = emqtt:start_link([
                {host, "127.0.0.1"},
                {port, Port},
                {connect_timeout, 10_000},
                {clean_start, true},
                {clientid, ClientId}
            ]),
            case emqtt:connect(Client) of
                {ok, _} ->
                    case emqtt:subscribe(Client, topic(ClientId), 1) of
                        {ok, _, _} ->
                            {ok, Client};
                        Other ->
                            catch emqtt:stop(Client),
                            {error, {subscribe, Other}}
                    end;
                Other ->
                    catch emqtt:stop(Client),
                    {error, {connect, Other}}
            end
        catch
            Class:Reason -> {error, {Class, Reason}}
        end,
    case Result of
        {ok, Connected} ->
            Connected;
        {error, AttemptError} when Attempts > 1 ->
            ct:pal("mnesia perf connect retry client=~s reason=~p", [ClientId, AttemptError]),
            timer:sleep(200),
            connect_on_port(Port, ClientId, Attempts - 1);
        {error, AttemptError} ->
            ct:fail({client_connect_failed, ClientId, AttemptError})
    end.

stop_clients(Clients) ->
    lists:foreach(fun(C) -> catch emqtt:stop(C) end, Clients).

chunks([], _Size) ->
    [];
chunks(List, Size) ->
    {Head, Tail} = lists:split(min(Size, length(List)), List),
    [Head | chunks(Tail, Size)].

wait_registry_size(Node, Expected) ->
    wait_until(
        fun() ->
            erpc:call(Node, ets, info, [bcast_device_registry, size]) >= Expected
        end,
        600
    ).

disable_cleanup(Node) ->
    ok = erpc:call(
        Node, supervisor, terminate_child, [emqx_bcast_sup, emqx_bcast_cleanup]
    ).

maybe_start_mnesia_perf(Node) ->
    case os:getenv("MNESIA_PERF_INSTRUMENT") of
        "0" -> ok;
        _ -> erpc:call(Node, emqx_bcast_mnesia_perf, start, [])
    end.

stop_mnesia_perf(Node) ->
    case os:getenv("MNESIA_PERF_INSTRUMENT") of
        "0" ->
            #{
                calls => 0,
                in_bytes => 0,
                out_bytes => 0,
                ops => #{},
                hot_paths => #{},
                transaction_log_writes => 0
            };
        _ ->
            erpc:call(Node, emqx_bcast_mnesia_perf, stop, [])
    end.

publish_varied_requests(Node, DNs) ->
    Requests = split_requests(DNs, ?MNESIA_REQUEST_SIZES),
    ?assertEqual(length(DNs), lists:sum([length(Targets) || Targets <- Requests])),
    lists:foreach(
        fun({Seq, Targets}) ->
            publish_request(Node, Seq, Targets)
        end,
        lists:zip(lists:seq(1, length(Requests)), Requests)
    ).

publish_request(Node, Seq, Targets) ->
    Body = #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"default">>,
        <<"DeviceName">> => Targets,
        <<"MessageContent">> => b64(<<"mnesia_perf_", (integer_to_binary(Seq))/binary>>),
        <<"Qos">> => 1
    },
    {ok, 200, _, _} = api_call(Node, Body).

split_requests(DNs, Sizes) ->
    split_requests(DNs, Sizes, []).

split_requests(Rest, [], Acc) ->
    ?assertEqual([], Rest),
    lists:reverse(Acc);
split_requests(DNs, [Size | Rest], Acc) ->
    {Head, Tail} = lists:split(Size, DNs),
    split_requests(Tail, Rest, [Head | Acc]).

pending_delivery_count(Node) ->
    erpc:call(Node, emqx_bcast_storage, pending_delivery_count, []).

wait_pipeline_drained(_Node, 0) ->
    false;
wait_pipeline_drained(Node, Attempts) ->
    wait_until(
        fun() ->
            pending_delivery_count(Node) =:= 0 andalso
                erpc:call(Node, mnesia, table_info, [bcast_msg, size]) =:= 0
        end,
        Attempts
    ).

wait_pending_count(Node, Expected, Attempts) ->
    wait_until(fun() -> pending_delivery_count(Node) =:= Expected end, Attempts).

mnesia_recv(Count, Timeout) ->
    mnesia_recv(Count, Timeout, []).

mnesia_recv(0, _Timeout, Messages) ->
    Messages;
mnesia_recv(Count, Timeout, Messages) ->
    receive
        {publish, Msg} ->
            mnesia_recv(Count - 1, Timeout, [Msg | Messages])
    after Timeout ->
        Messages
    end.

plugin_version(Node) ->
    {ok, Vsn} = erpc:call(Node, application, get_key, [emqx_bcast, vsn]),
    list_to_binary(Vsn).

assert_mnesia_baseline(_Phase, Stats) ->
    ?assertEqual(12000, hot_path_calls(emqx_bcast_index_owner, ack_one_index, Stats)),
    ?assert(
        hot_path_calls(emqx_bcast_index_owner, report_part_accounted, Stats) >= 384
    ),
    ?assertEqual(0, hot_path_calls(emqx_bcast_index_owner, report_part_done, Stats)),
    %% Recovery notification retries must not rewrite the immutable account row.
    ?assertEqual(
        384,
        mnesia_table_metric(dirty_write, bcast_msg_part_accounted, calls, Stats)
    ),
    PartialFlushes = hot_path_calls(
        emqx_bcast_index_owner, flush_partial_ack_marker, Stats
    ),
    ?assert(maps:get(calls, Stats) - 2 * PartialFlushes =< ?MNESIA_FIXED_CALLS_MAX),
    ?assert(
        mnesia_table_metric(dirty_read, bcast_msg, calls, Stats) =<
            ?MNESIA_BATCH_MSG_READ_CALLS_MAX
    ),
    ?assert(
        mnesia_table_metric(dirty_read, bcast_msg, out_bytes, Stats) =<
            ?MNESIA_BATCH_MSG_READ_BYTES_MAX
    ),
    ?assert(maps:get(calls, Stats) > 0),
    ?assert(maps:get(calls, Stats) =< ?MNESIA_TOTAL_CALLS_MAX),
    ?assert(maps:get(in_bytes, Stats) =< ?MNESIA_IN_BYTES_MAX),
    ?assert(maps:get(out_bytes, Stats) =< ?MNESIA_OUT_BYTES_MAX),
    Ops = maps:get(ops, Stats),
    maps:foreach(
        fun(Op, MaxCalls) ->
            ?assert(mnesia_op_calls(Op, Ops) =< MaxCalls)
        end,
        ?MNESIA_OP_CALLS_MAX
    ).

mnesia_op_calls(Op, Ops) ->
    maps:get(calls, maps:get(Op, Ops, #{}), 0).

mnesia_table_metric(Op, Table, Metric, Stats) ->
    Ops = maps:get(ops, Stats),
    OpStats = maps:get(Op, Ops, #{}),
    Tables = maps:get(tables, OpStats, #{}),
    maps:get(Metric, maps:get(Table, Tables, #{}), 0).

assert_p1_scaled_baseline(
    Deliveries, PartReports, ReductionsPerPart, Restarts, ElapsedUs, Stats
) ->
    AccountedReports = hot_path_calls(
        emqx_bcast_index_owner, report_part_accounted, Stats
    ),
    LegacyReports = hot_path_calls(emqx_bcast_index_owner, report_part_done, Stats),
    ?assertEqual(PartReports, AccountedReports + LegacyReports),
    ?assert(maps:get(calls, Stats) =< ?MNESIA_BURST_CALLS_MAX),
    ?assert(maps:get(in_bytes, Stats) =< ?MNESIA_BURST_IN_BYTES_MAX),
    ?assert(maps:get(out_bytes, Stats) =< ?MNESIA_BURST_OUT_BYTES_MAX),
    ?assert(ReductionsPerPart =< ?MNESIA_BURST_REDUCTIONS_PER_PART_MAX),
    ?assert(ElapsedUs =< ?MNESIA_BURST_ELAPSED_US_MAX),
    ?assert(Restarts =< ?MNESIA_BURST_RESTARTS_MAX),
    %% 0.4.3 uses one completion transaction per delivery. A per-part
    %% transaction adds one transaction per part and must fail this budget.
    ?assert(mnesia_op_calls(transaction, maps:get(ops, Stats)) =< 2 * Deliveries).

burst_devices(PK, ShardCount) ->
    [
        begin
            Find = fun F(I) ->
                DN = <<
                    "p1_burst_",
                    (integer_to_binary(Shard))/binary,
                    "_",
                    (integer_to_binary(I))/binary
                >>,
                case emqx_bcast_index_owner:shard_of({PK, DN}) of
                    Shard -> DN;
                    _ -> F(I + 1)
                end
            end,
            Find(1)
        end
     || Shard <- lists:seq(0, ShardCount - 1)
    ].

index_owner_reductions(Node) ->
    erpc:call(Node, fun() ->
        lists:sum([
            element(
                2,
                process_info(
                    whereis(list_to_atom("emqx_bcast_index_owner_" ++ integer_to_list(Shard))),
                    reductions
                )
            )
         || Shard <- lists:seq(0, emqx_bcast_index_owner:shard_count() - 1)
        ])
    end).

hot_path_calls(Module, Function, Stats) ->
    lists:sum([
        Calls
     || {{M, F, _Arity}, Calls} <- maps:to_list(maps:get(hot_paths, Stats)),
        M =:= Module,
        F =:= Function
    ]).

qos_body(DN, Qos) ->
    #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"default">>,
        <<"DeviceName">> => [DN],
        <<"MessageContent">> => b64(?PAYLOAD),
        <<"Qos">> => Qos
    }.

api_call(Node, Body) ->
    erpc:call(Node, emqx_bcast_api, handle, [post, [<<"pub">>], #{body => Body}]).

connect(Node, ClientId) ->
    Port = emqx_cth_cluster:get_tcp_mqtt_port(Node),
    {ok, C} = emqtt:start_link([
        {host, "127.0.0.1"},
        {port, Port},
        {clean_start, true},
        {clientid, ClientId}
    ]),
    {ok, _} = emqtt:connect(C),
    C.

sub(C, Topic) ->
    emqtt:subscribe(C, Topic, 1).

topic(DN) -> <<"/default/", DN/binary, "/user/get">>.

b64(S) -> base64:encode(S).

mono_us() -> erlang:monotonic_time(microsecond).

recv(Count) ->
    recv(Count, 3_000).

recv(Count, Timeout) ->
    recv(Count, Timeout, []).

recv(0, _Timeout, Msgs) ->
    Msgs;
recv(Count, Timeout, Msgs) ->
    receive
        {publish, Msg} -> recv(Count - 1, Timeout, [Msg | Msgs])
        %% Delivery-latency budget. settle_cluster/0 runs after every cluster
        %% start so the async QoS1 pipeline (intake -> promoter -> trigger ->
        %% claim -> prepare -> deliver) is already warm; a delivery that
        %% still misses this budget is a real regression, not cold-start
        %% noise.
    after Timeout -> Msgs
    end.

wait_sub(Node, ClientId, Topic) ->
    wait_until(
        fun() ->
            erpc:call(
                Node,
                fun() ->
                    case emqx_cm:lookup_channels(ClientId) of
                        [Pid | _] ->
                            lists:any(
                                fun({Filter, _}) -> emqx_topic:match(Topic, Filter) end,
                                emqx_broker:subscriptions(Pid)
                            );
                        _ ->
                            false
                    end
                end
            )
        end,
        100
    ).

%% The connect and subscribe hooks cast into the pull shard asynchronously.
%% QoS0 reads that node-local registry, and its fanout is computed from the
%% API core's view of the global channel registry. Wait for both views so a
%% publish issued right after SUBACK cannot race either one.
wait_ready(Core, Devices) ->
    lists:foreach(
        fun({Node, DeviceName}) ->
            ?assert(wait_registered(Node, DeviceName)),
            ?assert(wait_channel_visible(Core, DeviceName)),
            ?assert(wait_plugin_running(Core, Node))
        end,
        Devices
    ).

%% QoS0 fanout and QoS1 triggers are filtered through the API core's
%% plugin_running_nodes/0 view. The probe is cached for a few seconds, so wait
%% for this node to appear before publishing; otherwise a fresh trigger can be
%% dropped even though the client and its channel are already visible.
wait_plugin_running(Core, Node) ->
    wait_until(
        fun() ->
            lists:member(Node, erpc:call(Core, emqx_bcast, plugin_running_nodes, []))
        end,
        100
    ).

wait_registered(Node, DeviceName) ->
    wait_until(
        fun() ->
            case erpc:call(Node, emqx_bcast, lookup_device, [{<<"default">>, DeviceName}]) of
                {ok, Pid} -> erpc:call(Node, erlang, is_process_alive, [Pid]);
                _ -> false
            end
        end,
        100
    ).

wait_channel_visible(Node, DeviceName) ->
    case erpc:call(Node, emqx_cm_registry, is_enabled, []) of
        false ->
            %% With the global registry disabled, qos0_fanout_nodes/1
            %% broadcasts to every node and does not need this view.
            true;
        true ->
            wait_until(
                fun() ->
                    erpc:call(Node, emqx_cm, lookup_channels, [DeviceName]) =/= []
                end,
                100
            )
    end.

%% QoS1 promotion is asynchronous. Reconnecting before the delivery reaches
%% the index races the one-shot trigger, and a missed wakeup is only retried
%% by the 5s deferred-claim sweep. Wait for the queued row so this case tests
%% replay rather than promotion timing.
wait_queued(Node, DeviceName) ->
    wait_until(
        fun() ->
            case
                erpc:call(Node, emqx_bcast_storage, get_device_deliveries, [
                    {<<"default">>, DeviceName}
                ])
            of
                {ok, [_ | _]} -> true;
                _ -> false
            end
        end,
        100
    ).

wait_channel_gone(Node, ClientId) ->
    wait_until(
        fun() ->
            erpc:call(Node, emqx_cm, lookup_channels, [ClientId]) =:= []
        end,
        100
    ).

wait_device_ack(Node, PK, DN) ->
    wait_until(
        fun() ->
            erpc:call(Node, emqx_bcast_storage, get_device_deliveries, [{PK, DN}]) =:= {ok, []}
        end,
        100
    ).

wait_until(_F, 0) ->
    false;
wait_until(F, Attempts) ->
    case F() of
        true ->
            true;
        false ->
            ct:sleep(100),
            wait_until(F, Attempts - 1)
    end.
