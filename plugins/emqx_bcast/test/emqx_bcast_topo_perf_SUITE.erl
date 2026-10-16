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

all() -> [t_1c2r_e2e, t_3c_e2e, t_perf_1c2r, t_perf_3c].

suite() -> [{timetrap, {minutes, 10}}].

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
    settle_cluster(),
    DN1 = <<"topo_1c2r_dn1">>,
    DN2 = <<"topo_1c2r_dn2">>,
    C1 = connect(Rep1, DN1),
    C2 = connect(Rep2, DN2),
    try
        sub(C1, topic(DN1)),
        sub(C2, topic(DN2)),
        ?assert(wait_sub(Rep1, DN1, topic(DN1))),
        ?assert(wait_sub(Rep2, DN2, topic(DN2))),

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
        C1b = connect(Rep2, DN1),
        sub(C1b, topic(DN1)),
        ?assert(wait_sub(Rep2, DN1, topic(DN1))),
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
    settle_cluster(),
    DN1 = <<"topo_3c_dn1">>,
    DN2 = <<"topo_3c_dn2">>,
    Client1 = connect(C1, DN1),
    Client2 = connect(C2, DN2),
    try
        sub(Client1, topic(DN1)),
        sub(Client2, topic(DN2)),
        ?assert(wait_sub(C1, DN1, topic(DN1))),
        ?assert(wait_sub(C2, DN2, topic(DN2))),

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
        [M1b] = recv(1),
        [M2b] = recv(1),
        ?assertEqual(?PAYLOAD, maps:get(payload, M1b)),
        ?assertEqual(?PAYLOAD, maps:get(payload, M2b)),

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
        Client1b = connect(C3, DN1),
        sub(Client1b, topic(DN1)),
        ?assert(wait_sub(C3, DN1, topic(DN1))),
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
    [Core, Rep1, _Rep2] = emqx_cth_cluster:start(?config(cluster, Config)),
    settle_cluster(),
    DN = <<"perf_1c2r_dn">>,
    C = connect(Rep1, DN),
    try
        sub(C, topic(DN)),
        ?assert(wait_sub(Rep1, DN, topic(DN))),
        run_perf(Core, Core, DN, <<"1core-2replicant">>)
    after
        catch emqtt:stop(C)
    end.

-doc "Latency probe: QoS=0 and QoS=1 round trips in a 3-core cluster.".
t_perf_3c(Config) ->
    [C1, _C2, C3] = emqx_cth_cluster:start(?config(cluster, Config)),
    settle_cluster(),
    DN = <<"perf_3c_dn">>,
    C = connect(C1, DN),
    try
        sub(C, topic(DN)),
        ?assert(wait_sub(C1, DN, topic(DN))),
        run_perf(C3, C1, DN, <<"3core">>)
    after
        catch emqtt:stop(C)
    end.

%% Cold-start settle: the async QoS1 pipeline runs at full speed only
%% after mria converges and the plugin workers are warm. The tests used to
%% start timing right after emqx_cth_cluster:start/1, so the first QoS1
%% round-trip could exceed the recv budget on a cold multi-node cluster
%% (t_3c_e2e / t_perf_1c2r flakes on shared machines). Settle before
%% measuring so the delivery-latency numbers are meaningful. (The test
%% process runs on the CT node, not a cluster node, so this is a plain
%% settle sleep; emqx_cth_cluster:start/1 already waited for the nodes.)
settle_cluster() ->
    timer:sleep(3000).

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

recv(Count) -> recv(Count, []).
recv(0, Msgs) ->
    Msgs;
recv(Count, Msgs) ->
    receive
        {publish, Msg} -> recv(Count - 1, [Msg | Msgs])
        %% Delivery-latency budget. settle_cluster/0 runs after every cluster
        %% start so the async QoS1 pipeline (intake -> promoter -> trigger ->
        %% claim -> prepare -> deliver) is already warm; a delivery that
        %% still misses this budget is a real regression, not cold-start
        %% noise.
    after 3000 -> Msgs
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
