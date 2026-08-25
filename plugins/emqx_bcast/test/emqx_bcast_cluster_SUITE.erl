%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
%% Multi-node e2e: BatchPub cross-node delivery and replay index
%% replication across a two-core-node cluster.
-module(emqx_bcast_cluster_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("emqx_bcast.hrl").

-define(PAYLOAD, <<"cluster_test_payload">>).

all() -> emqx_common_test_helpers:all(?MODULE).

suite() -> [{timetrap, {minutes, 3}}].

init_per_testcase(Name, Config) ->
    Cluster = emqx_cth_cluster:mk_nodespecs(
        [
            {bcast_cl1, #{role => core, apps => apps()}},
            {bcast_cl2, #{role => core, apps => apps()}}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Name, Config)}
    ),
    [{cluster, Cluster} | Config].

end_per_testcase(_, Config) ->
    emqx_cth_cluster:stop(?config(cluster, Config)).

apps() ->
    [
        {emqx, #{config => #{<<"authorization">> => #{<<"no_match">> => <<"allow">>}}}},
        emqx_bcast
    ].

%% helpers

connect(Node, ClientId) ->
    Port = emqx_cth_cluster:get_tcp_mqtt_port(Node),
    {ok, C} = emqtt:start_link([
        {host, "127.0.0.1"}, {port, Port}, {clean_start, true}, {clientid, ClientId}
    ]),
    {ok, _} = emqtt:connect(C),
    C.

sub(C, Topic) ->
    emqtt:subscribe(C, Topic, 1).

recv(Count) -> recv(Count, []).
recv(0, Msgs) ->
    Msgs;
recv(Count, Msgs) ->
    receive
        {publish, Msg} -> recv(Count - 1, [Msg | Msgs])
    after 3000 -> Msgs
    end.

topic(DN) -> <<"/default/", DN/binary, "/user/get">>.

api_call(Node, Body) ->
    erpc:call(Node, emqx_bcast_api, handle, [post, [<<"pub">>], #{body => Body}]).

wait_until(Fun, Attempts) when Attempts > 0 ->
    case Fun() of
        true ->
            true;
        _ ->
            ct:sleep(100),
            wait_until(Fun, Attempts - 1)
    end;
wait_until(_Fun, 0) ->
    false.

%% tests

%% Devices connected to different nodes must all receive the message when the
%% API request lands on only one of them. Resolve happens on every node, and
%% pool workers hand the message to remote channel pids via Erlang `!`.
-doc "QoS=0 BatchPub reaches devices connected to different cluster nodes.".
t_cluster_cross_node_delivery(Config) ->
    [N1, N2] = emqx_cth_cluster:start(?config(cluster, Config)),
    DN1 = <<"cl_dn1">>,
    DN2 = <<"cl_dn2">>,
    C1 = connect(N1, DN1),
    C2 = connect(N2, DN2),
    sub(C1, topic(DN1)),
    sub(C2, topic(DN2)),
    %% The subscribe hook casts into pull_pool asynchronously; poll the
    %% subscription table on both nodes until the subscriptions landed.
    Subscribed =
        wait_until(
            fun() ->
                erpc:call(N1, emqx_bcast_subscription, match, [DN1, topic(DN1)]) =/= false andalso
                    erpc:call(N2, emqx_bcast_subscription, match, [DN2, topic(DN2)]) =/= false
            end,
            50
        ),
    ?assert(Subscribed),
    {ok, 200, _, _Resp} = api_call(N1, #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"default">>,
        <<"DeviceName">> => [DN1, DN2],
        <<"MessageContent">> => base64:encode(?PAYLOAD),
        <<"Qos">> => 0
    }),
    Msgs1 = recv(1),
    Msgs2 = recv(1),
    ?assertEqual(1, length(Msgs1)),
    ?assertEqual(1, length(Msgs2)),
    ?assertMatch(#{payload := ?PAYLOAD}, hd(Msgs1)),
    ?assertMatch(#{payload := ?PAYLOAD}, hd(Msgs2)),
    emqtt:stop(C1),
    emqtt:stop(C2).

%% A QoS=1 delivery created via node1 must be replayable on node2: the
%% delivery record replicates through the mria shard, and the replay index
%% entry propagates to every node's local ETS via rpc cast.
-doc "QoS=1 delivery created on one node is replayed after reconnect on another node.".
t_cluster_offline_replay_on_other_node(Config) ->
    [N1, N2] = emqx_cth_cluster:start(?config(cluster, Config)),
    DN = <<"cl_offline_dn">>,
    PK = <<"default">>,
    {ok, 200, _, _Resp} = api_call(N1, #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => PK,
        <<"DeviceName">> => [DN],
        <<"MessageContent">> => base64:encode(?PAYLOAD),
        <<"Qos">> => 1
    }),
    %% Index propagation and mria replication are async; wait until node2 can
    %% see both the index entry and the delivery record locally.
    Indexed =
        wait_until(
            fun() ->
                case erpc:call(N2, emqx_bcast_storage, get_device_deliveries, [{PK, DN}]) of
                    {ok, [_ | _] = DeliveryIds} ->
                        [] =/= erpc:call(N2, mnesia, dirty_read, [bcast_msg, hd(DeliveryIds)]);
                    _ ->
                        false
                end
            end,
            50
        ),
    ?assert(Indexed),
    C = connect(N2, DN),
    sub(C, topic(DN)),
    Msgs = recv(1),
    ?assertEqual(1, length(Msgs)),
    ?assertMatch(#{payload := ?PAYLOAD}, hd(Msgs)),
    emqtt:stop(C).

%% A QoS=1 delivery for D1 and D2 created while both are offline, replayed and
%% acked by D1 on node1, must not be replayed again when D1 reconnects on
%% node2: ack state must be cluster-wide, not node-local.
-doc "Ack state is cluster-wide: a delivery acked on one node is not replayed on another.".
t_cluster_ack_idempotent_across_nodes(Config) ->
    [N1, N2] = emqx_cth_cluster:start(?config(cluster, Config)),
    DN1 = <<"cl_ack_dn1">>,
    DN2 = <<"cl_ack_dn2">>,
    PK = <<"default">>,
    {ok, 200, _, _} = api_call(N1, #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => PK,
        <<"DeviceName">> => [DN1, DN2],
        <<"MessageContent">> => base64:encode(?PAYLOAD),
        <<"Qos">> => 1
    }),
    %% Index propagation and mria replication are async; wait until node2 can
    %% see the index entry locally.
    Indexed =
        wait_until(
            fun() ->
                case erpc:call(N2, emqx_bcast_storage, get_device_deliveries, [{PK, DN1}]) of
                    {ok, [_ | _]} -> true;
                    _ -> false
                end
            end,
            50
        ),
    ?assert(Indexed),
    C1 = connect(N1, DN1),
    sub(C1, topic(DN1)),
    Msgs1 = recv(1),
    ?assertEqual(1, length(Msgs1)),
    %% D1 acks on node1; wait until node1 has processed the ack and removed
    %% the local index entry.
    Acked =
        wait_until(
            fun() ->
                {ok, []} =:= erpc:call(N1, emqx_bcast_storage, get_device_deliveries, [{PK, DN1}]) andalso
                    {ok, []} =:=
                        erpc:call(N2, emqx_bcast_storage, get_device_deliveries, [{PK, DN1}])
            end,
            50
        ),
    ?assert(Acked),
    emqtt:stop(C1),
    C2 = connect(N2, DN1),
    sub(C2, topic(DN1)),
    %% node2's local index still holds the entry, so with a node-local ack
    %% state it replays the already-acked delivery once more.
    Msgs2 = recv(1),
    ?assertEqual(0, length(Msgs2)),
    emqtt:stop(C2).
