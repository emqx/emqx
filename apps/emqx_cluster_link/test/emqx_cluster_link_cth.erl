%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster_link_cth).

-compile(export_all).
-compile(nowarn_export_all).

-type cluster_seqnum() :: 1..10.
-type cluster_name() :: atom() | binary().

%%

-define(BASE_CLINK_MQTT_PORT, 1883).
-define(BASE_CLUSTER_NODE_PORT, 10000).

-doc """
Construct `emqx_cth_cluster`'s specs for a Cluster-Link-ready cluster.
* Argument `N` is a "number" of a cluster being specced, to ensure no port collisions.
* Special `clink` MQTT listener is configured to start _on the first node only_.
""".
-spec mk_cluster(
    cluster_seqnum(),
    cluster_name(),
    _Size :: integer() | {integer(), _NodeSpec :: #{atom() => _}} | [_NodeSpec :: #{atom() => _}],
    _CTConfig :: proplists:proplist()
) ->
    [emqx_cth_cluster:bakedspec()].
mk_cluster(N, ClusterName, Size, CTConfig) when is_integer(Size) ->
    mk_cluster(N, ClusterName, lists:duplicate(Size, #{}), CTConfig);
mk_cluster(N, ClusterName, {Size, BaseSpec}, CTConfig) when is_integer(Size) ->
    mk_cluster(N, ClusterName, lists:duplicate(Size, BaseSpec), CTConfig);
mk_cluster(N, ClusterName, BaseSpecs, CTConfig) when is_list(BaseSpecs) ->
    Specs = [
        mk_cluster_nodespec(N, ClusterName, S, I)
     || {I, S} <- lists:enumerate(BaseSpecs)
    ],
    emqx_cth_cluster:mk_nodespecs(
        Specs,
        #{work_dir => emqx_cth_suite:work_dir(CTConfig)}
    ).

mk_cluster_nodespec(N, ClusterName, BaseSpec, NodeI) ->
    Conf = mk_emqx_conf(N, ClusterName, NodeI),
    Spec = #{
        apps => [
            {emqx_conf, Conf},
            {emqx_cluster_link, #{override_env => [{routerepl_actor_reconnect_timeout, 1000}]}}
        ],
        base_port => N * ?BASE_CLUSTER_NODE_PORT + NodeI * 100
    },
    SpecMerged = maps:merge_with(fun merge_nodespec/3, BaseSpec, Spec),
    {mk_nodename(ClusterName, NodeI), SpecMerged}.

merge_nodespec(apps, BaseApps, Apps) ->
    MergedApps = [
        {A, emqx_cth_suite:merge_appspec(proplists:get_value(A, BaseApps, #{}), Opts)}
     || {A, Opts} <- Apps
    ],
    RestApps = lists:foldl(fun proplists:delete/2, BaseApps, proplists:get_keys(Apps)),
    MergedApps ++ RestApps;
merge_nodespec(_, _, V) ->
    V.

mk_emqx_conf(N, ClusterName, _NodeI = 1) ->
    ListenerConf = conf_mqtt_listener(mqtt_listener_port(N)),
    combine_conf([conf_cluster(ClusterName), ListenerConf]);
mk_emqx_conf(_, ClusterName, _NodeI) ->
    conf_cluster(ClusterName).

mk_nodename(BaseName, Idx) ->
    binary_to_atom(emqx_utils:format("emqx_clink_~s~b", [BaseName, Idx])).

conf_mqtt_listener(LPort) when is_integer(LPort) ->
    emqx_utils:format("listeners.tcp.clink { bind = ~p }", [LPort]);
conf_mqtt_listener(_) ->
    "".

conf_cluster(ClusterName) ->
    emqx_utils:format("cluster.name = ~s", [ClusterName]).

combine_conf([Entry | Rest]) ->
    lists:foldl(fun emqx_cth_suite:merge_config/2, Entry, Rest).

mqtt_listener_port(N) ->
    ?BASE_CLINK_MQTT_PORT + N * 10000.

%%

-doc """
Construct a Cluster Link config towards a cluster to be instantiated through `mk_cluster/5`.
""".
-spec mk_link_conf(cluster_seqnum(), cluster_name(), #{binary() => _}) ->
    _LinkConfig :: #{binary() => _}.
mk_link_conf(NTarget, TargetClusterName, Overrides) ->
    LPort = mqtt_listener_port(NTarget),
    link_conf(TargetClusterName, [LPort], Overrides).

-doc """
Construct a Cluster Link config towards `Cluster`.
Cluster is usually started from specs produced by `mk_cluster/5`. Specifically, at least
one node is expected to have dedicated `clink` MQTT listener.
""".
-spec mk_link_conf_to([node()], #{binary() => _}) ->
    _LinkConfig :: #{binary() => _}.
mk_link_conf_to(Cluster = [Node | _], Overrides) ->
    Name = cluster_name(Node),
    LPorts = [P || N <- Cluster, P <- [tcp_port(N, clink, undefined)], P =/= undefined],
    link_conf(Name, LPorts, Overrides).

link_conf(Name, LPorts, Overrides) ->
    maps:merge(
        #{
            <<"enable">> => true,
            <<"name">> => emqx_utils_conv:bin(Name),
            <<"topics">> => [<<"#">>],
            <<"server">> => iolist_to_binary(
                lists:join(",", [["localhost:", integer_to_list(P)] || P <- LPorts])
            ),
            <<"pool_size">> => 1,
            <<"resource_opts">> => #{<<"health_check_interval">> => 1000}
        },
        Overrides
    ).

%%

-spec connect_client_unlink(_ClientID :: binary(), node()) -> _Pid :: emqtt:client().
connect_client_unlink(ClientId, Node) ->
    Client = connect_client(ClientId, Node),
    _ = erlang:unlink(Client),
    Client.

-spec connect_client(_ClientID :: binary(), node()) -> _Pid :: emqtt:client().
connect_client(ClientId, Node) ->
    Port = tcp_port(Node),
    {ok, Client} = emqtt:start_link([{proto_ver, v5}, {clientid, ClientId}, {port, Port}]),
    {ok, _} = emqtt:connect(Client),
    Client.

-spec disconnect_client(emqtt:client()) -> ok.
disconnect_client(Pid) ->
    emqtt:disconnect(Pid).

%%

cluster_name(Node) ->
    erpc:call(Node, emqx_config, get, [[cluster, name]]).

tcp_port(Node) ->
    tcp_port(Node, default).

tcp_port(Node, Listener) ->
    get_bind_port(erpc:call(Node, emqx_config, get, [[listeners, tcp, Listener, bind]])).

tcp_port(Node, Listener, Default) ->
    Conf = erpc:call(Node, emqx_config, get, [[listeners, tcp, Listener, bind], Default]),
    emqx_maybe:apply(fun get_bind_port/1, Conf).

get_bind_port({_Host, Port}) ->
    Port;
get_bind_port(Port) when is_integer(Port) ->
    Port.
