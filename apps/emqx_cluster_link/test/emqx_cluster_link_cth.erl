%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster_link_cth).

-compile(export_all).
-compile(nowarn_export_all).

%%

-define(BASE_CLINK_MQTT_PORT, 1883).
-define(BASE_CLUSTER_NODE_PORT, 10000).

-doc """
Construct `emqx_cth_cluster`'s specs for a Cluster-Link-ready cluster.
* Argument `N` is a "number" of a cluster being specced, to ensure no port collisions.
* Argument `ExtraConf` is a snippet of `emqx_conf` configuration according to the schema.
* Special `clink` MQTT listener is configured to start _on the first node only_.
""".
mk_cluster(N, ClusterName, Size, ExtraConf, CTConfig) when is_integer(Size) ->
    mk_cluster(N, ClusterName, lists:duplicate(Size, #{}), ExtraConf, CTConfig);
mk_cluster(N, ClusterName, BaseSpecs, ExtraConf, CTConfig) when is_list(BaseSpecs) ->
    Specs = [
        mk_cluster_nodespec(N, ClusterName, S, I, ExtraConf)
     || {I, S} <- lists:enumerate(BaseSpecs)
    ],
    emqx_cth_cluster:mk_nodespecs(
        Specs,
        #{work_dir => emqx_cth_suite:work_dir(CTConfig)}
    ).

mk_cluster_nodespec(N, ClusterName, BaseSpec, NodeI, ExtraConf) ->
    Conf = mk_emqx_conf(N, ClusterName, NodeI, ExtraConf),
    Spec = BaseSpec#{
        apps => [{emqx_conf, Conf}, emqx_cluster_link],
        base_port => N * ?BASE_CLUSTER_NODE_PORT + NodeI * 100
    },
    {mk_nodename(ClusterName, NodeI), Spec}.

mk_emqx_conf(N, ClusterName, _NodeI = 1, ExtraConf) ->
    MQTTPort = ?BASE_CLINK_MQTT_PORT + N * 10000,
    ListenerConf = conf_mqtt_listener(MQTTPort),
    combine_conf([conf_cluster(ClusterName), ListenerConf, ExtraConf]);
mk_emqx_conf(_, ClusterName, _NodeI, ExtraConf) ->
    combine_conf([conf_cluster(ClusterName), ExtraConf]).

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

%%

-doc """
Construct a Cluster Link config towards `Cluster`.
Cluster is usually started from specs produced by `mk_cluster/5`. Specifically, at least
one node is expected to have dedicated `clink` MQTT listener.
""".
mk_link_conf_to(Cluster = [Node | _], Overrides) ->
    Name = cluster_name(Node),
    LPorts = [P || N <- Cluster, P <- [tcp_port(N, clink, undefined)], P =/= undefined],
    maps:merge(
        #{
            <<"enable">> => true,
            <<"name">> => emqx_utils_conv:bin(Name),
            <<"topics">> => [<<"#">>],
            <<"server">> => iolist_to_binary(
                lists:join(",", [["localhost:", integer_to_list(P)] || P <- LPorts])
            ),
            <<"pool_size">> => 1
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
