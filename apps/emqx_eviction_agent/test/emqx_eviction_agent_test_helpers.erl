%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_eviction_agent_test_helpers).

-export([
    emqtt_connect/0,
    emqtt_connect/1,
    emqtt_connect/2,
    emqtt_connect_for_publish/1,
    emqtt_connect_many/2,
    emqtt_connect_many/3,
    stop_many/1,

    emqtt_try_connect/1,

    start_cluster/3,
    stop_cluster/1,

    case_specific_node_name/2,
    case_specific_node_name/3,
    concat_atoms/1,

    get_mqtt_port/2,
    nodes_with_mqtt_tcp_ports/1
]).

emqtt_connect() ->
    emqtt_connect(<<"client1">>, true).

emqtt_connect(ClientId, CleanStart) ->
    emqtt_connect([{clientid, ClientId}, {clean_start, CleanStart}]).

emqtt_connect(Opts) ->
    {ok, C} = emqtt:start_link(
        Opts ++
            [
                {proto_ver, v5},
                {properties, #{'Session-Expiry-Interval' => 600}}
            ]
    ),
    case emqtt:connect(C) of
        {ok, _} -> {ok, C};
        {error, _} = Error -> Error
    end.

emqtt_connect_for_publish(Port) ->
    ClientId = <<"pubclient-", (integer_to_binary(erlang:unique_integer([positive])))/binary>>,
    {ok, C} = emqtt:start_link([{clientid, ClientId}, {port, Port}]),
    case emqtt:connect(C) of
        {ok, _} -> {ok, C};
        {error, _} = Error -> Error
    end.

emqtt_connect_many(Port, Count) ->
    emqtt_connect_many(Port, Count, _StartN = 1).

emqtt_connect_many(Port, Count, StartN) ->
    lists:map(
        fun(N) ->
            NBin = integer_to_binary(N),
            ClientId = <<"client-", NBin/binary>>,
            {ok, C} = emqtt_connect([{clientid, ClientId}, {clean_start, false}, {port, Port}]),
            C
        end,
        lists:seq(StartN, StartN + Count - 1)
    ).

stop_many(Clients) ->
    lists:foreach(
        fun(C) ->
            catch emqtt:disconnect(C)
        end,
        Clients
    ),
    ct:sleep(100).

emqtt_try_connect(Opts) ->
    case emqtt_connect(Opts) of
        {ok, C} ->
            emqtt:disconnect(C),
            ok;
        {error, _} = Error ->
            Error
    end.

start_cluster(Config, NodeNames = [Node1 | _], Apps) ->
    Spec = #{
        role => core,
        join_to => emqx_cth_cluster:node_name(Node1),
        listeners => true,
        apps => Apps
    },
    Cluster = [{NodeName, Spec} || NodeName <- NodeNames],
    ClusterNodes = emqx_cth_cluster:start(
        Cluster,
        %% Use Node1 to scope the work dirs for all the nodes
        #{work_dir => emqx_cth_suite:work_dir(Node1, Config)}
    ),
    nodes_with_mqtt_tcp_ports(ClusterNodes).

stop_cluster(NamesWithPorts) ->
    {Nodes, _Ports} = lists:unzip(NamesWithPorts),
    ok = emqx_cth_cluster:stop(Nodes).

case_specific_node_name(Module, Case) ->
    concat_atoms([Module, '__', Case]).

case_specific_node_name(Module, Case, Node) ->
    concat_atoms([Module, '__', Case, '__', Node]).

concat_atoms(Atoms) ->
    binary_to_atom(
        iolist_to_binary(
            lists:map(
                fun atom_to_binary/1,
                Atoms
            )
        )
    ).

get_mqtt_port(Node, Type) ->
    {_IP, Port} = erpc:call(Node, emqx_config, get, [[listeners, Type, default, bind]]),
    Port.

nodes_with_mqtt_tcp_ports(Nodes) ->
    lists:map(
        fun(Node) ->
            {Node, get_mqtt_port(Node, tcp)}
        end,
        Nodes
    ).
