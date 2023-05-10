%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_eviction_agent_test_helpers).

-export([
    emqtt_connect/0,
    emqtt_connect/1,
    emqtt_connect/2,
    emqtt_connect_many/2,
    stop_many/1,

    emqtt_try_connect/1,

    start_cluster/2,
    start_cluster/3,
    stop_cluster/2,

    case_specific_node_name/2,
    case_specific_node_name/3,
    concat_atoms/1
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

emqtt_connect_many(Port, Count) ->
    lists:map(
        fun(N) ->
            NBin = integer_to_binary(N),
            ClientId = <<"client-", NBin/binary>>,
            {ok, C} = emqtt_connect([{clientid, ClientId}, {clean_start, false}, {port, Port}]),
            C
        end,
        lists:seq(1, Count)
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

start_cluster(NamesWithPorts, Apps) ->
    start_cluster(NamesWithPorts, Apps, []).

start_cluster(NamesWithPorts, Apps, Env) ->
    Specs = lists:map(
        fun({ShortName, Port}) ->
            {core, ShortName, #{listener_ports => [{tcp, Port}]}}
        end,
        NamesWithPorts
    ),
    Opts0 = [
        {env, [{emqx, boot_modules, [broker, listeners]}] ++ Env},
        {apps, Apps},
        {conf,
            [{[listeners, Proto, default, enabled], false} || Proto <- [ssl, ws, wss]] ++
                [{[rpc, mode], async}]}
    ],
    Cluster = emqx_common_test_helpers:emqx_cluster(
        Specs,
        Opts0
    ),
    NodesWithPorts = [
        {
            emqx_common_test_helpers:start_slave(Name, Opts),
            proplists:get_value(Name, NamesWithPorts)
        }
     || {Name, Opts} <- Cluster
    ],
    NodesWithPorts.

stop_cluster(NodesWithPorts, Apps) ->
    lists:foreach(
        fun({Node, _Port}) ->
            lists:foreach(
                fun(App) ->
                    rpc:call(Node, application, stop, [App])
                end,
                Apps
            ),
            %% This sleep is just to make logs cleaner
            ct:sleep(100),
            _ = rpc:call(Node, emqx_common_test_helpers, stop_apps, []),
            emqx_common_test_helpers:stop_slave(Node)
        end,
        NodesWithPorts
    ).

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
