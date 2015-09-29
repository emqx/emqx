%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2012-2015 eMQTT.IO, All Rights Reserved.
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a copy
%%% of this software and associated documentation files (the "Software"), to deal
%%% in the Software without restriction, including without limitation the rights
%%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%%% copies of the Software, and to permit persons to whom the Software is
%%% furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in all
%%% copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%%% SOFTWARE.
%%%-----------------------------------------------------------------------------
%%% @doc
%%% emqttd control commands.
%%%
%%% @end
%%%-----------------------------------------------------------------------------

-module(emqttd_ctl).

-author("Feng Lee <feng@emqtt.io>").

-include("emqttd.hrl").

-include("emqttd_cli.hrl").

-export([init/0,
         register_cmd/3,
         unregister_cmd/1,
         run/1]).

-export([status/1,
         vm/1,
         stats/1,
         metrics/1,
         cluster/1,
         clients/1,
         sessions/1,
         listeners/1,
         bridges/1]).

-define(CMD_TAB, mqttd_ctl_cmd).

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc Init cmd table.
%% @end
%%------------------------------------------------------------------------------
init() ->
    ets:new(?CMD_TAB, [ordered_set, named_table, public]),
    register_cmd(status,  {?MODULE, status}, []),
    register_cmd(vm,      {?MODULE, vm}, []),
    register_cmd(cluster, {?MODULE, cluster}, []).

%%------------------------------------------------------------------------------
%% @doc Register a command
%% @end
%%------------------------------------------------------------------------------
-spec register_cmd(atom(), {module(), atom()}, list()) -> true.
register_cmd(Cmd, MF, Opts) ->
    ets:insert(?CMD_TAB, {Cmd, MF, Opts}).

%%------------------------------------------------------------------------------
%% @doc Unregister a command
%% @end
%%------------------------------------------------------------------------------
-spec unregister_cmd(atom()) -> true.
unregister_cmd(Cmd) ->
    ets:delete(?CMD_TAB, Cmd).

%%------------------------------------------------------------------------------
%% @doc Run a command
%% @end
%%------------------------------------------------------------------------------

run([]) -> usage();

run([CmdS|Args]) ->
    case ets:lookup(?CMD_TAB, list_to_atom(CmdS)) of
        [{_, {Mod, Fun}, _}] -> Mod:Fun(Args);
        [] -> usage() 
    end.
    
%%------------------------------------------------------------------------------
%% @doc Usage
%% @end
%%------------------------------------------------------------------------------
usage() ->
    ?PRINT("Usage: ~s~n", [?MODULE]),
    [Mod:Cmd(["help"]) || {_, {Mod, Cmd}, _} <- ets:tab2list(?CMD_TAB)].

%%%=============================================================================
%%% Commands
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc Query node status
%% @end
%%------------------------------------------------------------------------------
status([]) ->
    {InternalStatus, _ProvidedStatus} = init:get_status(),
    ?PRINT("Node ~p is ~p~n", [node(), InternalStatus]),
    case lists:keysearch(emqttd, 1, application:which_applications()) of
    false ->
        ?PRINT_MSG("emqttd is not running~n");
    {value,_Version} ->
        ?PRINT_MSG("emqttd is running~n")
    end;
status(_) ->
     ?PRINT_CMD("status", "#query broker status").

vm([]) ->
    vm(["all"]);

vm(["all"]) ->
    [begin vm([Name]), ?PRINT_MSG("~n") end || Name <- ["load", "memory", "process", "io"]];

vm(["load"]) ->
    [?PRINT("cpu/~-20s~s~n", [L, V]) || {L, V} <- emqttd_vm:loads()];

vm(["memory"]) ->
    [?PRINT("memory/~-17s~w~n", [Cat, Val]) || {Cat, Val} <- erlang:memory()];

vm(["process"]) ->
    lists:foreach(fun({Name, Key}) ->
        ?PRINT("process/~-16s~w~n", [Name, erlang:system_info(Key)])
    end, [{limit, process_limit}, {count, process_count}]);

vm(["io"]) ->
    ?PRINT("io/~-21s~w~n", [max_fds, proplists:get_value(max_fds, erlang:system_info(check_io))]);

vm(_) ->
    ?PRINT_CMD("vm all",     "#query info of erlang vm"),
    ?PRINT_CMD("vm load",    "#query load of erlang vm"),
    ?PRINT_CMD("vm memory",  "#query memory of erlang vm"),
    ?PRINT_CMD("vm process", "#query process of erlang vm"),
    ?PRINT_CMD("vm io",      "#queue io of erlang vm").

%%------------------------------------------------------------------------------
%% @doc Cluster with other node
%% @end
%%------------------------------------------------------------------------------
cluster([]) ->
    Nodes = emqttd_broker:running_nodes(),
    ?PRINT("cluster nodes: ~p~n", [Nodes]);

cluster([SNode]) ->
    Node = node_name(SNode),
    case net_adm:ping(Node) of
    pong ->
        case emqttd:is_running(Node) of
            true ->
                emqttd_plugins:unload(),
                application:stop(emqttd),
                application:stop(esockd),
                application:stop(gproc),
                emqttd_mnesia:cluster(Node),
                application:start(gproc),
                application:start(esockd),
                application:start(emqttd),
                ?PRINT("cluster with ~p successfully.~n", [Node]);
            false ->
                ?PRINT("emqttd is not running on ~p~n", [Node])
        end;
    pang ->
        ?PRINT("failed to connect to ~p~n", [Node])
    end;

cluster(_) ->
    ?PRINT_CMD("cluster [<Node>]", "#cluster with node, query cluster info ").


stats([]) ->
    [?PRINT("~s: ~p~n", [Stat, Val]) || {Stat, Val} <- emqttd_stats:getstats()].
    
metrics([]) ->
    [?PRINT("~s: ~p~n", [Metric, Val]) || {Metric, Val} <- emqttd_metrics:all()].

clients(["list"]) ->
    dump(client, mqtt_client);

clients(["show", ClientId]) ->
    case emqttd_cm:lookup(list_to_binary(ClientId)) of
        undefined ->
            ?PRINT_MSG("Not Found.~n");
        Client -> 
            print(client, Client)
    end;

clients(["kick", ClientId]) ->
    case emqttd_cm:lookup(list_to_binary(ClientId)) of
        undefined ->
            ?PRINT_MSG("Not Found.~n");
        #mqtt_client{client_pid = Pid} -> 
            emqttd_client:kick(Pid)
    end.

sessions(["list"]) ->
    dump(session, mqtt_transient_session),
    dump(session, mqtt_persistent_session);

sessions(["show", ClientId]) ->
    MP = {{list_to_binary(ClientId), '_'}, '_'},
    case {ets:match_object(mqtt_transient_session, MP),
          ets:match_object(mqtt_persistent_session, MP)} of
        {[], []} ->
            ?PRINT_MSG("Not Found.~n");
        {[SessInfo], _} ->
            print(session, SessInfo);
        {_, [SessInfo]} ->
            print(session, SessInfo)
    end.
    
listeners([]) ->
    lists:foreach(fun({{Protocol, Port}, Pid}) ->
                ?PRINT("listener ~s:~w~n", [Protocol, Port]),
                ?PRINT("  acceptors: ~w~n", [esockd:get_acceptors(Pid)]),
                ?PRINT("  max_clients: ~w~n", [esockd:get_max_clients(Pid)]),
                ?PRINT("  current_clients: ~w~n", [esockd:get_current_clients(Pid)]),
                ?PRINT("  shutdown_count: ~p~n", [esockd:get_shutdown_count(Pid)])
        end, esockd:listeners()).

bridges(["list"]) ->
    lists:foreach(fun({{Node, Topic}, _Pid}) ->
                ?PRINT("bridge: ~s ~s~n", [Node, Topic])
        end, emqttd_bridge_sup:bridges());

bridges(["options"]) ->
    ?PRINT_MSG("Options:~n"),
    ?PRINT_MSG("  qos     = 0 | 1 | 2~n"),
    ?PRINT_MSG("  prefix  = string~n"),
    ?PRINT_MSG("  suffix  = string~n"),
    ?PRINT_MSG("  queue   = integer~n"),
    ?PRINT_MSG("Example:~n"),
    ?PRINT_MSG("  qos=2,prefix=abc/,suffix=/yxz,queue=1000~n");

bridges(["start", SNode, Topic]) ->
    case emqttd_bridge_sup:start_bridge(list_to_atom(SNode), bin(Topic)) of
        {ok, _} -> ?PRINT_MSG("bridge is started.~n");
        {error, Error} -> ?PRINT("error: ~p~n", [Error])
    end;

bridges(["start", SNode, Topic, OptStr]) ->
    Opts = parse_opts(bridge, OptStr),
    case emqttd_bridge_sup:start_bridge(list_to_atom(SNode), bin(Topic), Opts) of
        {ok, _} -> ?PRINT_MSG("bridge is started.~n");
        {error, Error} -> ?PRINT("error: ~p~n", [Error])
    end;

bridges(["stop", SNode, Topic]) ->
    case emqttd_bridge_sup:stop_bridge(list_to_atom(SNode), bin(Topic)) of
        ok -> ?PRINT_MSG("bridge is stopped.~n");
        {error, Error} -> ?PRINT("error: ~p~n", [Error])
    end.


node_name(SNode) ->
    SNode1 =
    case string:tokens(SNode, "@") of
    [_Node, _Server] ->
        SNode;
    _ ->
        case net_kernel:longnames() of
         true ->
             SNode ++ "@" ++ inet_db:gethostname() ++
                  "." ++ inet_db:res_option(domain);
         false ->
             SNode ++ "@" ++ inet_db:gethostname();
         _ ->
             SNode
         end
    end,
    list_to_atom(SNode1).

bin(S) when is_list(S) -> list_to_binary(S);
bin(B) when is_binary(B) -> B.

parse_opts(Cmd, OptStr) ->
    Tokens = string:tokens(OptStr, ","),
    [parse_opt(Cmd, list_to_atom(Opt), Val)
        || [Opt, Val] <- [string:tokens(S, "=") || S <- Tokens]].

parse_opt(bridge, qos, Qos) ->
    {qos, list_to_integer(Qos)};
parse_opt(bridge, suffix, Suffix) ->
    {topic_suffix, list_to_binary(Suffix)};
parse_opt(bridge, prefix, Prefix) ->
    {topic_prefix, list_to_binary(Prefix)};
parse_opt(bridge, queue, Len) ->
    {max_queue_len, list_to_integer(Len)};
parse_opt(_Cmd, Opt, _Val) ->
    ?PRINT("Bad Option: ~s~n", [Opt]).

dump(Type, Table) ->
    dump(Type, Table, ets:first(Table)).

dump(_Type, _Table, '$end_of_table') ->
    ok;
dump(Type, Table, Key) ->
    case ets:lookup(Table, Key) of
        [Record] -> print(Type, Record);
        [] -> ignore
    end,
    dump(Type, Table, ets:next(Table, Key)).

print(client, #mqtt_client{client_id = ClientId, clean_sess = CleanSess,
                           username = Username, peername = Peername,
                           connected_at = ConnectedAt}) ->
    ?PRINT("Client(~s, clean_sess=~s, username=~s, peername=~s, connected_at=~p)~n",
            [ClientId, CleanSess, Username,
             emqttd_net:format(Peername),
             emqttd_util:now_to_secs(ConnectedAt)]);

print(session, {{ClientId, _ClientPid}, SessInfo}) ->
    InfoKeys = [clean_sess, 
                max_inflight,
                inflight_queue,
                message_queue,
                message_dropped,
                awaiting_rel,
                awaiting_ack,
                awaiting_comp,
                created_at,
                subscriptions],
    ?PRINT("Session(~s, clean_sess=~s, max_inflight=~w, inflight_queue=~w, "
           "message_queue=~w, message_dropped=~w, "
           "awaiting_rel=~w, awaiting_ack=~w, awaiting_comp=~w, "
           "created_at=~w, subscriptions=~s)~n",
            [ClientId | [format(Key, proplists:get_value(Key, SessInfo)) || Key <- InfoKeys]]).


format(created_at, Val) ->
    emqttd_util:now_to_secs(Val);

format(subscriptions, List) ->
    string:join([io_lib:format("~s:~w", [Topic, Qos]) || {Topic, Qos} <- List], ",");

format(_, Val) ->
    Val.

