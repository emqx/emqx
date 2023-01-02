%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

%% @doc This module implements EMQX Bridge transport layer based on gen_rpc.

-module(emqx_bridge_rpc).

-behaviour(emqx_bridge_connect).

%% behaviour callbacks
-export([ start/1
        , send/2
        , stop/1
        ]).

%% Internal exports
-export([ handle_send/1
        , heartbeat/2
        ]).

-type ack_ref() :: emqx_bridge_worker:ack_ref().
-type batch() :: emqx_bridge_worker:batch().
-type node_or_tuple() :: atom() | {atom(), term()}.

-define(HEARTBEAT_INTERVAL, timer:seconds(1)).

-define(RPC, emqx_rpc).

start(#{address := Remote}) ->
    case poke(Remote) of
        ok ->
            Pid = proc_lib:spawn_link(?MODULE, heartbeat, [self(), Remote]),
            {ok, #{client_pid => Pid, address => Remote}};
        Error ->
            Error
    end.

stop(#{client_pid := Pid}) when is_pid(Pid) ->
    Ref = erlang:monitor(process, Pid),
    unlink(Pid),
    Pid ! stop,
    receive
        {'DOWN', Ref, process, Pid, _Reason} ->
            ok
    after
        1000 ->
            exit(Pid, kill)
    end,
    ok.

%% @doc Callback for `emqx_bridge_connect' behaviour
-spec send(#{address := node_or_tuple(), _ => _}, batch()) -> {ok, ack_ref()} | {error, any()}.
send(#{address := Remote}, Batch) ->
    case ?RPC:call(Remote, ?MODULE, handle_send, [Batch]) of
        ok ->
            Ref = make_ref(),
            self() ! {batch_ack, Ref},
            {ok, Ref};
        {badrpc, Reason} -> {error, Reason}
    end.

%% @doc Handle send on receiver side.
-spec handle_send(batch()) -> ok.
handle_send(Batch) ->
    lists:foreach(fun(Msg) -> emqx_broker:publish(Msg) end, Batch).

%% @hidden Heartbeat loop
heartbeat(Parent, RemoteNode) ->
    Interval = ?HEARTBEAT_INTERVAL,
    receive
        stop -> exit(normal)
    after
        Interval ->
            case poke(RemoteNode) of
                ok ->
                    ?MODULE:heartbeat(Parent, RemoteNode);
                {error, Reason} ->
                    Parent ! {disconnected, self(), Reason},
                    exit(normal)
            end
    end.

poke(Node) ->
    case ?RPC:call(Node, erlang, node, []) of
        Node -> ok;
        {badrpc, Reason} -> {error, Reason}
    end.
