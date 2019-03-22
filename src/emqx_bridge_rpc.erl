%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc This module implements EMQX Bridge transport layer based on gen_rpc.

-module(emqx_bridge_rpc).
-behaviour(emqx_bridge_connect).

%% behaviour callbacks
-export([ start/1
        , send/2
        , stop/2
        ]).

%% Internal exports
-export([ handle_send/2
        , handle_ack/2
        , heartbeat/2
        ]).

-type ack_ref() :: emqx_bridge:ack_ref().
-type batch() :: emqx_bridge:batch().

-define(HEARTBEAT_INTERVAL, timer:seconds(1)).

-define(RPC, gen_rpc).

start(#{address := Remote}) ->
    case poke(Remote) of
        ok ->
            Pid = proc_lib:spawn_link(?MODULE, heartbeat, [self(), Remote]),
            {ok, Pid, Remote};
        Error ->
            Error
    end.

stop(Pid, _Remote) when is_pid(Pid) ->
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
-spec send(node(), batch()) -> {ok, ack_ref()} | {error, any()}.
send(Remote, Batch) ->
    Sender = self(),
    case ?RPC:call(Remote, ?MODULE, handle_send, [Sender, Batch]) of
        {ok, Ref} -> {ok, Ref};
        {badrpc, Reason} -> {error, Reason}
    end.

%% @doc Handle send on receiver side.
-spec handle_send(pid(), batch()) -> {ok, ack_ref()} | {error, any()}.
handle_send(SenderPid, Batch) ->
    SenderNode = node(SenderPid),
    Ref = make_ref(),
    AckFun = fun() -> ?RPC:cast(SenderNode, ?MODULE, handle_ack, [SenderPid, Ref]), ok end,
    case emqx_bridge:import_batch(Batch, AckFun) of
        ok -> {ok, Ref};
        Error -> Error
    end.

%% @doc Handle batch ack in sender node.
handle_ack(SenderPid, Ref) ->
    ok = emqx_bridge:handle_ack(SenderPid, Ref).

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
