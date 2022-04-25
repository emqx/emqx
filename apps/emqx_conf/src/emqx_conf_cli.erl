%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_conf_cli).
-export([
    load/0,
    admins/1,
    unload/0
]).

-define(CMD, cluster_call).

load() ->
    emqx_ctl:register_command(?CMD, {?MODULE, admins}, []).

unload() ->
    emqx_ctl:unregister_command(?CMD).

admins(["status"]) ->
    status();
admins(["skip"]) ->
    status(),
    Nodes = mria_mnesia:running_nodes(),
    lists:foreach(fun emqx_cluster_rpc:skip_failed_commit/1, Nodes),
    status();
admins(["skip", Node0]) ->
    status(),
    Node = list_to_existing_atom(Node0),
    emqx_cluster_rpc:skip_failed_commit(Node),
    status();
admins(["tnxid", TnxId0]) ->
    TnxId = list_to_integer(TnxId0),
    emqx_ctl:print("~p~n", [emqx_cluster_rpc:query(TnxId)]);
admins(["fast_forward"]) ->
    status(),
    Nodes = mria_mnesia:running_nodes(),
    TnxId = emqx_cluster_rpc:latest_tnx_id(),
    lists:foreach(fun(N) -> emqx_cluster_rpc:fast_forward_to_commit(N, TnxId) end, Nodes),
    status();
admins(["fast_forward", ToTnxId]) ->
    status(),
    Nodes = mria_mnesia:running_nodes(),
    TnxId = list_to_integer(ToTnxId),
    lists:foreach(fun(N) -> emqx_cluster_rpc:fast_forward_to_commit(N, TnxId) end, Nodes),
    status();
admins(["fast_forward", Node0, ToTnxId]) ->
    status(),
    TnxId = list_to_integer(ToTnxId),
    Node = list_to_existing_atom(Node0),
    emqx_cluster_rpc:fast_forward_to_commit(Node, TnxId),
    status();
admins(_) ->
    emqx_ctl:usage(
        [
            {"cluster_call status", "status"},
            {"cluster_call skip [node]", "increase one commit on specific node"},
            {"cluster_call tnxid <TnxId>", "get detailed about TnxId"},
            {"cluster_call fast_forward [node] [tnx_id]", "fast forwards to tnx_id"}
        ]
    ).

status() ->
    emqx_ctl:print("-----------------------------------------------\n"),
    {atomic, Status} = emqx_cluster_rpc:status(),
    lists:foreach(
        fun(S) ->
            #{
                node := Node,
                tnx_id := TnxId,
                mfa := {M, F, A},
                created_at := CreatedAt
            } = S,
            emqx_ctl:print(
                "~p:[~w] CreatedAt:~p ~p:~p/~w\n",
                [Node, TnxId, CreatedAt, M, F, length(A)]
            )
        end,
        Status
    ),
    emqx_ctl:print("-----------------------------------------------\n").
