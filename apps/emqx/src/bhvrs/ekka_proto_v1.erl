%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(ekka_proto_v1).

-behaviour(emqx_bpapi).

-include("bpapi.hrl").

-export([introduced_in/0]).
-export([acquire_lock_1/4, acquire_lock_m/5, release_lock_1/3, release_lock_m/4, process_info/3]).
-export([is_running/2]).

-elvis([{elvis_style, atom_naming_convention, disable}]).

introduced_in() ->
    "1.0.0".

%% ekka_locker:
-spec acquire_lock_1(
    node(), ekka_locker:locker(), ekka_locker:lock_obj(), ekka_locker:piggyback()
) ->
    ekka_locker:lock_result() | emqx_rpc:badrpc().
acquire_lock_1(Node, Name, LockObj, Piggyback) ->
    rpc:call(Node, ekka_locker, acquire_lock, [Name, LockObj, Piggyback]).

-spec acquire_lock_m(
    [node()], ekka_locker:locker(), ekka_locker:lock_obj(), ekka_locker:piggyback(), timeout()
) ->
    emqx_rpc:multicall_result(ekka_locker:lock_result()).
acquire_lock_m(Nodes, Name, LockObj, Piggyback, Timeout) ->
    rpc:multicall(Nodes, ekka_locker, acquire_lock, [Name, LockObj, Piggyback], Timeout).

-spec release_lock_1(node(), ekka_locker:locker(), ekka_locker:lock_obj()) ->
    ekka_locker:release_result() | emqx_rpc:badrpc().
release_lock_1(Node, Name, LockObj) ->
    rpc:call(Node, ekka_locker, release_lock, [Name, LockObj]).

-spec release_lock_m([node()], ekka_locker:locker(), ekka_locker:lock_obj(), timeout()) ->
    emqx_rpc:multicall_result(ekka_locker:release_result()).
release_lock_m(Nodes, Name, LockObj, Timeout) ->
    rpc:multicall(Nodes, ekka_locker, release_lock, [Name, LockObj], Timeout).

-spec process_info(node(), pid(), list()) -> list() | emqx_rpc:badrpc().
process_info(Node, Pid, Fields) ->
    rpc:call(Node, erlang, process_info, [Pid, Fields], 5000).

%% ekka_node:
-spec is_running(node(), atom()) -> boolean() | emqx_rpc:badrpc().
is_running(Node, App) ->
    rpc:call(Node, ekka_node, is_running, [App]).
