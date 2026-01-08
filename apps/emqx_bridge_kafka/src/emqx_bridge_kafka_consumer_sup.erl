%%--------------------------------------------------------------------
%% Copyright (c) 2022-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_kafka_consumer_sup).

-behaviour(brod_supervisor3).

%% `supervisor' API
-export([init/1]).

%% API
-export([
    start_link/0,
    child_spec/2,
    start_child/2,
    ensure_child_deleted/1
]).

-type child_id() :: binary().
-export_type([child_id/0]).

%%--------------------------------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------------------------------

start_link() ->
    brod_supervisor3:start_link({local, ?MODULE}, ?MODULE, []).

-spec child_spec(child_id(), map()) -> brod_supervisor3:child_spec().
child_spec(Id, GroupSubscriberConfig) ->
    Mod = brod_group_subscriber_v2,
    DelaySecs = 5,
    {
        Id,
        _Start = {Mod, start_link, [GroupSubscriberConfig]},
        _Restart = {permanent, DelaySecs},
        _Shutdown = 10_000,
        _Type = worker,
        _Module = [Mod]
    }.

-spec start_child(child_id(), map()) -> {ok, pid()} | {error, term()}.
start_child(Id, GroupSubscriberConfig) ->
    ChildSpec = child_spec(Id, GroupSubscriberConfig),
    case brod_supervisor3:start_child(?MODULE, ChildSpec) of
        {ok, Pid} ->
            {ok, Pid};
        {ok, Pid, _Info} ->
            {ok, Pid};
        {error, already_present} ->
            brod_supervisor3:restart_child(?MODULE, Id);
        {error, {already_started, Pid}} ->
            {ok, Pid};
        {error, Error} ->
            {error, Error}
    end.

-spec ensure_child_deleted(child_id()) -> ok.
ensure_child_deleted(Id) ->
    case brod_supervisor3:terminate_child(?MODULE, Id) of
        ok ->
            ok = brod_supervisor3:delete_child(?MODULE, Id),
            ok;
        {error, not_found} ->
            ok
    end.

%%--------------------------------------------------------------------------------------------
%% `supervisor' API
%%--------------------------------------------------------------------------------------------

init([]) ->
    SupFlags = {one_for_one, 0, 1},
    ChildSpecs = [],
    {ok, {SupFlags, ChildSpecs}}.
