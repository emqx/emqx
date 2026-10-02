%%--------------------------------------------------------------------
%% Copyright (c) 2022-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_kafka_consumer_sup).

-behaviour(brod_supervisor3).

-include_lib("snabbkaffe/include/trace.hrl").

%% `supervisor' API
-export([init/1]).

%% API
-export([
    start_link/0,
    child_spec/2,
    start_child/2,
    ensure_child_deleted/1
]).

%% Child start functions
-export([start_subscriber/2, start_waiting_child/1]).

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
        _Start = {?MODULE, start_subscriber, [Id, GroupSubscriberConfig]},
        _Restart = {permanent, DelaySecs},
        _Shutdown = 10_000,
        _Type = worker,
        _Module = [Mod]
    }.

-spec start_child(child_id(), map()) -> {ok, pid() | undefined} | {error, term()}.
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
            case brod_supervisor3:delete_child(?MODULE, Id) of
                ok ->
                    ok;
                {error, Reason} when Reason =:= running; Reason =:= restarting ->
                    %% The child was restarted between the two calls, by
                    %% `emqx_resource_ready_waiter' or a pending delayed restart.
                    ensure_child_deleted(Id)
            end;
        {error, not_found} ->
            ok
    end.

%%--------------------------------------------------------------------------------------------
%% Child start functions
%%--------------------------------------------------------------------------------------------

-doc """
Starts the group subscriber, or leaves the child without a process until the
node is ready.
""".
-spec start_subscriber(child_id(), map()) -> {ok, pid()} | ignore | {error, term()}.
start_subscriber(Id, GroupSubscriberConfig) ->
    case
        emqx_resource_ready_waiter:when_ready(
            {?MODULE, Id}, {?MODULE, start_waiting_child, [Id]}
        )
    of
        now -> brod_group_subscriber_v2:start_link(GroupSubscriberConfig);
        deferred -> ignore
    end.

-doc "Restarts a child left without a process by `start_subscriber/2`.".
-spec start_waiting_child(child_id()) -> ok | {error, term()}.
start_waiting_child(Id) ->
    case brod_supervisor3:restart_child(?MODULE, Id) of
        {ok, Pid} when is_pid(Pid) ->
            ?tp(info, "kafka_consumer_subscriber_started_after_node_ready", #{subscriber_id => Id}),
            ok;
        {ok, undefined} ->
            %% The node is not ready again, and `start_subscriber/2' deferred it again.
            ok;
        {error, Reason} when
            Reason =:= not_found; Reason =:= running; Reason =:= restarting
        ->
            %% Its source was removed, re-added or restarted while it waited.
            ok;
        {error, _} = Error ->
            Error
    end.

%%--------------------------------------------------------------------------------------------
%% `supervisor' API
%%--------------------------------------------------------------------------------------------

init([]) ->
    SupFlags = {one_for_one, 0, 1},
    ChildSpecs = [],
    {ok, {SupFlags, ChildSpecs}}.
