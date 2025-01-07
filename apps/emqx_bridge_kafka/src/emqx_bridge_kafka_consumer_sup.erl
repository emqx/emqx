%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_kafka_consumer_sup).

-behaviour(supervisor).

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
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec child_spec(child_id(), map()) -> supervisor:child_spec().
child_spec(Id, GroupSubscriberConfig) ->
    Mod = brod_group_subscriber_v2,
    #{
        id => Id,
        start => {Mod, start_link, [GroupSubscriberConfig]},
        restart => permanent,
        shutdown => 10_000,
        type => worker,
        modules => [Mod]
    }.

-spec start_child(child_id(), map()) -> {ok, pid()} | {error, term()}.
start_child(Id, GroupSubscriberConfig) ->
    ChildSpec = child_spec(Id, GroupSubscriberConfig),
    case supervisor:start_child(?MODULE, ChildSpec) of
        {ok, Pid} ->
            {ok, Pid};
        {ok, Pid, _Info} ->
            {ok, Pid};
        {error, already_present} ->
            supervisor:restart_child(?MODULE, Id);
        {error, {already_started, Pid}} ->
            {ok, Pid};
        {error, Error} ->
            {error, Error}
    end.

-spec ensure_child_deleted(child_id()) -> ok.
ensure_child_deleted(Id) ->
    case supervisor:terminate_child(?MODULE, Id) of
        ok ->
            ok = supervisor:delete_child(?MODULE, Id),
            ok;
        {error, not_found} ->
            ok
    end.

%%--------------------------------------------------------------------------------------------
%% `supervisor' API
%%--------------------------------------------------------------------------------------------

init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 100,
        period => 10
    },
    ChildSpecs = [],
    {ok, {SupFlags, ChildSpecs}}.
