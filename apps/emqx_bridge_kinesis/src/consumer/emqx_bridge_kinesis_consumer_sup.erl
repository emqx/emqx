%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_kinesis_consumer_sup).

-behaviour(supervisor).

%% `supervisor' API
-export([init/1]).

%% API
-export([
    start_link/0,
    ensure_supervisor_started/0,
    coordinator_spec/2,
    start_child/2,
    ensure_child_deleted/1
]).

%% TODO: leader name
-type child_id() :: binary().
-export_type([child_id/0]).

%%--------------------------------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------------------------------

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec ensure_supervisor_started() -> ok.
ensure_supervisor_started() ->
    MyChildSpec =
        #{
            id => ?MODULE,
            start => {?MODULE, start_link, []},
            restart => permanent,
            shutdown => infinity,
            type => supervisor
        },
    case supervisor:start_child(emqx_bridge_sup, MyChildSpec) of
        {ok, _Pid} ->
            ok;
        {error, already_present} ->
            ok;
        {error, {already_started, _Pid}} ->
            ok
    end.

%% TODO: config typespec
-spec coordinator_spec(child_id(), map()) -> supervisor:child_spec().
coordinator_spec(Id, CoordinatorConfig) ->
    Mod = emqx_bridge_kinesis_consumer_coordinator,
    #{
        id => Id,
        start => {Mod, start_link, [CoordinatorConfig]},
        restart => permanent,
        shutdown => 5_000,
        type => worker
    }.

-spec start_child(child_id(), supervisor:child_spec()) -> {ok, pid()} | {error, term()}.
start_child(Id, ChildSpec) ->
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
