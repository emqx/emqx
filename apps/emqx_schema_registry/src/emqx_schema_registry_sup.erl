%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_schema_registry_sup).

-behaviour(supervisor).

%% API
-export([
    start_link/0,
    start_external_registry_sup/0,

    ensure_external_registry_worker_started/1,
    ensure_external_registry_worker_absent/1
]).

%% `supervisor' API
-export([init/1]).

%% @doc
%%
%% emqx_schema_registry_sup
%% |
%% +-- emqx_schema_registry(1)  % registry process controlling local schemas
%% |
%% +-- emqx_schema_registry_external_sup(1)  % supervisor for external registry workers
%%     |
%%     +-- emqx_schema_registry_external(1) % for managing external registry context and cache
%%     |
%%     +-- avlizer_confluent(0..n) % for interacting with confluent schema registry

%%------------------------------------------------------------------------------
%% Type definitions
%%------------------------------------------------------------------------------

-define(root, root).
-define(ROOT_SUP, ?MODULE).

-define(external, external).
-define(EXTERNAL_SUP, emqx_schema_registry_external_sup).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

start_link() ->
    supervisor:start_link({local, ?ROOT_SUP}, ?MODULE, ?root).

start_external_registry_sup() ->
    supervisor:start_link({local, ?EXTERNAL_SUP}, ?MODULE, ?external).

ensure_external_registry_worker_started(ChildSpec) ->
    case supervisor:start_child(?EXTERNAL_SUP, ChildSpec) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            ok
    end.

ensure_external_registry_worker_absent(WorkerId) ->
    case supervisor:terminate_child(?EXTERNAL_SUP, WorkerId) of
        ok ->
            _ = supervisor:delete_child(?EXTERNAL_SUP, WorkerId),
            ok;
        {error, not_found} ->
            ok
    end.

%%------------------------------------------------------------------------------
%% `supervisor' API
%%------------------------------------------------------------------------------

init(?root) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 100
    },
    ChildSpecs = [
        worker_spec(emqx_schema_registry),
        external_sup_spec()
    ],
    {ok, {SupFlags, ChildSpecs}};
init(?external) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 5
    },
    Children = [
        worker_spec(emqx_schema_registry_external)
    ],
    {ok, {SupFlags, Children}}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

worker_spec(Mod) ->
    #{
        id => Mod,
        start => {Mod, start_link, []},
        restart => permanent,
        shutdown => 5_000,
        type => worker
    }.

external_sup_spec() ->
    #{
        id => ?EXTERNAL_SUP,
        start => {?MODULE, start_external_registry_sup, []},
        restart => permanent,
        shutdown => infinity,
        type => supervisor
    }.
