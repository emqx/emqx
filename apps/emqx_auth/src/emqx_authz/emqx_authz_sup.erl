%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%%%-------------------------------------------------------------------
%% @doc emqx_authz top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(emqx_authz_sup).

-include("emqx_authz.hrl").

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).
-define(METRICS_WORKER_NAME, authz_metrics).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    emqx_authz_source_registry:create(),
    SupFlags = #{
        strategy => one_for_all,
        intensity => 0,
        period => 1
    },
    ChildSpecs = [
        emqx_metrics_worker:child_spec(
            emqx_authz_metrics, ?METRICS_WORKER_NAME
        ),
        emqx_auth_cache:child_spec(
            ?AUTHZ_CACHE,
            [?EMQX_AUTHORIZATION_CONFIG_ROOT_NAME_ATOM, node_cache],
            ?METRICS_WORKER_NAME
        )
    ],
    {ok, {SupFlags, ChildSpecs}}.
