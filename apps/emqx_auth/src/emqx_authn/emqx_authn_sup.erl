%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_sup).

-behaviour(supervisor).

-include("emqx_authn.hrl").

-export([
    start_link/0,
    init/1
]).

-define(METRICS_WORKER_NAME, authn_metrics).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 100,
        period => 10
    },

    Metrics = emqx_metrics_worker:child_spec(emqx_authn_metrics, ?METRICS_WORKER_NAME),

    AuthNCache = emqx_auth_cache:child_spec(
        ?AUTHN_CACHE, [authentication_settings, node_cache], ?METRICS_WORKER_NAME
    ),

    AuthN = #{
        id => emqx_authn_chains,
        start => {emqx_authn_chains, start_link, []},
        restart => permanent,
        shutdown => 1000,
        type => worker,
        modules => [emqx_authn_chains]
    },

    ChildSpecs = [Metrics, AuthNCache, AuthN],

    {ok, {SupFlags, ChildSpecs}}.
