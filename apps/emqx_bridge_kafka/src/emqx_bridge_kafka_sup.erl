%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_kafka_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% `supervisor' API
-export([init/1]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%------------------------------------------------------------------------------
%% `supervisor' API
%%------------------------------------------------------------------------------

init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 10
    },
    ConsumerSup = sup_spec(emqx_bridge_kafka_consumer_sup),
    TokenCache = worker_spec(emqx_bridge_kafka_token_cache),
    _ = emqx_bridge_kafka_token_cache:create_tables(),
    ChildSpecs = [ConsumerSup, TokenCache],
    {ok, {SupFlags, ChildSpecs}}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

sup_spec(Mod) ->
    #{
        id => Mod,
        start => {Mod, start_link, []},
        restart => permanent,
        shutdown => infinity,
        type => supervisor
    }.

worker_spec(Mod) ->
    #{
        id => Mod,
        start => {Mod, start_link, []},
        restart => permanent,
        shutdown => 5_000,
        type => worker
    }.
