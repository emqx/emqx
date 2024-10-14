%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(alinkalarm_handler_sup).

-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    PoolSize = erlang:system_info(schedulers),
    Pool = alinkalarm_pool_sup:spec([alinkalarm_handler_pool, round_robin, PoolSize,
        {alinkalarm_handler, start_link, []}]),

    {ok, {#{strategy => one_for_all,
        intensity => 5,
        period => 30},
        [Pool]}
    }.


%%%===================================================================
%%% Internal functions
%%%===============================================================