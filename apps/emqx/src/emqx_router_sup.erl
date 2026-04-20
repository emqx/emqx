%%--------------------------------------------------------------------
%% Copyright (c) 2018-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_router_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

start_link() ->
    %% Init and log routing table type
    ok = emqx_router:init_schema(),
    ok = mria:wait_for_tables(
        emqx_router:create_tables() ++
            emqx_router_helper:create_tables()
    ),
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    %% Router helper
    Helper = #{
        id => helper,
        start => {emqx_router_helper, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [emqx_router_helper]
    },
    HelperPostStart = #{
        id => helper_post_start,
        start => {emqx_router_helper, post_start, []},
        restart => transient,
        shutdown => brutal_kill,
        type => worker
    },
    %% Router pool
    RouterPool = emqx_pool_sup:spec([
        router_pool,
        hash,
        {emqx_router, start_link, []}
    ]),
    Children =
        case emqx_router:get_schema_vsn() of
            v3 ->
                [RouterPool];
            v2 ->
                [Helper, HelperPostStart, RouterPool]
        end,
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 100
    },
    {ok, {SupFlags, Children}}.
