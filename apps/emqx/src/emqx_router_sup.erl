%%--------------------------------------------------------------------
%% Copyright (c) 2018-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_router_sup).

-behaviour(supervisor).

-export([start_link/0, ensure_helper/0]).

-export([init/1]).

start_link() ->
    %% Init and log routing table type
    ok = emqx_router:init_schema(),
    ok = mria:wait_for_tables(
        emqx_router:create_tables() ++
            emqx_router_helper:create_tables()
    ),
    case supervisor:start_link({local, ?MODULE}, ?MODULE, []) of
        {ok, _} = Ret ->
            %% Setup periodic stats reporting (handled by `emqx_stats' server):
            ok = emqx_stats:update_interval(route_stats, fun emqx_router_helper:stats_fun/0),
            Ret;
        Err ->
            Err
    end.

%% Start the router helper when it is not running. Does nothing when it is.
%% A node that runs an older version of this module does not start the helper with
%% routing schema v3. A hot patch calls this function after it loads the new modules.
-spec ensure_helper() -> ok | {error, term()}.
ensure_helper() ->
    case supervisor:start_child(?MODULE, helper_spec()) of
        {ok, _Pid} ->
            ok;
        {error, {already_started, _Pid}} ->
            ok;
        {error, already_present} ->
            case supervisor:restart_child(?MODULE, helper) of
                {ok, _Pid} -> ok;
                {error, running} -> ok;
                {error, Reason} -> {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

init([]) ->
    Helper = helper_spec(),
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
                [Helper, RouterPool];
            v2 ->
                [Helper, HelperPostStart, RouterPool]
        end,
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 100
    },
    {ok, {SupFlags, Children}}.

%% Router helper
helper_spec() ->
    #{
        id => helper,
        start => {emqx_router_helper, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [emqx_router_helper]
    }.
