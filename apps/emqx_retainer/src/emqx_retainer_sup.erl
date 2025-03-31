%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_retainer_sup).

-include("emqx_retainer.hrl").

-export([start_link/0, start_worker_sup/0, start_workers/0, stop_workers/0]).

-export([start_gc/2]).

-behaviour(supervisor).
-export([init/1]).

-define(worker_sup, emqx_retainer_worker_sup).
-define(root_sup, emqx_retainer_root_sup).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec start_link() -> supervisor:startchild_ret().
start_link() ->
    supervisor:start_link({local, ?root_sup}, ?MODULE, [?root_sup]).

-spec start_worker_sup() -> supervisor:startchild_ret().
start_worker_sup() ->
    supervisor:start_link({local, ?worker_sup}, ?MODULE, [?worker_sup]).

-spec start_gc(emqx_retainer:context(), emqx_retainer_gc:opts()) ->
    supervisor:startchild_ret().
start_gc(Context, Opts) ->
    ChildSpec = #{
        id => gc,
        start => {emqx_retainer_gc, start_link, [Context, Opts]},
        restart => temporary,
        type => worker
    },
    supervisor:start_child(?worker_sup, ChildSpec).

-spec start_workers() -> ok.
start_workers() ->
    {ok, _} = start_dispatcher(),
    ok.

-spec stop_workers() -> ok.
stop_workers() ->
    ok = stop_child(dispatcher),
    ok.

%%--------------------------------------------------------------------
%% supervisor callbacks
%%--------------------------------------------------------------------

init([?root_sup]) ->
    {ok,
        {{one_for_one, 10, 3600}, [
            #{
                id => worker_sup,
                start => {?MODULE, start_worker_sup, []},
                restart => permanent,
                shutdown => infinity,
                type => supervisor,
                modules => [?MODULE]
            },
            #{
                id => retainer,
                start => {emqx_retainer, start_link, []},
                restart => permanent,
                shutdown => 5000,
                type => worker,
                modules => [emqx_retainer]
            }
        ]}};
init([?worker_sup]) ->
    {ok, {{one_for_one, 10, 3600}, []}}.

%%--------------------------------------------------------------------
%% Private functions
%%--------------------------------------------------------------------

start_dispatcher() ->
    ChildSpec = emqx_pool_sup:spec(
        dispatcher,
        [
            ?DISPATCHER_POOL,
            hash,
            emqx_vm:schedulers(),
            {emqx_retainer_dispatcher, start_link, []}
        ]
    ),
    supervisor:start_child(?worker_sup, ChildSpec).

stop_child(ChildId) ->
    case supervisor:terminate_child(?worker_sup, ChildId) of
        ok -> supervisor:delete_child(?worker_sup, ChildId);
        {error, not_found} -> ok
    end.
