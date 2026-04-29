%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_bigtable_client_sup).

-behaviour(supervisor).

%% API
-export([
    ensure_started/2,
    ensure_stopped/1,
    start_link_root_sup/1,
    start_link_receiver_sup/1
]).

%% `supervisor' API
-export([init/1]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(root, root).
-define(receiver, receiver).

-define(root_id(RESID), {RESID, root_client_sup}).
-define(grpc_client_name(RESID), {RESID, grpc_client_sup}).
-define(async_receiver_name(RESID), {RESID, async_receiver_sup}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

ensure_started(ConnResId, Opts) ->
    Spec = #{id := Id} = root_sup_spec(ConnResId, Opts),
    _ = emqx_bridge_bigtable_sup:delete_child(Id),
    maybe
        {ok, _} ?= emqx_bridge_bigtable_sup:start_child(Spec),
        {ok, #{recv_pool => ?async_receiver_name(ConnResId)}}
    end.

ensure_stopped(ConnResId) ->
    Id = ?root_id(ConnResId),
    _ = emqx_bridge_bigtable_sup:delete_child(Id),
    Pool = ?async_receiver_name(ConnResId),
    ensure_worker_pool_removed(Pool),
    ok.

start_link_root_sup(Opts) ->
    supervisor:start_link(?MODULE, {?root, Opts}).

start_link_receiver_sup(Opts) ->
    supervisor:start_link(?MODULE, {?receiver, Opts}).

%%------------------------------------------------------------------------------
%% `supervisor' API
%%------------------------------------------------------------------------------

init({?root, Opts}) ->
    #{
        conn_res_id := ConnResId,
        grpc_opts := GRPCOpts,
        url := URL0,
        pool_size := _PoolSize
    } = Opts,
    URL = str(URL0),
    %% TODO: handle url error more gracefully (or in the schema)
    {ok, ClientSpec} = grpc_client_sup:spec(?grpc_client_name(ConnResId), URL, GRPCOpts),
    ReceiverSpec = receiver_sup_spec(Opts),
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 1
    },
    ChildSpecs = [ClientSpec, ReceiverSpec],
    {ok, {SupFlags, ChildSpecs}};
init({?receiver, Opts}) ->
    #{conn_res_id := ConnResId, pool_size := PoolSize} = Opts,
    Pool = ?async_receiver_name(ConnResId),
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 1
    },
    ensure_worker_pool(Pool, hash, [{size, PoolSize}]),
    ChildSpecs = lists:map(
        fun(I) ->
            ensure_worker_added(Pool, I),
            receiver_worker_spec(Pool, I)
        end,
        lists:seq(1, PoolSize)
    ),
    {ok, {SupFlags, ChildSpecs}}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

str(X) -> emqx_utils_conv:str(X).

root_sup_spec(ConnResId, Opts0) ->
    Opts = Opts0#{conn_res_id => ConnResId},
    #{
        id => ?root_id(ConnResId),
        start => {?MODULE, start_link_root_sup, [Opts]},
        type => supervisor,
        restart => permanent,
        shutdown => infinity
    }.

receiver_sup_spec(Opts) ->
    #{
        id => async_receiver_sup,
        start => {?MODULE, start_link_receiver_sup, [Opts]},
        type => supervisor,
        restart => permanent,
        shutdown => infinity
    }.

receiver_worker_spec(Pool, I) ->
    Opts = #{pool => Pool, id => I},
    #{
        id => {worker, I},
        start => {emqx_bridge_bigtable_async_receiver_worker, start_link, [Opts]},
        type => worker,
        restart => permanent,
        shutdown => 5_000
    }.

ensure_worker_pool(Pool, Type, Opts) ->
    ensure_worker_pool_removed(Pool),
    try
        gproc_pool:new(Pool, Type, Opts)
    catch
        error:exists -> ok
    end,
    ok.

ensure_worker_added(Pool, Idx) ->
    try
        gproc_pool:add_worker(Pool, {Pool, Idx}, Idx)
    catch
        error:exists -> ok
    end,
    ok.

ensure_worker_pool_removed(Pool) ->
    gproc_pool:force_delete(Pool),
    ok.
