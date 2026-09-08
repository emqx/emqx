%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_gcp_pubsub_consumer_grpc_sup).

-behaviour(supervisor).

%% API
-export([
    ensure_connector_started/2,
    ensure_connector_stopped/1,
    ensure_source_started/3,
    ensure_source_stopped/2,
    start_link_connector_sup/1,
    start_link_sources_sup/2,
    start_link_source_sup/3
]).

%% `supervisor' API
-export([init/1]).

-ifdef(TEST).
-export([grpc_client_pool/1, grpc_worker_pool/2, whereis_source_sup/2]).
-endif.

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-include("emqx_bridge_gcp_pubsub_consumer_grpc.hrl").

-define(connector, connector).
-define(sources, sources).
-define(source, source).

-define(name(ID), {n, l, {?MODULE, ID}}).
-define(via(ID), {via, gproc, ?name(ID)}).

-define(connector_sup_id(CONNRESID), {CONNRESID, connector_sup}).
-define(sources_sup_id(CONNRESID), {CONNRESID, sources_sup}).
-define(source_sup_id(CONNRESID, SOURCERESID), {CONNRESID, source_sup, SOURCERESID}).

-define(grpc_client_pool(CONNRESID), {CONNRESID, grpc_client}).
-define(grpc_worker_pool(CONNRESID, SOURCERESID), {CONNRESID, grpc_worker, SOURCERESID}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

ensure_connector_started(ConnResId, Opts) ->
    Spec = #{id := Id} = connector_sup_spec(ConnResId, Opts),
    _ = ?TOP_SUP:delete_child(Id),
    maybe
        {ok, _} ?= ?TOP_SUP:start_child(Spec),
        {ok, #{client_pool => ?grpc_client_pool(ConnResId)}}
    end.

ensure_connector_stopped(ConnResId) ->
    SourceResIds = list_sources_under(ConnResId),
    lists:foreach(
        fun(SourceResId) ->
            ensure_source_stopped(ConnResId, SourceResId)
        end,
        SourceResIds
    ),
    Id = ?connector_sup_id(ConnResId),
    _ = ?TOP_SUP:delete_child(Id),
    _ = ensure_worker_pool_removed(?grpc_client_pool(ConnResId)),
    ok.

ensure_source_started(ConnResId, SourceResId, Opts) ->
    Spec = source_sup_spec(ConnResId, SourceResId, Opts),
    ensure_source_stopped(ConnResId, SourceResId),
    maybe
        {ok, _} ?= supervisor:start_child(?via(?sources_sup_id(ConnResId)), Spec),
        {ok, #{
            worker_pool => ?grpc_worker_pool(ConnResId, SourceResId)
        }}
    end.

ensure_source_stopped(ConnResId, SourceResId) ->
    _ = supervisor:terminate_child(
        ?via(?sources_sup_id(ConnResId)),
        ?source_sup_id(ConnResId, SourceResId)
    ),
    _ = supervisor:delete_child(
        ?via(?sources_sup_id(ConnResId)),
        ?source_sup_id(ConnResId, SourceResId)
    ),
    _ = ensure_worker_pool_removed(?grpc_worker_pool(ConnResId, SourceResId)),
    ok.

start_link_connector_sup(Opts) ->
    supervisor:start_link(?MODULE, {?connector, Opts}).

start_link_sources_sup(ConnResId, Opts) ->
    supervisor:start_link(?via(?sources_sup_id(ConnResId)), ?MODULE, {?sources, Opts}).

start_link_source_sup(ConnResId, SourceResId, Opts) ->
    supervisor:start_link(?via(?source_sup_id(ConnResId, SourceResId)), ?MODULE, {?source, Opts}).

-ifdef(TEST).
grpc_client_pool(ConnResId) ->
    ?grpc_client_pool(ConnResId).

grpc_worker_pool(ConnResId, SourceResId) ->
    ?grpc_worker_pool(ConnResId, SourceResId).

whereis_source_sup(ConnResId, SourceResId) ->
    gproc:where(?name(?source_sup_id(ConnResId, SourceResId))).
-endif.

%%------------------------------------------------------------------------------
%% `supervisor' API
%%------------------------------------------------------------------------------

-doc """
* connector (1 per connector; rest_for_one)
  |
  *--- grpc_client_sup (singleton; share amongst all sources in connector)
  |    |
  |    *--- grpc_client (n workers, n = pool_size (1, in practice))
  |
  *--- sources_sup (singleton, exists even when there are 0 sources; one_for_one)
       |
       *--- source_sup (1 per source; one_for_one)
            |
            *--- grpc_worker (1 per source)
""".
init({?connector, Opts}) ->
    #{
        conn_res_id := ConnResId,
        grpc_opts := GRPCOpts,
        url := URL0
    } = Opts,
    SupFlags = #{
        strategy => rest_for_one,
        intensity => 10,
        period => 5
    },
    URL = str(URL0),
    %% TODO: handle url error more gracefully (or in the schema)
    {ok, ClientSpec} = grpc_client_sup:spec(?grpc_client_pool(ConnResId), URL, GRPCOpts),
    SourcesSpec = sources_sup_spec(Opts),
    Children = [ClientSpec, SourcesSpec],
    {ok, {SupFlags, Children}};
init({?sources, _Opts}) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 5
    },
    Children = [],
    {ok, {SupFlags, Children}};
init({?source, Opts}) ->
    #{
        conn_res_id := ConnResId,
        source_res_id := SourceResId
    } = Opts,
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 1
    },
    %% Only one consumer per topic
    PoolSize = 1,
    WorkerPool = ?grpc_worker_pool(ConnResId, SourceResId),
    ensure_worker_pool(WorkerPool, hash, [{size, PoolSize}]),
    Children = lists:map(
        fun(Idx) ->
            ensure_worker_added(WorkerPool, Idx),
            grpc_worker_spec(Idx, Opts)
        end,
        lists:seq(1, PoolSize)
    ),
    {ok, {SupFlags, Children}}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

str(X) -> emqx_utils_conv:str(X).

connector_sup_spec(ConnResId, Opts0) ->
    Opts = Opts0#{conn_res_id => ConnResId},
    #{
        id => ?connector_sup_id(ConnResId),
        start => {?MODULE, start_link_connector_sup, [Opts]},
        type => supervisor,
        restart => permanent,
        shutdown => infinity
    }.

sources_sup_spec(Opts) ->
    #{conn_res_id := ConnResId} = Opts,
    #{
        id => ?sources_sup_id(ConnResId),
        start => {?MODULE, start_link_sources_sup, [ConnResId, Opts]},
        type => supervisor,
        restart => permanent,
        shutdown => infinity
    }.

source_sup_spec(ConnResId, SourceResId, Opts0) ->
    Opts = Opts0#{conn_res_id => ConnResId, source_res_id => SourceResId},
    #{
        id => ?source_sup_id(ConnResId, SourceResId),
        start => {?MODULE, start_link_source_sup, [ConnResId, SourceResId, Opts]},
        type => supervisor,
        restart => permanent,
        shutdown => infinity
    }.

grpc_worker_spec(Idx, Opts0) ->
    #{conn_res_id := ConnResId, source_res_id := SourceResId} = Opts0,
    Opts = Opts0#{pool => ?grpc_worker_pool(ConnResId, SourceResId), idx => Idx},
    #{
        id => {grpc_worker, Idx},
        start => {emqx_bridge_gcp_pubsub_consumer_grpc_worker, start_link, [Opts]},
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

list_sources_under(ConnResId) ->
    Key = ?name(?source_sup_id(ConnResId, '$1')),
    MS = [{{Key, '_', '_'}, [], ['$1']}],
    gproc:select({local, names}, MS).
