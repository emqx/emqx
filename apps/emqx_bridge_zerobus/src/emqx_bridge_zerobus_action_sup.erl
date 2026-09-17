%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_zerobus_action_sup).

-behaviour(supervisor).

%% API
-export([
    ensure_connector_started/2,
    ensure_connector_stopped/1,
    ensure_action_started/3,
    ensure_action_stopped/2,
    start_link_connector_sup/1,
    start_link_actions_sup/2,
    start_link_action_sup/3
]).

%% `supervisor' API
-export([init/1]).

-ifdef(TEST).
-export([stream_writer_pool/2, whereis_action_sup/2]).
-endif.

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(connector, connector).
-define(actions, actions).
-define(action, action).

-define(name(ID), {n, l, {?MODULE, ID}}).
-define(via(ID), {via, gproc, ?name(ID)}).

-define(connector_sup_id(CONNRESID), {CONNRESID, connector_sup}).
-define(actions_sup_id(CONNRESID), {CONNRESID, actions_sup}).
-define(action_sup_id(CONNRESID, ACTIONRESID), {CONNRESID, action_sup, ACTIONRESID}).

-define(grpc_client_pool(CONNRESID), {CONNRESID, grpc_client}).
-define(stream_writer_pool(CONNRESID, ACTIONRESID), {CONNRESID, stream_writer, ACTIONRESID}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

ensure_connector_started(ConnResId, Opts) ->
    Spec = #{id := Id} = connector_sup_spec(ConnResId, Opts),
    _ = emqx_bridge_zerobus_sup:delete_child(Id),
    maybe
        {ok, _} ?= emqx_bridge_zerobus_sup:start_child(Spec),
        {ok, #{client_pool => ?grpc_client_pool(ConnResId)}}
    end.

ensure_connector_stopped(ConnResId) ->
    ActionResIds = list_actions_under(ConnResId),
    lists:foreach(
        fun(ActionResId) ->
            ensure_action_stopped(ConnResId, ActionResId)
        end,
        ActionResIds
    ),
    Id = ?connector_sup_id(ConnResId),
    _ = emqx_bridge_zerobus_sup:delete_child(Id),
    _ = ensure_worker_pool_removed(?grpc_client_pool(ConnResId)),
    ok.

ensure_action_started(ConnResId, ActionResId, Opts) ->
    Spec = action_sup_spec(ConnResId, ActionResId, Opts),
    ensure_action_stopped(ConnResId, ActionResId),
    maybe
        {ok, _} ?= supervisor:start_child(?via(?actions_sup_id(ConnResId)), Spec),
        {ok, #{
            writer_pool => ?stream_writer_pool(ConnResId, ActionResId)
        }}
    end.

ensure_action_stopped(ConnResId, ActionResId) ->
    _ = supervisor:terminate_child(
        ?via(?actions_sup_id(ConnResId)),
        ?action_sup_id(ConnResId, ActionResId)
    ),
    _ = supervisor:delete_child(
        ?via(?actions_sup_id(ConnResId)),
        ?action_sup_id(ConnResId, ActionResId)
    ),
    _ = ensure_worker_pool_removed(?stream_writer_pool(ConnResId, ActionResId)),
    ok.

start_link_connector_sup(Opts) ->
    supervisor:start_link(?MODULE, {?connector, Opts}).

start_link_actions_sup(ConnResId, Opts) ->
    supervisor:start_link(?via(?actions_sup_id(ConnResId)), ?MODULE, {?actions, Opts}).

start_link_action_sup(ConnResId, ActionResId, Opts) ->
    supervisor:start_link(?via(?action_sup_id(ConnResId, ActionResId)), ?MODULE, {?action, Opts}).

-ifdef(TEST).
stream_writer_pool(ConnResId, ActionResId) ->
    ?stream_writer_pool(ConnResId, ActionResId).

whereis_action_sup(ConnResId, ActionResId) ->
    gproc:where(?name(?action_sup_id(ConnResId, ActionResId))).
-endif.

%%------------------------------------------------------------------------------
%% `supervisor' API
%%------------------------------------------------------------------------------

-doc """
* connector (1 per connector; rest_for_one)
  |
  *--- grpc_client_sup (singleton; share amongst all actions in connector)
  |    |
  |    *--- grpc_client (n workers, n = pool_size)
  |
  *--- actions_sup (singleton, exists even when there are 0 actions; one_for_one)
       |
       *--- action_sup (1 per action; one_for_one)
            |
            *--- stream_writer (n = pool_size)
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
    ActionsSpec = actions_sup_spec(Opts),
    Children = [ClientSpec, ActionsSpec],
    {ok, {SupFlags, Children}};
init({?actions, _Opts}) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 5
    },
    Children = [],
    {ok, {SupFlags, Children}};
init({?action, Opts}) ->
    #{
        conn_res_id := ConnResId,
        action_res_id := ActionResId,
        pool_size := PoolSize
    } = Opts,
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 1
    },
    WriterPool = ?stream_writer_pool(ConnResId, ActionResId),
    ensure_worker_pool(WriterPool, hash, [{size, PoolSize}]),
    Children = lists:map(
        fun(Idx) ->
            ensure_worker_added(WriterPool, Idx),
            stream_writer_spec(Idx, Opts)
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

actions_sup_spec(Opts) ->
    #{conn_res_id := ConnResId} = Opts,
    #{
        id => ?actions_sup_id(ConnResId),
        start => {?MODULE, start_link_actions_sup, [ConnResId, Opts]},
        type => supervisor,
        restart => permanent,
        shutdown => infinity
    }.

action_sup_spec(ConnResId, ActionResId, Opts0) ->
    Opts = Opts0#{conn_res_id => ConnResId, action_res_id => ActionResId},
    #{
        id => ?action_sup_id(ConnResId, ActionResId),
        start => {?MODULE, start_link_action_sup, [ConnResId, ActionResId, Opts]},
        type => supervisor,
        restart => permanent,
        shutdown => infinity
    }.

stream_writer_spec(Idx, Opts0) ->
    #{conn_res_id := ConnResId, action_res_id := ActionResId} = Opts0,
    Opts = Opts0#{pool => ?stream_writer_pool(ConnResId, ActionResId), idx => Idx},
    #{
        id => {stream_writer, Idx},
        start => {emqx_bridge_zerobus_stream_writer_worker, start_link, [Opts]},
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

list_actions_under(ConnResId) ->
    Key = ?name(?action_sup_id(ConnResId, '$1')),
    MS = [{{Key, '_', '_'}, [], ['$1']}],
    gproc:select({local, names}, MS).
