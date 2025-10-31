%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Supervisor that contains all the processes that belong to a
%% given builtin DS database.
-module(emqx_ds_builtin_local_db_sup).

-behaviour(supervisor).

%% API:
-export([
    start_db/4,
    start_shard/1,
    stop_shard/1,
    terminate_storage/1,
    restart_storage/1,
    ensure_shard/1
]).
-export([which_dbs/0, which_shards/1]).

%% Debug:
-export([
    get_shard_workers/1
]).

%% behaviour callbacks:
-export([init/1]).

%% internal exports:
-export([start_link_sup/2]).

%%================================================================================
%% Type declarations
%%================================================================================

-define(via(REC), {via, gproc, {n, l, REC}}).

-define(db_sup, ?MODULE).
-define(shards_sup, emqx_ds_builtin_local_db_shards_sup).
-define(shard_sup, emqx_ds_builtin_local_db_shard_sup).

-record(?db_sup, {db}).
-record(?shards_sup, {db}).
-record(?shard_sup, {db, shard}).

%%================================================================================
%% API functions
%%================================================================================

-spec start_db(
    emqx_ds:db(),
    boolean(),
    emqx_ds_builtin_local:db_schema(),
    emqx_ds_builtin_local:db_runtime_config()
) -> {ok, pid()}.
start_db(DB, Create, Schema, RTOpts) ->
    start_link_sup(#?db_sup{db = DB}, {Create, Schema, RTOpts}).

-spec start_shard(emqx_ds_storage_layer:dbshard()) ->
    supervisor:startchild_ret().
start_shard({DB, Shard}) ->
    supervisor:start_child(?via(#?shards_sup{db = DB}), shard_spec(DB, Shard)).

-spec stop_shard(emqx_ds_storage_layer:dbshard()) -> ok | {error, not_found}.
stop_shard({DB, Shard}) ->
    Sup = ?via(#?shards_sup{db = DB}),
    case supervisor:terminate_child(Sup, Shard) of
        ok ->
            supervisor:delete_child(Sup, Shard);
        {error, Reason} ->
            {error, Reason}
    end.

-spec terminate_storage(emqx_ds_storage_layer:dbshard()) -> ok | {error, _Reason}.
terminate_storage({DB, Shard}) ->
    Sup = ?via(#?shard_sup{db = DB, shard = Shard}),
    supervisor:terminate_child(Sup, {Shard, storage}).

-spec restart_storage(emqx_ds_storage_layer:dbshard()) -> {ok, _Child} | {error, _Reason}.
restart_storage({DB, Shard}) ->
    Sup = ?via(#?shard_sup{db = DB, shard = Shard}),
    supervisor:restart_child(Sup, {Shard, storage}).

-spec ensure_shard(emqx_ds_storage_layer:dbshard()) ->
    ok | {error, _Reason}.
ensure_shard(Shard) ->
    ensure_started(start_shard(Shard)).

-spec which_shards(emqx_ds:db()) ->
    [_Child].
which_shards(DB) ->
    supervisor:which_children(?via(#?shards_sup{db = DB})).

%% @doc Return the list of builtin DS databases that are currently
%% active on the node.
-spec which_dbs() -> [emqx_ds:db()].
which_dbs() ->
    Key = {n, l, #?db_sup{_ = '_', db = '$1'}},
    gproc:select({local, names}, [{{Key, '_', '_'}, [], ['$1']}]).

%% @doc Get pids of all local shard servers for the given DB.
-spec get_shard_workers(emqx_ds:db()) -> #{_Shard => pid()}.
get_shard_workers(DB) ->
    Shards = supervisor:which_children(?via(#?shards_sup{db = DB})),
    L = lists:flatmap(
        fun
            ({_Shard, Sup, _, _}) when is_pid(Sup) ->
                [{Id, Pid} || {Id, Pid, _, _} <- supervisor:which_children(Sup), is_pid(Pid)];
            (_) ->
                []
        end,
        Shards
    ),
    maps:from_list(L).

%%================================================================================
%% behaviour callbacks
%%================================================================================

init({#?db_sup{db = DB}, {_Create, _Schema, _RTConf}}) ->
    %% Spec for the top-level supervisor for the database:
    logger:notice("Starting DS DB ~p", [DB]),
    emqx_ds_builtin_metrics:init_for_db(DB),
    Children = [
        sup_spec(#?shards_sup{db = DB}, undefined),
        meta_spec(DB)
    ],
    SupFlags = #{
        strategy => one_for_all,
        intensity => 0,
        period => 1
    },
    {ok, {SupFlags, Children}};
init({#?shards_sup{db = DB}, _}) ->
    %% Spec for the supervisor that manages the supervisors for
    %% each local shard of the DB:
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 1
    },
    Children = [shard_spec(DB, Shard) || Shard <- emqx_ds_builtin_local_meta:shards(DB)],
    {ok, {SupFlags, Children}};
init({#?shard_sup{db = DB, shard = Shard}, _}) ->
    SupFlags = #{
        strategy => rest_for_one,
        intensity => 10,
        period => 100
    },
    Opts = emqx_ds_builtin_local_meta:db_config(DB),
    Setup = fun() -> emqx_ds:set_shard_ready(DB, Shard, true) end,
    Teardown = fun() -> emqx_ds:set_shard_ready(DB, Shard, false) end,
    Children = [
        shard_storage_spec(DB, Shard, Opts),
        otx_leader_spec(DB, Shard),
        shard_beamformers_spec(DB, Shard),
        emqx_ds_lib:autoclean(autoclean, 5_000, Setup, Teardown)
    ],
    {ok, {SupFlags, Children}}.

%%================================================================================
%% Internal exports
%%================================================================================

start_link_sup(Id, Options) ->
    supervisor:start_link(?via(Id), ?MODULE, {Id, Options}).

%%================================================================================
%% Internal functions
%%================================================================================

meta_spec(DB) ->
    #{
        id => meta_worker,
        start => {emqx_ds_builtin_local_meta_worker, start_link, [DB]},
        shutdown => 5_000,
        restart => permanent,
        type => worker
    }.

sup_spec(Id, Options) ->
    #{
        id => element(1, Id),
        start => {?MODULE, start_link_sup, [Id, Options]},
        type => supervisor,
        shutdown => infinity
    }.

shard_spec(DB, Shard) ->
    #{
        id => Shard,
        start => {?MODULE, start_link_sup, [#?shard_sup{db = DB, shard = Shard}, []]},
        shutdown => infinity,
        restart => permanent,
        type => supervisor
    }.

shard_storage_spec(DB, Shard, Opts) ->
    #{
        id => {Shard, storage},
        start =>
            {emqx_ds_storage_layer, start_link, [{DB, Shard}, emqx_ds_lib:resolve_db_group(Opts)]},
        shutdown => 5_000,
        restart => permanent,
        type => worker
    }.

otx_leader_spec(DB, Shard) ->
    #{
        id => {Shard, otx_leader},
        start => {emqx_ds_optimistic_tx, start_link, [DB, Shard, emqx_ds_builtin_local]},
        shutdown => 5_000,
        restart => permanent,
        type => worker
    }.

shard_beamformers_spec(DB, Shard) ->
    #{n_workers_per_shard := NWorkers} = emqx_ds_builtin_local:beamformer_config(DB),
    BeamformerOpts = #{n_workers => NWorkers},
    #{
        id => {Shard, beamformers},
        type => supervisor,
        shutdown => infinity,
        start =>
            {emqx_ds_beamformer_sup, start_link, [
                emqx_ds_builtin_local, {DB, Shard}, BeamformerOpts
            ]}
    }.

ensure_started(Res) ->
    case Res of
        {ok, _Pid} ->
            ok;
        {error, {already_started, _Pid}} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.
