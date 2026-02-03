%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Supervisor that contains all the processes that belong to a
%% given builtin DS database.
-module(emqx_ds_builtin_raft_db_sup).

-behaviour(supervisor).

%% API:
-export([
    start_link_db/4,
    whereis_db/1,

    start_shard/1,
    stop_shard/1,
    shard_info/2,
    shard_sup/1,
    terminate_storage/1,
    restart_storage/1,
    ensure_shard/1,

    start_otx_leader/2,
    stop_otx_leader/2
]).
-export([which_dbs/0, which_shards/1]).

%% behaviour callbacks:
-export([init/1]).

%% internal exports:
-export([start_link_shard/2, start_link_sup/2]).

-include("emqx_ds_builtin_raft.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(name(REC), {n, l, REC}).
-define(via(REC), {via, gproc, ?name(REC)}).

-define(db_sup, ?MODULE).
-define(shards_sup, emqx_ds_builtin_raft_db_shards_sup).
-define(shard_sup, emqx_ds_builtin_raft_db_shard_sup).

-record(?db_sup, {db}).
-record(?shards_sup, {db}).
-record(?shard_sup, {db, shard}).

%%================================================================================
%% API functions
%%================================================================================

-spec start_link_db(
    emqx_ds:db(), Create, emqx_ds_builtin_raft:db_schema(), emqx_ds_builtin_raft:db_runtime_config()
) -> {ok, pid()} when Create :: boolean().
start_link_db(DB, Create, Schema, RTConf) ->
    start_link_sup(#?db_sup{db = DB}, [Create, Schema, RTConf]).

-spec whereis_db(emqx_ds:db()) -> pid() | undefined.
whereis_db(DB) ->
    gproc:where(?name(#?db_sup{db = DB})).

-spec start_shard(emqx_ds_storage_layer:dbshard()) ->
    supervisor:startchild_ret().
start_shard({DB, Shard}) ->
    supervisor:start_child(?via(#?shards_sup{db = DB}), [Shard]).

-spec stop_shard(emqx_ds_storage_layer:dbshard()) -> ok | {error, not_found}.
stop_shard({DB, Shard}) ->
    ShardsSup = ?via(#?shards_sup{db = DB}),
    case gproc:where(?name(#?shard_sup{db = DB, shard = Shard})) of
        Pid when is_pid(Pid) ->
            supervisor:terminate_child(ShardsSup, Pid);
        undefined ->
            {error, not_found}
    end.

-spec shard_info(emqx_ds_storage_layer:dbshard(), ready) -> boolean() | down.
shard_info({DB, Shard}, Info) ->
    case emqx_ds:is_shard_up(DB, Shard) of
        true -> emqx_ds_builtin_raft_shard:shard_info(DB, Shard, Info);
        false -> down
    end.

-spec shard_sup(emqx_ds_storage_layer:dbshard()) -> pid() | undefined.
shard_sup({DB, Shard}) ->
    gproc:where(?name(#?shard_sup{db = DB, shard = Shard})).

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

-spec which_shards(emqx_ds:db()) -> [emqx_ds:shard()].
which_shards(DB) ->
    Key = {n, l, #?shard_sup{db = DB, shard = '$1', _ = '_'}},
    gproc:select({local, names}, [{{Key, '_', '_'}, [], ['$1']}]).

%% @doc Return the list of builtin DS databases that are currently
%% active on the node.
-spec which_dbs() -> [emqx_ds:db()].
which_dbs() ->
    Key = {n, l, #?db_sup{_ = '_', db = '$1'}},
    gproc:select({local, names}, [{{Key, '_', '_'}, [], ['$1']}]).

%% @doc Start a supervisor for processes that should only run on the
%% shard leader.
-spec start_otx_leader(emqx_ds:db(), emqx_ds:shard()) ->
    {ok, pid()} | {error, _}.
start_otx_leader(DB, Shard) ->
    Sup = ?via(#?shard_sup{db = DB, shard = Shard}),
    case supervisor:start_child(Sup, shard_optimistic_tx_spec(DB, Shard)) of
        {ok, Pid} ->
            {ok, Pid};
        {error, {already_started, Pid}} ->
            {ok, Pid};
        Other ->
            Other
    end.

%% @doc Shut down the leader-specific processes when the node loses
%% leader status
-spec stop_otx_leader(emqx_ds:db(), emqx_ds:shard()) -> ok | {error, _}.
stop_otx_leader(DB, Shard) ->
    Sup = ?via(#?shard_sup{db = DB, shard = Shard}),
    Child = optimistic_tx,
    try
        supervisor:terminate_child(Sup, Child)
    catch
        exit:{noproc, _} ->
            {error, shard_sup_is_not_running}
    end.

%%================================================================================
%% behaviour callbacks
%%================================================================================

%% erlfmt-ignore
init({#?db_sup{db = DB}, [_Create, Schema, RTConf]}) ->
    %% Spec for the top-level supervisor for the database:
    logger:notice("Starting DS DB ~p", [DB]),
    %% FIXME: remove
    DefaultOpts = maps:merge(Schema, RTConf),
    emqx_ds_builtin_metrics:init_for_db(DB),
    Opts = emqx_ds_builtin_raft_meta:open_db(DB, DefaultOpts),
    ok = start_ra_system(DB, Opts),
    Children = [
        sup_spec(#?shards_sup{db = DB}, []),
        shard_allocator_spec(DB)
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
        strategy => simple_one_for_one,
        intensity => 10,
        period => 1
    },
    Children =
        [
         #{
           id => shard,
           start => {?MODULE, start_link_shard, [DB]},
           shutdown => infinity,
           restart => transient,
           type => supervisor
          }
        ],
    {ok, {SupFlags, Children}};
init({#?shard_sup{db = DB, shard = Shard}, _}) ->
    ok = emqx_ds_builtin_raft_metrics:init_local_shard(DB, Shard),
    SupFlags = #{
        strategy => rest_for_one,
        intensity => 10,
        period => 100
    },
    Setup = fun() -> ok end,
    Teardown = fun() ->
                       emqx_ds_builtin_raft:full_shard_cleanup(DB, Shard)
               end,
    #{runtime := RTConf} = emqx_dsch:get_db_runtime(DB),
    Children =
        [emqx_ds_lib:autoclean(shard_autoclean, 5_000, Setup, Teardown),
         shard_storage_spec(DB, Shard, RTConf),
         shard_replication_spec(DB, Shard, RTConf)] ++
         shard_beamformers_spec(DB, Shard),
    {ok, {SupFlags, Children}}.

start_ra_system(DB, #{replication_options := ReplicationOpts}) ->
    DataDir = filename:join([emqx_ds_storage_layer:base_dir(), DB, dsrepl]),
    Config = lists:foldr(fun maps:merge/2, #{}, [
        ra_system:default_config(),
        #{
            name => DB,
            data_dir => DataDir,
            wal_data_dir => DataDir,
            names => ra_system:derive_names(DB)
        },
        maps:with(
            [
                wal_max_size_bytes,
                wal_max_batch_size,
                wal_write_strategy,
                wal_sync_method,
                wal_compute_checksums
            ],
            ReplicationOpts
        )
    ]),
    case ra_system:start(Config) of
        {ok, _System} ->
            ok;
        {error, {already_started, _System}} ->
            ok
    end.

%%================================================================================
%% Internal exports
%%================================================================================

start_link_shard(DB, Shard) ->
    start_link_sup(#?shard_sup{db = DB, shard = Shard}, []).

start_link_sup(Id, Options) ->
    supervisor:start_link(?via(Id), ?MODULE, {Id, Options}).

%%================================================================================
%% Internal functions
%%================================================================================

sup_spec(Id, Options) ->
    #{
        id => element(1, Id),
        start => {?MODULE, start_link_sup, [Id, Options]},
        type => supervisor,
        shutdown => infinity
    }.

shard_storage_spec(DB, Shard, Opts) ->
    #{
        id => {Shard, storage},
        start =>
            {emqx_ds_storage_layer, start_link_no_schema, [
                {DB, Shard}, emqx_ds_lib:resolve_db_group(Opts)
            ]},
        shutdown => 5_000,
        restart => permanent,
        type => worker
    }.

shard_replication_spec(DB, Shard, RTConf) ->
    #{
        id => {Shard, replication},
        start => {emqx_ds_builtin_raft_shard, start_link, [DB, Shard, RTConf]},
        shutdown => 10_000,
        restart => transient,
        type => worker
    }.

shard_allocator_spec(DB) ->
    #{
        id => shard_allocator,
        start => {emqx_ds_builtin_raft_shard_allocator, start_link, [DB]},
        restart => permanent,
        type => worker
    }.

shard_beamformers_spec(DB, Shard) ->
    %% TODO: don't hardcode value
    BeamformerOpts = #{
        n_workers => 5
    },
    [
        #{
            id => {Shard, beamformers},
            type => supervisor,
            shutdown => infinity,
            start =>
                {emqx_ds_beamformer_sup, start_link, [
                    emqx_ds_builtin_raft, {DB, Shard}, BeamformerOpts
                ]}
        }
    ].

shard_optimistic_tx_spec(DB, Shard) ->
    %% Note: supervision and restarting of this server is handled by
    %% the ra process. We just attach it to this supervisor just for the
    %% cleanup reasons.
    #{
        id => optimistic_tx,
        type => worker,
        shutdown => 5_000,
        restart => temporary,
        start => {emqx_ds_optimistic_tx, start_link, [DB, Shard, emqx_ds_builtin_raft]}
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
