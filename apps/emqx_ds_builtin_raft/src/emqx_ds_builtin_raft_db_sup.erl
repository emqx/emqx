%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Supervisor that contains all the processes that belong to a
%% given builtin DS database.
-module(emqx_ds_builtin_raft_db_sup).

-behaviour(supervisor).

%% API:
-export([
    start_db/4,
    start_shard/1,
    stop_shard/1,
    shard_info/2,
    shard_sup/1,
    terminate_storage/1,
    restart_storage/1,
    ensure_shard/1,

    start_shard_leader_sup/2,
    stop_shard_leader_sup/2
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

-include("emqx_ds_builtin_raft.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(name(REC), {n, l, REC}).
-define(via(REC), {via, gproc, ?name(REC)}).

-define(db_sup, ?MODULE).
-define(shards_sup, emqx_ds_builtin_raft_db_shards_sup).
-define(shard_sup, emqx_ds_builtin_raft_db_shard_sup).
-define(shard_leader_sup, emqx_ds_builtin_raft_db_shard_leader_sup).

-record(?db_sup, {db}).
-record(?shards_sup, {db}).
-record(?shard_sup, {db, shard}).
-record(?shard_leader_sup, {db, shard}).

%%================================================================================
%% API functions
%%================================================================================

-spec start_db(
    emqx_ds:db(), Create, emqx_ds_builtin_raft:db_schema(), emqx_ds_builtin_raft:db_runtime_config()
) -> {ok, pid()} when Create :: boolean().
start_db(DB, Create, Schema, RTConf) ->
    start_link_sup(#?db_sup{db = DB}, [Create, Schema, RTConf]).

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

%% @doc Start a supervisor for processes that should only run on the
%% shard leader.
-spec start_shard_leader_sup(emqx_ds:db(), emqx_ds:shard()) ->
    {ok, pid()} | {error, _}.
start_shard_leader_sup(DB, Shard) ->
    Sup = ?via(#?shard_sup{db = DB, shard = Shard}),
    _ = stop_shard_leader_sup(DB, Shard),
    case supervisor:start_child(Sup, shard_leader_spec(DB, Shard)) of
        {ok, Pid} ->
            {ok, Pid};
        Err ->
            Err
    end.

%% @doc Shut down the leader-specific processes when the node loses
%% leader status
-spec stop_shard_leader_sup(emqx_ds:db(), emqx_ds:shard()) -> ok.
stop_shard_leader_sup(DB, Shard) ->
    Sup = ?via(#?shard_sup{db = DB, shard = Shard}),
    Child = ?shard_leader_sup,
    case supervisor:terminate_child(Sup, Child) of
        ok ->
            supervisor:delete_child(Sup, Child);
        {error, Reason} ->
            {error, Reason}
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
        emqx_ds_lib:autoclean(
          autoclean,
          20_000,
          fun() -> ok = emqx_dsch:open_db(DB, RTConf) end,
          fun() ->
                  _ = emqx_ds_db_group_mgr:detach(DB),
                  ok = emqx_dsch:close_db(DB)
          end
         ),
        sup_spec(#?shards_sup{db = DB}, []),
        shard_allocator_spec(DB),
        db_lifecycle_spec(DB)
    ],
    SupFlags = #{
        strategy => one_for_all,
        intensity => 0,
        period => 1
    },
    {ok, {SupFlags, Children}};
init({#?shards_sup{db = _DB}, _}) ->
    %% Spec for the supervisor that manages the supervisors for
    %% each local shard of the DB:
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 1
    },
    {ok, {SupFlags, []}};
init({#?shard_sup{db = DB, shard = Shard}, _}) ->
    ok = emqx_ds_builtin_raft_metrics:init_local_shard(DB, Shard),
    SupFlags = #{
        strategy => rest_for_one,
        intensity => 10,
        period => 100
    },
    Schema = emqx_dsch:get_db_schema(DB),
    #{runtime := RTConf} = emqx_dsch:get_db_runtime(DB),
    Opts = maps:merge(Schema, RTConf),
    Children =
        [shard_storage_spec(DB, Shard, Opts),
         shard_replication_spec(DB, Shard, Schema, RTConf)] ++
         shard_beamformers_spec(DB, Shard, Opts),
    {ok, {SupFlags, Children}};
init({#?shard_leader_sup{db = DB, shard = Shard}, _}) ->
    %% Spec for a temporary supervisor that runs on the node only when
    %% it happens to be the raft leader for the shard.
    SupFlags = #{
                 strategy => one_for_all,
                 intensity => 10,
                 period => 100
                },
    Setup = fun() -> ok end,
    Teardown = fun() -> emqx_dsch:gvar_unset_all(DB, Shard, ?gv_sc_leader) end,
    Children = [
                emqx_ds_lib:autoclean(shard_autoclean, 5_000, Setup, Teardown),
                shard_optimistic_tx_spec(DB, Shard)
               ],
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

shard_spec(DB, Shard) ->
    #{
        id => Shard,
        start => {?MODULE, start_link_sup, [#?shard_sup{db = DB, shard = Shard}, []]},
        shutdown => infinity,
        restart => transient,
        type => supervisor
    }.

shard_leader_spec(DB, Shard) ->
    %% Note: ra machines have some rudimentary facilities for process
    %% supervision, however they aren't very reliable and/or easy to
    %% use. Since some features will break if the leader-specific
    %% processes die while the node assumes leadership, we address
    %% this problem using a traditional supervisor.
    #{
        id => ?shard_leader_sup,
        start => {?MODULE, start_link_sup, [#?shard_leader_sup{db = DB, shard = Shard}, []]},
        shutdown => infinity,
        restart => transient,
        type => supervisor
    }.

shard_storage_spec(DB, Shard, Opts) ->
    #{
        id => {Shard, storage},
        start => {emqx_ds_storage_layer, start_link, [{DB, Shard}, Opts]},
        shutdown => 5_000,
        restart => permanent,
        type => worker
    }.

shard_replication_spec(DB, Shard, Schema, RTConf) ->
    #{
        id => {Shard, replication},
        start => {emqx_ds_builtin_raft_shard, start_link, [DB, Shard, Schema, RTConf]},
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

db_lifecycle_spec(DB) ->
    #{
        id => lifecycle,
        start => {emqx_ds_builtin_raft_db_lifecycle, start_link, [DB]},
        restart => permanent,
        type => worker
    }.

shard_beamformers_spec(DB, Shard, _Opts) ->
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
    #{
        id => optimistic_tx,
        type => worker,
        shutdown => 1_000,
        restart => permanent,
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
