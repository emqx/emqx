%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Supervisor that contains all the processes that belong to a
%% given builtin DS database.
-module(emqx_ds_builtin_db_sup).

-behaviour(supervisor).

%% API:
-export([start_db/2, start_shard/1, start_egress/1, stop_shard/1, ensure_shard/1]).

%% behaviour callbacks:
-export([init/1]).

%% internal exports:
-export([start_link_sup/2]).

%%================================================================================
%% Type declarations
%%================================================================================

-define(via(REC), {via, gproc, {n, l, REC}}).

-define(db_sup, ?MODULE).
-define(shard_sup, emqx_ds_builtin_db_shard_sup).
-define(egress_sup, emqx_ds_builtin_db_egress_sup).

-record(?db_sup, {db}).
-record(?shard_sup, {db}).
-record(?egress_sup, {db}).

%%================================================================================
%% API funcions
%%================================================================================

-spec start_db(emqx_ds:db(), emqx_ds_replication_layer:builtin_db_opts()) -> {ok, pid()}.
start_db(DB, Opts) ->
    start_link_sup(#?db_sup{db = DB}, Opts).

-spec start_shard(emqx_ds_storage_layer:shard_id()) ->
    supervisor:startchild_ret().
start_shard(Shard = {DB, _}) ->
    supervisor:start_child(?via(#?shard_sup{db = DB}), shard_spec(DB, Shard)).

-spec start_egress(emqx_ds_storage_layer:shard_id()) ->
    supervisor:startchild_ret().
start_egress({DB, Shard}) ->
    supervisor:start_child(?via(#?egress_sup{db = DB}), egress_spec(DB, Shard)).

-spec stop_shard(emqx_ds_storage_layer:shard_id()) -> ok | {error, _}.
stop_shard(Shard = {DB, _}) ->
    Sup = ?via(#?shard_sup{db = DB}),
    ok = supervisor:terminate_child(Sup, Shard),
    ok = supervisor:delete_child(Sup, Shard).

-spec ensure_shard(emqx_ds_storage_layer:shard_id()) ->
    ok | {error, _Reason}.
ensure_shard(Shard) ->
    case start_shard(Shard) of
        {ok, _Pid} ->
            ok;
        {error, {already_started, _Pid}} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

%%================================================================================
%% behaviour callbacks
%%================================================================================

init({#?db_sup{db = DB}, DefaultOpts}) ->
    %% Spec for the top-level supervisor for the database:
    logger:notice("Starting DS DB ~p", [DB]),
    _ = emqx_ds_replication_layer_meta:open_db(DB, DefaultOpts),
    %% TODO: before the leader election is implemented, we set ourselves as the leader for all shards:
    MyShards = emqx_ds_replication_layer_meta:my_shards(DB),
    lists:foreach(
        fun(Shard) ->
            emqx_ds_replication_layer:maybe_set_myself_as_leader(DB, Shard)
        end,
        MyShards
    ),
    Children = [sup_spec(#?shard_sup{db = DB}, []), sup_spec(#?egress_sup{db = DB}, [])],
    SupFlags = #{
        strategy => one_for_all,
        intensity => 0,
        period => 1
    },
    {ok, {SupFlags, Children}};
init({#?shard_sup{db = DB}, _}) ->
    %% Spec for the supervisor that manages the worker processes for
    %% each local shard of the DB:
    MyShards = emqx_ds_replication_layer_meta:my_shards(DB),
    Children = [shard_spec(DB, Shard) || Shard <- MyShards],
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 1
    },
    {ok, {SupFlags, Children}};
init({#?egress_sup{db = DB}, _}) ->
    %% Spec for the supervisor that manages the egress proxy processes
    %% managing traffic towards each of the shards of the DB:
    Shards = emqx_ds_replication_layer_meta:shards(DB),
    Children = [egress_spec(DB, Shard) || Shard <- Shards],
    SupFlags = #{
        strategy => one_for_one,
        intensity => 0,
        period => 1
    },
    {ok, {SupFlags, Children}}.

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
    Options = emqx_ds_replication_layer_meta:get_options(DB),
    #{
        id => Shard,
        start => {emqx_ds_storage_layer, start_link, [{DB, Shard}, Options]},
        shutdown => 5_000,
        restart => permanent,
        type => worker
    }.

egress_spec(DB, Shard) ->
    #{
        id => Shard,
        start => {emqx_ds_replication_layer_egress, start_link, [DB, Shard]},
        shutdown => 5_000,
        restart => permanent,
        type => worker
    }.
