%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

%% @doc Supervisor that contains all the processes that belong to a
%% given builtin DS database.
-module(emqx_ds_builtin_local_db_sup).

-behaviour(supervisor).

%% API:
-export([
    start_db/2,
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

-spec start_db(emqx_ds:db(), emqx_ds_builtin_local:db_opts()) -> {ok, pid()}.
start_db(DB, Opts) ->
    start_link_sup(#?db_sup{db = DB}, Opts).

-spec start_shard(emqx_ds_storage_layer:shard_id()) ->
    supervisor:startchild_ret().
start_shard({DB, Shard}) ->
    supervisor:start_child(?via(#?shards_sup{db = DB}), shard_spec(DB, Shard)).

-spec stop_shard(emqx_ds_storage_layer:shard_id()) -> ok | {error, not_found}.
stop_shard({DB, Shard}) ->
    Sup = ?via(#?shards_sup{db = DB}),
    case supervisor:terminate_child(Sup, Shard) of
        ok ->
            supervisor:delete_child(Sup, Shard);
        {error, Reason} ->
            {error, Reason}
    end.

-spec terminate_storage(emqx_ds_storage_layer:shard_id()) -> ok | {error, _Reason}.
terminate_storage({DB, Shard}) ->
    Sup = ?via(#?shard_sup{db = DB, shard = Shard}),
    supervisor:terminate_child(Sup, {Shard, storage}).

-spec restart_storage(emqx_ds_storage_layer:shard_id()) -> {ok, _Child} | {error, _Reason}.
restart_storage({DB, Shard}) ->
    Sup = ?via(#?shard_sup{db = DB, shard = Shard}),
    supervisor:restart_child(Sup, {Shard, storage}).

-spec ensure_shard(emqx_ds_storage_layer:shard_id()) ->
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

init({#?db_sup{db = DB}, DefaultOpts}) ->
    %% Spec for the top-level supervisor for the database:
    logger:notice("Starting DS DB ~p", [DB]),
    emqx_ds_builtin_metrics:init_for_db(DB),
    Opts = emqx_ds_builtin_local_meta:open_db(DB, DefaultOpts),
    Children = [
        sup_spec(#?shards_sup{db = DB}, Opts),
        meta_spec(DB)
    ],
    SupFlags = #{
        strategy => one_for_all,
        intensity => 0,
        period => 1
    },
    {ok, {SupFlags, Children}};
init({#?shards_sup{db = DB}, _Opts}) ->
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
    Children = [
        shard_storage_spec(DB, Shard, Opts),
        shard_buffer_spec(DB, Shard, Opts),
        shard_batch_serializer_spec(DB, Shard, Opts),
        shard_beamformers_spec(DB, Shard, Opts)
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
        start => {emqx_ds_storage_layer, start_link, [{DB, Shard}, Opts]},
        shutdown => 5_000,
        restart => permanent,
        type => worker
    }.

shard_buffer_spec(DB, Shard, Options) ->
    #{
        id => {Shard, buffer},
        start => {emqx_ds_buffer, start_link, [emqx_ds_builtin_local, Options, DB, Shard]},
        shutdown => 5_000,
        restart => permanent,
        type => worker
    }.

shard_batch_serializer_spec(DB, Shard, Opts) ->
    #{
        id => {Shard, batch_serializer},
        start => {emqx_ds_builtin_local_batch_serializer, start_link, [DB, Shard, Opts]},
        shutdown => 5_000,
        restart => permanent,
        type => worker
    }.

shard_beamformers_spec(DB, Shard, _Options) ->
    %% TODO: don't hardcode value
    BeamformerOpts = #{
        n_workers => 5
    },
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
