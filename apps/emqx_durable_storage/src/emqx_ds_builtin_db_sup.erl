%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_ds_builtin_db_sup).

-behaviour(supervisor).

%% API:
-export([start_db/2, start_shard/1, start_egress/1, stop_shard/1, ensure_shard/1]).
-export([status/1]).

%% behaviour callbacks:
-export([init/1]).
-export([handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([start_link_sup/2]).

%% FIXME
-export([lookup_shard_meta/2]).

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

-define(shard_meta(DB, SHARD), {?MODULE, DB, SHARD}).

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

status(DB) ->
    State = sys:get_state(?via({allocator, DB})),
    maps:get(status, State).

lookup_shard_meta(DB, Shard) ->
    persistent_term:get(?shard_meta(DB, Shard)).

%%================================================================================
%% behaviour callbacks
%%================================================================================

init({#?db_sup{db = DB}, DefaultOpts}) ->
    %% Spec for the top-level supervisor for the database:
    logger:notice("Starting DS DB ~p", [DB]),
    Opts = emqx_ds_replication_layer_meta:open_db(DB, DefaultOpts),
    Children = [
        sup_spec(#?shard_sup{db = DB}, []),
        sup_spec(#?egress_sup{db = DB}, []),
        shard_allocator_spec(DB, Opts)
    ],
    SupFlags = #{
        strategy => one_for_all,
        intensity => 0,
        period => 1
    },
    {ok, {SupFlags, Children}};
init({#?shard_sup{db = _DB}, _}) ->
    %% Spec for the supervisor that manages the worker processes for
    %% each local shard of the DB:
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 1
    },
    {ok, {SupFlags, []}};
init({#?egress_sup{db = _DB}, _}) ->
    %% Spec for the supervisor that manages the egress proxy processes
    %% managing traffic towards each of the shards of the DB:
    SupFlags = #{
        strategy => one_for_one,
        intensity => 0,
        period => 1
    },
    {ok, {SupFlags, []}};
init({allocator, DB, Opts}) ->
    _ = erlang:process_flag(trap_exit, true),
    _ = logger:set_process_metadata(#{db => DB, domain => [ds, db, shard_allocator]}),
    init_allocator(DB, Opts).

start_shards(DB, Shards) ->
    SupRef = ?via(#?shard_sup{db = DB}),
    lists:foreach(
        fun(Shard) ->
            {ok, _} = supervisor:start_child(SupRef, shard_spec(DB, Shard)),
            {ok, _} = supervisor:start_child(SupRef, shard_replication_spec(DB, Shard))
        end,
        Shards
    ).

start_egresses(DB, Shards) ->
    SupRef = ?via(#?egress_sup{db = DB}),
    lists:foreach(
        fun(Shard) ->
            {ok, _} = supervisor:start_child(SupRef, egress_spec(DB, Shard))
        end,
        Shards
    ).

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
        id => {Shard, storage},
        start => {emqx_ds_storage_layer, start_link, [{DB, Shard}, Options]},
        shutdown => 5_000,
        restart => permanent,
        type => worker
    }.

shard_replication_spec(DB, Shard) ->
    #{
        id => {Shard, replication},
        start => {emqx_ds_replication_layer_shard, start_link, [DB, Shard]},
        restart => transient,
        type => worker
    }.

shard_allocator_spec(DB, Opts) ->
    #{
        id => shard_allocator,
        start =>
            {gen_server, start_link, [?via({allocator, DB}), ?MODULE, {allocator, DB, Opts}, []]},
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

%% Allocator

-define(ALLOCATE_RETRY_TIMEOUT, 1_000).

init_allocator(DB, Opts) ->
    State = #{db => DB, opts => Opts, status => allocating},
    case allocate_shards(State) of
        NState = #{} ->
            {ok, NState};
        {error, Data} ->
            _ = logger:notice(
                Data#{
                    msg => "Shard allocation still in progress",
                    retry_in => ?ALLOCATE_RETRY_TIMEOUT
                }
            ),
            {ok, State, ?ALLOCATE_RETRY_TIMEOUT}
    end.

handle_call(_Call, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(timeout, State) ->
    case allocate_shards(State) of
        NState = #{} ->
            {noreply, NState};
        {error, Data} ->
            _ = logger:notice(
                Data#{
                    msg => "Shard allocation still in progress",
                    retry_in => ?ALLOCATE_RETRY_TIMEOUT
                }
            ),
            {noreply, State, ?ALLOCATE_RETRY_TIMEOUT}
    end.

terminate(_Reason, #{db := DB, shards := Shards}) ->
    erase_shards_meta(DB, Shards).

%%

allocate_shards(State = #{db := DB, opts := Opts}) ->
    case emqx_ds_replication_layer_meta:allocate_shards(DB, Opts) of
        {ok, Shards} ->
            logger:notice(#{msg => "Shards allocated", shards => Shards}),
            ok = save_shards_meta(DB, Shards),
            ok = start_shards(DB, emqx_ds_replication_layer_meta:my_shards(DB)),
            logger:notice(#{
                msg => "Shards started", shards => emqx_ds_replication_layer_meta:my_shards(DB)
            }),
            ok = start_egresses(DB, Shards),
            logger:notice(#{msg => "Egresses started", shards => Shards}),
            State#{shards => Shards, status := ready};
        {error, Reason} ->
            {error, Reason}
    end.

save_shards_meta(DB, Shards) ->
    lists:foreach(fun(Shard) -> save_shard_meta(DB, Shard) end, Shards).

save_shard_meta(DB, Shard) ->
    Servers = emqx_ds_replication_layer_shard:shard_servers(DB, Shard),
    persistent_term:put(?shard_meta(DB, Shard), #{
        servers => Servers
    }).

erase_shards_meta(DB, Shards) ->
    lists:foreach(fun(Shard) -> erase_shard_meta(DB, Shard) end, Shards).

erase_shard_meta(DB, Shard) ->
    persistent_term:erase(?shard_meta(DB, Shard)).
