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

-module(emqx_ds_replication_shard_allocator).

-include_lib("snabbkaffe/include/trace.hrl").

-export([start_link/1]).

-export([n_shards/1]).
-export([shard_meta/2]).

-behaviour(gen_server).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-export([handle_transition/4]).

-define(db_meta(DB), {?MODULE, DB}).
-define(shard_meta(DB, SHARD), {?MODULE, DB, SHARD}).

-define(ALLOCATE_RETRY_TIMEOUT, 1_000).

-define(TRANS_RETRY_TIMEOUT, 5_000).
-define(REMOVE_REPLICA_DELAY, {10_000, 5_000}).

-ifdef(TEST).
-undef(TRANS_RETRY_TIMEOUT).
-undef(REMOVE_REPLICA_DELAY).
-define(TRANS_RETRY_TIMEOUT, 1_000).
-define(REMOVE_REPLICA_DELAY, {4_000, 2_000}).
-endif.

%%

start_link(DB) ->
    gen_server:start_link(?MODULE, DB, []).

n_shards(DB) ->
    Meta = persistent_term:get(?db_meta(DB)),
    maps:get(n_shards, Meta).

shard_meta(DB, Shard) ->
    persistent_term:get(?shard_meta(DB, Shard)).

%%

init(DB) ->
    _ = erlang:process_flag(trap_exit, true),
    _ = logger:set_process_metadata(#{db => DB, domain => [emqx, ds, DB, shard_allocator]}),
    State = #{db => DB, transitions => #{}, status => allocating},
    {ok, handle_allocate_shards(State)}.

handle_call(_Call, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info({timeout, _TRef, allocate}, State) ->
    {noreply, handle_allocate_shards(State)};
handle_info({changed, {shard, DB, Shard}}, State = #{db := DB}) ->
    {noreply, handle_shard_changed(Shard, State)};
handle_info({changed, _}, State) ->
    {noreply, State};
handle_info({'EXIT', Pid, Reason}, State) ->
    {noreply, handle_exit(Pid, Reason, State)};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State = #{db := DB, shards := Shards}) ->
    unsubscribe_db_changes(State),
    erase_db_meta(DB),
    erase_shards_meta(DB, Shards);
terminate(_Reason, #{}) ->
    ok.

%%

handle_allocate_shards(State) ->
    case allocate_shards(State) of
        {ok, NState} ->
            ok = subscribe_db_changes(State),
            NState;
        {error, Data} ->
            _ = logger:notice(
                Data#{
                    msg => "Shard allocation still in progress",
                    retry_in => ?ALLOCATE_RETRY_TIMEOUT
                }
            ),
            _TRef = erlang:start_timer(?ALLOCATE_RETRY_TIMEOUT, self(), allocate),
            State
    end.

subscribe_db_changes(#{db := DB}) ->
    emqx_ds_replication_layer_meta:subscribe(self(), DB).

unsubscribe_db_changes(_State) ->
    emqx_ds_replication_layer_meta:unsubscribe(self()).

%%

handle_shard_changed(Shard, State = #{db := DB}) ->
    ok = save_shard_meta(DB, Shard),
    Transitions = emqx_ds_replication_layer_meta:replica_set_transitions(DB, Shard),
    handle_shard_transitions(Shard, Transitions, State).

handle_shard_transitions(Shard, Transitions, State = #{db := DB}) ->
    ThisSite = emqx_ds_replication_layer_meta:this_site(),
    case Transitions of
        [] ->
            %% We reached the target allocation.
            State;
        [Trans = {add, ThisSite} | _Rest] ->
            ensure_transition_handler(Shard, Trans, fun trans_add_local/3, State);
        [Trans = {del, ThisSite} | _Rest] ->
            ensure_transition_handler(Shard, Trans, fun trans_drop_local/3, State);
        [Trans = {del, Site} | _Rest] ->
            ReplicaSet = emqx_ds_replication_layer_meta:replica_set(DB, Shard),
            case lists:member(Site, ReplicaSet) of
                true ->
                    %% NOTE
                    %% Putting this transition handler on separate "track" so that it
                    %% won't block any changes with higher priority (e.g. managing
                    %% local replicas).
                    Handler = fun trans_rm_unresponsive/3,
                    ensure_transition_handler(unresp, Shard, Trans, Handler, State);
                false ->
                    State
            end;
        [_Trans | _Rest] ->
            %% This site is not involved in the next queued transition.
            State
    end.

handle_transition(DB, Shard, Trans, Fun) ->
    logger:set_process_metadata(#{
        db => DB,
        shard => Shard,
        domain => [emqx, ds, DB, shard_transition]
    }),
    ?tp(
        dsrepl_shard_transition_begin,
        #{shard => Shard, db => DB, transition => Trans, pid => self()}
    ),
    erlang:apply(Fun, [DB, Shard, Trans]).

trans_add_local(DB, Shard, {add, Site}) ->
    logger:info(#{msg => "Adding new local shard replica", site => Site}),
    do_add_local(membership, DB, Shard).

do_add_local(membership = Stage, DB, Shard) ->
    ok = start_shard(DB, Shard),
    case emqx_ds_replication_layer_shard:add_local_server(DB, Shard) of
        ok ->
            do_add_local(readiness, DB, Shard);
        {error, recoverable, Reason} ->
            logger:warning(#{
                msg => "Shard membership change failed",
                reason => Reason,
                retry_in => ?TRANS_RETRY_TIMEOUT
            }),
            ok = timer:sleep(?TRANS_RETRY_TIMEOUT),
            do_add_local(Stage, DB, Shard)
    end;
do_add_local(readiness = Stage, DB, Shard) ->
    LocalServer = emqx_ds_replication_layer_shard:local_server(DB, Shard),
    case emqx_ds_replication_layer_shard:server_info(readiness, LocalServer) of
        ready ->
            logger:info(#{msg => "Local shard replica ready"});
        Status ->
            logger:warning(#{
                msg => "Still waiting for local shard replica to be ready",
                status => Status,
                retry_in => ?TRANS_RETRY_TIMEOUT
            }),
            ok = timer:sleep(?TRANS_RETRY_TIMEOUT),
            do_add_local(Stage, DB, Shard)
    end.

trans_drop_local(DB, Shard, {del, Site}) ->
    logger:info(#{msg => "Dropping local shard replica", site => Site}),
    do_drop_local(DB, Shard).

do_drop_local(DB, Shard) ->
    case emqx_ds_replication_layer_shard:drop_local_server(DB, Shard) of
        ok ->
            ok = emqx_ds_builtin_db_sup:stop_shard({DB, Shard}),
            ok = emqx_ds_storage_layer:drop_shard({DB, Shard}),
            logger:info(#{msg => "Local shard replica dropped"});
        {error, recoverable, Reason} ->
            logger:warning(#{
                msg => "Shard membership change failed",
                reason => Reason,
                retry_in => ?TRANS_RETRY_TIMEOUT
            }),
            ok = timer:sleep(?TRANS_RETRY_TIMEOUT),
            do_drop_local(DB, Shard)
    end.

trans_rm_unresponsive(DB, Shard, Trans = {del, Site}) ->
    %% NOTE
    %% Let the replica handle its own removal first, thus the delay.
    ok = delay(?REMOVE_REPLICA_DELAY),
    Transitions = emqx_ds_replication_layer_meta:replica_set_transitions(DB, Shard),
    case Transitions of
        [Trans | _] ->
            logger:info(#{msg => "Removing unresponsive shard replica", site => Site}),
            do_rm_unresponsive(DB, Shard, Site);
        _Outdated ->
            exit({shutdown, skipped})
    end.

do_rm_unresponsive(DB, Shard, Site) ->
    Server = emqx_ds_replication_layer_shard:shard_server(DB, Shard, Site),
    case emqx_ds_replication_layer_shard:remove_server(DB, Shard, Server) of
        ok ->
            logger:info(#{msg => "Unresponsive shard replica removed"});
        {error, recoverable, Reason} ->
            logger:warning(#{
                msg => "Shard membership change failed",
                reason => Reason,
                retry_in => ?TRANS_RETRY_TIMEOUT
            }),
            ok = timer:sleep(?TRANS_RETRY_TIMEOUT),
            do_rm_unresponsive(DB, Shard, Site)
    end.

%%

ensure_transition_handler(Shard, Trans, Handler, State) ->
    ensure_transition_handler(Shard, Shard, Trans, Handler, State).

ensure_transition_handler(Track, Shard, Trans, Handler, State = #{transitions := Ts}) ->
    case maps:get(Track, Ts, undefined) of
        undefined ->
            Pid = start_transition_handler(Shard, Trans, Handler, State),
            State#{transitions := Ts#{Track => {Shard, Trans, Pid}}};
        _AlreadyRunning ->
            %% NOTE: Avoiding multiple transition handlers for the same shard for safety.
            State
    end.

start_transition_handler(Shard, Trans, Handler, #{db := DB}) ->
    proc_lib:spawn_link(?MODULE, handle_transition, [DB, Shard, Trans, Handler]).

handle_exit(Pid, Reason, State = #{db := DB, transitions := Ts}) ->
    case maps:to_list(maps:filter(fun(_, {_S, _T, P}) -> P == Pid end, Ts)) of
        [{Track, {Shard, Trans, Pid}}] ->
            ?tp(
                dsrepl_shard_transition_end,
                #{shard => Shard, db => DB, transition => Trans, pid => Pid, reason => Reason}
            ),
            ok = handle_transition_exit(Shard, Trans, Reason, State),
            State#{transitions := maps:remove(Track, Ts)};
        [] ->
            logger:warning(#{msg => "Unexpected exit signal", pid => Pid, reason => Reason}),
            State
    end.

handle_transition_exit(Shard, Trans, normal, _State = #{db := DB}) ->
    %% NOTE: This will trigger the next transition if any.
    ok = emqx_ds_replication_layer_meta:update_replica_set(DB, Shard, Trans);
handle_transition_exit(_Shard, _Trans, {shutdown, skipped}, _State) ->
    ok;
handle_transition_exit(Shard, Trans, Reason, _State) ->
    logger:warning(#{
        msg => "Shard membership transition failed",
        shard => Shard,
        transition => Trans,
        reason => Reason
    }),
    %% FIXME: retry
    ok.

%%

allocate_shards(State = #{db := DB}) ->
    case emqx_ds_replication_layer_meta:allocate_shards(DB) of
        {ok, Shards} ->
            logger:info(#{msg => "Shards allocated", shards => Shards}),
            ok = start_shards(DB, emqx_ds_replication_layer_meta:my_shards(DB)),
            ok = start_egresses(DB, Shards),
            ok = save_db_meta(DB, Shards),
            ok = save_shards_meta(DB, Shards),
            {ok, State#{shards => Shards, status := ready}};
        {error, Reason} ->
            {error, Reason}
    end.

start_shards(DB, Shards) ->
    lists:foreach(fun(Shard) -> start_shard(DB, Shard) end, Shards).

start_shard(DB, Shard) ->
    ok = emqx_ds_builtin_db_sup:ensure_shard({DB, Shard}),
    ok = logger:info(#{msg => "Shard started", shard => Shard}),
    ok.

start_egresses(DB, Shards) ->
    lists:foreach(fun(Shard) -> start_egress(DB, Shard) end, Shards).

start_egress(DB, Shard) ->
    ok = emqx_ds_builtin_db_sup:ensure_egress({DB, Shard}),
    ok = logger:info(#{msg => "Egress started", shard => Shard}),
    ok.

%%

save_db_meta(DB, Shards) ->
    persistent_term:put(?db_meta(DB), #{
        shards => Shards,
        n_shards => length(Shards)
    }).

save_shards_meta(DB, Shards) ->
    lists:foreach(fun(Shard) -> save_shard_meta(DB, Shard) end, Shards).

save_shard_meta(DB, Shard) ->
    Servers = emqx_ds_replication_layer_shard:shard_servers(DB, Shard),
    persistent_term:put(?shard_meta(DB, Shard), #{
        servers => Servers
    }).

erase_db_meta(DB) ->
    persistent_term:erase(?db_meta(DB)).

erase_shards_meta(DB, Shards) ->
    lists:foreach(fun(Shard) -> erase_shard_meta(DB, Shard) end, Shards).

erase_shard_meta(DB, Shard) ->
    persistent_term:erase(?shard_meta(DB, Shard)).

%%

delay({MinDelay, Variance}) ->
    timer:sleep(MinDelay + rand:uniform(Variance)).
