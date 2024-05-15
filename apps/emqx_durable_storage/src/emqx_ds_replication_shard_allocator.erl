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

%% Maintenace purposes:
-export([trigger_transitions/1]).

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
-define(CRASH_RETRY_DELAY, 20_000).
-define(REMOVE_REPLICA_DELAY, {10_000, 5_000}).

-ifdef(TEST).
-undef(TRANS_RETRY_TIMEOUT).
-undef(REMOVE_REPLICA_DELAY).
-define(TRANS_RETRY_TIMEOUT, 1_000).
-define(REMOVE_REPLICA_DELAY, {3_000, 2_000}).
-endif.

%%

-record(trigger_transitions, {}).

-spec start_link(emqx_ds:db()) -> {ok, pid()}.
start_link(DB) ->
    gen_server:start_link(?MODULE, DB, []).

-spec trigger_transitions(pid()) -> ok.
trigger_transitions(Pid) ->
    gen_server:cast(Pid, #trigger_transitions{}).

-spec n_shards(emqx_ds:db()) -> non_neg_integer().
n_shards(DB) ->
    Meta = persistent_term:get(?db_meta(DB)),
    maps:get(n_shards, Meta).

shard_meta(DB, Shard) ->
    persistent_term:get(?shard_meta(DB, Shard)).

%%

-record(transhdl, {
    shard :: emqx_ds_replication_layer:shard_id(),
    trans :: emqx_ds_replication_layer_meta:transition(),
    pid :: pid()
}).

-type state() :: #{
    db := emqx_ds:db(),
    shards := [emqx_ds_replication_layer:shard_id()],
    status := allocating | ready,
    transitions := #{_Track => #transhdl{}}
}.

-spec init(emqx_ds:db()) -> {ok, state()}.
init(DB) ->
    _ = erlang:process_flag(trap_exit, true),
    _ = logger:set_process_metadata(#{db => DB, domain => [emqx, ds, DB, shard_allocator]}),
    State = #{
        db => DB,
        shards => [],
        status => allocating,
        transitions => #{}
    },
    {ok, handle_allocate_shards(State)}.

-spec handle_call(_Call, _From, state()) -> {reply, ignored, state()}.
handle_call(_Call, _From, State) ->
    {reply, ignored, State}.

-spec handle_cast(_Cast, state()) -> {noreply, state()}.
handle_cast(#trigger_transitions{}, State) ->
    {noreply, handle_pending_transitions(State)};
handle_cast(_Cast, State) ->
    {noreply, State}.

-spec handle_info(Info, state()) -> {noreply, state()} when
    Info ::
        emqx_ds_replication_layer_meta:subscription_event()
        | {timeout, reference(), allocate}
        | {'EXIT', pid(), _Reason}.
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

-spec terminate(_Reason, state()) -> _Ok.
terminate(_Reason, State = #{db := DB, shards := Shards}) ->
    unsubscribe_db_changes(State),
    erase_db_meta(DB),
    erase_shards_meta(DB, Shards);
terminate(_Reason, #{}) ->
    ok.

%%

handle_allocate_shards(State0) ->
    case allocate_shards(State0) of
        {ok, State} ->
            %% NOTE
            %% Subscribe to shard changes and trigger any yet unhandled transitions.
            ok = subscribe_db_changes(State),
            ok = trigger_transitions(self()),
            State;
        {error, Data} ->
            _ = logger:notice(
                Data#{
                    msg => "Shard allocation still in progress",
                    retry_in => ?ALLOCATE_RETRY_TIMEOUT
                }
            ),
            _TRef = erlang:start_timer(?ALLOCATE_RETRY_TIMEOUT, self(), allocate),
            State0
    end.

subscribe_db_changes(#{db := DB}) ->
    emqx_ds_replication_layer_meta:subscribe(self(), DB).

unsubscribe_db_changes(_State) ->
    emqx_ds_replication_layer_meta:unsubscribe(self()).

%%

handle_shard_changed(Shard, State = #{db := DB}) ->
    ok = save_shard_meta(DB, Shard),
    handle_shard_transitions(Shard, next_transitions(DB, Shard), State).

handle_pending_transitions(State = #{db := DB, shards := Shards}) ->
    lists:foldl(
        fun(Shard, StateAcc) ->
            handle_shard_transitions(Shard, next_transitions(DB, Shard), StateAcc)
        end,
        State,
        Shards
    ).

next_transitions(DB, Shard) ->
    emqx_ds_replication_layer_meta:replica_set_transitions(DB, Shard).

handle_shard_transitions(_Shard, [], State) ->
    %% We reached the target allocation.
    State;
handle_shard_transitions(Shard, [Trans | _Rest], State) ->
    case transition_handler(Shard, Trans, State) of
        {Track, Handler} ->
            ensure_transition_handler(Track, Shard, Trans, Handler, State);
        undefined ->
            State
    end.

transition_handler(Shard, Trans, _State = #{db := DB}) ->
    ThisSite = catch emqx_ds_replication_layer_meta:this_site(),
    case Trans of
        {add, ThisSite} ->
            {Shard, fun trans_add_local/3};
        {del, ThisSite} ->
            {Shard, fun trans_drop_local/3};
        {del, Site} ->
            ReplicaSet = emqx_ds_replication_layer_meta:replica_set(DB, Shard),
            case lists:member(Site, ReplicaSet) of
                true ->
                    %% NOTE
                    %% Let the replica handle its own removal first, but still set
                    %% up a removal handler after a delay, in case the replica is
                    %% unresponsive.
                    Handler = {fun trans_delay/5, [
                        ?REMOVE_REPLICA_DELAY,
                        fun trans_rm_unresponsive/3
                    ]},
                    %% NOTE
                    %% Putting this transition handler on separate "track" so that it
                    %% won't block any changes with higher priority (e.g. managing
                    %% local replicas).
                    {{unresp, Shard}, Handler};
                false ->
                    undefined
            end;
        _NotOurs ->
            %% This site is not involved in the next queued transition.
            undefined
    end.

handle_transition(DB, Shard, Trans, Handler) ->
    logger:set_process_metadata(#{
        db => DB,
        shard => Shard,
        domain => [emqx, ds, DB, shard_transition]
    }),
    ?tp(
        dsrepl_shard_transition_begin,
        #{shard => Shard, db => DB, transition => Trans, pid => self()}
    ),
    apply_handler(Handler, DB, Shard, Trans).

apply_handler({Fun, Args}, DB, Shard, Trans) ->
    erlang:apply(Fun, [DB, Shard, Trans | Args]);
apply_handler(Fun, DB, Shard, Trans) ->
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

trans_rm_unresponsive(DB, Shard, {del, Site}) ->
    logger:info(#{msg => "Removing unresponsive shard replica", site => Site}),
    do_rm_unresponsive(DB, Shard, Site).

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

trans_delay(DB, Shard, Trans, Delay, NextHandler) ->
    ok = delay(Delay),
    %% NOTE: Proceed only if the transition we are going to handle is still desired.
    case next_transitions(DB, Shard) of
        [Trans | _] ->
            apply_handler(NextHandler, DB, Shard, Trans);
        _Outdated ->
            exit({shutdown, skipped})
    end.

%%

ensure_transition_handler(Track, Shard, Trans, Handler, State = #{transitions := Ts}) ->
    case maps:get(Track, Ts, undefined) of
        undefined ->
            Pid = start_transition_handler(Shard, Trans, Handler, State),
            Record = #transhdl{shard = Shard, trans = Trans, pid = Pid},
            State#{transitions := Ts#{Track => Record}};
        _AlreadyRunning ->
            %% NOTE: Avoiding multiple transition handlers for the same shard for safety.
            State
    end.

start_transition_handler(Shard, Trans, Handler, #{db := DB}) ->
    proc_lib:spawn_link(?MODULE, handle_transition, [DB, Shard, Trans, Handler]).

handle_exit(Pid, Reason, State0 = #{db := DB, transitions := Ts}) ->
    case maps:to_list(maps:filter(fun(_, TH) -> TH#transhdl.pid == Pid end, Ts)) of
        [{Track, #transhdl{shard = Shard, trans = Trans}}] ->
            ?tp(
                dsrepl_shard_transition_end,
                #{shard => Shard, db => DB, transition => Trans, pid => Pid, reason => Reason}
            ),
            State = State0#{transitions := maps:remove(Track, Ts)},
            handle_transition_exit(Shard, Trans, Reason, State);
        [] ->
            %% NOTE
            %% Actually, it's sort of expected to have a portion of exit signals here,
            %% because of `mria:with_middleman/3`. But it's impossible to tell them apart
            %% from other singals.
            logger:warning(#{msg => "Unexpected exit signal", pid => Pid, reason => Reason}),
            State0
    end.

handle_transition_exit(Shard, Trans, normal, State = #{db := DB}) ->
    %% NOTE: This will trigger the next transition if any.
    ok = emqx_ds_replication_layer_meta:update_replica_set(DB, Shard, Trans),
    State;
handle_transition_exit(_Shard, _Trans, {shutdown, skipped}, State) ->
    State;
handle_transition_exit(Shard, Trans, Reason, State) ->
    logger:warning(#{
        msg => "Shard membership transition failed",
        shard => Shard,
        transition => Trans,
        reason => Reason,
        retry_in => ?CRASH_RETRY_DELAY
    }),
    %% NOTE
    %% In case of `{add, Site}` transition failure, we have no choice but to retry:
    %% no other node can perform the transition and make progress towards the desired
    %% state.
    case Trans of
        {add, _ThisSite} ->
            {Track, Handler} = transition_handler(Shard, Trans, State),
            RetryHandler = {fun trans_delay/5, [?CRASH_RETRY_DELAY, Handler]},
            ensure_transition_handler(Track, Shard, Trans, RetryHandler, State);
        _Another ->
            State
    end.

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
    timer:sleep(MinDelay + rand:uniform(Variance));
delay(Delay) ->
    timer:sleep(Delay).
