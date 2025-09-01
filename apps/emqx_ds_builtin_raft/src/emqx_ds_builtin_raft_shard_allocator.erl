%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Shard allocator is a process responsible for orchestrating initial allocation
%% and subsequent transitions of each shard's replica set to the desired state,
%% according to the Metadata DB.
%% Each DB runs a single shard allocator.
-module(emqx_ds_builtin_raft_shard_allocator).

-include_lib("snabbkaffe/include/trace.hrl").
%%-include("emqx_ds_replication_layer.hrl").

-export([start_link/1]).

-export([n_shards/1, shards/1]).

%% Maintenace purposes:
-export([trigger_transitions/1]).

-behaviour(gen_server).
-export([
    init/1,
    handle_continue/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-export([handle_transition/4]).

-define(db_meta(DB), {?MODULE, DB}).

-define(ALLOCATE_RETRY_TIMEOUT, 1_000).
-define(TRIGGER_PENDING_TIMEOUT, 60_000).

-define(TRANS_RETRY_TIMEOUT, 5_000).

-ifdef(TEST).
-undef(TRANS_RETRY_TIMEOUT).
-undef(TRIGGER_PENDING_TIMEOUT).
-define(TRANS_RETRY_TIMEOUT, 1_000).
-define(TRIGGER_PENDING_TIMEOUT, 5_000).
-endif.

-elvis([{elvis_style, no_catch_expressions, disable}]).

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

-spec shards(emqx_ds:db()) -> [emqx_ds:shard()].
shards(DB) ->
    Meta = persistent_term:get(?db_meta(DB)),
    maps:get(shards, Meta, []).

%%

-record(transhdl, {
    shard :: emqx_ds:shard(),
    trans :: emqx_ds_builtin_raft_meta:transition(),
    pid :: pid()
}).

-type state() :: #{
    db := emqx_ds:db(),
    shards := [emqx_ds:shard()],
    status := allocating | ready,
    transitions := #{_Track => #transhdl{}},
    timers := #{atom() => reference()}
}.

-spec init(emqx_ds:db()) -> {ok, undefined, {continue, {init, emqx_ds:db()}}}.
init(DB) ->
    _ = erlang:process_flag(trap_exit, true),
    _ = logger:set_process_metadata(#{db => DB, domain => [emqx, ds, DB, shard_allocator]}),
    {ok, undefined, {continue, {init, DB}}}.

handle_continue({init, DB}, _) ->
    State = #{
        db => DB,
        shards => [],
        status => allocating,
        transitions => #{},
        timers => #{}
    },
    {noreply, handle_allocate_shards(State)}.

-spec handle_call(_Call, _From, state()) -> {reply, ignored, state()}.
handle_call(_Call, _From, State) ->
    {reply, ignored, State}.

-spec handle_cast(_Cast, state()) -> {noreply, state()}.
handle_cast(#trigger_transitions{}, State0) ->
    State1 = handle_pending_transitions(State0),
    State = restart_fallback_timer(State1),
    {noreply, State};
handle_cast(_Cast, State) ->
    {noreply, State}.

-spec handle_info(Info, state()) -> {noreply, state()} when
    Info ::
        emqx_ds_builtin_raft_meta:subscription_event()
        | {timeout, reference(), allocate | fallback}
        | {'EXIT', pid(), _Reason}.
handle_info({timeout, TRef, Name}, State0) ->
    State = clear_timer(Name, State0),
    handle_timer(Name, TRef, State);
handle_info({changed, {shard, DB, Shard}}, State0 = #{db := DB}) ->
    State1 = handle_shard_changed(Shard, State0),
    State = restart_fallback_timer(State1),
    {noreply, State};
handle_info({changed, _}, State) ->
    {noreply, State};
handle_info({'EXIT', Pid, Reason}, State) ->
    {noreply, handle_exit(Pid, Reason, State)};
handle_info(_Info, State) ->
    {noreply, State}.

handle_timer(allocate, _TRef, State0) ->
    State = handle_allocate_shards(State0),
    {noreply, State};
handle_timer(fallback, _TRef, State0) ->
    State1 = handle_pending_transitions(State0),
    State = restart_fallback_timer(State1),
    {noreply, State}.

-spec terminate(_Reason, state()) -> _Ok.
terminate(_Reason, State = #{db := DB, shards := Shards}) ->
    unsubscribe_db_changes(State),
    clear_shard_cache(DB, Shards),
    erase_db_meta(DB);
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
            start_fallback_timer(State);
        {error, Data} ->
            _ = logger:notice(
                Data#{
                    msg => "Shard allocation still in progress",
                    retry_in => ?ALLOCATE_RETRY_TIMEOUT
                }
            ),
            start_allocate_timer(State0)
    end.

subscribe_db_changes(#{db := DB}) ->
    emqx_ds_builtin_raft_meta:subscribe(self(), DB).

unsubscribe_db_changes(_State) ->
    emqx_ds_builtin_raft_meta:unsubscribe(self()).

start_allocate_timer(State) ->
    restart_timer(allocate, ?ALLOCATE_RETRY_TIMEOUT, State).

start_fallback_timer(State) ->
    %% NOTE
    %% Adding random initial delay to reduce chances that different nodes will
    %% act on some transitions roughly at the same moment.
    Timeout = ?TRIGGER_PENDING_TIMEOUT,
    InitialTimeout = Timeout + round(Timeout * rand:uniform()),
    restart_timer(fallback, InitialTimeout, State).

restart_fallback_timer(State) ->
    restart_timer(fallback, ?TRIGGER_PENDING_TIMEOUT, State).

restart_timer(Name, Timeout, State = #{timers := Timers}) ->
    ok = emqx_utils:cancel_timer(maps:get(Name, Timers, undefined)),
    TRef = emqx_utils:start_timer(Timeout, Name),
    State#{timers := Timers#{Name => TRef}}.

clear_timer(Name, State = #{timers := Timers}) ->
    State#{timers := maps:remove(Name, Timers)}.

%%

handle_shard_changed(Shard, State = #{db := DB}) ->
    ok = cache_shard_info(DB, Shard),
    handle_shard_transitions(Shard, local, next_transitions(DB, Shard), State).

handle_pending_transitions(State = #{db := DB, shards := Shards}) ->
    lists:foldl(
        fun(Shard, StateAcc) ->
            handle_shard_transitions(Shard, any, next_transitions(DB, Shard), StateAcc)
        end,
        State,
        Shards
    ).

next_transitions(DB, Shard) ->
    emqx_ds_builtin_raft_meta:replica_set_transitions(DB, Shard).

handle_shard_transitions(_Shard, _, [], State) ->
    %% We reached the target allocation.
    State;
handle_shard_transitions(Shard, Scope, [Trans | _Rest], State) ->
    case transition_handler(Shard, Scope, Trans, State) of
        {Track, Handler} ->
            ensure_transition(Track, Shard, Trans, Handler, State);
        undefined ->
            State
    end.

transition_handler(Shard, Scope, Trans, _State = #{db := DB}) ->
    ThisSite = catch emqx_ds_builtin_raft_meta:this_site(),
    case Trans of
        {add, ThisSite} ->
            {Shard, {fun trans_claim/4, [fun trans_add_local/3]}};
        {del, ThisSite} ->
            {Shard, {fun trans_claim/4, [fun trans_drop_local/3]}};
        {del, Site} when Scope =:= any ->
            %% NOTE
            %% Letting the replica handle its own removal first, acting on the
            %% transition only when triggered explicitly or by `?TRIGGER_PENDING_TIMEOUT`
            %% timer. In other cases `Scope` is `local`.
            ReplicaSet = emqx_ds_builtin_raft_meta:replica_set(DB, Shard),
            case lists:member(Site, ReplicaSet) of
                true ->
                    Handler = {fun trans_claim/4, [fun trans_rm_unresponsive/3]},
                    {Shard, Handler};
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
        debug,
        dsrepl_shard_transition_begin,
        #{shard => Shard, db => DB, transition => Trans, pid => self()}
    ),
    emqx_ds_builtin_raft_metrics:shard_transition_started(DB, Shard, Trans),
    apply_handler(Handler, DB, Shard, Trans).

apply_handler({Fun, Args}, DB, Shard, Trans) ->
    erlang:apply(Fun, [DB, Shard, Trans | Args]);
apply_handler(Fun, DB, Shard, Trans) ->
    erlang:apply(Fun, [DB, Shard, Trans]).

trans_claim(DB, Shard, Trans, TransHandler) ->
    case claim_transition(DB, Shard, Trans) of
        ok ->
            apply_handler(TransHandler, DB, Shard, Trans);
        {error, {outdated, Expected}} ->
            ?tp(debug, "Transition became outdated", #{
                db => DB,
                shard => Shard,
                trans => Trans,
                expected => Expected
            }),
            exit({shutdown, skipped})
    end.

trans_add_local(DB, Shard, Trans = {add, Site}) ->
    ?tp(info, "Adding new local shard replica", #{site => Site, db => DB, shard => Shard}),
    do_add_local(membership, DB, Shard, Trans).

do_add_local(membership = Stage, DB, Shard, Trans) ->
    ok = start_shard(DB, Shard),
    case emqx_ds_builtin_raft_shard:add_local_server(DB, Shard) of
        ok ->
            do_add_local(readiness, DB, Shard, Trans);
        {error, recoverable, Reason} ->
            ?tp(warning, "Adding local shard replica failed", #{
                db => DB,
                shard => Shard,
                reason => Reason,
                retry_in => ?TRANS_RETRY_TIMEOUT
            }),
            emqx_ds_builtin_raft_metrics:shard_transition_error(DB, Shard, Trans),
            ok = timer:sleep(?TRANS_RETRY_TIMEOUT),
            do_add_local(Stage, DB, Shard, Trans)
    end;
do_add_local(readiness = Stage, DB, Shard, Trans) ->
    LocalServer = emqx_ds_builtin_raft_shard:local_server(DB, Shard),
    case emqx_ds_builtin_raft_shard:server_info(readiness, LocalServer) of
        ready ->
            ?tp(info, "Local shard replica ready", #{db => DB, shard => Shard});
        Status ->
            ?tp(notice, "Still waiting for local shard replica to be ready", #{
                db => DB,
                shard => Shard,
                status => Status,
                retry_in => ?TRANS_RETRY_TIMEOUT
            }),
            ok = timer:sleep(?TRANS_RETRY_TIMEOUT),
            do_add_local(Stage, DB, Shard, Trans)
    end.

trans_drop_local(DB, Shard, Trans = {del, Site}) ->
    ?tp(notice, "Dropping local shard replica", #{site => Site, db => DB, shard => Shard}),
    do_drop_local(DB, Shard, Trans).

do_drop_local(DB, Shard, Trans) ->
    case emqx_ds_builtin_raft_shard:drop_local_server(DB, Shard) of
        ok ->
            ok = emqx_ds_builtin_raft_db_sup:stop_shard({DB, Shard}),
            ok = emqx_ds_storage_layer:drop_shard({DB, Shard}),
            ?tp(notice, "Local shard replica dropped", #{db => DB, shard => Shard});
        {error, recoverable, Reason} ->
            ?tp(warning, "Dropping local shard replica failed", #{
                db => DB,
                shard => Shard,
                reason => Reason,
                retry_in => ?TRANS_RETRY_TIMEOUT
            }),
            emqx_ds_builtin_raft_metrics:shard_transition_error(DB, Shard, Trans),
            ok = timer:sleep(?TRANS_RETRY_TIMEOUT),
            do_drop_local(DB, Shard, Trans)
    end.

trans_rm_unresponsive(DB, Shard, Trans = {del, Site}) ->
    ?tp(notice, "Removing unresponsive shard replica", #{site => Site, db => DB, shard => Shard}),
    do_rm_unresponsive(DB, Shard, Trans, 1).

do_rm_unresponsive(DB, Shard, Trans = {del, Site}, NAttempt) ->
    Server = emqx_ds_builtin_raft_shard:shard_server(DB, Shard, Site),
    case emqx_ds_builtin_raft_shard:remove_server(DB, Shard, Server) of
        ok ->
            ?tp(notice, "Unresponsive shard replica removed", #{
                db => DB,
                shard => Shard,
                site => Site
            });
        {error, recoverable, Reason} ->
            ?tp(warning, "Removing shard replica failed", #{
                db => DB,
                shard => Shard,
                site => Site,
                reason => Reason,
                attempt => NAttempt,
                retry_in => ?TRANS_RETRY_TIMEOUT
            }),
            emqx_ds_builtin_raft_metrics:shard_transition_error(DB, Shard, Trans),
            ok = timer:sleep(?TRANS_RETRY_TIMEOUT),
            retry_rm_unresponsive(DB, Shard, Trans, Reason, NAttempt)
    end.

do_forget_lost(DB, Shard, Trans = {del, Site}) ->
    Server = emqx_ds_builtin_raft_shard:shard_server(DB, Shard, Site),
    case emqx_ds_builtin_raft_shard:forget_server(DB, Shard, Server) of
        ok ->
            ?tp(notice, "Unresponsive shard replica forcefully forgotten", #{
                db => DB,
                shard => Shard,
                site => Site
            });
        {error, recoverable, Reason} ->
            ?tp(warning, "Forgetting shard replica failed", #{
                db => DB,
                shard => Shard,
                site => Site,
                reason => Reason,
                retry_in => ?TRANS_RETRY_TIMEOUT
            }),
            emqx_ds_builtin_raft_metrics:shard_transition_error(DB, Shard, Trans),
            ok = timer:sleep(?TRANS_RETRY_TIMEOUT),
            do_forget_lost(DB, Shard, Trans);
        {error, unrecoverable, Reason} ->
            %% NOTE: Revert to `rm_unresponsive/3` on next `?TRIGGER_PENDING_TIMEOUT`.
            ?tp(warning, "Forgetting shard replica error", #{
                db => DB,
                shard => Shard,
                site => Site,
                reason => Reason
            }),
            exit({shutdown, {forget_lost_error, Reason}})
    end.

retry_rm_unresponsive(DB, Shard, Trans, _Reason, NAttempt) when NAttempt < 2 ->
    %% Retry regular safe replica removal first couple of times.
    do_rm_unresponsive(DB, Shard, Trans, NAttempt + 1);
retry_rm_unresponsive(DB, Shard, Trans, Reason, NAttempt) ->
    %% If unsuccessful, perhaps it's time to tell cluster to "forget" it forcefully.
    %% We only do that if:
    case Reason of
        %% Cluster change times out, quorum is probably lost and unreachable.
        {timeout, _Server} ->
            do_forget_lost(DB, Shard, Trans);
        %% Cluster change is in the Ra log, but couldn't be confired, quorum is
        %% probably lost and unreachable.
        {error, _Server, cluster_change_not_permitted} ->
            do_forget_lost(DB, Shard, Trans);
        %% Otherwise, let's try the safe way.
        _Otherwise ->
            do_rm_unresponsive(DB, Shard, Trans, NAttempt + 1)
    end.

%%

ensure_transition(Track, Shard, Trans, Handler, State = #{transitions := Ts}) ->
    case maps:get(Track, Ts, undefined) of
        undefined ->
            Pid = start_transition_handler(Shard, Trans, Handler, State),
            Record = #transhdl{shard = Shard, trans = Trans, pid = Pid},
            State#{transitions := Ts#{Track => Record}};
        _AlreadyRunning ->
            %% NOTE: Avoiding multiple transition handlers for the same shard for safety.
            State
    end.

claim_transition(DB, Shard, Trans) ->
    emqx_ds_builtin_raft_meta:claim_transition(DB, Shard, Trans).

commit_transition(Shard, Trans, #{db := DB}) ->
    emqx_ds_builtin_raft_meta:update_replica_set(DB, Shard, Trans).

start_transition_handler(Shard, Trans, Handler, #{db := DB}) ->
    proc_lib:spawn_link(?MODULE, handle_transition, [DB, Shard, Trans, Handler]).

handle_exit(Pid, Reason, State0 = #{db := DB, transitions := Ts}) ->
    case maps:to_list(maps:filter(fun(_, TH) -> TH#transhdl.pid == Pid end, Ts)) of
        [{Track, #transhdl{shard = Shard, trans = Trans}}] ->
            ?tp(
                debug,
                dsrepl_shard_transition_end,
                #{shard => Shard, db => DB, transition => Trans, pid => Pid, reason => Reason}
            ),
            State = State0#{transitions := maps:remove(Track, Ts)},
            handle_transition_exit(Shard, Trans, Reason, State);
        [] ->
            logger:warning(#{msg => "Unexpected exit signal", pid => Pid, reason => Reason}),
            State0
    end.

handle_transition_exit(Shard, Trans, normal, State = #{db := DB}) ->
    %% NOTE: This will trigger the next transition if any.
    emqx_ds_builtin_raft_metrics:shard_transition_complete(DB, Shard, Trans),
    ok = commit_transition(Shard, Trans, State),
    State;
handle_transition_exit(Shard, Trans, {shutdown, skipped}, State = #{db := DB}) ->
    emqx_ds_builtin_raft_metrics:shard_transition_skipped(DB, Shard, Trans),
    State;
handle_transition_exit(Shard, Trans, {shutdown, Reason}, State) ->
    handle_transition_exit(Shard, Trans, Reason, State);
handle_transition_exit(Shard, Trans, Reason, State = #{db := DB}) ->
    %% NOTE
    %% In case of `{add, Site}` transition failure, we have no choice but to retry:
    %% no other node can perform the transition and make progress towards the desired
    %% state. Assuming `?TRIGGER_PENDING_TIMEOUT` timer will take care of that.
    ?tp(warning, "Shard membership transition failed", #{
        db => DB,
        shard => Shard,
        transition => Trans,
        reason => Reason,
        retry_in => ?TRIGGER_PENDING_TIMEOUT
    }),
    emqx_ds_builtin_raft_metrics:shard_transition_crashed(DB, Shard, Trans),
    State.

%%

allocate_shards(State = #{db := DB}) ->
    case emqx_ds_builtin_raft_meta:allocate_shards(DB) of
        {ok, Shards} ->
            logger:info(#{msg => "Shards allocated", shards => Shards}),
            ok = start_shards(DB, emqx_ds_builtin_raft_meta:my_shards(DB)),
            ok = save_db_meta(DB, Shards),
            ok = cache_shard_info(DB, Shards),
            ok = lists:foreach(
                fun(S) -> ok = emqx_ds_builtin_raft_metrics:init_local_shard(DB, S) end,
                Shards
            ),
            emqx_ds:set_db_ready(DB, true),
            {ok, State#{shards => Shards, status := ready}};
        {error, Reason} ->
            {error, Reason}
    end.

start_shards(DB, Shards) ->
    lists:foreach(fun(Shard) -> start_shard(DB, Shard) end, Shards).

start_shard(DB, Shard) ->
    ok = emqx_ds_builtin_raft_db_sup:ensure_shard({DB, Shard}),
    ok = logger:info(#{msg => "Shard started", shard => Shard}),
    ok.

%%

save_db_meta(DB, Shards) ->
    persistent_term:put(?db_meta(DB), #{
        shards => Shards,
        n_shards => length(Shards)
    }).

cache_shard_info(DB, Shards) when is_list(Shards) ->
    lists:foreach(fun(Shard) -> cache_shard_info(DB, Shard) end, Shards);
cache_shard_info(DB, Shard) ->
    emqx_ds_builtin_raft_shard:cache_shard_servers(DB, Shard),
    emqx_ds:set_shard_ready(DB, Shard, true).

erase_db_meta(DB) ->
    emqx_ds:set_db_ready(DB, false),
    persistent_term:erase(?db_meta(DB)).

clear_shard_cache(DB, Shards) when is_list(Shards) ->
    lists:foreach(fun(Shard) -> clear_shard_cache(DB, Shard) end, Shards);
clear_shard_cache(DB, Shard) ->
    emqx_ds:set_shard_ready(DB, Shard, false),
    emqx_ds_builtin_raft_shard:clear_cache(DB, Shard).
