%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_router_syncer).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-behaviour(gen_server).

-export([start_link/1]).
-export([start_link/2]).
-export([start_link_pooled/2]).

-export([push/4]).
-export([push/5]).
-export([wait/1]).

-export([suspend/1]).
-export([activate/1]).

-export([stats/0]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-type action() :: add | delete.

-type options() :: #{
    max_batch_size => pos_integer(),
    min_sync_interval => non_neg_integer(),
    error_delay => non_neg_integer(),
    error_retry_interval => non_neg_integer(),
    initial_state => activated | suspended,
    batch_handler => {module(), _Function :: atom(), _Args :: list()}
}.

-define(POOL, router_syncer_pool).

-define(MAX_BATCH_SIZE, 1000).

%% How long to idle (ms) after receiving a new operation before triggering batch sync?
%% Zero effectively just schedules out the process, so that it has a chance to receive
%% more operations, and introduce no minimum delay.
-define(MIN_SYNC_INTERVAL, 0).

%% How long (ms) to idle after observing a batch sync error?
%% Should help to avoid excessive retries in situations when errors are caused by
%% conditions that take some time to resolve (e.g. restarting an upstream core node).
-define(ERROR_DELAY, 10).

%% How soon (ms) to retry last failed batch sync attempt?
%% Only matter in absence of new operations, otherwise batch sync is triggered as
%% soon as `?ERROR_DELAY` is over.
-define(ERROR_RETRY_INTERVAL, 500).

-define(PRIO_HI, 1).
-define(PRIO_LO, 2).
-define(PRIO_BG, 3).

-define(PUSH(PRIO, OP), {PRIO, OP}).
-define(OP(ACT, TOPIC, DEST, CTX), {ACT, TOPIC, DEST, CTX}).

-define(ROUTEOP(ACT), {ACT, _, _}).
-define(ROUTEOP(ACT, PRIO), {ACT, PRIO, _}).
-define(ROUTEOP(ACT, PRIO, CTX), {ACT, PRIO, CTX}).

-ifdef(TEST).
-undef(MAX_BATCH_SIZE).
-undef(MIN_SYNC_INTERVAL).
-define(MAX_BATCH_SIZE, 40).
-define(MIN_SYNC_INTERVAL, 10).
-endif.

%%

-spec start_link(options()) ->
    {ok, pid()} | {error, _Reason}.
start_link(Options) ->
    gen_server:start_link(?MODULE, mk_state(Options), []).

-spec start_link(_Name, options()) ->
    {ok, pid()} | {error, _Reason}.
start_link(Name, Options) ->
    gen_server:start_link(Name, ?MODULE, mk_state(Options), []).

-spec start_link_pooled(atom(), pos_integer()) ->
    {ok, pid()}.
start_link_pooled(Pool, Id) ->
    gen_server:start_link(
        {local, emqx_utils:proc_name(?MODULE, Id)},
        ?MODULE,
        {Pool, Id, mk_state(#{})},
        []
    ).

-spec push(action(), emqx_types:topic(), emqx_router:dest(), Opts) ->
    ok | _WaitRef :: reference()
when
    Opts :: #{reply => pid()}.
push(Action, Topic, Dest, Opts) ->
    Worker = gproc_pool:pick_worker(?POOL, Topic),
    push(Worker, Action, Topic, Dest, Opts).

-spec push(_Ref, action(), emqx_types:topic(), emqx_router:dest(), Opts) ->
    ok | _WaitRef :: reference()
when
    Opts :: #{reply => pid()}.
push(Ref, Action, Topic, Dest, Opts) ->
    Prio = designate_prio(Action, Opts),
    Context = mk_push_context(Opts),
    _ = gproc:send(Ref, ?PUSH(Prio, {Action, Topic, Dest, Context})),
    case Context of
        [{MRef, _}] ->
            MRef;
        [] ->
            ok
    end.

-spec wait(_WaitRef :: reference()) ->
    ok | {error, _Reason}.
wait(MRef) ->
    %% NOTE
    %% No timeouts here because (as in `emqx_broker:call/2` case) callers do not
    %% really expect this to fail with timeout exception. However, waiting
    %% indefinitely is not the best option since it blocks the caller from receiving
    %% other messages, so for instance channel (connection) process may not be able
    %% to react to socket close event in time. Better option would probably be to
    %% introduce cancellable operation, which will be able to check if the caller
    %% would still be interested in the result.
    receive
        {MRef, Result} ->
            Result
    end.

designate_prio(_, #{reply := _To}) ->
    ?PRIO_HI;
designate_prio(add, #{}) ->
    ?PRIO_LO;
designate_prio(delete, #{}) ->
    ?PRIO_BG.

mk_push_context(#{reply := To}) ->
    MRef = erlang:make_ref(),
    [{MRef, To}];
mk_push_context(_) ->
    [].

%%

%% Suspended syncer receives and accumulates route ops but doesn't apply them
%% until it is activated.
suspend(Ref) ->
    gen_server:call(Ref, suspend, infinity).

activate(Ref) ->
    gen_server:call(Ref, activate, infinity).

%%

-type stats() :: #{
    size := non_neg_integer(),
    n_add := non_neg_integer(),
    n_delete := non_neg_integer(),
    prio_highest := non_neg_integer() | undefined,
    prio_lowest := non_neg_integer() | undefined
}.

-spec stats() -> [stats()].
stats() ->
    Workers = gproc_pool:active_workers(?POOL),
    [gen_server:call(Pid, stats, infinity) || {_Name, Pid} <- Workers].

%%

mk_state(Options) ->
    #{
        state => maps:get(initial_state, Options, active),
        stash => stash_new(),
        retry_timer => undefined,
        max_batch_size => maps:get(max_batch_size, Options, ?MAX_BATCH_SIZE),
        min_sync_interval => maps:get(min_sync_interval, Options, ?MIN_SYNC_INTERVAL),
        error_delay => maps:get(error_delay, Options, ?ERROR_DELAY),
        error_retry_interval => maps:get(error_retry_interval, Options, ?ERROR_RETRY_INTERVAL),
        batch_handler => maps:get(batch_handler, Options, default)
    }.

%%

init({Pool, Id, State}) ->
    true = gproc_pool:connect_worker(Pool, {Pool, Id}),
    {ok, State};
init(State) ->
    {ok, State}.

handle_call(suspend, _From, State) ->
    NState = State#{state := suspended},
    {reply, ok, NState};
handle_call(activate, _From, State = #{state := suspended}) ->
    NState = run_batch_loop([], State#{state := active}),
    {reply, ok, NState};
handle_call(activate, _From, State) ->
    {reply, ok, State};
handle_call(stats, _From, State = #{stash := Stash}) ->
    {reply, stash_stats(Stash), State};
handle_call(_Call, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({timeout, _TRef, retry}, State) ->
    NState = run_batch_loop([], State#{retry_timer := undefined}),
    {noreply, NState};
handle_info(Push = ?PUSH(_, _), State = #{min_sync_interval := MSI}) ->
    %% NOTE: Wait a bit to collect potentially overlapping operations.
    ok = timer:sleep(MSI),
    NState = run_batch_loop([Push], State),
    {noreply, NState}.

terminate(_Reason, _State) ->
    ok.

%%

run_batch_loop(Incoming, State = #{stash := Stash0, state := suspended}) ->
    Stash1 = stash_add(Incoming, Stash0),
    Stash2 = stash_drain(Stash1),
    State#{stash := Stash2};
run_batch_loop(Incoming, State = #{stash := Stash0, max_batch_size := MBS}) ->
    Stash1 = stash_add(Incoming, Stash0),
    Stash2 = stash_drain(Stash1),
    {Batch, Stash3} = mk_batch(Stash2, MBS),
    ?tp_ignore_side_effects_in_prod(router_syncer_new_batch, batch_stats(Batch, Stash3)),
    case run_batch(Batch, State) of
        Status = #{} ->
            ok = send_replies(Status, Batch),
            NState = cancel_retry_timer(State#{stash := Stash3}),
            %% NOTE
            %% We could postpone batches where only `?PRIO_BG` operations left, which
            %% would allow to do less work in situations when there are intermittently
            %% reconnecting clients with moderately unique subscriptions. However, this
            %% would also require us to forego the idempotency of batch syncs (see
            %% `merge_route_op/2`).
            case is_stash_empty(Stash3) of
                true ->
                    NState;
                false ->
                    run_batch_loop([], NState)
            end;
        BatchError ->
            ?SLOG(warning, #{
                msg => "router_batch_sync_failed",
                reason => BatchError,
                batch => batch_stats(Batch, Stash3)
            }),
            NState = State#{stash := Stash2},
            ok = error_cooldown(NState),
            ensure_retry_timer(NState)
    end.

error_cooldown(#{error_delay := ED}) ->
    timer:sleep(ED).

ensure_retry_timer(State = #{retry_timer := undefined, error_retry_interval := ERI}) ->
    TRef = emqx_utils:start_timer(ERI, retry),
    State#{retry_timer := TRef};
ensure_retry_timer(State = #{retry_timer := _TRef}) ->
    State.

cancel_retry_timer(State = #{retry_timer := TRef}) ->
    ok = emqx_utils:cancel_timer(TRef),
    State#{retry_timer := undefined};
cancel_retry_timer(State) ->
    State.

%%

mk_batch(Stash, BatchSize) when map_size(Stash) =< BatchSize ->
    %% This is perfect situation, we just use stash as batch w/o extra reallocations.
    {Stash, stash_new()};
mk_batch(Stash, BatchSize) ->
    %% Take a subset of stashed operations to form a batch.
    %% Note that stash is an unordered map, it's not a queue. The order of operations is
    %% not preserved strictly, only loosely, because of how we start from high priority
    %% operations and go down to low priority ones. This might cause some operations to
    %% stay in stash for unfairly long time, when there are many high priority operations.
    %% However, it's unclear how likely this is to happen in practice.
    mk_batch(?PRIO_HI, #{}, BatchSize, Stash).

mk_batch(Prio, Batch, SizeLeft, Stash) ->
    mk_batch(Prio, Batch, SizeLeft, Stash, maps:iterator(Stash)).

mk_batch(Prio, Batch, SizeLeft, Stash, It) when SizeLeft > 0 ->
    %% Iterating over stash, only taking operations with priority equal to `Prio`.
    case maps:next(It) of
        {Route, Op = ?ROUTEOP(_Action, Prio), NIt} ->
            NBatch = Batch#{Route => Op},
            NStash = maps:remove(Route, Stash),
            mk_batch(Prio, NBatch, SizeLeft - 1, NStash, NIt);
        {_Route, _Op, NIt} ->
            %% This is lower priority operation, skip it.
            mk_batch(Prio, Batch, SizeLeft, Stash, NIt);
        none ->
            %% No more operations with priority `Prio`, go to the next priority level.
            true = Prio < ?PRIO_BG,
            mk_batch(Prio + 1, Batch, SizeLeft, Stash)
    end;
mk_batch(_Prio, Batch, _, Stash, _It) ->
    {Batch, Stash}.

send_replies(Errors, Batch) ->
    maps:foreach(
        fun(Route, {_Action, _Prio, Ctx}) ->
            case Ctx of
                [] ->
                    ok;
                _ ->
                    replyctx_send(maps:get(Route, Errors, ok), Ctx)
            end
        end,
        Batch
    ).

replyctx_send(_Result, []) ->
    noreply;
replyctx_send(Result, RefsPids) ->
    _ = lists:foreach(fun({MRef, Pid}) -> erlang:send(Pid, {MRef, Result}) end, RefsPids),
    ok.

%%

run_batch(Empty, _State) when Empty =:= #{} ->
    #{};
run_batch(Batch, #{batch_handler := default}) ->
    catch emqx_router:do_batch(Batch);
run_batch(Batch, #{batch_handler := {Module, Function, Args}}) ->
    erlang:apply(Module, Function, [Batch | Args]).

%%

stash_new() ->
    #{}.

is_stash_empty(Stash) ->
    maps:size(Stash) =:= 0.

stash_drain(Stash) ->
    receive
        ?PUSH(Prio, Op) ->
            stash_drain(stash_add(Prio, Op, Stash))
    after 0 ->
        Stash
    end.

stash_add(Pushes, Stash) ->
    lists:foldl(
        fun(?PUSH(Prio, Op), QAcc) -> stash_add(Prio, Op, QAcc) end,
        Stash,
        Pushes
    ).

stash_add(Prio, ?OP(Action, Topic, Dest, Ctx), Stash) ->
    Route = {Topic, Dest},
    case maps:get(Route, Stash, undefined) of
        undefined ->
            Stash#{Route => {Action, Prio, Ctx}};
        RouteOp ->
            RouteOpMerged = merge_route_op(RouteOp, ?ROUTEOP(Action, Prio, Ctx)),
            Stash#{Route := RouteOpMerged}
    end.

merge_route_op(?ROUTEOP(Action, _Prio1, Ctx1), ?ROUTEOP(Action, Prio2, Ctx2)) ->
    %% NOTE: This can happen as topic shard can be processed concurrently
    %% by different broker worker, see emqx_broker for more details.
    MergedCtx = Ctx1 ++ Ctx2,
    ?ROUTEOP(Action, Prio2, MergedCtx);
merge_route_op(?ROUTEOP(_Action1, _Prio1, Ctx1), DestOp = ?ROUTEOP(_Action2, _Prio2, _Ctx2)) ->
    %% NOTE: Latter cancel the former.
    %% Strictly speaking, in ideal conditions we could just cancel both, because they
    %% essentially do not change the global state. However, we decided to stay on the
    %% safe side and cancel only the former, making batch syncs idempotent.
    _ = replyctx_send(ok, Ctx1),
    DestOp.

%%

batch_stats(Batch, Stash) ->
    BatchStats = stash_stats(Batch),
    BatchStats#{
        stashed => maps:size(Stash)
    }.

stash_stats(Stash) ->
    #{
        size => maps:size(Stash),
        n_add => maps:size(maps:filter(fun(_, ?ROUTEOP(A)) -> A == add end, Stash)),
        n_delete => maps:size(maps:filter(fun(_, ?ROUTEOP(A)) -> A == delete end, Stash)),
        prio_highest => maps:fold(fun(_, ?ROUTEOP(_, P), M) -> min(P, M) end, none, Stash),
        prio_lowest => maps:fold(fun(_, ?ROUTEOP(_, P), M) -> max(P, M) end, 0, Stash)
    }.

%%

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

batch_test() ->
    Dest = node(),
    Ctx = fun(N) -> [{N, self()}] end,
    Stash = stash_add(
        [
            ?PUSH(?PRIO_BG, ?OP(delete, <<"t/2">>, Dest, Ctx(1))),
            ?PUSH(?PRIO_HI, ?OP(add, <<"t/1">>, Dest, Ctx(2))),
            ?PUSH(?PRIO_LO, ?OP(add, <<"t/1">>, Dest, Ctx(3))),
            ?PUSH(?PRIO_HI, ?OP(add, <<"t/2">>, Dest, Ctx(4))),
            ?PUSH(?PRIO_HI, ?OP(add, <<"t/3">>, Dest, Ctx(5))),
            ?PUSH(?PRIO_HI, ?OP(add, <<"t/4">>, Dest, Ctx(6))),
            ?PUSH(?PRIO_LO, ?OP(delete, <<"t/3">>, Dest, Ctx(7))),
            ?PUSH(?PRIO_BG, ?OP(delete, <<"t/3">>, Dest, Ctx(8))),
            ?PUSH(?PRIO_BG, ?OP(delete, <<"t/2">>, Dest, Ctx(9))),
            ?PUSH(?PRIO_BG, ?OP(delete, <<"old/1">>, Dest, Ctx(10))),
            ?PUSH(?PRIO_HI, ?OP(add, <<"t/2">>, Dest, Ctx(11))),
            ?PUSH(?PRIO_BG, ?OP(delete, <<"old/2">>, Dest, Ctx(12))),
            ?PUSH(?PRIO_HI, ?OP(add, <<"t/3">>, Dest, Ctx(13))),
            ?PUSH(?PRIO_HI, ?OP(add, <<"t/3">>, Dest, Ctx(14))),
            ?PUSH(?PRIO_LO, ?OP(delete, <<"old/3">>, Dest, Ctx(15))),
            ?PUSH(?PRIO_LO, ?OP(delete, <<"t/2">>, Dest, Ctx(16)))
        ],
        stash_new()
    ),
    {Batch, StashLeft} = mk_batch(Stash, 5),

    ?assertMatch(
        #{
            {<<"t/1">>, Dest} := {add, ?PRIO_LO, _},
            {<<"t/3">>, Dest} := {add, ?PRIO_HI, _},
            {<<"t/2">>, Dest} := {delete, ?PRIO_LO, _},
            {<<"t/4">>, Dest} := {add, ?PRIO_HI, _},
            {<<"old/3">>, Dest} := {delete, ?PRIO_LO, _}
        },
        Batch
    ),
    ?assertMatch(
        #{
            {<<"old/1">>, Dest} := {delete, ?PRIO_BG, _},
            {<<"old/2">>, Dest} := {delete, ?PRIO_BG, _}
        },
        StashLeft
    ),

    %% Replies are only sent to superseded ops:
    ?assertEqual(
        [
            {1, ok},
            {5, ok},
            {4, ok},
            {9, ok},
            {7, ok},
            {8, ok},
            {11, ok}
        ],
        emqx_utils_stream:consume(emqx_utils_stream:mqueue(0))
    ).

-endif.
