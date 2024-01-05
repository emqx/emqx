%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include_lib("snabbkaffe/include/trace.hrl").

-behaviour(gen_server).

-export([start_link/2]).

-export([push/4]).
-export([wait/1]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-type action() :: add | delete.

-define(POOL, router_syncer_pool).

-define(MAX_BATCH_SIZE, 1000).
-define(MIN_SYNC_INTERVAL, 1).

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

-spec start_link(atom(), pos_integer()) ->
    {ok, pid()}.
start_link(Pool, Id) ->
    gen_server:start_link(
        {local, emqx_utils:proc_name(?MODULE, Id)},
        ?MODULE,
        [Pool, Id],
        []
    ).

-spec push(action(), emqx_types:topic(), emqx_router:dest(), Opts) ->
    ok | _WaitRef :: reference()
when
    Opts :: #{reply => pid()}.
push(Action, Topic, Dest, Opts) ->
    Worker = gproc_pool:pick_worker(?POOL, Topic),
    Prio = designate_prio(Action, Opts),
    Context = mk_push_context(Opts),
    Worker ! ?PUSH(Prio, {Action, Topic, Dest, Context}),
    case Context of
        {MRef, _} ->
            MRef;
        [] ->
            ok
    end.

-spec wait(_WaitRef :: reference()) ->
    ok | {error, _Reason}.
wait(MRef) ->
    %% FIXME: timeouts
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
    {MRef, To};
mk_push_context(_) ->
    [].

%%

init([Pool, Id]) ->
    true = gproc_pool:connect_worker(Pool, {Pool, Id}),
    {ok, #{stash => stash_new()}}.

handle_call(_Call, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(Push = ?PUSH(_, _), State) ->
    %% NOTE: Wait a bit to collect potentially overlapping operations.
    ok = timer:sleep(?MIN_SYNC_INTERVAL),
    NState = run_batch_loop([Push], State),
    {noreply, NState}.

terminate(_Reason, _State) ->
    ok.

%%

run_batch_loop(Incoming, State = #{stash := Stash0}) ->
    Stash1 = stash_add(Incoming, Stash0),
    Stash2 = stash_drain(Stash1),
    {Batch, Stash3} = mk_batch(Stash2),
    ?tp_ignore_side_effects_in_prod(router_syncer_new_batch, #{
        size => maps:size(Batch),
        stashed => maps:size(Stash3),
        n_add => maps:size(maps:filter(fun(_, ?ROUTEOP(A)) -> A == add end, Batch)),
        n_delete => maps:size(maps:filter(fun(_, ?ROUTEOP(A)) -> A == delete end, Batch)),
        prio_highest => maps:fold(fun(_, ?ROUTEOP(_, P), M) -> min(P, M) end, none, Batch),
        prio_lowest => maps:fold(fun(_, ?ROUTEOP(_, P), M) -> max(P, M) end, 0, Batch)
    }),
    %% TODO: retry if error?
    Errors = run_batch(Batch),
    ok = send_replies(Errors, Batch),
    NState = State#{stash := Stash3},
    %% TODO: postpone if only ?PRIO_BG operations left?
    case stash_empty(Stash3) of
        true ->
            NState;
        false ->
            run_batch_loop([], NState)
    end.

%%

mk_batch(Stash) when map_size(Stash) =< ?MAX_BATCH_SIZE ->
    %% This is perfect situation, we just use stash as batch w/o extra reallocations.
    {Stash, stash_new()};
mk_batch(Stash) ->
    %% Take a subset of stashed operations to form a batch.
    %% Note that stash is an unordered map, it's not a queue. The order of operations is
    %% not preserved strictly, only loosely, because of how we start from high priority
    %% operations and go down to low priority ones. This might cause some operations to
    %% stay in stash for unfairly long time, when there are many high priority operations.
    %% However, it's unclear how likely this is to happen in practice.
    mk_batch(Stash, ?MAX_BATCH_SIZE).

mk_batch(Stash, BatchSize) ->
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
replyctx_send(Result, {MRef, Pid}) ->
    Pid ! {MRef, Result}.

%%

run_batch(Batch) ->
    emqx_router:do_batch(Batch).

%%

stash_new() ->
    #{}.

stash_empty(Stash) ->
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
            case merge_route_op(RouteOp, ?ROUTEOP(Action, Prio, Ctx)) of
                undefined ->
                    maps:remove(Route, Stash);
                RouteOpMerged ->
                    Stash#{Route := RouteOpMerged}
            end
    end.

merge_route_op(?ROUTEOP(Action, _Prio1, Ctx1), DestOp = ?ROUTEOP(Action)) ->
    %% NOTE: This should not happen anyway.
    _ = replyctx_send(ignored, Ctx1),
    DestOp;
merge_route_op(?ROUTEOP(_Action1, _Prio1, Ctx1), ?ROUTEOP(_Action2, _Prio2, Ctx2)) ->
    %% NOTE: Operations cancel each other.
    _ = replyctx_send(ok, Ctx1),
    _ = replyctx_send(ok, Ctx2),
    undefined.

%%

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

batch_test() ->
    Dest = node(),
    Ctx = fun(N) -> {N, self()} end,
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
    ?assertEqual(
        [
            {2, ignored},
            {1, ok},
            {4, ok},
            {5, ok},
            {7, ok},
            {9, ok},
            {11, ok},
            {8, ok},
            {13, ok}
        ],
        emqx_utils_stream:consume(emqx_utils_stream:mqueue(0))
    ).

-endif.
