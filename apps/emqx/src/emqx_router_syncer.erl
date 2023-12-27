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

-define(MAX_BATCH_SIZE, 4000).
-define(MIN_SYNC_INTERVAL, 1).

-define(HIGHEST_PRIO, 1).
-define(LOWEST_PRIO, 4).

-define(PUSH(PRIO, OP), {PRIO, OP}).

-define(OP(ACT, TOPIC, DEST, CTX), {ACT, TOPIC, DEST, CTX}).

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

designate_prio(_, #{reply := true}) ->
    ?HIGHEST_PRIO;
designate_prio(add, #{}) ->
    2;
designate_prio(delete, #{}) ->
    3.

mk_push_context(#{reply := To}) ->
    MRef = erlang:make_ref(),
    {MRef, To};
mk_push_context(_) ->
    [].

%%

init([Pool, Id]) ->
    true = gproc_pool:connect_worker(Pool, {Pool, Id}),
    {ok, #{queue => []}}.

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

run_batch_loop(Incoming, State = #{queue := Queue}) ->
    NQueue = queue_join(Queue, gather_operations(Incoming)),
    {Batch, N, FQueue} = mk_batch(NQueue),
    %% TODO: retry if error?
    Errors = run_batch(Batch),
    0 = send_replies(Errors, N, NQueue),
    %% TODO: squash queue
    NState = State#{queue := queue_fix(FQueue)},
    case queue_empty(FQueue) of
        true ->
            NState;
        false ->
            run_batch_loop([], NState)
    end.

%%

mk_batch(Queue) ->
    mk_batch(Queue, 0, #{}).

mk_batch(Queue, N, Batch) when map_size(Batch) =:= ?MAX_BATCH_SIZE ->
    {Batch, N, Queue};
mk_batch([Op = ?OP(_, _, _, _) | Queue], N, Batch) ->
    NBatch = batch_add_operation(Op, Batch),
    mk_batch(Queue, N + 1, NBatch);
mk_batch([Run | Queue], N, Batch) when is_list(Run) ->
    case mk_batch(Run, N, Batch) of
        {NBatch, N1, []} ->
            mk_batch(Queue, N1, NBatch);
        {NBatch, N1, Left} ->
            {NBatch, N1, [Left | Queue]}
    end;
mk_batch([], N, Batch) ->
    {Batch, N, []}.

batch_add_operation(?OP(Action, Topic, Dest, _ReplyCtx), Batch) ->
    case Batch of
        #{{Topic, Dest} := Action} ->
            Batch;
        #{{Topic, Dest} := delete} when Action == add ->
            Batch#{{Topic, Dest} := add};
        #{{Topic, Dest} := add} when Action == delete ->
            maps:remove({Topic, Dest}, Batch);
        #{} ->
            maps:put({Topic, Dest}, Action, Batch)
    end.

send_replies(_Result, 0, _Queue) ->
    0;
send_replies(Result, N, [Op = ?OP(_, _, _, _) | Queue]) ->
    _ = replyctx_send(Result, Op),
    send_replies(Result, N - 1, Queue);
send_replies(Result, N, [Run | Queue]) when is_list(Run) ->
    N1 = send_replies(Result, N, Run),
    send_replies(Result, N1, Queue);
send_replies(_Result, N, []) ->
    N.

replyctx_send(_Result, ?OP(_, _, _, [])) ->
    noreply;
replyctx_send(Result, ?OP(_, Topic, Dest, {MRef, Pid})) ->
    case Result of
        #{{Topic, Dest} := Error} ->
            Pid ! {MRef, Error};
        #{} ->
            Pid ! {MRef, ok}
    end.

%%

run_batch(Batch) ->
    emqx_router:do_batch(Batch).

%%

queue_fix([]) ->
    [];
queue_fix(Queue) when length(Queue) < ?LOWEST_PRIO ->
    queue_fix([[] | Queue]);
queue_fix(Queue) ->
    Queue.

queue_join(Q1, []) ->
    Q1;
queue_join([], Q2) ->
    Q2;
queue_join(Q1, Q2) ->
    lists:zipwith(fun join_list/2, Q1, Q2).

join_list(L1, []) ->
    L1;
join_list([], L2) ->
    L2;
join_list(L1, L2) ->
    [L1, L2].

queue_empty(Queue) ->
    lists:all(fun(L) -> L == [] end, Queue).

gather_operations(Incoming) ->
    [
        pick_operations(Prio, Incoming) ++ drain_operations(Prio)
     || Prio <- lists:seq(?HIGHEST_PRIO, ?LOWEST_PRIO)
    ].

drain_operations(Prio) ->
    receive
        {Prio, Op} ->
            [Op | drain_operations(Prio)]
    after 0 ->
        []
    end.

pick_operations(Prio, Incoming) ->
    [Op || {P, Op} <- Incoming, P =:= Prio].
