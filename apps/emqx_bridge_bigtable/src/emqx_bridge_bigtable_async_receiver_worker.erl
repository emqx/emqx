%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_bigtable_async_receiver_worker).

-behaviour(gen_server).

%% API
-export([
    start_link/1,

    async_recv/5
]).

% `gen_server' API
-export([
    init/1,
    terminate/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

%% Internal exports
-export([do_recv_stream_once/2]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-include_lib("snabbkaffe/include/trace.hrl").

-define(id, id).
-define(pool, pool).
-define(queue, queue).
-define(partial_results, partial_results).
-define(nudge_enqueued, nudge_enqueued).

%% calls/casts/infos/continues
-record(recv, {stream, opts, reply_fn_and_args, deadline}).
-record(continue, {}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

start_link(Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

async_recv(Pool, Stream, Opts, ReplyFnAndArgs, Deadline) ->
    RecvReq = #recv{
        stream = Stream,
        opts = Opts,
        reply_fn_and_args = ReplyFnAndArgs,
        deadline = Deadline
    },
    Worker = gproc_pool:pick_worker(Pool, self()),
    _ = erlang:send(Worker, RecvReq),
    {ok, Worker}.

%%------------------------------------------------------------------------------
%% `gen_server' API
%%------------------------------------------------------------------------------

init(Opts) ->
    process_flag(trap_exit, true),
    process_flag(message_queue_data, off_heap),
    #{pool := Pool, id := Id} = Opts,
    proc_lib:set_label({bigtable_async_recv_worker, Pool, Id}),
    true = gproc_pool:connect_worker(Pool, {Pool, Id}),
    State = #{
        ?id => Id,
        ?pool => Pool,
        ?queue => queue:new(),
        ?partial_results => [],
        ?nudge_enqueued => false
    },
    {ok, State}.

terminate(_Reason, State) ->
    #{?pool := Pool, ?id := Id} = State,
    gproc_pool:disconnect_worker(Pool, {Pool, Id}),
    ok.

handle_call(Call, _From, State) ->
    {reply, {error, {unknown_call, Call}}, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(#recv{} = RecvReq, State0) ->
    State1 = enqueue(State0, RecvReq),
    State = pull(State1),
    {noreply, State};
handle_info(#continue{}, State0) ->
    State1 = State0#{?nudge_enqueued := false},
    State = pull(State1),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% Internal exports
%%------------------------------------------------------------------------------

do_recv_stream_once(Stream, Opts) ->
    ?tp("bigtable_will_recv0", #{}),
    ?tp("bigtable_will_recv1", #{}),
    try grpc_client:recv(Stream, Opts) of
        {ok, Resp} ->
            case is_end_of_stream(Resp) of
                {true, Results0, Trailers} ->
                    {done, Results0, Trailers};
                false ->
                    {more, Resp}
            end;
        {error, Reason} ->
            ?tp("bigtable_recv_error", #{kind => error, reason => Reason}),
            Ctx = #{kind => error, reason => Reason},
            {error, Ctx}
    catch
        error:Reason ->
            ?tp("bigtable_recv_error", #{kind => exception, reason => Reason}),
            Ctx = #{kind => exception, reason => Reason},
            {error, Ctx}
    end.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

apply_reply_fn(ReplyFnAndArgs, Result) ->
    %% fire-and-forget without blocking current worker.
    spawn(emqx_resource, apply_reply_fun, [ReplyFnAndArgs, Result]).

dispatch_completed(RevBatches, Trailers, ReplyFnAndArgs) ->
    %% fire-and-forget without blocking current worker.
    spawn(fun() ->
        Result = lists:flatten(lists:reverse(RevBatches)),
        emqx_resource:apply_reply_fun(ReplyFnAndArgs, {ok, Result, Trailers})
    end).

is_end_of_stream(Resp) ->
    maybe
        {value, {eos, Trailers}, Rest} ?= lists:keytake(eos, 1, Resp),
        {true, Rest, Trailers}
    end.

%% assumes queue is non-empty
pop(State0) ->
    #{?queue := Queue0} = State0,
    {{value, Req}, Queue} = queue:out(Queue0),
    {Req, State0#{?queue := Queue}}.

peek(State0) ->
    #{?queue := Queue0} = State0,
    queue:peek(Queue0).

enqueue(State0, RecvReq) ->
    #{?queue := Queue0} = State0,
    Queue = queue:in(RecvReq, Queue0),
    State0#{?queue := Queue}.

ensure_nudged(#{?nudge_enqueued := true} = State0) ->
    ?tp("bigtable_already_nudged", #{}),
    State0;
ensure_nudged(#{} = State0) ->
    case peek(State0) of
        {value, _} ->
            self() ! #continue{},
            State0#{?nudge_enqueued := true};
        empty ->
            State0
    end.

pull(State0) ->
    ?tp("bigtable_will_peek", #{}),
    case peek(State0) of
        empty ->
            State0;
        {value, #recv{} = RecvReq} ->
            case is_expired(RecvReq) of
                true ->
                    State1 = drop_and_reply(State0),
                    pull(State1);
                false ->
                    State1 = do_recv_one(RecvReq, State0),
                    ensure_nudged(State1)
            end
    end.

now_ms() ->
    erlang:monotonic_time(millisecond).

is_expired(#recv{deadline = infinity}) ->
    false;
is_expired(#recv{deadline = Deadline}) ->
    Deadline =< now_ms().

do_recv_one(#recv{} = RecvReq, State0) ->
    #{
        ?partial_results := PartialResults0,
        ?queue := Queue0
    } = State0,
    #recv{stream = Stream, opts = Opts, reply_fn_and_args = ReplyFnAndArgs} = RecvReq,
    case do_recv_stream_once(Stream, Opts) of
        {done, Results0, Trailers} ->
            RevBatches = [Results0 | PartialResults0],
            dispatch_completed(RevBatches, Trailers, ReplyFnAndArgs),
            Queue = queue:drop(Queue0),
            State0#{?partial_results := [], ?queue := Queue};
        {more, Results0} ->
            PartialResults = [Results0 | PartialResults0],
            State0#{?partial_results := PartialResults};
        {error, Ctx0} ->
            Ctx = Ctx0#{partial_results => PartialResults0},
            apply_reply_fn(ReplyFnAndArgs, {error, Ctx}),
            Queue = queue:drop(Queue0),
            State0#{?partial_results := [], ?queue := Queue}
    end.

%% even if the request is already expired, we reply the async request so that buffer
%% workers correctly account for this request as expired/late reply in their metrics.
drop_and_reply(State0) ->
    {RecvReq, State1} = pop(State0),
    #recv{reply_fn_and_args = ReplyFnAndArgs} = RecvReq,
    apply_reply_fn(ReplyFnAndArgs, {error, request_expired}),
    State1#{?partial_results := []}.
