%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_retainer_dispatcher).

-behaviour(gen_server).

-include("emqx_retainer.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% API
-export([
    start_link/2,
    %% RPC target (`emqx_retainer_proto_v2:wait_dispatch_complete/2`)
    wait_dispatch_complete/1,
    worker/0,
    fetch/4,
    batch_size/0
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-elvis([{elvis_style, invalid_dynamic_call, disable}]).

-record(fetch, {target, topic, cursor}).
-record(wait_dispatch_complete, {}).
-record(dispatch, {}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

%% RPC target (`emqx_retainer_proto_v2:wait_dispatch_complete/2`).
-spec wait_dispatch_complete(timeout()) -> ok.
wait_dispatch_complete(Timeout) ->
    lists:foreach(
        fun({_, Pid}) -> ok = gen_server:call(Pid, #wait_dispatch_complete{}, Timeout) end,
        gproc_pool:active_workers(?DISPATCHER_POOL)
    ).

-spec worker() -> pid().
worker() ->
    gproc_pool:pick_worker(?DISPATCHER_POOL, self()).

-doc "Queue one retained-message batch for a handler.".
-spec fetch(pid(), emqx_extsub_handler:message_target(), binary(), term()) -> ok.
fetch(Worker, Target, Topic, Cursor) ->
    gen_server:cast(Worker, #fetch{
        target = Target,
        topic = Topic,
        cursor = Cursor
    }).

-spec start_link(atom(), pos_integer()) -> {ok, pid()}.
start_link(Pool, Id) ->
    gen_server:start_link(
        {local, emqx_utils:proc_name(?MODULE, Id)},
        ?MODULE,
        [Pool, Id],
        [{hibernate_after, 1000}]
    ).

-doc "Return the configured dispatcher batch size.".
-spec batch_size() -> pos_integer().
batch_size() ->
    case emqx_retainer:batch_read_number() of
        all_remaining -> 1000;
        N -> N
    end.

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

init([Pool, Id]) ->
    erlang:process_flag(trap_exit, true),
    true = gproc_pool:connect_worker(Pool, {Pool, Id}),
    Limiter = emqx_limiter:connect({?RETAINER_LIMITER_GROUP, ?DISPATCHER_LIMITER_NAME}),
    {ok, #{
        pool => Pool,
        id => Id,
        queue => queue:new(),
        scheduled => false,
        limiter => Limiter,
        next_demand => 0
    }}.

handle_call(#wait_dispatch_complete{}, _From, State) ->
    {reply, ok, State};
handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignored, State}.

handle_cast(#fetch{} = Request, #{queue := Queue} = State) ->
    {noreply, schedule(State#{queue := queue:in(Request, Queue)})};
handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Msg}),
    {noreply, State}.

handle_info(#dispatch{}, State) ->
    {noreply, dispatch(State#{scheduled := false})};
handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
    {noreply, State}.

terminate(_Reason, #{pool := Pool, id := Id}) ->
    gproc_pool:disconnect_worker(Pool, {Pool, Id}).

%%------------------------------------------------------------------------------
%% Private
%%------------------------------------------------------------------------------

schedule(#{scheduled := true} = State) ->
    State;
schedule(#{queue := Queue} = State) ->
    case queue:is_empty(Queue) of
        true ->
            State;
        false ->
            self() ! #dispatch{},
            State#{scheduled := true}
    end.

dispatch(#{queue := Queue0} = State0) ->
    case queue:out(Queue0) of
        {empty, _} ->
            State0;
        {{value, #fetch{target = Target} = Request}, Queue} ->
            case emqx_extsub_handler:is_target_alive(Target) of
                false -> schedule(State0#{queue := Queue});
                true -> serve(Request, Queue, State0)
            end
    end.

serve(#fetch{target = Target, topic = Topic, cursor = Cursor0} = Request, Queue, State0) ->
    #{limiter := Limiter0, next_demand := Demand} = State0,
    BatchSize = batch_size(),
    %% Repay the previous batch before reading again. Overshoot is one batch per worker.
    Amount = consume_size(Demand),
    case emqx_limiter_client:try_consume(Limiter0, Amount) of
        {true, Limiter} when Amount < Demand ->
            serve(Request, Queue, State0#{limiter := Limiter, next_demand := Demand - Amount});
        {true, Limiter} ->
            {Messages, Cursor, Cost} = read_batch(Topic, Cursor0, BatchSize),
            emqx_extsub_handler:send(Target, {retained_batch, Messages, Cursor}),
            ?tp(retainer_dispatcher_batch, #{topic => Topic, cost => Cost}),
            schedule(State0#{queue := Queue, limiter := Limiter, next_demand := Cost});
        {false, Limiter, _Reason} ->
            ?tp(retained_fetch_rate_limit_exceeded, #{topic => Topic}),
            _ = erlang:send_after(100, self(), #dispatch{}),
            State0#{limiter := Limiter, scheduled := true}
    end.

read_batch(Topic, Cursor0, N) ->
    Opts = #{batch_read_number => N},
    Context = emqx_retainer:context(),
    Mod = emqx_retainer:backend_module(Context),
    BackendState = emqx_retainer:backend_state(Context),
    {ok, Messages, Cursor, Cost} =
        case emqx_topic:wildcard(Topic) of
            true ->
                Mod:match_messages(BackendState, Topic, Cursor0, Opts);
            false ->
                {ok, Msgs} = Mod:read_message(BackendState, Topic),
                {ok, Msgs, undefined, 1}
        end,
    case Cursor of
        undefined ->
            {Messages, done, Cost};
        _ ->
            {Messages, Cursor, Cost}
    end.

limiter_capacity() ->
    case emqx_config:get([retainer, flow_control, batch_deliver_limiter]) of
        infinity -> infinity;
        {Capacity, _Interval} -> Capacity
    end.

consume_size(Demand) ->
    case limiter_capacity() of
        infinity ->
            Demand;
        Capacity ->
            max(min(Capacity div 2, Demand), 1)
    end.
