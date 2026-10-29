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
    fetch/5
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

-record(fetch, {target, request_ref, topic, cursor}).
-record(wait_dispatch_complete, {}).
-record(dispatch, {}).

-define(DEFAULT_BATCH_SIZE, 1000).
-define(MIN_BATCH_SIZE, 10).
-define(MAX_REQUESTS_PER_DISPATCH, 100).
-define(LIMITER_RETRY_INTERVAL_MS, 100).

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

-spec worker() -> pid() | false.
worker() ->
    gproc_pool:pick_worker(?DISPATCHER_POOL, self()).

-doc """
Queue one retained-message batch for a handler.
The worker replies with `{retained_batch, RequestRef, Messages, Cursor | done}`.
""".
-spec fetch(pid(), emqx_extsub_handler:message_target(), reference(), binary(), term()) -> ok.
fetch(Worker, Target, RequestRef, Topic, Cursor) ->
    gen_server:cast(Worker, #fetch{
        target = Target,
        request_ref = RequestRef,
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
    {noreply, dispatch(State#{scheduled := false}, ?MAX_REQUESTS_PER_DISPATCH)};
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

%% Serve up to N requests, then yield so that calls and casts are handled.
dispatch(State, 0) ->
    schedule(State);
dispatch(#{queue := Queue0} = State0, N) ->
    case queue:out(Queue0) of
        {empty, _} ->
            State0;
        {{value, #fetch{target = Target} = Request}, Queue} ->
            case emqx_extsub_handler:is_target_alive(Target) of
                false ->
                    dispatch(State0#{queue := Queue}, N - 1);
                true ->
                    case serve(Request, State0) of
                        {served, State} -> dispatch(State#{queue := Queue}, N - 1);
                        {paused, State} -> State
                    end
            end
    end.

%% Pay for the previous read, then read. A worker can exceed the limit by one batch.
%% Read the settings for each request: workers restart before a config update is saved.
serve(Request, State) ->
    #{batch_read_number := BatchReadNumber, batch_deliver_limiter := Limiter} =
        emqx_config:get([retainer, flow_control]),
    Capacity = limiter_capacity(Limiter),
    serve(Request, Capacity, batch_size(BatchReadNumber, Capacity), State).

serve(Request, infinity, BatchSize, State) ->
    read(Request, BatchSize, State);
serve(Request, _Capacity, BatchSize, #{next_demand := 0} = State) ->
    read(Request, BatchSize, State);
serve(#fetch{topic = Topic} = Request, Capacity, BatchSize, State0) ->
    #{limiter := Limiter0, next_demand := Demand} = State0,
    %% An amount above the capacity succeeds only after the shared bucket is idle for more
    %% than one interval, and other workers can prevent that. Pay in parts of at most half
    %% the capacity, so that each part succeeds when the bucket is half full.
    Amount = max(1, min(Capacity div 2, Demand)),
    case emqx_limiter_client:try_consume(Limiter0, Amount) of
        {true, Limiter} ->
            serve(Request, Capacity, BatchSize, State0#{
                limiter := Limiter, next_demand := Demand - Amount
            });
        {false, Limiter, Reason} ->
            ?tp(retained_fetch_rate_limit_exceeded, #{topic => Topic}),
            ?SLOG_THROTTLE(
                warning,
                #{
                    msg => retained_fetch_rate_limit_exceeded,
                    reason => Reason,
                    topic => Topic
                },
                #{tag => "RETAINER"}
            ),
            _ = erlang:send_after(?LIMITER_RETRY_INTERVAL_MS, self(), #dispatch{}),
            {paused, State0#{limiter := Limiter, scheduled := true}}
    end.

read(
    #fetch{target = Target, request_ref = RequestRef, topic = Topic, cursor = Cursor0},
    BatchSize,
    State
) ->
    {Messages, Cursor, Cost} = read_batch(Topic, Cursor0, BatchSize),
    emqx_extsub_handler:send(Target, {retained_batch, RequestRef, Messages, Cursor}),
    ?tp(retainer_dispatcher_batch, #{topic => Topic, cost => Cost}),
    {served, State#{next_demand := Cost}}.

%% A failed read ends delivery for its subscription only.
read_batch(Topic, Cursor, BatchSize) ->
    try
        do_read_batch(Topic, Cursor, BatchSize)
    catch
        Class:Reason:Stacktrace ->
            ?SLOG(error, #{
                msg => "retained_message_read_failed",
                topic => Topic,
                class => Class,
                reason => Reason,
                stacktrace => Stacktrace
            }),
            {[], done, 1}
    end.

do_read_batch(Topic, Cursor0, BatchSize) ->
    Opts = #{batch_read_number => BatchSize},
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

%% Limit each worker's read to its share of the limiter capacity.
%% `batch_read_number = 0' reads all remaining messages, which is capped at the default.
batch_size(BatchReadNumber, Capacity) ->
    MaxBatchSize =
        case BatchReadNumber of
            0 -> ?DEFAULT_BATCH_SIZE;
            N -> N
        end,
    case Capacity of
        infinity ->
            MaxBatchSize;
        _ ->
            Share = Capacity div emqx_vm:schedulers(),
            min(MaxBatchSize, max(?MIN_BATCH_SIZE, Share))
    end.

limiter_capacity(infinity) ->
    infinity;
limiter_capacity({Capacity, _Interval}) ->
    Capacity.
