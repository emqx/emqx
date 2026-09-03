%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_controller).

-moduledoc """
Controller for the Message Streams application.
Enables and disables its integration into the EMQX.

Lifecycle operations run in a linked worker so the controller can accept
target changes while startup or shutdown is in progress.
""".

-include("emqx_streams_internal.hrl").

-behaviour(gen_server).

-export([start_link/0, child_spec/0]).

-export([
    start_streams/0,
    stop_streams/0,
    status/0
]).

-export([
    wait_status/1,
    can_be_stopped/0
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-type status() :: started | starting | stopping | stopped.

-record(state, {
    target_status :: started | stopped,
    worker = undefined :: undefined | map(),
    waiters = [] :: [gen_server:from()]
}).

-record(set_target, {status :: started | stopped}).
-record(wait_status, {}).

-define(STATUS_PT_KEY, ?MODULE).

%%------------------------------------------------------------------------------
%% Public API
%%------------------------------------------------------------------------------

-spec child_spec() -> supervisor:child_spec().
child_spec() ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [?MODULE]
    }.

-spec start_link() -> gen_server:start_ret().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-doc """
Requests the controller to enter the `starting` phase.
Call `wait_status/1` to know when subsystem is fully `started`.
""".
-spec start_streams() -> ok.
start_streams() ->
    gen_server:call(?MODULE, #set_target{status = started}, infinity).

-spec stop_streams() -> ok | {error, cannot_stop_streams_with_existing_streams}.
stop_streams() ->
    gen_server:call(?MODULE, #set_target{status = stopped}, infinity).

-spec status() -> status().
status() ->
    persistent_term:get(?STATUS_PT_KEY, stopped).

%%------------------------------------------------------------------------------
%% Test API
%%------------------------------------------------------------------------------

-spec wait_status(timeout()) -> status().
wait_status(Timeout) ->
    gen_server:call(?MODULE, #wait_status{}, Timeout).

%%------------------------------------------------------------------------------
%% Gen Server callbacks
%%------------------------------------------------------------------------------

init([]) ->
    process_flag(trap_exit, true),
    PreviousStatus = status(),
    ok = clear_status(),
    case {need_start(), PreviousStatus} of
        {true, _} ->
            {ok, start_operation(start, #state{target_status = started})};
        {false, stopped} ->
            {ok, #state{target_status = stopped}};
        {false, _} ->
            State = start_operation(stop, #state{target_status = stopped}),
            ?tp(streams_controller_init_cleanup, #{previous_status => PreviousStatus}),
            {ok, State}
    end.

handle_call(#set_target{status = started}, _From, State) ->
    NewState = set_target(started, State),
    {reply, ok, NewState};
handle_call(#set_target{status = stopped}, _From, State) ->
    case can_be_stopped() of
        true ->
            NewState = set_target(stopped, State),
            {reply, ok, NewState};
        false ->
            {reply, {error, cannot_stop_streams_with_existing_streams}, State}
    end;
handle_call(#wait_status{}, _From, State = #state{target_status = Status, worker = undefined}) ->
    {reply, Status, State};
handle_call(#wait_status{}, From, State) ->
    {noreply, State#state{waiters = [From | State#state.waiters]}};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Message, State) ->
    {noreply, State}.

handle_info(
    {'EXIT', Pid, normal},
    State = #state{worker = #{pid := Pid, operation := Operation}}
) ->
    Status = operation_result(Operation),
    ok = set_status(Status),
    {noreply, operation_complete(Status, State#state{worker = undefined})};
handle_info(
    {'EXIT', Pid, Reason},
    State = #state{worker = #{pid := Pid, operation := Operation}}
) ->
    {stop, {lifecycle_operation_failed, Operation, Reason}, State#state{worker = undefined}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(Reason, State) ->
    ?tp(info, streams_controller_terminate, #{reason => Reason}),
    ok = stop_worker(State),
    _ = do_stop_streams(),
    clear_status().

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

set_target(TargetStatus, State = #state{target_status = TargetStatus, worker = undefined}) ->
    State;
set_target(TargetStatus, State = #state{worker = undefined}) ->
    start_operation(target_operation(TargetStatus), State#state{target_status = TargetStatus});
set_target(TargetStatus, State) ->
    State#state{target_status = TargetStatus}.

operation_complete(Status, State = #state{target_status = Status}) ->
    reply_waiters(Status, State);
operation_complete(_Status, State = #state{target_status = TargetStatus}) ->
    start_operation(target_operation(TargetStatus), State).

target_operation(started) -> start;
target_operation(stopped) -> stop.

start_operation(Operation, State = #state{worker = undefined}) ->
    Status = operation_status(Operation),
    ok = set_status(Status),
    Pid = spawn_link(fun() ->
        ?tp(streams_controller_worker_start, #{operation => Operation, status => Status}),
        ok = run_operation(Operation)
    end),
    State#state{
        worker = #{pid => Pid, operation => Operation}
    }.

operation_status(start) -> starting;
operation_status(stop) -> stopping.

operation_result(start) -> started;
operation_result(stop) -> stopped.

run_operation(start) -> do_start_streams();
run_operation(stop) -> do_stop_streams().

reply_waiters(Status, State = #state{waiters = Waiters}) ->
    lists:foreach(fun(From) -> gen_server:reply(From, Status) end, Waiters),
    State#state{waiters = []}.

stop_worker(#state{worker = undefined}) ->
    ok;
stop_worker(#state{worker = #{pid := Pid}}) ->
    exit(Pid, shutdown),
    receive
        {'EXIT', Pid, _Reason} -> ok
    end.

do_start_streams() ->
    ?tp(debug, streams_controller_start_streams, #{}),
    ok = emqx_streams_message_db:open(),
    ok = emqx_streams_sup:start_metrics(),
    ?tp(debug, streams_controller_wait_ready, #{}),
    ok = emqx_streams_message_db:wait_readiness(infinity),
    ok = emqx_mq_quota_buffer:start(?STREAMS_QUOTA_BUFFER, quota_buffer_options()),
    ok = emqx_streams_sup:start_gc_scheduler(),
    ok = emqx_streams:register_hooks(),
    ?tp(debug, streams_controller_start_streams_done, #{}),

    ok.

do_stop_streams() ->
    ?tp(debug, streams_controller_stop_streams, #{}),
    ok = emqx_streams:unregister_hooks(),
    _ = emqx_mq_quota_buffer:stop(?STREAMS_QUOTA_BUFFER),
    ok = emqx_streams_sup:stop_gc_scheduler(),
    ok = emqx_streams_sup:stop_metrics(),
    _ = emqx_streams_message_db:close(),
    ?tp(debug, streams_controller_stop_streams_done, #{}),

    ok.

need_start() ->
    case emqx_streams_config:enabled() of
        auto ->
            not can_be_stopped();
        Value ->
            Value
    end.

can_be_stopped() ->
    emqx_streams_registry:stream_count() == 0.

quota_buffer_options() ->
    #{
        cbm => emqx_streams_message_db,
        pool_size => emqx_streams_config:quota_buffer_pool_size()
    }.

set_status(Status) ->
    _ = persistent_term:put(?STATUS_PT_KEY, Status),
    ok.

clear_status() ->
    _ = persistent_term:erase(?STATUS_PT_KEY),
    ok.
