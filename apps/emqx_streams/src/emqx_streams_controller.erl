%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_controller).

-moduledoc """
Controller for the Message Streams application.
Enables and disables its integration into the EMQX.

Startup is split into two phases to avoid blocking config propagation:
- `starting`: DB opened, metrics started.
- `started`: DB readiness confirmed, hooks and services started.
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
    handle_continue/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-type status() :: started | starting | stopped.

-record(state, {
    status :: status() | undefined,
    target_status :: status()
}).

-record(start_streams, {}).
-record(stop_streams, {}).
-record(wait_status, {}).
-record(control_streams, {}).

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
Returns as soon as contoller enters `starting` phase.
Call `wait_status/1` to know when subsystem is fully `started`.
""".
-spec start_streams() -> starting.
start_streams() ->
    gen_server:call(?MODULE, #start_streams{}, infinity).

-spec stop_streams() -> stopped | {error, cannot_stop_streams_with_existing_streams}.
stop_streams() ->
    gen_server:call(?MODULE, #stop_streams{}, infinity).

-spec status() -> status().
status() ->
    persistent_term:get(?STATUS_PT_KEY, stopped).

%%------------------------------------------------------------------------------
%% Test API
%%------------------------------------------------------------------------------

-spec wait_status(timeout()) -> status().
wait_status(Timeout) ->
    ok = gen_server:call(?MODULE, #wait_status{}, Timeout),
    status().

%%------------------------------------------------------------------------------
%% Gen Server callbacks
%%------------------------------------------------------------------------------

init([]) ->
    process_flag(trap_exit, true),
    case need_start() of
        true ->
            TargetStatus = started;
        false ->
            TargetStatus = stopped
    end,
    State = #state{
        status = undefined,
        target_status = TargetStatus
    },
    {ok, State, {continue, #control_streams{}}}.

handle_continue(#control_streams{}, State) ->
    NewState = control_streams(State),
    case status_reached(NewState) of
        true ->
            {noreply, NewState};
        false ->
            {noreply, NewState, {continue, #control_streams{}}}
    end.

handle_call(#start_streams{}, _From, State) ->
    NewState = control_streams(State#state{target_status = started}),
    NewStatus = NewState#state.status,
    case status_reached(NewState) of
        true ->
            {reply, NewStatus, NewState};
        false ->
            {reply, NewStatus, NewState, {continue, #control_streams{}}}
    end;
handle_call(#stop_streams{}, _From, State) ->
    maybe
        true ?= can_be_stopped(),
        NewState = control_streams(State#state{target_status = stopped}),
        NewStatus = NewState#state.status,
        case status_reached(NewState) of
            true ->
                {reply, NewStatus, NewState};
            false ->
                {reply, NewStatus, NewState, {continue, #control_streams{}}}
        end
    else
        false ->
            {reply, {error, cannot_stop_streams_with_existing_streams}, State}
    end;
handle_call(#wait_status{}, _From, State) ->
    {reply, ok, State};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Message, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(Reason, State) ->
    ?tp(info, streams_controller_terminate, #{reason => Reason}),
    control_streams(State#state{target_status = stopped}).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

control_streams(#state{target_status = stopped, status = _Started} = State) ->
    do_stop_streams(State);
control_streams(#state{target_status = _Started, status = stopped} = State) ->
    do_start_streams(State);
control_streams(#state{target_status = _Started, status = undefined} = State) ->
    do_start_streams(State);
control_streams(#state{target_status = started, status = starting} = State) ->
    do_ready_streams(State);
control_streams(#state{target_status = Status, status = Status} = State) ->
    State.

status_reached(#state{status = Status, target_status = TargetStatus}) ->
    Status =:= TargetStatus.

%% Phase 1 (starting): Open DB, start metrics.
do_start_streams(State) ->
    ?tp(debug, streams_controller_start_streams, #{}),
    ok = set_status(starting),

    %% Open DB
    ok = emqx_streams_message_db:open(),

    %% Start services that don't require DB readiness
    ok = emqx_streams_sup:start_metrics(),

    %% Defer DB-dependent components to the next phase
    State#state{status = starting}.

%% Phase 2 (started): Wait for DB readiness, then start DB-dependent components.
do_ready_streams(State = #state{}) ->
    ?tp(debug, streams_controller_wait_ready, #{}),

    %% Block until DB is ready (allows config to propagate to cluster first)
    ok = emqx_streams_message_db:wait_readiness(infinity),

    %% Start components that require DB readiness
    ok = emqx_mq_quota_buffer:start(?STREAMS_QUOTA_BUFFER, quota_buffer_options()),
    ok = emqx_streams_sup:start_gc_scheduler(),
    ok = emqx_streams:register_hooks(),

    ok = set_status(started),
    ?tp(debug, streams_controller_start_streams_done, #{}),

    State#state{status = started}.

do_stop_streams(State = #state{status = Status}) ->
    ?tp(debug, streams_controller_stop_streams, #{}),

    case Status of
        started ->
            ok = emqx_streams:unregister_hooks(),
            _ = emqx_mq_quota_buffer:stop(?STREAMS_QUOTA_BUFFER),
            ok = emqx_streams_sup:stop_gc_scheduler();
        _ ->
            %% Not fully started: only metrics and DB were started
            ok
    end,

    ok = emqx_streams_sup:stop_metrics(),
    _ = emqx_streams_message_db:close(),

    ok = clear_status(),
    ?tp(debug, streams_controller_stop_streams_done, #{}),

    State#state{status = stopped}.

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
