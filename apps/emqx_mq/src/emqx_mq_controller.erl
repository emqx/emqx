%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_controller).

-moduledoc """
Controller for the Message Queue application.
Enables and disables its integration into the EMQX.

Startup is split into two phases to avoid blocking config propagation:
- `starting`: DB opened, metrics started.
- `started`: DB readiness confirmed, hooks and services started.
""".

-include("emqx_mq_internal.hrl").

-behaviour(gen_server).

-export([start_link/0, child_spec/0]).

-export([
    start_mqs/0,
    stop_mqs/0,
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

-record(start_mqs, {}).
-record(stop_mqs, {}).
-record(wait_status, {}).
-record(control_mqs, {}).

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
Returns as soon as controller enters `starting` phase.
Call `wait_status/1` to know when subsystem is fully `started`.
""".
-spec start_mqs() -> starting.
start_mqs() ->
    gen_server:call(?MODULE, #start_mqs{}, infinity).

-spec stop_mqs() -> stopped | {error, cannot_stop_mqs_with_existing_queues}.
stop_mqs() ->
    gen_server:call(?MODULE, #stop_mqs{}, infinity).

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
    {ok, State, {continue, #control_mqs{}}}.

handle_continue(#control_mqs{}, State) ->
    NewState = control_mqs(State),
    case status_reached(NewState) of
        true ->
            {noreply, NewState};
        false ->
            {noreply, NewState, {continue, #control_mqs{}}}
    end.

handle_call(#start_mqs{}, _From, State) ->
    NewState = control_mqs(State#state{target_status = started}),
    NewStatus = NewState#state.status,
    case status_reached(NewState) of
        true ->
            {reply, NewStatus, NewState};
        false ->
            {reply, NewStatus, NewState, {continue, #control_mqs{}}}
    end;
handle_call(#stop_mqs{}, _From, State) ->
    maybe
        true ?= can_be_stopped(),
        NewState = control_mqs(State#state{target_status = stopped}),
        NewStatus = NewState#state.status,
        case status_reached(NewState) of
            true ->
                {reply, NewStatus, NewState};
            false ->
                {reply, NewStatus, NewState, {continue, #control_mqs{}}}
        end
    else
        false ->
            {reply, {error, cannot_stop_mqs_with_existing_queues}, State}
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
    ?tp(info, mq_controller_terminate, #{reason => Reason}),
    control_mqs(State#state{target_status = stopped}).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

control_mqs(#state{target_status = stopped, status = _Started} = State) ->
    do_stop_mqs(State);
control_mqs(#state{target_status = _Started, status = stopped} = State) ->
    do_start_mqs(State);
control_mqs(#state{target_status = _Started, status = undefined} = State) ->
    do_start_mqs(State);
control_mqs(#state{target_status = started, status = starting} = State) ->
    do_ready_mqs(State);
control_mqs(#state{target_status = Status, status = Status} = State) ->
    State.

status_reached(#state{status = Status, target_status = TargetStatus}) ->
    Status =:= TargetStatus.

%% Phase 1 (starting): Open DB, start metrics.
do_start_mqs(State) ->
    ?tp(debug, mq_controller_start_mqs, #{}),
    ok = set_status(starting),

    %% Open DB
    ok = emqx_mq_message_db:open(),
    ok = emqx_mq_state_storage:open_db(),

    %% Start services that don't require DB readiness
    ok = emqx_mq_sup:start_metrics(),

    %% Defer DB-dependent components to the next phase
    State#state{status = starting}.

%% Phase 2 (started): Wait for DB readiness, then start DB-dependent components.
do_ready_mqs(State = #state{}) ->
    ?tp(debug, mq_controller_wait_ready, #{}),

    %% Block until DB is ready (allows config to propagate to cluster first)
    ok = emqx_mq_message_db:wait_readiness(infinity),
    ok = emqx_mq_state_storage:wait_readiness(infinity),

    %% Start components that require DB readiness
    ok = emqx_mq_quota_buffer:start(?MQ_QUOTA_BUFFER, quota_buffer_options()),
    ok = emqx_mq_sup:start_gc_scheduler(),

    %% Hook into EMQX
    %% Claim handling of `$queue` topics to ourselves
    ok = emqx_topic:enable_queue_alias_to_share(false),
    ok = emqx_mq:register_hooks(),

    ok = set_status(started),
    ?tp(debug, mq_controller_start_mqs_done, #{}),

    State#state{status = started}.

do_stop_mqs(State = #state{status = Status}) ->
    ?tp(debug, mq_controller_stop_mqs, #{}),

    case Status of
        started ->
            ok = emqx_topic:enable_queue_alias_to_share(true),
            ok = emqx_mq:unregister_hooks(),
            _ = emqx_mq_quota_buffer:stop(?MQ_QUOTA_BUFFER),
            ok = emqx_mq_sup:stop_gc_scheduler();
        _ ->
            %% Not fully started: only metrics and DB were started
            ok
    end,

    ok = emqx_mq_sup:stop_metrics(),
    _ = emqx_mq_message_db:close(),
    _ = emqx_mq_state_storage:close_db(),

    ok = clear_status(),
    ?tp(debug, mq_controller_stop_mqs_done, #{}),

    State#state{status = stopped}.

need_start() ->
    case emqx_mq_config:enabled() of
        auto ->
            not can_be_stopped();
        Value ->
            Value
    end.

can_be_stopped() ->
    emqx_mq_registry:queue_count() == 0.

quota_buffer_options() ->
    #{
        cbm => emqx_mq_message_db,
        pool_size => emqx_mq_config:quota_buffer_pool_size()
    }.

set_status(Status) ->
    _ = persistent_term:put(?STATUS_PT_KEY, Status),
    ok.

clear_status() ->
    _ = persistent_term:erase(?STATUS_PT_KEY),
    ok.
