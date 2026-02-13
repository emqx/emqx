%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_controller).

-moduledoc """
Controller for the Message Queue application.
Enables and disables its integration into the EMQX.
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
    started = false
}).

-record(start_mqs, {}).
-record(stop_mqs, {}).
-record(wait_status, {}).
-record(init_mqs, {need_start :: boolean()}).

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

-spec start_mqs() -> ok.
start_mqs() ->
    gen_server:call(?MODULE, #start_mqs{}, infinity).

-spec stop_mqs() -> ok | {error, cannot_stop_mqs_with_existing_queues}.
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
            {ok, #state{}, {continue, #init_mqs{need_start = true}}};
        false ->
            {ok, #state{}, {continue, #init_mqs{need_start = false}}}
    end.

handle_continue(#init_mqs{need_start = true}, State) ->
    {noreply, do_start_mqs(State)};
handle_continue(#init_mqs{need_start = false}, State) ->
    {noreply, do_stop_mqs(State)}.

handle_call(#start_mqs{}, _From, State) ->
    {reply, ok, handle_start_mqs(State)};
handle_call(#stop_mqs{}, _From, State) ->
    case can_be_stopped() of
        true ->
            {reply, ok, handle_stop_mqs(State)};
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
    handle_stop_mqs(State).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

handle_start_mqs(#state{started = false} = State) ->
    do_start_mqs(State);
handle_start_mqs(State) ->
    State.

do_start_mqs(State) ->
    ?tp(debug, mq_controller_start_mqs, #{}),
    ok = set_status(starting),
    %% Init DS
    ok = emqx_mq_message_db:open(),
    ok = emqx_mq_state_storage:open_db(),
    ok = emqx_mq_message_db:wait_readiness(infinity),
    ok = emqx_mq_state_storage:wait_readiness(infinity),

    %% Start services
    ok = emqx_mq_sup:start_metrics(),
    ok = emqx_mq_quota_buffer:start(?MQ_QUOTA_BUFFER, quota_buffer_options()),
    ok = emqx_mq_sup:start_gc_scheduler(),

    %% Hook into EMQX
    %% Claim handling of `$queue` topics to ourselves
    ok = emqx_topic:enable_queue_alias_to_share(false),
    ok = emqx_mq:register_hooks(),
    ok = set_status(started),

    ?tp(debug, mq_controller_start_mqs_done, #{}),
    State#state{started = true}.

handle_stop_mqs(#state{started = true} = State) ->
    do_stop_mqs(State);
handle_stop_mqs(State) ->
    State.

do_stop_mqs(State) ->
    ?tp(debug, mq_controller_stop_mqs, #{}),
    ok = clear_status(),

    %% Unhook from EMQX
    ok = emqx_topic:enable_queue_alias_to_share(true),
    ok = emqx_mq:unregister_hooks(),

    %% Stop services
    ok = emqx_mq_sup:stop_metrics(),
    _ = emqx_mq_quota_buffer:stop(?MQ_QUOTA_BUFFER),
    ok = emqx_mq_sup:stop_gc_scheduler(),

    %% Close DS
    _ = emqx_mq_message_db:close(),
    _ = emqx_mq_state_storage:close_db(),

    ?tp(debug, mq_controller_stop_mqs_done, #{}),
    State#state{started = false}.

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
