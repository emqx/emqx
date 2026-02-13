%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_controller).

-moduledoc """
Controller for the Message Streams application.
Enables and disables its integration into the EMQX.
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
    started = false
}).

-record(start_streams, {}).
-record(stop_streams, {}).
-record(wait_status, {}).
-record(init_streams, {need_start :: boolean()}).

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

-spec start_streams() -> ok.
start_streams() ->
    gen_server:call(?MODULE, #start_streams{}, infinity).

-spec stop_streams() -> ok | {error, cannot_stop_streams_with_existing_streams}.
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
            {ok, #state{}, {continue, #init_streams{need_start = true}}};
        false ->
            {ok, #state{}, {continue, #init_streams{need_start = false}}}
    end.

handle_continue(#init_streams{need_start = true}, State) ->
    {noreply, do_start_streams(State)};
handle_continue(#init_streams{need_start = false}, State) ->
    {noreply, do_stop_streams(State)}.

handle_call(#start_streams{}, _From, State) ->
    {reply, ok, handle_start_streams(State)};
handle_call(#stop_streams{}, _From, State) ->
    case can_be_stopped() of
        true ->
            {reply, ok, handle_stop_streams(State)};
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
    handle_stop_streams(State).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

handle_start_streams(#state{started = false} = State) ->
    do_start_streams(State);
handle_start_streams(State) ->
    State.

do_start_streams(State) ->
    ?tp(debug, streams_controller_start_streams, #{}),
    ok = set_status(starting),

    %% Init DS
    ok = emqx_streams_message_db:open(),
    ok = emqx_streams_message_db:wait_readiness(infinity),

    %% Start services
    ok = emqx_streams_sup:start_metrics(),
    ok = emqx_mq_quota_buffer:start(?STREAMS_QUOTA_BUFFER, quota_buffer_options()),
    ok = emqx_streams_sup:start_gc_scheduler(),

    %% Hook into EMQX
    ok = emqx_streams:register_hooks(),
    ok = set_status(started),

    ?tp(debug, streams_controller_start_streams_done, #{}),
    State#state{started = true}.

handle_stop_streams(#state{started = true} = State) ->
    do_stop_streams(State);
handle_stop_streams(State) ->
    State.

do_stop_streams(State) ->
    ?tp(debug, streams_controller_stop_streams, #{}),
    ok = clear_status(),

    %% Unhook from EMQX
    ok = emqx_streams:unregister_hooks(),

    %% Stop services
    ok = emqx_streams_sup:stop_metrics(),
    _ = emqx_mq_quota_buffer:stop(?STREAMS_QUOTA_BUFFER),
    ok = emqx_streams_sup:stop_gc_scheduler(),

    %% Close DS
    _ = emqx_streams_message_db:close(),

    ?tp(debug, streams_controller_stop_streams_done, #{}),
    State#state{started = false}.

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
