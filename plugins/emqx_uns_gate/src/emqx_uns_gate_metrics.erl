%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_uns_gate_metrics).

-behaviour(gen_server).

-export([
    start_link/0,
    reset/0,
    record_allowed/0,
    record_exempt/0,
    record_drop/3,
    snapshot/0
]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(MAX_RECENT_DROPS, 100).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

reset() ->
    gen_server:call(?MODULE, reset).

record_allowed() ->
    gen_server:cast(?MODULE, allowed).

record_exempt() ->
    gen_server:cast(?MODULE, exempt).

record_drop(Topic, ErrorType, ErrorDetail) ->
    gen_server:cast(?MODULE, {drop, Topic, ErrorType, ErrorDetail}).

snapshot() ->
    gen_server:call(?MODULE, snapshot).

init([]) ->
    {ok, init_state()}.

handle_call(reset, _From, _State) ->
    {reply, ok, init_state()};
handle_call(snapshot, _From, State) ->
    {reply, format_stats(State), State};
handle_call(_Call, _From, State) ->
    {reply, {error, bad_request}, State}.

handle_cast(allowed, State) ->
    {noreply, bump(State, [messages_total, messages_allowed])};
handle_cast(exempt, State0) ->
    State = bump(State0, [messages_total, messages_allowed, exempt]),
    {noreply, State};
handle_cast({drop, Topic, ErrorType0, ErrorDetail}, State0) ->
    ErrorType = normalize_error_type(ErrorType0),
    State1 = bump(State0, [messages_total, messages_dropped, ErrorType]),
    Drop = #{
        timestamp_ms => erlang:system_time(millisecond),
        topic => Topic,
        error_type => ErrorType,
        error_detail => ErrorDetail
    },
    Recent0 = maps:get(recent_drops, State1, []),
    Recent = [Drop | Recent0],
    Recent1 = lists:sublist(Recent, ?MAX_RECENT_DROPS),
    {noreply, State1#{recent_drops => Recent1}};
handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

init_state() ->
    #{
        started_at_ms => erlang:system_time(millisecond),
        messages_total => 0,
        messages_allowed => 0,
        messages_dropped => 0,
        topic_invalid => 0,
        payload_invalid => 0,
        not_endpoint => 0,
        exempt => 0,
        recent_drops => []
    }.

bump(State, Keys) ->
    lists:foldl(
        fun(Key, Acc) -> Acc#{Key => maps:get(Key, Acc, 0) + 1} end,
        State,
        Keys
    ).

normalize_error_type(payload_invalid) -> payload_invalid;
normalize_error_type(not_endpoint) -> not_endpoint;
normalize_error_type(topic_invalid) -> topic_invalid;
normalize_error_type(_Other) -> topic_invalid.

format_stats(State) ->
    StartedAtMs = maps:get(started_at_ms, State),
    UptimeSec = max(0, (erlang:system_time(millisecond) - StartedAtMs) div 1000),
    TopicInvalid = maps:get(topic_invalid, State, 0),
    PayloadInvalid = maps:get(payload_invalid, State, 0),
    NotEndpoint = maps:get(not_endpoint, State, 0),
    #{
        messages_total => maps:get(messages_total, State, 0),
        messages_allowed => maps:get(messages_allowed, State, 0),
        messages_dropped => maps:get(messages_dropped, State, 0),
        topic_invalid => TopicInvalid,
        payload_invalid => PayloadInvalid,
        not_endpoint => NotEndpoint,
        exempt => maps:get(exempt, State, 0),
        uptime_seconds => UptimeSec,
        drop_breakdown => #{
            topic_invalid => TopicInvalid,
            payload_invalid => PayloadInvalid,
            not_endpoint => NotEndpoint
        },
        recent_drops => lists:reverse(maps:get(recent_drops, State, []))
    }.
