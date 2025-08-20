%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ranch_limiter).

-include("logger.hrl").

-behaviour(ranch_conns_limiter).
-export([
    create/1,
    allow/2,
    accepted/2,
    retired/2
]).

-type options() :: #{
    listener := emqx_listeners:listener_id(),
    rate_limiter => emqx_limiter_client:t(),
    max_connections => pos_integer() | infinity
}.

-type state() :: #{
    listener := emqx_listeners:listener_id(),
    rate_limiter => emqx_limiter_client:t(),
    capacity_ref => capacity()
}.

-type capacity() :: atomics:atomics_ref().

-export_type([options/0, capacity/0]).

-define(PAUSE_INTERVAL, 100).

%%--------------------------------------------------------------------
%%  API
%%--------------------------------------------------------------------

-doc "Create a Ranch conns limiter, each conns sup will have its own".
-spec create(options()) -> state().
create(Options = #{listener := ListenerID}) ->
    S0 = #{listener => ListenerID},
    case Options of
        #{rate_limiter := RL} -> S1 = S0#{rate_limiter => RL};
        #{} -> S1 = S0
    end,
    case Options of
        #{max_connections := infinity} -> S1;
        #{max_connections := MC} -> S1#{capacity_ref => create_capacity(MC)};
        #{} -> S1
    end.

-doc "Create a *shared* capacity tracker, to track it across conns sups".
-spec create_capacity(pos_integer()) -> capacity().
create_capacity(MaxConnections) ->
    ARef = atomics:new(1, []),
    ok = atomics:put(ARef, 1, MaxConnections),
    ARef.

-spec allow(_Socket, state()) -> {ok | ranch_conns_limiter:penalty(), state()}.
allow(_Socket, State0) ->
    maybe
        {ok, State1} ?= allow_capacity(State0),
        {ok, State} ?= allow_limiter(State1),
        {ok, State}
    end.

allow_capacity(#{capacity_ref := ARef} = State) ->
    case atomics:get(ARef, 1) of
        N when N > 0 ->
            {ok, State};
        _ ->
            %% Max connections limit / no more capacity:
            ?SLOG_THROTTLE(
                warning,
                #{
                    msg => listener_accept_refused_reached_max_connections,
                    listener => maps:get(listener, State)
                },
                #{tag => "LISTENER"}
            ),
            {close_connection, State}
    end;
allow_capacity(State) ->
    %% Unlimited capacity.
    {ok, State}.

allow_limiter(#{rate_limiter := RateLimiter0} = State) ->
    %% Connection rate limit:
    case emqx_limiter_client:try_consume(RateLimiter0, 1) of
        {true, RateLimiter} ->
            Ret = ok;
        {false, RateLimiter, Reason} ->
            ?SLOG_THROTTLE(
                warning,
                #{
                    msg => listener_accept_throttled_due_to_quota_exceeded,
                    listener => maps:get(listener, State),
                    pause_interval => ?PAUSE_INTERVAL,
                    reason => Reason
                },
                #{tag => "LISTENER"}
            ),
            Ret = {close_connections_for, ?PAUSE_INTERVAL}
    end,
    {Ret, State#{rate_limiter := RateLimiter}};
allow_limiter(State) ->
    %% No rate limiting.
    {ok, State}.

-spec accepted(pid(), state()) -> state().
accepted(_Pid, State = #{capacity_ref := ARef}) ->
    ok = atomics:sub(ARef, 1, 1),
    State;
accepted(_Pid, State) ->
    %% Unlimited capacity.
    State.

-spec retired(pid(), state()) -> state().
retired(_Pid, State = #{capacity_ref := ARef}) ->
    ok = atomics:add(ARef, 1, 1),
    State;
retired(_Pid, State) ->
    %% Unlimited capacity.
    State.
