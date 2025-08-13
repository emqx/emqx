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
    capacity => non_neg_integer()
}.

-export_type([options/0]).

-define(PAUSE_INTERVAL, 100).

%%--------------------------------------------------------------------
%%  API
%%--------------------------------------------------------------------

-spec create(options()) -> state().
create(Options = #{listener := ListenerID}) ->
    S0 = #{listener => ListenerID},
    case Options of
        #{rate_limiter := RL} -> S1 = S0#{rate_limiter => RL};
        #{} -> S1 = S0
    end,
    case Options of
        #{max_connections := infinity} -> S1;
        #{max_connections := MC} -> S1#{capacity => MC};
        #{} -> S1
    end.

-spec allow(_Socket, state()) -> {ok | ranch_conns_limiter:penalty(), state()}.
allow(_Socket, #{capacity := 0} = State) ->
    %% Max connections limit / no more capacity:
    ?SLOG_THROTTLE(
        warning,
        #{
            msg => listener_accept_refused_reached_max_connections,
            listener => maps:get(listener, State)
        },
        #{tag => "LISTENER"}
    ),
    {close_connection, State};
allow(_Socket, #{rate_limiter := RateLimiter0} = State) ->
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
allow(_Socket, State) ->
    {ok, State}.

-spec accepted(pid(), state()) -> state().
accepted(_Pid, State = #{capacity := C}) ->
    State#{capacity := C - 1};
accepted(_Pid, State) ->
    %% Unlimited capacity.
    State.

-spec retired(pid(), state()) -> state().
retired(_Pid, State = #{capacity := C}) ->
    State#{capacity := C + 1};
retired(_Pid, State) ->
    %% Unlimited capacity.
    State.
