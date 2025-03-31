%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_limiter_client).

-include("logger.hrl").

-export([
    new/2,
    try_consume/2,
    put_back/2
]).

-type state() :: term().

-type t() :: #{
    module := module(),
    state := state()
}.

-type reason() :: term().

-export_type([t/0, state/0, reason/0]).

%%--------------------------------------------------------------------
%% Callbacks
%%--------------------------------------------------------------------

-callback try_consume(state(), non_neg_integer()) ->
    true | {true, state()} | {false, state(), reason()}.

-callback put_back(state(), non_neg_integer()) -> state().

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec new(module(), state()) -> t().
new(Module, State) ->
    #{module => Module, state => State}.

-spec try_consume(t(), non_neg_integer()) -> {true, t()} | {false, t(), reason()}.
try_consume(#{module := Module, state := State} = Limiter, Amount) ->
    try Module:try_consume(State, Amount) of
        true ->
            {true, Limiter};
        {true, NewState} ->
            {true, Limiter#{state := NewState}};
        {false, NewState, Reason} ->
            {false, Limiter#{state := NewState}, Reason}
    catch
        error:Reason ->
            ?SLOG_THROTTLE(
                error,
                #{
                    msg => failed_to_consume_from_limiter,
                    reason => Reason,
                    module => Module
                },
                #{tag => "QUOTA"}
            ),
            {true, Limiter}
    end.

-spec put_back(t(), non_neg_integer()) -> t().
put_back(#{module := Module, state := State} = Limiter, Amount) ->
    try Module:put_back(State, Amount) of
        NewState ->
            Limiter#{state := NewState}
    catch
        error:Reason ->
            ?SLOG_THROTTLE(
                error,
                #{
                    msg => failed_to_put_back_to_limiter,
                    reason => Reason,
                    module => Module
                },
                #{tag => "QUOTA"}
            ),
            Limiter
    end.
