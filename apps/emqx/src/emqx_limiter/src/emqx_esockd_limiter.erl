%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_esockd_limiter).

-behaviour(esockd_generic_limiter).

-include("logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% API
-export([
    create_options/1, create/1, delete/1, consume/2
]).

-type create_options() :: #{
    limiter_client := emqx_limiter_client:t(),
    module := ?MODULE
}.

%% esockd_generic_limiter:limiter() subtype
-type state() :: #{
    module := ?MODULE,
    name := ?MODULE,
    limiter_client := emqx_limiter_client:t()
}.

-export_type([
    create_options/0,
    state/0
]).

-define(PAUSE_INTERVAL, 100).

%%--------------------------------------------------------------------
%%  API
%%--------------------------------------------------------------------

-spec create_options(emqx_limiter_client:t()) -> create_options().
create_options(LimiterClient) ->
    #{limiter_client => LimiterClient, module => ?MODULE}.

%% NOTE
%% All the state we need is passed as the options
%% so we just keep it almost as is.
-spec create(create_options()) -> state().
create(#{limiter_client := LimiterClient}) ->
    #{
        module => ?MODULE,
        name => ?MODULE,
        limiter_client => LimiterClient
    }.

-spec delete(state()) -> ok.
delete(_State) ->
    ok.

-spec consume(non_neg_integer(), state()) ->
    {ok, state()} | {pause, non_neg_integer(), state()}.
consume(Amount, #{limiter_client := LimiterClient0} = State) ->
    case emqx_limiter_client:try_consume(LimiterClient0, Amount) of
        {true, LimiterClient} ->
            {ok, State#{limiter_client := LimiterClient}};
        {false, LimiterClient, Reason} ->
            ?tp(esockd_limiter_consume_pause, #{amount => Amount, reason => Reason}),
            ?SLOG_THROTTLE(
                warning,
                #{
                    msg => listener_accept_throttled_due_to_quota_exceeded,
                    pause_interval => ?PAUSE_INTERVAL,
                    reason => Reason
                },
                #{tag => "LISTENER"}
            ),
            {pause, ?PAUSE_INTERVAL, State#{limiter_client := LimiterClient}}
    end.
