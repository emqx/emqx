%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

%% @doc This module implements the private limiter.
%%
%% Exclusive limiter is a limiter that is not shared between different processes.
%% I.e. different processes do not consume tokens concurrently.
%%
%% This is a more simple version of a limiter because its own toket bucket
%% is refilled algorithmically, without the help of the external emqx_limiter_allocator.

-module(emqx_limiter_exclusive).

-behaviour(emqx_limiter_client).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    create_group/2,
    delete_group/1,
    update_group_configs/2
]).

%% emqx_limiter_client API
-export([
    try_consume/2,
    put_back/2
]).

-export([
    connect/1
]).

-type millisecond() :: integer().

-type state() :: #{
    tokens := number(),
    last_time := millisecond()
}.

%%--------------------------------------------------------------------
%%  API
%%--------------------------------------------------------------------

-spec create_group(emqx_limiter:group(), [{emqx_limiter:name(), emqx_limiter:options()}]) -> ok.
create_group(Group, LimiterConfigs) ->
    ok = register_group(Group, LimiterConfigs).

-spec delete_group(emqx_limiter:group()) -> ok.
delete_group(Group) ->
    ok = unregister_group(Group).

-spec update_group_configs(emqx_limiter:group(), [{emqx_limiter:name(), emqx_limiter:options()}]) ->
    ok.
update_group_configs(Group, LimiterConfigs) ->
    ok = register_group(Group, LimiterConfigs).

-spec connect(emqx_limiter:id()) -> emqx_limiter_client:t().
connect({_Group, _Name} = LimiterId) ->
    InitialTokens = initial_tokens(emqx_limiter_registry:get_limiter_options(LimiterId)),
    State = #{
        limiter_id => LimiterId,
        tokens => InitialTokens,
        last_time => now_ms_monotonic()
    },
    emqx_limiter_client:new(?MODULE, State).

%%--------------------------------------------------------------------
%% emqx_limiter_client API
%%--------------------------------------------------------------------

-spec try_consume(state(), number()) -> {boolean(), state()}.
try_consume(#{limiter_id := LimiterId} = State0, Amount) ->
    LimiterOptions = emqx_limiter_registry:get_limiter_options(LimiterId),
    {Result, State1} = try_consume(State0, Amount, LimiterOptions),
    ?tp(limiter_exclusive_try_consume, #{
        limiter_id => LimiterId,
        amount => Amount,
        result => Result
    }),
    {Result, State1}.

-spec put_back(state(), number()) -> state().
put_back(#{tokens := Tokens} = State, Amount) ->
    State#{tokens := Tokens + Amount}.

%%--------------------------------------------------------------------
%%  Internal functions
%%--------------------------------------------------------------------

%% TODO
%% handle burst_capacity
try_consume(State, _Amount, #{capacity := infinity}) ->
    {true, State};
try_consume(#{tokens := Tokens} = State, Amount, _Setting) when Amount =< Tokens ->
    {true, State#{tokens := Tokens - Amount}};
try_consume(
    #{tokens := Tokens0, last_time := LastTime} = State0,
    Amount,
    #{capacity := Capacity, interval := Interval} = _LimiterOptions
) when Amount > Tokens0 ->
    case now_ms_monotonic() of
        Now when Now < LastTime + Interval ->
            {false, State0};
        Now ->
            %% NOTE
            %% This calculation are mostly redundant now, because
            %% the capacity is equal to the max capacity
            Inc = Capacity * (Now - LastTime) / Interval,
            Tokens = erlang:min(Capacity, Tokens0 + Inc),
            State1 = State0#{last_time := Now, tokens := Tokens},
            case Tokens >= Amount of
                true ->
                    {true, State1#{tokens := Tokens - Amount}};
                _ ->
                    {false, State1}
            end
    end.

register_group(Group, LimiterConfigs) ->
    emqx_limiter_registry:register_group(Group, ?MODULE, LimiterConfigs).

unregister_group(Group) ->
    emqx_limiter_registry:unregister_group(Group).

now_ms_monotonic() ->
    erlang:monotonic_time(millisecond).

initial_tokens(#{capacity := infinity}) -> 0;
initial_tokens(#{capacity := Capacity}) when is_integer(Capacity) -> Capacity.
