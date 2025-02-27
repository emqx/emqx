%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc This module implements the shared limiter.
%%
%% Shared limiter is a limiter that is shared between different processes.
%% I.e. different processes consume tokens concurrently.
%%
%% The shared limiter is a wrapper around a handle to a bucket managed
%% and refilled by the `emqx_limiter_allocator`.

-module(emqx_limiter_shared).

-behaviour(emqx_limiter_client).
-behaviour(emqx_limiter).

-include("logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% emqx_limiter callbacks
-export([
    create_group/2,
    update_group/2,
    delete_group/1,
    connect/1
]).

%% emqx_limiter_client callbacks
-export([
    try_consume/2,
    put_back/2
]).

-type bucket_ref() :: #{
    counter := counters:counters_ref(),
    index := pos_integer()
}.

-type client_state() :: emqx_limiter:id().

-export_type([bucket_ref/0, client_state/0]).

%%--------------------------------------------------------------------
%%  API
%%--------------------------------------------------------------------

-spec create_group(emqx_limiter:group(), [
    {emqx_limiter:name(), emqx_limiter:options()}
]) -> ok | {error, term()}.
create_group(Group, LimiterConfigs) when length(LimiterConfigs) > 0 ->
    {Names, _} = lists:unzip(LimiterConfigs),
    ChildSpec = child_spec(Group, Names),
    start_child(emqx_limiter_shared_sup, ChildSpec).

-spec delete_group(emqx_limiter:group()) -> ok | {error, term()}.
delete_group(Group) ->
    case supervisor:terminate_child(emqx_limiter_shared_sup, Group) of
        ok ->
            supervisor:delete_child(emqx_limiter_shared_sup, Group);
        {error, not_found} ->
            ok
    end.

-spec update_group(emqx_limiter:group(), [{emqx_limiter:name(), emqx_limiter:options()}]) ->
    ok | no_return().
update_group(Group, _LimiterConfigs) ->
    ok = emqx_limiter_allocator:update(Group).

-spec child_spec(emqx_limiter:group(), [emqx_limiter:name()]) ->
    supervisor:child_spec().
child_spec(Group, Names) ->
    #{
        id => Group,
        start => {emqx_limiter_allocator, start_link, [Group, Names]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [?MODULE, emqx_limiter_allocator]
    }.

-spec connect(emqx_limiter:id()) -> emqx_limiter_client:t().
connect({Group, Name}) ->
    emqx_limiter_client:new(
        ?MODULE,
        _State = {Group, Name}
    ).

%%--------------------------------------------------------------------
%% emqx_limiter_client callbacks
%%--------------------------------------------------------------------

-spec try_consume(client_state(), non_neg_integer()) -> boolean().
try_consume(LimiterId, Amount) ->
    Options = emqx_limiter_registry:get_limiter_options(LimiterId),
    case Options of
        #{capacity := infinity} ->
            true;
        _ ->
            Bucket = emqx_limiter_bucket_registry:find_bucket(LimiterId),
            case Bucket of
                undefined ->
                    %% Treat as unlimited
                    true;
                #{counter := Counter, index := Index} ->
                    {_AvailableTokens, Result} = try_consume(Counter, Index, Amount),
                    ?tp(limiter_shared_try_consume, #{
                        limiter_id => LimiterId,
                        amount => Amount,
                        available_tokens => _AvailableTokens,
                        result => Result
                    }),
                    Result
            end
    end.

-spec put_back(client_state(), non_neg_integer()) -> client_state().
put_back(LimiterId = State, Amount) ->
    case emqx_limiter_bucket_registry:find_bucket(LimiterId) of
        undefined ->
            State;
        #{counter := Counter, index := Index} ->
            ok = put_back(Counter, Index, Amount),
            State
    end.

%%--------------------------------------------------------------------
%%  Internal functions
%%--------------------------------------------------------------------

start_child(Sup, ChildSpec) ->
    case supervisor:start_child(Sup, ChildSpec) of
        {ok, _} ->
            ok;
        {error, _} = Error ->
            Error
    end.

try_consume(Counter, Index, Amount) ->
    case counters:get(Counter, Index) of
        AvailableTokens when AvailableTokens >= Amount ->
            counters:sub(Counter, Index, Amount),
            {AvailableTokens, true};
        AvailableTokens ->
            {AvailableTokens, false}
    end.

put_back(Counter, Index, Amount) ->
    counters:add(Counter, Index, Amount).
