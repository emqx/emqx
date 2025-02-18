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

%% @doc This module implements the shared limiter.
%%
%% Shared limiter is a limiter that is shared between different processes.
%% I.e. different processes consume tokens concurrently.
%%
%% The shared limiter is a wrapper around a handle to a bucket managed
%% and refilled by the `emqx_limiter_allocator`.

-module(emqx_limiter_shared).

-behaviour(emqx_limiter_client).

-include("logger.hrl").

%% API
-export([
    create_group/2,
    update_group_configs/2,
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
    ok = register_group(Group, LimiterConfigs),
    {Names, _} = lists:unzip(LimiterConfigs),
    ChildSpec = child_spec(Group, Names),
    supervisor:start_child(emqx_limiter_shared_sup, ChildSpec).

-spec delete_group(emqx_limiter:group()) -> ok | {error, term()}.
delete_group(Group) ->
    ok =
        case supervisor:terminate_child(emqx_limiter_shared_sup, Group) of
            ok ->
                supervisor:delete_child(emqx_limiter_shared_sup, Group);
            {error, not_found} ->
                ok
        end,
    ok = unregister_group(Group).

-spec update_group_configs(emqx_limiter:group(), [{emqx_limiter:name(), emqx_limiter:options()}]) ->
    ok | no_return().
update_group_configs(Group, LimiterConfigs) ->
    case emqx_limiter_registry:find_group(Group) of
        undefined ->
            error({group_not_found, Group});
        {_Module, _OtherLimiterConfigs} ->
            ok = register_group(Group, LimiterConfigs),
            ok = emqx_limiter_allocator:update(Group)
    end.

-spec child_spec(emqx_limiter:group(), [emqx_limiter:name()]) ->
    emqx_child_spec:child_spec().
child_spec(Group, Names) ->
    #{
        id => Group,
        start => {emqx_limiter_allocator, start_link, [Group, Names]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [?MODULE]
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

-spec try_consume(client_state(), non_neg_integer()) -> {boolean(), client_state()}.
try_consume(LimiterId = _State, Amount) ->
    Options = emqx_limiter_registry:get_limiter_options(LimiterId),
    ?SLOG(warning, #{
        limiter_id => LimiterId,
        msg => "limiter_shared_try_consume",
        options => Options
    }),
    case Options of
        #{capacity := infinity} ->
            {true, LimiterId};
        _ ->
            Bucket = emqx_limiter_bucket_registry:find_bucket(LimiterId),
            ?SLOG(warning, #{
                limiter_id => LimiterId,
                msg => "limiter_shared_try_consume",
                bucket => Bucket,
                amount => Amount
            }),
            case Bucket of
                undefined ->
                    %% Treat as unlimited
                    {true, LimiterId};
                #{counter := Counter, index := Index} ->
                    Result = try_consume(Counter, Index, Amount),
                    {Result, LimiterId}
            end
    end.

-spec put_back(client_state(), non_neg_integer()) -> ok.
put_back({Group, Name}, Amount) ->
    case emqx_limiter_bucket_registry:find_bucket({Group, Name}) of
        undefined ->
            ok;
        #{counter := Counter, index := Index} ->
            put_back(Counter, Index, Amount)
    end.

%%--------------------------------------------------------------------
%%  Internal API
%%--------------------------------------------------------------------

register_group(Group, LimiterConfigs) ->
    emqx_limiter_registry:register_group(Group, ?MODULE, LimiterConfigs).

unregister_group(Group) ->
    emqx_limiter_registry:unregister_group(Group).

%%--------------------------------------------------------------------
%%  Internal functions
%%--------------------------------------------------------------------

try_consume(Counter, Index, Amount) ->
    case counters:get(Counter, Index) of
        AvailableTokens when AvailableTokens >= Amount ->
            counters:sub(Counter, Index, Amount),
            true;
        _ ->
            false
    end.

put_back(Counter, Index, Amount) ->
    counters:add(Counter, Index, Amount).
