%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_esockd_limiter).

-behaviour(esockd_generic_limiter).

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
        {false, LimiterClient} ->
            {pause, ?PAUSE_INTERVAL, State#{limiter_client := LimiterClient}}
    end.
