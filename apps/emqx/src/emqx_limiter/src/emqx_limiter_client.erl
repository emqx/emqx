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

-module(emqx_limiter_client).

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

-export_type([t/0, state/0]).

%%--------------------------------------------------------------------
%% Callbacks
%%--------------------------------------------------------------------

-callback try_consume(state(), non_neg_integer()) -> {boolean(), state()} | boolean().

-callback put_back(state(), non_neg_integer()) -> state().

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec new(module(), state()) -> t().
new(Module, State) ->
    #{module => Module, state => State}.

-spec try_consume(t(), non_neg_integer()) -> {boolean(), t()}.
try_consume(#{module := Module, state := State} = Limiter, Amount) ->
    case Module:try_consume(State, Amount) of
        {Result, NewState} when is_boolean(Result) ->
            {Result, Limiter#{state := NewState}};
        Result when is_boolean(Result) ->
            {Result, Limiter}
    end.

-spec put_back(t(), non_neg_integer()) -> t().
put_back(#{module := Module, state := State} = Limiter, Amount) ->
    NewState = Module:put_back(State, Amount),
    Limiter#{state := NewState}.
