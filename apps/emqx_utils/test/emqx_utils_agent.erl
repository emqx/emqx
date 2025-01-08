%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

%% @doc Similar to Elixir's [`Agent'](https://hexdocs.pm/elixir/Agent.html).

-module(emqx_utils_agent).

%% API
-export([start_link/1, get/1, get_and_update/2]).

%% `gen_server' API
-export([init/1, handle_call/3]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-type state() :: term().

-type get_and_update_fn() :: fun((state()) -> {term(), state()}).

-record(get_and_update, {fn :: get_and_update_fn()}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec start_link(state()) -> gen_server:start_ret().
start_link(InitState) ->
    gen_server:start_link(?MODULE, InitState, []).

-spec get(gen_server:server_ref()) -> term().
get(ServerRef) ->
    Fn = fun(St) -> {St, St} end,
    gen_server:call(ServerRef, #get_and_update{fn = Fn}).

-spec get_and_update(gen_server:server_ref(), get_and_update_fn()) -> term().
get_and_update(ServerRef, Fn) ->
    gen_server:call(ServerRef, #get_and_update{fn = Fn}).

%%------------------------------------------------------------------------------
%% `gen_server' API
%%------------------------------------------------------------------------------

init(InitState) ->
    {ok, InitState}.

handle_call(#get_and_update{fn = Fn}, _From, State0) ->
    {Reply, State} = Fn(State0),
    {reply, Reply, State}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------
