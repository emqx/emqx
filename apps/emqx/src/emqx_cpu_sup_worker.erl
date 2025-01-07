%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_cpu_sup_worker).

-behaviour(gen_server).

-include("logger.hrl").

%% gen_server APIs
-export([start_link/0]).

-export([
    cpu_util/0,
    cpu_util/1
]).

%% gen_server callbacks
-export([
    init/1,
    handle_continue/2,
    handle_call/3,
    handle_cast/2,
    terminate/2,
    code_change/3
]).

-define(CPU_USAGE_WORKER, ?MODULE).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

cpu_util() ->
    gen_server:call(?CPU_USAGE_WORKER, ?FUNCTION_NAME, infinity).

cpu_util(Args) ->
    gen_server:call(?CPU_USAGE_WORKER, {?FUNCTION_NAME, Args}, infinity).

%%--------------------------------------------------------------------
%% gen_server callbacks
%% simply handle cpu_sup:util/0,1 called in one process
%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?CPU_USAGE_WORKER}, ?MODULE, [], []).

init([]) ->
    {ok, undefined, {continue, setup}}.

handle_continue(setup, undefined) ->
    %% start os_mon temporarily
    {ok, _} = application:ensure_all_started(os_mon),
    %% The returned value of the first call to cpu_sup:util/0 or cpu_sup:util/1 by a
    %% process will on most systems be the CPU utilization since system boot,
    %% but this is not guaranteed and the value should therefore be regarded as garbage.
    %% This also applies to the first call after a restart of cpu_sup.
    _Val = cpu_sup:util(),
    {noreply, #{}}.

handle_call(cpu_util, _From, State) ->
    Val = cpu_sup:util(),
    {reply, Val, State};
handle_call({cpu_util, Args}, _From, State) ->
    Val = erlang:apply(cpu_sup, util, Args),
    {reply, Val, State};
handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Msg}),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
