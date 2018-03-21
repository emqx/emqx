%%--------------------------------------------------------------------
%% Copyright © 2013-2018 EMQ Inc. All rights reserved.
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

-module(emqx_broker_helper).

-behaviour(gen_server).

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {stats_fun, stats_timer}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec(start_link(fun()) -> {ok, pid()} | ignore | {error, any()}).
start_link(StatsFun) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [StatsFun], []).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([StatsFun]) ->
    {ok, TRef} = timer:send_interval(1000, stats),
    {ok, #state{stats_fun = StatsFun, stats_timer = TRef}}.

handle_call(Req, _From, State) ->
    emqx_log:error("[BrokerHelper] Unexpected request: ~p", [Req]),
    {reply, ignore, State}.

handle_cast(Msg, State) ->
    emqx_log:error("[BrokerHelper] Unexpected msg: ~p", [Msg]),
    {noreply, State}.

handle_info(stats, State = #state{stats_fun = StatsFun}) ->
    StatsFun(), {noreply, State, hibernate};

handle_info(Info, State) ->
    emqx_log:error("[BrokerHelper] Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #state{stats_timer = TRef}) ->
    timer:cancel(TRef).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

