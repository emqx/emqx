%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_broker_helper).

-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% internal export
-export([stats_fun/0]).

-define(HELPER, ?MODULE).

-record(state, {}).

-spec(start_link() -> {ok, pid()} | ignore | {error, any()}).
start_link() ->
    gen_server:start_link({local, ?HELPER}, ?MODULE, [], []).

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

init([]) ->
    %% Use M:F/A for callback, not anonymous function because
    %% fun M:F/A is small, also no badfun risk during hot beam reload
    emqx_stats:update_interval(broker_stats, fun ?MODULE:stats_fun/0),
    {ok, #state{}, hibernate}.

handle_call(Req, _From, State) ->
    emqx_logger:error("[BrokerHelper] unexpected call: ~p", [Req]),
   {reply, ignored, State}.

handle_cast(Msg, State) ->
    emqx_logger:error("[BrokerHelper] unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info(Info, State) ->
    emqx_logger:error("[BrokerHelper] unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #state{}) ->
    emqx_stats:cancel_update(broker_stats).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

stats_fun() ->
    safe_update_stats(emqx_subscriber,
                      'subscribers/count', 'subscribers/max'),
    safe_update_stats(emqx_subscription,
                      'subscriptions/count', 'subscriptions/max'),
    safe_update_stats(emqx_suboptions,
                      'suboptions/count', 'suboptions/max').

safe_update_stats(Tab, Stat, MaxStat) ->
    case ets:info(Tab, size) of
        undefined -> ok;
        Size -> emqx_stats:setstat(Stat, MaxStat, Size)
    end.

