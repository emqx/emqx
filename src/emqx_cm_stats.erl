%%--------------------------------------------------------------------
%% Copyright (c) 2013-2018 EMQ Inc. All Rights Reserved.
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

-module(emqx_cm_stats).

-behaviour(gen_statem).

-include("emqx.hrl").

%% API
-export([start_link/0]).

-export([set_client_stats/2, get_client_stats/1, del_client_stats/1]).

%% gen_statem callbacks
-export([init/1, callback_mode/0, handle_event/4, terminate/3, code_change/4]).

-define(TAB, client_stats).

-record(state, {statsfun}).

start_link() ->
    gen_statem:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec(set_client_stats(client_id(), emqx_stats:stats()) -> true).
set_client_stats(ClientId, Stats) ->
    ets:insert(?TAB, {ClientId, [{'$ts', emqx_time:now_secs()}|Stats]}).

-spec(get_client_stats(client_id()) -> emqx_stats:stats()).
get_client_stats(ClientId) ->
    case ets:lookup(?TAB, ClientId) of
        [{_, Stats}] -> Stats;
        [] -> []
    end.

-spec(del_client_stats(client_id()) -> true).
del_client_stats(ClientId) ->
    ets:delete(?TAB, ClientId).

init([]) ->
    _ = emqx_tables:create(?TAB, [public, {write_concurrency, true}]),
    StatsFun = emqx_stats:statsfun('clients/count', 'clients/max'),
    {ok, idle, #state{statsfun = StatsFun}, timer:seconds(1)}.

callback_mode() -> handle_event_function.

handle_event(timeout, _Timeout, idle, State = #state{statsfun = StatsFun}) ->
    case ets:info(client, size) of
        undefined -> ok;
        Size      -> StatsFun(Size)
    end,
    {next_state, idle, State, timer:seconds(1)}.

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

