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

-module(emqx_sm_helper).

-behaviour(gen_server).

-include("emqx.hrl").

-include_lib("stdlib/include/ms_transform.hrl").

%% API Function Exports
-export([start_link/1]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {stats_fun, ticker}).

-define(LOCK, {?MODULE, clean_sessions}).

%% @doc Start a session helper
-spec(start_link(fun()) -> {ok, pid()} | ignore | {error, term()}).
start_link(StatsFun) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [StatsFun], []).

init([StatsFun]) ->
    ekka:monitor(membership),
    {ok, TRef} = timer:send_interval(timer:seconds(1), tick),
    {ok, #state{stats_fun = StatsFun, ticker = TRef}}.

handle_call(Req, _From, State) ->
    lager:error("[SM-HELPER] Unexpected Call: ~p", [Req]),
    {reply, ignore, State}.

handle_cast(Msg, State) ->
    lager:error("[SM-HELPER] Unexpected Cast: ~p", [Msg]),
    {noreply, State}.

handle_info({membership, {mnesia, down, Node}}, State) ->
    Fun = fun() ->
            ClientIds =
            mnesia:select(mqtt_session, [{#mqtt_session{client_id = '$1', sess_pid = '$2', _ = '_'},
                                         [{'==', {node, '$2'}, Node}], ['$1']}]),
            lists:foreach(fun(ClientId) -> mnesia:delete({mqtt_session, ClientId}) end, ClientIds)
          end,
    global:trans({?LOCK, self()}, fun() -> mnesia:async_dirty(Fun) end),
    {noreply, State, hibernate};

handle_info({membership, _Event}, State) ->
    {noreply, State};

handle_info(tick, State) ->
    {noreply, setstats(State), hibernate};

handle_info(Info, State) ->
    lager:error("[SM-HELPER] Unexpected Info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State = #state{ticker = TRef}) ->
    timer:cancel(TRef),
    ekka:unmonitor(membership).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

setstats(State = #state{stats_fun = StatsFun}) ->
    StatsFun(ets:info(mqtt_local_session, size)), State.

