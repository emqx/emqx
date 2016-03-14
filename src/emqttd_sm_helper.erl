%%--------------------------------------------------------------------
%% Copyright (c) 2012-2016 Feng Lee <feng@emqtt.io>.
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

%% @doc Session Helper.
-module(emqttd_sm_helper).

-behaviour(gen_server).

-include("emqttd.hrl").

-include("emqttd_internal.hrl").

-include_lib("stdlib/include/ms_transform.hrl").

%% API Function Exports
-export([start_link/1]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {stats_fun, tick_tref}).

%% @doc Start a session helper
-spec(start_link(fun()) -> {ok, pid()} | ignore | {error, any()}).
start_link(StatsFun) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [StatsFun], []).

init([StatsFun]) ->
    mnesia:subscribe(system),
    {ok, TRef} = timer:send_interval(timer:seconds(1), tick),
    {ok, #state{stats_fun = StatsFun, tick_tref = TRef}}.

handle_call(Req, _From, State) ->
    ?UNEXPECTED_REQ(Req, State).

handle_cast(Msg, State) ->
    ?UNEXPECTED_MSG(Msg, State).

handle_info({mnesia_system_event, {mnesia_down, Node}}, State) ->
    lager:error("!!!Mnesia node down: ~s", [Node]),
    Fun = fun() ->
            ClientIds =
            mnesia:select(session, [{#mqtt_session{client_id = '$1', sess_pid = '$2', _ = '_'},
                                    [{'==', {node, '$2'}, Node}],
                                    ['$1']}]),
            lists:foreach(fun(ClientId) -> mnesia:delete({session, ClientId}) end, ClientIds)
          end,
    mnesia:async_dirty(Fun),
    {noreply, State};

handle_info({mnesia_system_event, {mnesia_up, _Node}}, State) ->
    {noreply, State};

handle_info(tick, State) ->
    {noreply, setstats(State), hibernate};

handle_info(Info, State) ->
    ?UNEXPECTED_INFO(Info, State).

terminate(_Reason, _State = #state{tick_tref = TRef}) ->
    timer:cancel(TRef),
    mnesia:unsubscribe(system).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

setstats(State = #state{stats_fun = StatsFun}) ->
    StatsFun(ets:info(mqtt_persistent_session, size)), State.

