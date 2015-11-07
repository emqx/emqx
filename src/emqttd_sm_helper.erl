%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2012-2015 eMQTT.IO, All Rights Reserved.
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a copy
%%% of this software and associated documentation files (the "Software"), to deal
%%% in the Software without restriction, including without limitation the rights
%%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%%% copies of the Software, and to permit persons to whom the Software is
%%% furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in all
%%% copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%%% SOFTWARE.
%%%-----------------------------------------------------------------------------
%%% @doc
%%% emqttd session helper.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_sm_helper).

-author("Feng Lee <feng@emqtt.io>").

-include("emqttd.hrl").

-include_lib("stdlib/include/ms_transform.hrl").

%% API Function Exports
-export([start_link/0]).

-behaviour(gen_server).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {stats_fun, tick_tref}).

%%------------------------------------------------------------------------------
%% @doc Start a session helper
%% @end
%%------------------------------------------------------------------------------
-spec start_link() -> {ok, pid()} | ignore | {error, any()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    mnesia:subscribe(system),
    {ok, TRef} = timer:send_interval(timer:seconds(1), tick),
    StatsFun = emqttd_stats:statsfun('sessions/count', 'sessions/max'),
    {ok, #state{stats_fun = StatsFun, tick_tref = TRef}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(Msg, State) ->
    lager:error("Unexpected Msg: ~p", [Msg]),
    {noreply, State}.

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
    lager:error("Unexpected Info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State = #state{tick_tref = TRef}) ->
    timer:cancel(TRef),
    mnesia:unsubscribe(system).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

setstats(State = #state{stats_fun = StatsFun}) ->
    StatsFun(ets:info(mqtt_persistent_session, size)), State.

