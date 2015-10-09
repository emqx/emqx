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

%% TODO: Monitor mnesia node down...

-module(emqttd_sm_helper).

-author("Feng Lee <feng@emqtt.io>").

-include("emqttd.hrl").

%% API Function Exports
-export([start_link/0]).

-behaviour(gen_server).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {statsfun, ticker}).

%%------------------------------------------------------------------------------
%% @doc Start a session helper
%% @end
%%------------------------------------------------------------------------------
-spec start_link() -> {ok, pid()} | ignore | {error, any()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    StatsFun = emqttd_stats:statsfun('sessions/count', 'sessions/max'),
    {ok, TRef} = timer:send_interval(1000, self(), tick),
    {ok, #state{statsfun = StatsFun, ticker = TRef}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(Msg, State) ->
    lager:critical("Unexpected Msg: ~p", [Msg]),
    {noreply, State}.

handle_info(tick, State) ->
    {noreply, setstats(State)};

handle_info(Info, State) ->
    lager:critical("Unexpected Info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State = #state{ticker = TRef}) ->
    timer:cancel(TRef).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

setstats(State = #state{statsfun = StatsFun}) ->
    StatsFun(ets:info(mqtt_persistent_session, size)), State.


