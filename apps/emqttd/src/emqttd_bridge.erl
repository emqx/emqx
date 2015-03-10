%%%-----------------------------------------------------------------------------
%%% @Copyright (C) 2012-2015, Feng Lee <feng@emqtt.io>
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
%%% emqttd bridge.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_bridge).

-author('feng@emqtt.io').

-behaviour(gen_server).

-include("emqttd_packet.hrl").

%% API Function Exports
-export([start_link/2]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {node, local_topic, status = running}).

%%%=============================================================================
%%% API
%%%=============================================================================

start_link(Node, LocalTopic) ->
    gen_server:start_link(?MODULE, [Node, LocalTopic], []).

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

init([Node, LocalTopic]) ->
    emqttd_pubsub:subscribe({LocalTopic, ?QOS_0}, self()),
    %%TODO: monitor nodes...
    {ok, #state{node = Node, local_topic = LocalTopic}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({nodedown, Node}, State = #state{node = Node}) ->
    %%....
    {noreply, State#state{status = down}};

handle_info({dispatch, {_From, Msg}}, State = #state{node = Node}) ->
    %%TODO: CAST
    rpc:call(Node, emqttd_router, route, [Msg]),
    {noreply, State};

handle_info(Info, State) ->
    lager:error("Unexpected Info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================



