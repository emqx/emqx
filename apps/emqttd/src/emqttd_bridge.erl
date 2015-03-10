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

-define(PING_INTERVAL, 1000).

-record(state, {node, local_topic, status = up}).

%%%=============================================================================
%%% API
%%%=============================================================================

start_link(Node, LocalTopic) ->
    gen_server:start_link(?MODULE, [Node, LocalTopic], []).

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

init([Node, LocalTopic]) ->
    process_flag(trap_exit, true),
    case net_kernel:connect_node(Node) of
        true -> 
            true = erlang:monitor_node(Node, true),
            emqttd_pubsub:subscribe({LocalTopic, ?QOS_0}, self()),
            {ok, #state{node = Node, local_topic = LocalTopic}};
        false -> 
            {stop, {cannot_connect, Node}}
    end.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({dispatch, {_From, Msg}}, State = #state{node = Node, status = down}) ->
    lager:warning("Bridge Dropped Msg for ~p Down:~n~p", [Node, Msg]),
    {noreply, State};

handle_info({dispatch, {_From, Msg}}, State = #state{node = Node, status = up}) ->
    rpc:cast(Node, emqttd_router, route, [Msg]),
    {noreply, State};

handle_info({nodedown, Node}, State = #state{node = Node}) ->
    lager:warning("Bridge Node Down: ~p", [Node]),
    erlang:send_after(?PING_INTERVAL, self(), ping_down_node),
    {noreply, State#state{status = down}};

handle_info({nodeup, Node}, State = #state{node = Node}) ->
    %% TODO: Really fast??
    case emqttd:is_running(Node) of
        true -> 
            lager:warning("Bridge Node Up: ~p", [Node]),
            {noreply, State#state{status = up}};
        false ->
            self() ! {nodedown, Node},
            {noreply, State#state{status = down}}
    end;

handle_info(ping_down_node, State = #state{node = Node}) ->
    Self = self(),
    spawn_link(fun() ->
                     case net_kernel:connect_node(Node) of
                         true -> %%TODO: this is not right... fixme later
                             Self ! {nodeup, Node};
                         false ->
                             erlang:send_after(?PING_INTERVAL, Self, ping_down_node)
                     end
               end),
    {noreply, State};

handle_info({'EXIT', _Pid, normal}, State) ->
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

