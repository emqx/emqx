%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2012-2016 Feng Lee <feng@emqtt.io>. All Rights Reserved.
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
%%% @doc PubSub Helper.
%%%
%%% @author Feng Lee <feng@emqtt.io>
%%%-----------------------------------------------------------------------------
-module(emqttd_pubsub_helper).

-behaviour(gen_server).

-include("emqttd.hrl").

-include("emqttd_internal.hrl").

%% API Function Exports
-export([start_link/1]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {statsfun}).

-define(SERVER, ?MODULE).

%% @doc Start PubSub Helper.
-spec start_link(fun()) -> {ok, pid()} | ignore | {error, any()}.
start_link(StatsFun) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [StatsFun], []).

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

init([StatsFun]) ->
    mnesia:subscribe(system),
    {ok, #state{statsfun = StatsFun}}.

handle_call(Req, _From, State) ->
    ?UNEXPECTED_REQ(Req, State).

handle_cast(Msg, State) ->
    ?UNEXPECTED_MSG(Msg, State).

handle_info({mnesia_system_event, {mnesia_down, Node}}, State) ->
    %% TODO: mnesia master?
    Pattern = #mqtt_topic{_ = '_', node = Node},
    F = fun() ->
            [mnesia:delete_object(topic, R, write) ||
                R <- mnesia:match_object(topic, Pattern, write)]
        end,
    mnesia:transaction(F), noreply(State);

handle_info(Info, State) ->
    ?UNEXPECTED_INFO(Info, State).

terminate(_Reason, _State) ->
    mnesia:unsubscribe(system).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

noreply(State = #state{statsfun = StatsFun}) ->
    StatsFun(topic), {noreply, State}.

