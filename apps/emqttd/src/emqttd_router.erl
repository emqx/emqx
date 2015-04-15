%%-----------------------------------------------------------------------------
%% Copyright (c) 2012-2015, Feng Lee <feng@emqtt.io>
%% 
%% Permission is hereby granted, free of charge, to any person obtaining a copy
%% of this software and associated documentation files (the "Software"), to deal
%% in the Software without restriction, including without limitation the rights
%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the Software is
%% furnished to do so, subject to the following conditions:
%% 
%% The above copyright notice and this permission notice shall be included in all
%% copies or substantial portions of the Software.
%% 
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%% SOFTWARE.
%%------------------------------------------------------------------------------

%%route chain... statistics
-module(emqttd_router).

-include_lib("emqtt/include/emqtt.hrl").
%-include("emqttd.hrl").

-behaviour(gen_server).

-define(SERVER, ?MODULE).

%% API Function Exports
-export([start_link/0]).

%%Router Chain--> --->In Out<---
-export([route/1]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {}).

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% Start emqttd router.
%%
%% @end
%%------------------------------------------------------------------------------
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%------------------------------------------------------------------------------
%% @doc
%% Route mqtt message.
%%
%% @end
%%------------------------------------------------------------------------------
-spec route(mqtt_message()) -> ok.
route(Msg) ->
    lager:debug("Route ~p", [emqtt_message:format(Msg)]),
    % TODO: need to retain?
    emqttd_retained:retain(Msg),
    % unset flag and pubsub
	emqttd_pubsub:publish(emqtt_message:unset_flag(Msg)).

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

init([]) ->
    {ok, #state{}, hibernate}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
	
%%%=============================================================================
%%% Internal functions
%%%=============================================================================

