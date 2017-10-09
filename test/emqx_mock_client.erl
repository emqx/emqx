%%--------------------------------------------------------------------
%% Copyright (c) 2013-2017 EMQ Enterprise, Inc. (http://emqtt.io)
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

-module(emqx_mock_client).

-behaviour(gen_server).

-export([start_link/1, start_session/1, stop/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {clientid, session}).

start_link(ClientId) ->
    gen_server:start_link(?MODULE, [ClientId], []).

start_session(CPid) ->
    gen_server:call(CPid, start_session).

stop(CPid) ->
    gen_server:call(CPid, stop).

init([ClientId]) ->
    {ok, #state{clientid = ClientId}}.

handle_call(start_session, _From, State = #state{clientid = ClientId}) ->
    {ok, SessPid, _} = emqx_sm:start_session(true, {ClientId, undefined}),
    {reply, {ok, SessPid}, State#state{session = SessPid}};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

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

