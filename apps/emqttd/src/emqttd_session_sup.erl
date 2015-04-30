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
%%% emqttd session supervisor.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_session_sup).

-author("Feng Lee <feng@emqtt.io>").

-behavior(supervisor).

-export([start_link/0, start_session/2]).

-export([init/1]).

%%------------------------------------------------------------------------------
%% @doc Start session supervisor
%% @end
%%------------------------------------------------------------------------------
-spec start_link() -> {ok, pid()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%------------------------------------------------------------------------------
%% @doc Start a session
%% @end
%%------------------------------------------------------------------------------
-spec start_session(binary(), pid()) -> {ok, pid()}.
start_session(ClientId, ClientPid) ->
    supervisor:start_child(?MODULE, [ClientId, ClientPid]).

%%%=============================================================================
%%% Supervisor callbacks
%%%=============================================================================

init([]) ->
    {ok, {{simple_one_for_one, 10, 10},
          [{session, {emqttd_session, start_link, []},
              transient, 10000, worker, [emqttd_session]}]}}.

