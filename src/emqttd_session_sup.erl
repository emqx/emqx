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

%% @doc emqttd session supervisor.
-module(emqttd_session_sup).

-behavior(supervisor).

-export([start_link/0, start_session/3]).

-export([init/1]).

%% @doc Start session supervisor
-spec(start_link() -> {ok, pid()}).
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @doc Start a session
-spec(start_session(boolean(), binary(), pid()) -> {ok, pid()}).
start_session(CleanSess, ClientId, ClientPid) ->
    supervisor:start_child(?MODULE, [CleanSess, ClientId, ClientPid]).

%%--------------------------------------------------------------------
%% Supervisor callbacks
%%--------------------------------------------------------------------

init([]) ->
    {ok, {{simple_one_for_one, 10, 10},
          [{session, {emqttd_session, start_link, []},
              temporary, 10000, worker, [emqttd_session]}]}}.

