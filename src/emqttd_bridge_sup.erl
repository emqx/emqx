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

-module(emqttd_bridge_sup).

-behavior(supervisor).

-export([start_link/3]).

-export([init/1]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% @doc Start bridge supervisor
-spec(start_link(atom(), binary(), [emqttd_bridge:option()]) -> {ok, pid()} | {error, any()}).
start_link(Node, Topic, Options) ->
    supervisor:start_link(?MODULE, [Node, Topic, Options]).

%%--------------------------------------------------------------------
%% Supervisor callbacks
%%--------------------------------------------------------------------

init([Node, Topic, Options]) ->
    {ok, {{one_for_all, 10, 100},
          [{bridge, {emqttd_bridge, start_link, [Node, Topic, Options]},
            transient, 10000, worker, [emqttd_bridge]}]}}.

