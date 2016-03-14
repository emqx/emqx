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

%% @doc emqttd top supervisor.
-module(emqttd_sup).

-behaviour(supervisor).

-include("emqttd.hrl").

%% API
-export([start_link/0, start_child/1, start_child/2]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(Mod, Type), {Mod, {Mod, start_link, []},
                                permanent, 5000, Type, [Mod]}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_child(ChildSpec) when is_tuple(ChildSpec) ->
    supervisor:start_child(?MODULE, ChildSpec).

-spec(start_child(Mod::atom(), Type :: worker | supervisor) -> {ok, pid()}).
start_child(Mod, Type) when is_atom(Mod) and is_atom(Type) ->
    supervisor:start_child(?MODULE, ?CHILD(Mod, Type)).

%%--------------------------------------------------------------------
%% Supervisor callbacks
%%--------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_all, 10, 3600}, []}}.

