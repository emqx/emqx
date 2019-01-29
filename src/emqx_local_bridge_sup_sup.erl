%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_local_bridge_sup_sup).

-behavior(supervisor).

-include("emqx.hrl").

-export([start_link/0, bridges/0]).
-export([start_bridge/2, start_bridge/3, stop_bridge/2]).

%% Supervisor callbacks
-export([init/1]).

-define(CHILD_ID(Node, Topic), {bridge_sup, Node, Topic}).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @doc List all bridges
-spec(bridges() -> [{node(), emqx_topic:topic(), pid()}]).
bridges() ->
    [{Node, Topic, Pid} || {?CHILD_ID(Node, Topic), Pid, supervisor, _}
                           <- supervisor:which_children(?MODULE)].

%% @doc Start a bridge
-spec(start_bridge(node(), emqx_topic:topic()) -> {ok, pid()} | {error, term()}).
start_bridge(Node, Topic) when is_atom(Node), is_binary(Topic) ->
    start_bridge(Node, Topic, []).

-spec(start_bridge(node(), emqx_topic:topic(), [emqx_bridge:option()])
      -> {ok, pid()} | {error, term()}).
start_bridge(Node, _Topic, _Options) when Node =:= node() ->
    {error, bridge_to_self};
start_bridge(Node, Topic, Options) when is_atom(Node), is_binary(Topic) ->
    Options1 = emqx_misc:merge_opts(emqx_config:get_env(bridge, []), Options),
    supervisor:start_child(?MODULE, bridge_spec(Node, Topic, Options1)).

%% @doc Stop a bridge
-spec(stop_bridge(node(), emqx_topic:topic()) -> ok | {error, term()}).
stop_bridge(Node, Topic) when is_atom(Node), is_binary(Topic) ->
    ChildId = ?CHILD_ID(Node, Topic),
    case supervisor:terminate_child(?MODULE, ChildId) of
        ok    -> supervisor:delete_child(?MODULE, ChildId);
        Error -> Error
    end.

%%------------------------------------------------------------------------------
%% Supervisor callbacks
%%------------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_one, 10, 3600}, []}}.

bridge_spec(Node, Topic, Options) ->
    #{id       => ?CHILD_ID(Node, Topic),
      start    => {emqx_local_bridge_sup, start_link, [Node, Topic, Options]},
      restart  => permanent,
      shutdown => infinity,
      type     => supervisor,
      modules  => [emqx_local_bridge_sup]}.

