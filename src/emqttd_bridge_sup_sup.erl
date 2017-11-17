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

-module(emqttd_bridge_sup_sup).

-behavior(supervisor).

-author("Feng Lee <feng@emqtt.io>").

-export([start_link/0, bridges/0, start_bridge/2, start_bridge/3, stop_bridge/2]).

-export([init/1]).

-define(CHILD_ID(Node, Topic), {bridge_sup, Node, Topic}).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% @doc List all bridges
-spec(bridges() -> [{node(), binary(), pid()}]).
bridges() ->
    [{Node, Topic, Pid} || {?CHILD_ID(Node, Topic), Pid, supervisor, _}
                             <- supervisor:which_children(?MODULE)].

%% @doc Start a bridge
-spec(start_bridge(atom(), binary()) -> {ok, pid()} | {error, term()}).
start_bridge(Node, Topic) when is_atom(Node) andalso is_binary(Topic) ->
    start_bridge(Node, Topic, []).

-spec(start_bridge(atom(), binary(), [emqttd_bridge:option()]) -> {ok, pid()} | {error, term()}).
start_bridge(Node, _Topic, _Options) when Node =:= node() ->
    {error, bridge_to_self};
start_bridge(Node, Topic, Options) when is_atom(Node) andalso is_binary(Topic) ->
    {ok, BridgeEnv} = emqttd:env(bridge),
    Options1 = emqttd_misc:merge_opts(BridgeEnv, Options),
    supervisor:start_child(?MODULE, bridge_spec(Node, Topic, Options1)).

%% @doc Stop a bridge
-spec(stop_bridge(atom(), binary()) -> {ok, pid()} | ok).
stop_bridge(Node, Topic) when is_atom(Node) andalso is_binary(Topic) ->
    ChildId = ?CHILD_ID(Node, Topic),
    case supervisor:terminate_child(?MODULE, ChildId) of
        ok    -> supervisor:delete_child(?MODULE, ChildId);
        Error -> Error
    end.

%%--------------------------------------------------------------------
%% Supervisor callbacks
%%--------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_one, 10, 100}, []}}.

bridge_spec(Node, Topic, Options) ->
    {?CHILD_ID(Node, Topic),
      {emqttd_bridge_sup, start_link, [Node, Topic, Options]},
        permanent, infinity, supervisor, [emqttd_bridge_sup]}.

