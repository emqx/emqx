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

-export([start_link/0, bridges/0, start_bridge/2, start_bridge/3, stop_bridge/2]).

-export([init/1]).

-define(BRIDGE_ID(Node, Topic), {bridge, Node, Topic}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% @doc Start bridge supervisor
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @doc List all bridges
-spec(bridges() -> [{tuple(), pid()}]).
bridges() ->
    [{{Node, Topic}, Pid} || {?BRIDGE_ID(Node, Topic), Pid, worker, _}
                             <- supervisor:which_children(?MODULE)].

%% @doc Start a bridge
-spec(start_bridge(atom(), binary()) -> {ok, pid()} | {error, any()}).
start_bridge(Node, Topic) when is_atom(Node) andalso is_binary(Topic) ->
    start_bridge(Node, Topic, []).

-spec(start_bridge(atom(), binary(), [emqttd_bridge:option()]) -> {ok, pid()} | {error, any()}).
start_bridge(Node, _Topic, _Options) when Node =:= node() ->
    {error, bridge_to_self};
start_bridge(Node, Topic, Options) when is_atom(Node) andalso is_binary(Topic) ->
    Options1 = emqttd_opts:merge(emqttd_broker:env(bridge), Options),
    supervisor:start_child(?MODULE, bridge_spec(Node, Topic, Options1)).

%% @doc Stop a bridge
-spec(stop_bridge(atom(), binary()) -> {ok, pid()} | ok).
stop_bridge(Node, Topic) when is_atom(Node) andalso is_binary(Topic) ->
    ChildId = ?BRIDGE_ID(Node, Topic),
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
    ChildId = ?BRIDGE_ID(Node, Topic),
    {ChildId, {emqttd_bridge, start_link, [Node, Topic, Options]},
        transient, 10000, worker, [emqttd_bridge]}.

