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
%%% emqttd bridge supervisor.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_bridge_sup).

-author("Feng Lee <feng@emqtt.io>").

-behavior(supervisor).

-export([start_link/0,
         bridges/0,
         start_bridge/2, start_bridge/3,
         stop_bridge/2]).

-export([init/1]).

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc Start bridge supervisor
%% @end
%%------------------------------------------------------------------------------
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%TODO: bridges...
-spec bridges() -> [{tuple(), pid()}].
bridges() ->
    [{{Node, SubTopic}, Pid} || {{bridge, Node, SubTopic}, Pid, worker, _} 
                                <- supervisor:which_children(?MODULE)].

%%------------------------------------------------------------------------------
%% @doc Start a bridge
%% @end
%%------------------------------------------------------------------------------
-spec start_bridge(atom(), binary()) -> {ok, pid()} | {error, any()}.
start_bridge(Node, SubTopic) when is_atom(Node) and is_binary(SubTopic) ->
    start_bridge(Node, SubTopic, []).

-spec start_bridge(atom(), binary(), [emqttd_bridge:option()]) -> {ok, pid()} | {error, any()}.
start_bridge(Node, SubTopic, Options) when is_atom(Node) and is_binary(SubTopic) ->
    Options1 = emqttd_opts:merge(emqttd_broker:env(bridge), Options),
    supervisor:start_child(?MODULE, bridge_spec(Node, SubTopic, Options1)).

%%------------------------------------------------------------------------------
%% @doc Stop a bridge
%% @end
%%------------------------------------------------------------------------------
-spec stop_bridge(atom(), binary()) -> {ok, pid()} | ok.
stop_bridge(Node, SubTopic) ->
    ChildId = bridge_id(Node, SubTopic),
    case supervisor:terminate_child(ChildId) of
        ok -> 
            supervisor:delete_child(?MODULE, ChildId);
        {error, Reason} ->
            {error, Reason}
	end.

%%%=============================================================================
%%% Supervisor callbacks
%%%=============================================================================

init([]) ->
    {ok, {{one_for_one, 10, 100}, []}}.

bridge_id(Node, SubTopic) ->
    {bridge, Node, SubTopic}.

bridge_spec(Node, SubTopic, Options) ->
    ChildId = bridge_id(Node, SubTopic),
    {ChildId, {emqttd_bridge, start_link, [Node, SubTopic, Options]},
        transient, 10000, worker, [emqttd_bridge]}.

