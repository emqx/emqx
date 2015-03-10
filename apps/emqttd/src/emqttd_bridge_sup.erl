%%%-----------------------------------------------------------------------------
%%% @Copyright (C) 2012-2015, Feng Lee <feng@emqtt.io>
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

-author('feng@emqtt.io').

-behavior(supervisor).

-export([start_link/0, start_bridge/2, stop_bridge/2]).

-export([init/1]).

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% Start bridge supervisor.
%%
%% @end
%%------------------------------------------------------------------------------
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%------------------------------------------------------------------------------
%% @doc
%% Start a bridge.
%%
%% @end
%%------------------------------------------------------------------------------
-spec start_bridge(atom(), binary()) -> {ok, pid()} | {error, any()}.
start_bridge(Node, LocalTopic) when is_atom(Node) and is_binary(LocalTopic) ->
    %%TODO: mv this code to emqttd_bridge???
    case net_kernel:connect_node(Node) of
        true -> 
            supervisor:start_child(?MODULE, bridge_spec(Node, LocalTopic));
        false -> 
            {error, {cannot_connect, Node}}
    end.

%%------------------------------------------------------------------------------
%% @doc
%% Stop a bridge.
%%
%% @end
%%------------------------------------------------------------------------------
-spec stop_bridge(atom(), binary()) -> {ok, pid()} | ok.
stop_bridge(Node, LocalTopic) ->
    ChildId = bridge_id(Node, LocalTopic),
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

bridge_id(Node, LocalTopic) ->
    {bridge, Node, LocalTopic}.

bridge_spec(Node, LocalTopic) ->
    ChildId = bridge_id(Node, LocalTopic),
    {ChildId, {emqttd_bridge, start_link, [Node, LocalTopic]},
        transient, 10000, worker, [emqttd_bridge]}.


