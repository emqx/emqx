%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is eMQTT
%%
%% The Initial Developer of the Original Code is <ery.lee at gmail dot com>
%% Copyright (C) 2012 Ery Lee All Rights Reserved.

-module(emqtt_sup).

-include("emqtt.hrl").

-behaviour(supervisor).

%% API
-export([start_link/1,
		start_child/1,
		start_child/2]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================
start_link(Listeners) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [Listeners]).


start_child(ChildSpec) when is_tuple(ChildSpec) ->
	supervisor:start_child(?MODULE, ChildSpec).

%%
%% start_child(Mod::atom(), Type::type()) -> {ok, pid()}
%% @type type() = worker | supervisor
%%
start_child(Mod, Type) when is_atom(Mod) and is_atom(Type) ->
	supervisor:start_child(?MODULE, ?CHILD(Mod, Type)).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([Listeners]) ->
    {ok, { {one_for_all, 5, 10}, [
		?CHILD(emqtt_auth, worker),
		?CHILD(emqtt_retained, worker),
		?CHILD(emqtt_router, worker),
		?CHILD(emqtt_registry, worker),
		?CHILD(emqtt_client_monitor, worker),
		?CHILD(emqtt_client_sup, supervisor)
		| listener_children(Listeners) ]}
	}.

listener_children(Listeners) ->
	lists:append([emqtt_listener:spec(Listener, 
		{emqtt_client_sup, start_client, []}) || Listener <- Listeners]).


