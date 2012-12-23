-module(emqtt_sup).

-include("emqtt.hrl").

-behaviour(supervisor).

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link(Listeners) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [Listeners]).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([Listeners]) ->
    {ok, { {one_for_all, 5, 10}, [
		?CHILD(emqtt_auth, worker),
		?CHILD(emqtt_retained, worker),
		?CHILD(emqtt_router, worker),
		?CHILD(emqtt_registry, worker),
		?CHILD(emqtt_client_sup, supervisor)
		| listener_children(Listeners) ]}
	}.

listener_children(Listeners) ->
	lists:append([emqtt_listener:spec(Listener, 
		{emqtt_client_sup, start_client, []}) || Listener <- Listeners]).


