
-module(emqtt_app).

-include("emqtt.hrl").

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
	{ok, Listeners} = application:get_env(listeners),
    emqtt_sup:start_link(Listeners).

stop(_State) ->
    ok.

