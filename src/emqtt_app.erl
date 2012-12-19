-module(emqtt_app).

-export([start/0]).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

-define(APPS, [sasl, mnesia, emqtt]).

start() ->
	[start_app(App) || App <- ?APPS].

start_app(mnesia) ->
	mnesia:create_schema([node()]),	
	mnesia:start();

start_app(App) ->
	application:start(App).


%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    emqtt_sup:start_link().

stop(_State) ->
    ok.

