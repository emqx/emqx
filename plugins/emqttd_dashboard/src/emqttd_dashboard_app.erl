-module(emqttd_dashboard_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqttd_dashboard_sup:start_link(),
    open_listener(application:get_env(listener)),
    {ok, Sup}.

stop(_State) ->
    ok.

%% open http port
open_listener({_Http, Port, Options}) ->
    MFArgs = {emqttd_dashboard, handle_request, []},
	mochiweb:start_http(Port, Options, MFArgs).

close_listener(Port) ->
    mochiweb:stop_http(Port).

