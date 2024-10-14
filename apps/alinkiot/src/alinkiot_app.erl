%%%-------------------------------------------------------------------
%% @doc alinkiot public API
%% @end
%%%-------------------------------------------------------------------

-module(alinkiot_app).

-behaviour(application).

-emqx_plugin(alinkiot).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    {ok, Pid} = alinkiot_sup:start_link(),
    % alinkiot_action:load([]),
    alinkiot_plugins:start(),
    {ok, Pid}.

stop(_State) ->
    ok.

%% internal functions
