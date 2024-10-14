%%%-------------------------------------------------------------------
%% @doc alinkformula public API
%% @end
%%%-------------------------------------------------------------------

-module(alinkformula_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    alinkformula_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
