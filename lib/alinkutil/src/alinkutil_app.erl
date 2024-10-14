%%%-------------------------------------------------------------------
%% @doc alinkutil public API
%% @end
%%%-------------------------------------------------------------------

-module(alinkutil_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    alinkutil_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
