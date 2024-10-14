%%%-------------------------------------------------------------------
%% @doc alinkalarm public API
%% @end
%%%-------------------------------------------------------------------

-module(alinkalarm_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    {ok, Pid} = alinkalarm_sup:start_link(),
    alinkalarm_consumer:start(),
    {ok, Pid}.

stop(_State) ->
    alinkalarm_consumer:stop(),
    ok.

%% internal functions
