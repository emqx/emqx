-module(alinkdata_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    {ok, Sup} = alinkdata_sup:start_link(),
    alinkdata:start(),
    alinkdata_user:start(),
    {ok, Sup}.

stop(_State) ->
    ok.
