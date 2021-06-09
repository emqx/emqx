%%%-------------------------------------------------------------------
%% @doc emqx_authorization public API
%% @end
%%%-------------------------------------------------------------------

-module(emqx_authorization_app).

-behaviour(application).

-export([start/2, stop/1]).

-emqx_plugin(?MODULE).

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqx_authorization_sup:start_link(),
    ok = emqx_authorization:init(),
    {ok, Sup}.

stop(_State) ->
    ok.

%% internal functions
