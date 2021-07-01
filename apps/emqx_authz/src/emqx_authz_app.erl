%%%-------------------------------------------------------------------
%% @doc emqx_authz public API
%% @end
%%%-------------------------------------------------------------------

-module(emqx_authz_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqx_authz_sup:start_link(),
    %ok = emqx_authz:init(),
    {ok, Sup}.

stop(_State) ->
    ok.

%% internal functions
