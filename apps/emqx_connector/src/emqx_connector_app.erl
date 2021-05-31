%%%-------------------------------------------------------------------
%% @doc emqx_connector public API
%% @end
%%%-------------------------------------------------------------------

-module(emqx_connector_app).

-behaviour(application).

-emqx_plugin(?MODULE).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    emqx_connector_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
