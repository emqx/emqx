%%%-------------------------------------------------------------------
%% @doc emqx_ctl public API
%% @end
%%%-------------------------------------------------------------------

-module(emqx_ctl_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    emqx_ctl_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
