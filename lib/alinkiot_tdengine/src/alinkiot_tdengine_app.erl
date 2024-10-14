%%%-------------------------------------------------------------------
%% @end
%%%-------------------------------------------------------------------

-module(alinkiot_tdengine_app).
-include("alinkiot_tdengine.hrl").

-emqx_plugin(?MODULE).
-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%%====================================================================
%% API
%%====================================================================

start(_StartType, _StartArgs) ->
    alinkiot_tdengine_rest:start(),
    {ok, Sup} = alinkiot_tdengine_sup:start_link(),
    alinkiot_tdengine:init_alink(),
    alinkiot_tdengine_sync:sync(),
    alinkiot_tdengine_subscribe:load(),
    alinkiot_tdengine_worker:start_workers(),
    alinkiot_tdengine_metrics_worker:start_workers(),
    {ok, Sup}.

%%--------------------------------------------------------------------
stop(_State) ->
    alinkiot_tdengine_subscribe:unload(),
    alinkiot_tdengine_worker:stop_workers(),
    ok.

%%====================================================================
%% Internal functions
%%====================================================================