%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_topic_metrics_app).

-behaviour(application).

-export([
    start/2,
    stop/1
]).

start(_Type, _Args) ->
    ok = mria:wait_for_tables(emqx_topic_metrics_registry:create_tables()),
    {ok, Sup} = emqx_topic_metrics_sup:start_link(),
    ok = emqx_topic_metrics2:load(),
    {ok, Sup}.

stop(_State) ->
    ok = emqx_topic_metrics2:unload(),
    ok.
