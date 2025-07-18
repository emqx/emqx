%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    ok = emqx_mq_db:open(),
    {ok, Sup} = emqx_mq_sup:start_link(),
    ok = emqx_mq:register_hooks(),
    {ok, Sup}.

stop(_State) ->
    ok = emqx_mq:unregister_hooks(),
    ok.
