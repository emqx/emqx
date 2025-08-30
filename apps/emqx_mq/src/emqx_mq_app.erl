%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    ok = mria:wait_for_tables(emqx_mq_registry:create_tables()),
    ok = emqx_variform:inject_allowed_module(emqx_mq_consumer_dispatch_bif),
    ok = emqx_mq_message_db:open(),
    ok = emqx_mq_consumer_db:open(),
    {ok, Sup} = emqx_mq_sup:start_link(),
    ok = emqx_mq:register_hooks(),
    {ok, Sup}.

stop(_State) ->
    ok = emqx_mq:unregister_hooks(),
    ok = emqx_variform:erase_allowed_module(emqx_mq_consumer_dispatch_bif),
    ok.
