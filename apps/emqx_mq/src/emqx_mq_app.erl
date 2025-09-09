%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    ok = mria:wait_for_tables(emqx_mq_registry:create_tables()),
    ok = emqx_mq_message_db:open(),
    ok = emqx_mq_state_storage:open_db(),
    {ok, Sup} = emqx_mq_sup:start_link(),
    ok = emqx_mq:register_hooks(),
    {ok, Sup}.

stop(_State) ->
    ok = emqx_mq:unregister_hooks(),
    ok = emqx_mq_message_db:close(),
    ok = emqx_mq_state_storage:close_db(),
    ok.
