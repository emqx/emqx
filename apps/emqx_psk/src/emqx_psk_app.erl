%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_psk_app).

-behaviour(application).

-export([
    start/2,
    stop/1
]).

-include("emqx_psk.hrl").

start(_Type, _Args) ->
    ok = mria:wait_for_tables(emqx_psk:create_tables()),
    emqx_conf:add_handler([?PSK_KEY], emqx_psk),
    {ok, Sup} = emqx_psk_sup:start_link(),
    {ok, Sup}.

stop(_State) ->
    ok.
