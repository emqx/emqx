%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mgmt_app).

-behaviour(application).

-define(APP, emqx_management).

-export([
    start/2,
    stop/1
]).

-include("emqx_mgmt.hrl").

start(_Type, _Args) ->
    ok = mria:wait_for_tables(emqx_mgmt_auth:create_tables()),
    emqx_mgmt_auth:try_init_bootstrap_file(),
    emqx_conf:add_handler([api_key], emqx_mgmt_auth),
    emqx_mgmt_sup:start_link().

stop(_State) ->
    emqx_conf:remove_handler([api_key]),
    ok.
