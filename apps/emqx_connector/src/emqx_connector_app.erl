%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_connector_app).

-behaviour(application).

-export([start/2, stop/1]).

-define(TOP_LELVE_HDLR_PATH, (emqx_connector:config_key_path())).
-define(LEAF_NODE_HDLR_PATH, (emqx_connector:config_key_path() ++ ['?', '?'])).

start(_StartType, _StartArgs) ->
    ok = emqx_connector:load(),
    ok = emqx_config_handler:add_handler(?TOP_LELVE_HDLR_PATH, emqx_connector),
    ok = emqx_config_handler:add_handler(?LEAF_NODE_HDLR_PATH, emqx_connector),
    emqx_connector_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
