%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_es_action_info).

-behaviour(emqx_action_info).

-elvis([{elvis_style, invalid_dynamic_call, disable}]).

%% behaviour callbacks
-export([
    action_type_name/0,
    connector_type_name/0,
    schema_module/0
]).

-define(ACTION_TYPE, elasticsearch).

action_type_name() -> ?ACTION_TYPE.
connector_type_name() -> ?ACTION_TYPE.

schema_module() -> emqx_bridge_es.
