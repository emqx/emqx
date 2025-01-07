%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_s3_upload_action_info).

-behaviour(emqx_action_info).

-include("emqx_bridge_s3.hrl").

-export([
    action_type_name/0,
    connector_type_name/0,
    schema_module/0
]).

action_type_name() -> ?ACTION_UPLOAD.

connector_type_name() -> s3.

schema_module() -> emqx_bridge_s3_upload.
