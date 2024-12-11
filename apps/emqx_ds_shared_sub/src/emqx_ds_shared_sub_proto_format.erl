%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_proto_format).

-export([
    format_agent_msg/1,
    format_leader_msg/1
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

format_agent_msg(Msg) ->
    Msg.

format_leader_msg(Msg) ->
    Msg.
