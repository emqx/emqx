%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_util).

-include("emqx_mq_internal.hrl").

-export([
    mq_info/1
]).

mq_info(ChannelPid) ->
    Self = alias([reply]),
    erlang:send(ChannelPid, #info_mq_info{receiver = Self}),
    receive
        {Self, InfoList} ->
            InfoList
    end.
