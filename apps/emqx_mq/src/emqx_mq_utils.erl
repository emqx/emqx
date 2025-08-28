%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_utils).

-include_lib("emqx_mq_internal.hrl").

-export([
    merge_maps/1,
    mq_info/2
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

mq_info(ChannelPid, TopicFilter) ->
    Self = alias([reply]),
    erlang:send(ChannelPid, #info_mq_info{receiver = Self, topic_filter = TopicFilter}),
    receive
        {Self, Info} ->
            Info
    end.

merge_maps(Maps) ->
    lists:foldl(fun(Map, Acc) -> maps:merge(Acc, Map) end, #{}, Maps).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------
