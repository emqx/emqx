%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_format).

-export([
    format_agent_msg/1,
    format_leader_msg/1,
    format_ssubscriber/1,
    format_stream/1,
    format_progress/1
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

format_agent_msg(Msg) ->
    Msg.

format_leader_msg(Msg) ->
    Msg.

format_ssubscriber({SessionId, SubscriptionId, PidRef}) ->
    iolist_to_binary(io_lib:format("~s:~p:~p", [SessionId, SubscriptionId, erlang:phash2(PidRef)])).

format_stream(Stream) ->
    iolist_to_binary(io_lib:format("stream-~p", [erlang:phash2(Stream)])).

format_progress(Progress) ->
    iolist_to_binary(io_lib:format("progress-~p", [erlang:phash2(Progress)])).
