%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_format).

-export([
    format_ssubscriber_msg/1,
    format_leader_msg/1,
    format_ssubscriber_id/1,
    format_stream/1,
    format_progress/1,
    format_ssubscriber_ids/1,
    format_ssubscriber_map/1,
    format_stream_map/1,
    format_streams/1,
    format_deep/1
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

format_ssubscriber_msg(Msg) ->
    format_deep(Msg).

format_leader_msg(Msg) ->
    format_deep(Msg).

format_ssubscriber_id({SessionId, SubscriptionId, PidRef}) ->
    iolist_to_binary(io_lib:format("~s:~p:~p", [SessionId, SubscriptionId, erlang:phash2(PidRef)])).

format_stream(Stream) ->
    iolist_to_binary(io_lib:format("stream-~p", [erlang:phash2(Stream)])).

format_progress(Progress) ->
    iolist_to_binary(io_lib:format("progress-~p", [erlang:phash2(Progress)])).

format_deep(Msg) when is_map(Msg) ->
    maps:fold(
        fun(Key, Value, Acc) ->
            Acc#{Key => format_key_value(Key, Value)}
        end,
        #{},
        Msg
    );
format_deep(List) when is_list(List) ->
    [format_deep(Item) || Item <- List];
format_deep(Value) ->
    Value.

format_stream_map(Map) when is_map(Map) ->
    maps:fold(
        fun(Key, Value, Acc) ->
            Acc#{format_stream(Key) => format_deep(Value)}
        end,
        #{},
        Map
    ).

format_ssubscriber_map(Map) when is_map(Map) ->
    maps:fold(
        fun(Key, Value, Acc) ->
            Acc#{format_ssubscriber_id(Key) => format_deep(Value)}
        end,
        #{},
        Map
    ).

format_ssubscriber_ids(SubscriptionIds) when is_list(SubscriptionIds) ->
    [format_ssubscriber_id(SubscriptionId) || SubscriptionId <- SubscriptionIds].

format_streams(Streams) when is_list(Streams) ->
    [format_stream(Stream) || Stream <- Streams].

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

format_key_value(ssubscriber_id, SSubscriberId) ->
    format_ssubscriber_id(SSubscriberId);
format_key_value(ssubscriber_ids, SSubscriberIds) ->
    format_ssubscriber_ids(SSubscriberIds);
format_key_value(stream, Stream) ->
    format_stream(Stream);
format_key_value(progress, Progress) ->
    format_progress(Progress);
format_key_value(_Key, Value) ->
    Value.
