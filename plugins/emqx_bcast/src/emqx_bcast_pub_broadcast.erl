%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_pub_broadcast).

-export([handle/2, deliver_local/3]).

-include("emqx_bcast.hrl").

handle(Body, RequestId) ->
    ProductKey = maps:get(<<"ProductKey">>, Body, undefined),
    MessageContent = maps:get(<<"MessageContent">>, Body, undefined),
    TopicFullName = maps:get(<<"TopicFullName">>, Body, undefined),

    case validate(ProductKey, MessageContent) of
        {error, Code, Msg} ->
            emqx_bcast_metrics:broadcast_error(),
            {ok, 400, #{}, emqx_bcast_api:error_response(RequestId, Code, Msg)};
        ok ->
            do_broadcast(ProductKey, MessageContent, TopicFullName, RequestId)
    end.

validate(undefined, _) ->
    {error, <<"InvalidProductKey">>, <<"ProductKey is required">>};
validate(_, undefined) ->
    {error, <<"InvalidBase64">>, <<"MessageContent is required">>};
validate(_ProductKey, MessageContent) ->
    case emqx_bcast_utils:decode_base64(MessageContent) of
        {ok, Payload} ->
            Config = persistent_term:get({?APP, config}, #{}),
            MaxSize = maps:get(max_message_size_broadcast, Config, 65536),
            case byte_size(Payload) =< MaxSize of
                true -> ok;
                false -> {error, <<"MessageTooLarge">>, <<"Message too large">>}
            end;
        {error, _} ->
            {error, <<"InvalidBase64">>, <<"Invalid Base64 encoding">>}
    end.

do_broadcast(ProductKey, _MessageContent, TopicFullName, RequestId) ->
    MessageId = emqx_bcast_utils:gen_api_uuid(),
    {ok, Payload} = emqx_bcast_utils:decode_base64(_MessageContent),
    TopicTemplate =
        case TopicFullName of
            undefined ->
                Config = persistent_term:get({?APP, config}, #{}),
                maps:get(broadcast_topic, Config, <<"/sys/broadcast/${productKey}">>);
            _ ->
                TopicFullName
        end,

    Nodes = emqx:running_nodes(),
    lists:foreach(
        fun(Node) ->
            case Node =:= node() of
                true ->
                    deliver_local(ProductKey, TopicTemplate, Payload);
                false ->
                    emqx_rpc:cast(Node, ?MODULE, deliver_local, [ProductKey, TopicTemplate, Payload])
            end
        end,
        Nodes
    ),

    emqx_bcast_metrics:broadcast_in(),
    {ok, 200, #{}, emqx_bcast_api:success_response(RequestId, MessageId)}.

deliver_local(ProductKey, TopicTemplate, Payload) ->
    Devices = emqx_bcast:lookup_devices_by_product(ProductKey),
    emqx_bcast_metrics:broadcast_devices_online(length(Devices)),
    lists:foreach(
        fun([DeviceName, Pid]) ->
            Topic = emqx_bcast_utils:expand_topic(TopicTemplate, ProductKey, DeviceName),
            case emqx_bcast_subscription:match(DeviceName, Topic) of
                {ok, _SubQos} ->
                    emqx_bcast_metrics:broadcast_delivery_count(1),
                    Msg = emqx_message:make(DeviceName, ?QOS_0, Topic, Payload),
                    Pid ! #deliver{topic = Topic, message = Msg};
                false ->
                    ok
            end
        end,
        Devices
    ).
