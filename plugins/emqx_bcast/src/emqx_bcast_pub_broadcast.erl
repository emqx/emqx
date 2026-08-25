%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_pub_broadcast).

-export([handle/2]).

-include("emqx_bcast.hrl").

handle(Body, RequestId) ->
    ProductKey = maps:get(<<"ProductKey">>, Body, undefined),
    MessageContent = maps:get(<<"MessageContent">>, Body, undefined),
    TopicFullName = maps:get(<<"TopicFullName">>, Body, undefined),

    case validate(ProductKey, MessageContent, TopicFullName) of
        {error, Code, Msg} ->
            emqx_bcast_metrics:broadcast_error(),
            {ok, 400, #{}, emqx_bcast_api:error_response(RequestId, Code, Msg)};
        ok ->
            do_broadcast(ProductKey, MessageContent, TopicFullName, RequestId)
    end.

validate(ProductKey, _, _) when not is_binary(ProductKey) orelse ProductKey =:= <<>> ->
    {error, <<"InvalidProductKey">>, <<"ProductKey is required">>};
validate(_, undefined, _) ->
    {error, <<"InvalidBase64">>, <<"MessageContent is required">>};
validate(ProductKey, MessageContent, TopicFullName) ->
    case emqx_bcast_utils:decode_base64(MessageContent) of
        {ok, Payload} ->
            Config = persistent_term:get({?APP, config}, #{}),
            MaxSize = maps:get(max_message_size_broadcast, Config, 65536),
            case byte_size(Payload) =< MaxSize of
                true -> validate_topic_full_name(ProductKey, TopicFullName);
                false -> {error, <<"MessageTooLarge">>, <<"Message too large">>}
            end;
        {error, _} ->
            {error, <<"InvalidBase64">>, <<"Invalid Base64 encoding">>}
    end.

%% A broadcast topic is a full concrete topic: reject wildcards and any
%% ${...} placeholder, and reject a ProductKey that would leak wildcard
%% characters into the topic.
validate_topic_full_name(ProductKey, undefined) ->
    validate_product_key_chars(ProductKey);
validate_topic_full_name(ProductKey, TopicFullName) when is_binary(TopicFullName) ->
    case re:run(TopicFullName, <<"[+#${}]">>) of
        nomatch -> validate_product_key_chars(ProductKey);
        _ -> {error, <<"InvalidTopicTemplate">>, <<"TopicFullName contains invalid characters">>}
    end;
validate_topic_full_name(_, _) ->
    {error, <<"InvalidTopicTemplate">>, <<"TopicFullName must be a string">>}.

validate_product_key_chars(ProductKey) ->
    case re:run(ProductKey, <<"[/+#$]">>) of
        nomatch -> ok;
        _ -> {error, <<"InvalidProductKey">>, <<"ProductKey contains invalid characters">>}
    end.

do_broadcast(ProductKey, MessageContent, TopicFullName, RequestId) ->
    MessageId = emqx_bcast_utils:gen_api_uuid(),
    {ok, Payload} = emqx_bcast_utils:decode_base64(MessageContent),
    TopicTemplate =
        case TopicFullName of
            undefined ->
                Config = persistent_term:get({?APP, config}, #{}),
                maps:get(broadcast_topic, Config, <<"/sys/broadcast/${productKey}">>);
            _ ->
                TopicFullName
        end,

    ok = emqx_bcast_pull_server_pool:qos0_broadcast(ProductKey, undefined, TopicTemplate, Payload),
    emqx_bcast_metrics:broadcast_in(),
    {ok, 200, #{}, emqx_bcast_api:success_response(RequestId, MessageId)}.
