%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_coap_pubsub_resource).

-behaviour(coap_resource).

-include("emqx_coap.hrl").
-include_lib("gen_coap/include/coap.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/logger.hrl").

-logger_header("[CoAP-PS-RES]").

-export([ coap_discover/2
        , coap_get/5
        , coap_post/4
        , coap_put/4
        , coap_delete/3
        , coap_observe/5
        , coap_unobserve/1
        , handle_info/2
        , coap_ack/2
        ]).

-ifdef(TEST).
-export([topic/1]).
-endif.

-define(PS_PREFIX, [<<"ps">>]).

%%--------------------------------------------------------------------
%% Resource Callbacks
%%--------------------------------------------------------------------
coap_discover(_Prefix, _Args) ->
    [{absolute, [<<"ps">>], []}].

coap_get(ChId, ?PS_PREFIX, TopicPath, Query, Content=#coap_content{format = Format}) when TopicPath =/= [] ->
    Topic = topic(TopicPath),
    ?LOG(debug, "coap_get() Topic=~p, Query=~p~n", [Topic, Query]),
    #coap_mqtt_auth{clientid = Clientid, username = Usr, password = Passwd} = get_auth(Query),
    case emqx_coap_mqtt_adapter:client_pid(Clientid, Usr, Passwd, ChId) of
        {ok, Pid} ->
            put(mqtt_client_pid, Pid),
            case Format of
                <<"application/link-format">> ->
                    Content;
                _Other                        ->
                    %% READ the topic info
                    read_last_publish_message(emqx_topic:wildcard(Topic), Topic, Content)
            end;
        {error, auth_failure} ->
            put(mqtt_client_pid, undefined),
            {error, unauthorized};
        {error, bad_request} ->
            put(mqtt_client_pid, undefined),
            {error, bad_request};
        {error, _Other} ->
            put(mqtt_client_pid, undefined),
            {error, internal_server_error}
    end;
coap_get(ChId, Prefix, TopicPath, Query, _Content) ->
    ?LOG(error, "ignore bad get request ChId=~p, Prefix=~p, TopicPath=~p, Query=~p", [ChId, Prefix, TopicPath, Query]),
    {error, bad_request}.

coap_post(_ChId, ?PS_PREFIX, TopicPath, #coap_content{format = Format, payload = Payload, max_age = MaxAge}) when TopicPath =/= [] ->
    Topic = topic(TopicPath),
    ?LOG(debug, "coap_post() Topic=~p, MaxAge=~p, Format=~p~n", [Topic, MaxAge, Format]),
    case Format of
        %% We treat ct of "application/link-format" as CREATE message
        <<"application/link-format">> ->
            handle_received_create(Topic, MaxAge, Payload);
        %% We treat ct of other values as PUBLISH message
        Other ->
            ?LOG(debug, "coap_post() receive payload format=~p, will process as PUBLISH~n", [Format]),
            handle_received_publish(Topic, MaxAge, Other, Payload)
    end;

coap_post(_ChId, _Prefix, _TopicPath, _Content) ->
    {error, method_not_allowed}.

coap_put(_ChId, ?PS_PREFIX, TopicPath, #coap_content{max_age = MaxAge, format = Format, payload = Payload}) when TopicPath =/= [] ->
    Topic = topic(TopicPath),
    ?LOG(debug, "put message, Topic=~p, Payload=~p~n", [Topic, Payload]),
    handle_received_publish(Topic, MaxAge, Format, Payload);

coap_put(_ChId, Prefix, TopicPath, Content) ->
    ?LOG(error, "put has error, Prefix=~p, TopicPath=~p, Content=~p", [Prefix, TopicPath, Content]),
    {error, bad_request}.

coap_delete(_ChId, ?PS_PREFIX, TopicPath) ->
    delete_topic_info(topic(TopicPath));

coap_delete(_ChId, _Prefix, _TopicPath) ->
    {error, method_not_allowed}.

coap_observe(ChId, ?PS_PREFIX, TopicPath, Ack, Content) when TopicPath =/= [] ->
    Topic = topic(TopicPath),
    ?LOG(debug, "observe Topic=~p, Ack=~p，Content=~p", [Topic, Ack, Content]),
    Pid = get(mqtt_client_pid),
    case emqx_coap_mqtt_adapter:subscribe(Pid, Topic) of
        ok ->
            Code = case emqx_coap_pubsub_topics:is_topic_timeout(Topic) of
               true -> nocontent;
               false-> content
            end,
            {ok, {state, ChId, ?PS_PREFIX, [Topic]}, Code, Content};
        {error, Code} ->
            {error, Code}
    end;

coap_observe(ChId, Prefix, TopicPath, Ack, _Content) ->
    ?LOG(error, "unknown observe request ChId=~p, Prefix=~p, TopicPath=~p, Ack=~p", [ChId, Prefix, TopicPath, Ack]),
    {error, bad_request}.

coap_unobserve({state, _ChId, ?PS_PREFIX, TopicPath}) when TopicPath =/= [] ->
    Topic = topic(TopicPath),
    ?LOG(debug, "unobserve ~p", [Topic]),
    Pid = get(mqtt_client_pid),
    emqx_coap_mqtt_adapter:unsubscribe(Pid, Topic),
    ok;
coap_unobserve({state, ChId, Prefix, TopicPath}) ->
    ?LOG(error, "ignore unknown unobserve request ChId=~p, Prefix=~p, TopicPath=~p", [ChId, Prefix, TopicPath]),
    ok.

handle_info({dispatch, Topic, Payload}, State) ->
    %% This clause should never be matched any more. We keep it here to handle
    %% the old format messages during the release upgrade.
    %% In this case the second function clause of `coap_ack/2` will be called,
    %% and the ACKs is discarded.
    ?LOG(debug, "dispatch Topic=~p, Payload=~p", [Topic, Payload]),
    {ok, Ret} = emqx_coap_pubsub_topics:reset_topic_info(Topic, Payload),
    ?LOG(debug, "Updated publish info of topic=~p, the Ret is ~p", [Topic, Ret]),
    {notify, [], #coap_content{format = <<"application/octet-stream">>, payload = Payload}, State};
handle_info({dispatch, Msg}, State) ->
    Payload = emqx_coap_mqtt_adapter:message_payload(Msg),
    Topic = emqx_coap_mqtt_adapter:message_topic(Msg),
    {ok, Ret} = emqx_coap_pubsub_topics:reset_topic_info(Topic, Payload),
    ?LOG(debug, "Updated publish info of topic=~p, the Ret is ~p", [Topic, Ret]),
    {notify, {pub, Msg}, #coap_content{format = <<"application/octet-stream">>, payload = Payload}, State};
handle_info(Message, State) ->
    ?LOG(error, "Unknown Message ~p", [Message]),
    {noreply, State}.

coap_ack({pub, Msg}, State) ->
    ?LOG(debug, "received coap ack for publish msg: ~p", [Msg]),
    Pid = get(mqtt_client_pid),
    emqx_coap_mqtt_adapter:received_puback(Pid, Msg),
    {ok, State};
coap_ack(_Ref, State) ->
    ?LOG(debug, "received coap ack: ~p", [_Ref]),
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------
get_auth(Query) ->
    get_auth(Query, #coap_mqtt_auth{}).

get_auth([], Auth=#coap_mqtt_auth{}) ->
    Auth;
get_auth([<<$c, $=, Rest/binary>>|T], Auth=#coap_mqtt_auth{}) ->
    get_auth(T, Auth#coap_mqtt_auth{clientid = Rest});
get_auth([<<$u, $=, Rest/binary>>|T], Auth=#coap_mqtt_auth{}) ->
    get_auth(T, Auth#coap_mqtt_auth{username = Rest});
get_auth([<<$p, $=, Rest/binary>>|T], Auth=#coap_mqtt_auth{}) ->
    get_auth(T, Auth#coap_mqtt_auth{password = Rest});
get_auth([Param|T], Auth=#coap_mqtt_auth{}) ->
    ?LOG(error, "ignore unknown parameter ~p", [Param]),
    get_auth(T, Auth).

add_topic_info(publish, Topic, MaxAge, Format, Payload) when is_binary(Topic), Topic =/= <<>>  ->
    case emqx_coap_pubsub_topics:lookup_topic_info(Topic) of
        [{_, StoredMaxAge, StoredCT, _, _}] ->
            ?LOG(debug, "publish topic=~p already exists, need reset the topic info", [Topic]),
            %% check whether the ct value stored matches the ct option in this POST message
            case Format =:= StoredCT of
                true  ->
                    {ok, Ret} =
                        case StoredMaxAge =:= MaxAge of
                            true  ->
                                emqx_coap_pubsub_topics:reset_topic_info(Topic, Payload);
                            false ->
                                emqx_coap_pubsub_topics:reset_topic_info(Topic, MaxAge, Payload)
                        end,
                    {changed, Ret};
                false ->
                    ?LOG(debug, "ct values of topic=~p do not match, stored ct=~p, new ct=~p, ignore the PUBLISH", [Topic, StoredCT, Format]),
                    {changed, false}
            end;
        [] ->
            ?LOG(debug, "publish topic=~p will be created", [Topic]),
            {ok, Ret} = emqx_coap_pubsub_topics:add_topic_info(Topic, MaxAge, Format, Payload),
            {created, Ret}
    end;

add_topic_info(create, Topic, MaxAge, Format, _Payload) when is_binary(Topic), Topic =/= <<>> ->
    case emqx_coap_pubsub_topics:is_topic_existed(Topic) of
        true ->
            %% Whether we should support CREATE to an existed topic is TBD!!
            ?LOG(debug, "create topic=~p already exists, need reset the topic info", [Topic]),
            {ok, Ret} = emqx_coap_pubsub_topics:reset_topic_info(Topic, MaxAge, Format, <<>>);
        false ->
            ?LOG(debug, "create topic=~p will be created", [Topic]),
            {ok, Ret} = emqx_coap_pubsub_topics:add_topic_info(Topic, MaxAge, Format, <<>>)
    end,
    {created, Ret};

add_topic_info(_, Topic, _MaxAge, _Format, _Payload) ->
    ?LOG(debug, "create topic=~p info failed", [Topic]),
    {badarg, false}.

concatenate_location_path(List = [TopicPart1, TopicPart2, TopicPart3]) when is_binary(TopicPart1), is_binary(TopicPart2), is_binary(TopicPart3)  ->
    list_to_binary(lists:foldl( fun (Element, AccIn) when Element =/= <<>> ->
                                    AccIn ++ "/" ++ binary_to_list(Element);
                                (_Element, AccIn) ->
                                    AccIn
                                end, [], List)).

format_string_to_int(<<"application/octet-stream">>) ->
    <<"42">>;
format_string_to_int(<<"application/exi">>) ->
    <<"47">>;
format_string_to_int(<<"application/json">>) ->
    <<"50">>.

handle_received_publish(Topic, MaxAge, Format, Payload) ->
    case add_topic_info(publish, Topic, MaxAge, format_string_to_int(Format), Payload) of
        {Ret, true}  ->
            Pid = get(mqtt_client_pid),
            case emqx_coap_mqtt_adapter:publish(Pid, topic(Topic), Payload) of
                ok ->
                    {ok, Ret, case Ret of
                        changed -> #coap_content{};
                        created ->
                            #coap_content{location_path = [
                                concatenate_location_path([<<"ps">>, Topic, <<>>])]}
                     end};
                {error, Code} ->
                    {error, Code}
            end;
        {_, false} ->
            ?LOG(debug, "add_topic_info failed, will return bad_request", []),
            {error, bad_request}
    end.

handle_received_create(TopicPrefix, MaxAge, Payload) ->
    case core_link:decode(Payload) of
        [{rootless, [Topic], [{ct, CT}]}] when is_binary(Topic), Topic =/= <<>> ->
            TrueTopic = emqx_http_lib:uri_decode(Topic),
            ?LOG(debug, "decoded link-format payload, the Topic=~p, CT=~p~n", [TrueTopic, CT]),
            LocPath = concatenate_location_path([<<"ps">>, TopicPrefix, TrueTopic]),
            FullTopic = binary:part(LocPath, 4, byte_size(LocPath)-4),
            ?LOG(debug, "the location path is ~p, the full topic is ~p~n", [LocPath, FullTopic]),
            case add_topic_info(create, FullTopic, MaxAge, CT, <<>>) of
                {_, true}  ->
                    ?LOG(debug, "create topic info successfully, will return LocPath=~p", [LocPath]),
                    {ok, created, #coap_content{location_path = [LocPath]}};
                {_, false} ->
                    ?LOG(debug, "create topic info failed, will return bad_request", []),
                    {error, bad_request}
            end;
        Other  ->
            ?LOG(debug, "post with bad payload of link-format ~p, will return bad_request", [Other]),
            {error, bad_request}
    end.

%% When topic is timeout, server should return nocontent here,
%% but gen_coap only receive return value of #coap_content from coap_get, so temporarily we can't give the Code 2.07 {ok, nocontent} out.TBC!!!
return_resource(Topic, Payload, MaxAge, TimeStamp, Content) ->
    TimeElapsed = trunc((erlang:system_time(millisecond) - TimeStamp) / 1000),
    case TimeElapsed < MaxAge of
        true  ->
            LeftTime = (MaxAge - TimeElapsed),
            ?LOG(debug, "topic=~p has max age left time is ~p", [Topic, LeftTime]),
            Content#coap_content{max_age = LeftTime, payload = Payload};
        false ->
            ?LOG(debug, "topic=~p has been timeout, will return empty content", [Topic]),
            #coap_content{}
    end.

read_last_publish_message(false, Topic, Content=#coap_content{format = QueryFormat}) when is_binary(QueryFormat)->
    ?LOG(debug, "the QueryFormat=~p", [QueryFormat]),
    case emqx_coap_pubsub_topics:lookup_topic_info(Topic) of
        [] ->
            {error, not_found};
        [{_, MaxAge, CT, Payload, TimeStamp}] ->
            case CT =:= format_string_to_int(QueryFormat) of
                true  ->
                    return_resource(Topic, Payload, MaxAge, TimeStamp, Content);
                false ->
                    ?LOG(debug, "format value does not match, the queried format=~p, the stored format=~p", [QueryFormat, CT]),
                    {error, bad_request}
            end
    end;

read_last_publish_message(false, Topic, Content) ->
    case emqx_coap_pubsub_topics:lookup_topic_info(Topic) of
        [] ->
            {error, not_found};
        [{_, MaxAge, _, Payload, TimeStamp}] ->
            return_resource(Topic, Payload, MaxAge, TimeStamp, Content)
    end;

read_last_publish_message(true, Topic, _Content) ->
    ?LOG(debug, "the topic=~p is illegal wildcard topic", [Topic]),
    {error, bad_request}.

delete_topic_info(Topic) ->
    case emqx_coap_pubsub_topics:lookup_topic_info(Topic) of
        [] ->
            {error, not_found};
        [{_, _, _, _, _}] ->
            emqx_coap_pubsub_topics:delete_sub_topics(Topic)
    end.

topic(Topic) when is_binary(Topic) -> Topic;
topic([]) -> <<>>;
topic([Path | TopicPath]) ->
    case topic(TopicPath) of
        <<>> -> Path;
        RemTopic ->
            <<Path/binary, $/, RemTopic/binary>>
    end.
