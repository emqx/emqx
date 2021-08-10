%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% a coap to mqtt adapter with a retained topic message database
-module(emqx_coap_pubsub_resource).

-behaviour(emqx_coap_resource).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_gateway/src/coap/include/emqx_coap.hrl").


-export([ init/1
        , stop/1
        , get/2
        , put/2
        , post/2
        , delete/2
        ]).
-import(emqx_coap_mqtt_resource, [ check_topic/1, subscribe/3, unsubscribe/3
                                 , publish/3]).

-import(emqx_coap_message, [response/2, response/3, set_content/2]).
%%--------------------------------------------------------------------
%% Resource Callbacks
%%--------------------------------------------------------------------
init(_) ->
    emqx_coap_pubsub_topics:start_link().

stop(Pid) ->
    emqx_coap_pubsub_topics:stop(Pid).

%% get: read last publish message
%% get with observe 0: subscribe
%% get with observe 1: unsubscribe
get(#coap_message{token = Token} = Msg, Cfg) ->
    case check_topic(Msg) of
        {ok, Topic} ->
            case emqx_coap_message:get_option(observe, Msg) of
                undefined ->
                    Content = emqx_coap_message:get_content(Msg),
                    read_last_publish_message(emqx_topic:wildcard(Topic), Msg, Topic, Content);
                0 ->
                    case Token of
                        <<>> ->
                            response({error, bad_reuqest}, <<"observe without token">>, Msg);
                        _ ->
                            Ret = subscribe(Msg, Topic, Cfg),
                            RetMsg = response(Ret, Msg),
                            case Ret of
                                {ok, _} ->
                                    {has_sub, RetMsg, {Topic, Token}};
                                _ ->
                                    RetMsg
                            end
                    end;
                1 ->
                    unsubscribe(Msg, Topic, Cfg),
                    {has_sub, response({ok, deleted}, Msg), Topic}
            end;
        Any ->
            Any
    end.

%% put: insert a message into topic database
put(Msg, _) ->
    case check_topic(Msg) of
        {ok, Topic} ->
            Content = emqx_coap_message:get_content(Msg),
            #coap_content{payload = Payload,
                          format = Format,
                          max_age = MaxAge} = Content,
            handle_received_create(Msg, Topic, MaxAge, Format, Payload);
        Any ->
            Any
    end.

%% post: like put, but will publish the inserted message
post(Msg, Cfg) ->
    case check_topic(Msg) of
        {ok, Topic} ->
            Content = emqx_coap_message:get_content(Msg),
            #coap_content{max_age = MaxAge,
                          format = Format,
                          payload = Payload} = Content,
            handle_received_publish(Msg, Topic, MaxAge, Format, Payload, Cfg);
        Any ->
            Any
    end.

%% delete: delete a message from topic database
delete(Msg, _) ->
    case check_topic(Msg) of
        {ok, Topic} ->
            delete_topic_info(Msg, Topic);
        Any ->
            Any
    end.

%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------
add_topic_info(Topic, MaxAge, Format, Payload) when is_binary(Topic), Topic =/= <<>>  ->
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

add_topic_info(Topic, _MaxAge, _Format, _Payload) ->
    ?LOG(debug, "create topic=~p info failed", [Topic]),
    {badarg, false}.

format_string_to_int(<<"application/octet-stream">>) ->
    <<"42">>;
format_string_to_int(<<"application/exi">>) ->
    <<"47">>;
format_string_to_int(<<"application/json">>) ->
    <<"50">>;
format_string_to_int(_) ->
    <<"42">>.

handle_received_publish(Msg, Topic, MaxAge, Format, Payload, Cfg) ->
    case add_topic_info(Topic, MaxAge, format_string_to_int(Format), Payload) of
        {_, true}  ->
            response(publish(Msg, Topic, Cfg), Msg);
        {_, false} ->
            ?LOG(debug, "add_topic_info failed, will return bad_request", []),
            response({error, bad_request}, Msg)
    end.

handle_received_create(Msg, Topic, MaxAge, Format, Payload) ->
    case add_topic_info(Topic, MaxAge, format_string_to_int(Format), Payload) of
        {Ret, true}  ->
            response({ok, Ret}, Msg);
        {_, false} ->
            ?LOG(debug, "add_topic_info failed, will return bad_request", []),
            response({error, bad_request}, Msg)
    end.

return_resource(Msg, Topic, Payload, MaxAge, TimeStamp, Content) ->
    TimeElapsed = trunc((erlang:system_time(millisecond) - TimeStamp) / 1000),
    case TimeElapsed < MaxAge of
        true  ->
            LeftTime = (MaxAge - TimeElapsed),
            ?LOG(debug, "topic=~p has max age left time is ~p", [Topic, LeftTime]),
            set_content(Content#coap_content{max_age = LeftTime, payload = Payload},
                        response({ok, content}, Msg));
        false ->
            ?LOG(debug, "topic=~p has been timeout, will return empty content", [Topic]),
            response({ok, nocontent}, Msg)
    end.

read_last_publish_message(false, Msg, Topic, Content=#coap_content{format = QueryFormat}) when is_binary(QueryFormat)->
    ?LOG(debug, "the QueryFormat=~p", [QueryFormat]),
    case emqx_coap_pubsub_topics:lookup_topic_info(Topic) of
        [] ->
            response({error, not_found}, Msg);
        [{_, MaxAge, CT, Payload, TimeStamp}] ->
            case CT =:= format_string_to_int(QueryFormat) of
                true  ->
                    return_resource(Msg, Topic, Payload, MaxAge, TimeStamp, Content);
                false ->
                    ?LOG(debug, "format value does not match, the queried format=~p, the stored format=~p", [QueryFormat, CT]),
                    response({error, bad_request}, Msg)
            end
    end;

read_last_publish_message(false, Msg, Topic, Content) ->
    case emqx_coap_pubsub_topics:lookup_topic_info(Topic) of
        [] ->
            response({error, not_found}, Msg);
        [{_, MaxAge, _, Payload, TimeStamp}] ->
            return_resource(Msg, Topic, Payload, MaxAge, TimeStamp, Content)
    end;

read_last_publish_message(true, Msg, Topic, _Content) ->
    ?LOG(debug, "the topic=~p is illegal wildcard topic", [Topic]),
    response({error, bad_request}, Msg).

delete_topic_info(Msg, Topic) ->
    case emqx_coap_pubsub_topics:lookup_topic_info(Topic) of
        [] ->
            response({error, not_found}, Msg);
        [{_, _, _, _, _}] ->
            emqx_coap_pubsub_topics:delete_sub_topics(Topic),
            response({ok, deleted}, Msg)
    end.
