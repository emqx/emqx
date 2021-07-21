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

%% a coap to mqtt adapter
-module(emqx_coap_mqtt_resource).

-behaviour(emqx_coap_resource).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx_gateway/src/coap/include/emqx_coap.hrl").

-logger_header("[CoAP-RES]").

-export([ init/1
        , stop/1
        , get/2
        , put/2
        , post/2
        , delete/2
        ]).

-export([ check_topic/1
        , publish/3
        , subscribe/3
        , unsubscribe/3]).

-define(SUBOPTS, #{rh => 0, rap => 0, nl => 0, is_new => false}).

init(_) ->
    {ok, undefined}.

stop(_) ->
    ok.

%% get: subscribe, ignore observe option
get(#coap_message{token = Token} = Msg, Cfg) ->
    case check_topic(Msg) of
        {ok, Topic} ->
            case Token of
                <<>> ->
                    emqx_coap_message:response({error, bad_request}, <<"observer without token">>, Msg);
                _ ->
                    Ret = subscribe(Msg, Topic, Cfg),
                    RetMsg = emqx_coap_message:response(Ret, Msg),
                    case Ret of
                        {ok, _} ->
                            {has_sub, RetMsg, {Topic, Token}};
                        _ ->
                            RetMsg
                    end
            end;
        Any ->
            Any
    end.

%% put: equal post
put(Msg, Cfg) ->
    post(Msg, Cfg).

%% post: publish a message
post(Msg, Cfg) ->
    case check_topic(Msg) of
        {ok, Topic} ->
            emqx_coap_message:response(publish(Msg, Topic, Cfg), Msg);
        Any ->
            Any
    end.

%% delete: ubsubscribe
delete(Msg, Cfg) ->
    case check_topic(Msg) of
        {ok, Topic} ->
            unsubscribe(Msg, Topic, Cfg),
            {has_sub, emqx_coap_message:response({ok, deleted}, Msg), Topic};
        Any ->
            Any
    end.

check_topic(#coap_message{options = Options} = Msg) ->
    case maps:get(uri_path, Options, []) of
        [] ->
            emqx_coap_message:response({error, bad_request}, <<"invalid topic">> , Msg);
        UriPath ->
            Sep = <<"/">>,
            {ok, lists:foldl(fun(Part, Acc) ->
                                     <<Acc/binary, Sep/binary, Part/binary>>
                             end,
                             <<>>,
                             UriPath)}
    end.

publish(#coap_message{payload = Payload} = Msg,
        Topic,
        #{clientinfo := ClientInfo,
          publish_qos := QOS} = Cfg) ->
    case emqx_coap_channel:auth_publish(Topic, Cfg) of
        allow ->
            #{clientid := ClientId} = ClientInfo,
            MQTTMsg = emqx_message:make(ClientId, type_to_qos(QOS, Msg), Topic, Payload),
            MQTTMsg2 = emqx_message:set_flag(retain, false, MQTTMsg),
            _ = emqx_broker:publish(MQTTMsg2),
            {ok, changed};
        _ ->
            {error, unauthorized}
    end.

subscribe(Msg, Topic, #{clientinfo := ClientInfo}= Cfg) ->
    case emqx_topic:wildcard(Topic) of
        false ->
            case emqx_coap_channel:auth_subscribe(Topic, Cfg) of
                allow ->
                    #{clientid := ClientId} = ClientInfo,
                    SubOpts = get_sub_opts(Msg, Cfg),
                    emqx_broker:subscribe(Topic, ClientId, SubOpts),
                    emqx_hooks:run('session.subscribed', [ClientInfo, Topic, SubOpts]),
                    {ok, created};
                _ ->
                    {error, unauthorized}
            end;
        _ ->
            %% now, we don't support wildcard in subscribe topic
            {error, bad_request, <<"">>}
    end.

unsubscribe(Msg, Topic, #{clientinfo := ClientInfo} = Cfg) ->
    emqx_broker:unsubscribe(Topic),
    emqx_hooks:run('session.unsubscribed', [ClientInfo, Topic, get_sub_opts(Msg, Cfg)]).

get_sub_opts(Msg, #{subscribe_qos := Type}) ->
    ?SUBOPTS#{qos => type_to_qos(Type, Msg)}.

type_to_qos(qos0, _) -> ?QOS_0;
type_to_qos(qos1, _) -> ?QOS_1;
type_to_qos(qos2, _) -> ?QOS_2;
type_to_qos(coap, #coap_message{type = Type}) ->
    case Type of
        non ->
            ?QOS_0;
        _ ->
            ?QOS_1
    end.
