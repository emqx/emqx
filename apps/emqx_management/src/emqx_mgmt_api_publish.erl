%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_mgmt_api_publish).

-include_lib("emqx/include/emqx.hrl").
-include_lib("typerefl/include/types.hrl").

-behaviour(minirest_api).

-export([
    api_spec/0,
    paths/0,
    schema/1,
    fields/1
]).

-export([
    publish/2,
    publish_batch/2
]).

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true, translate_body => true}).

paths() ->
    ["/publish", "/publish/bulk"].

schema("/publish") ->
    #{
        'operationId' => publish,
        post => #{
            description => <<"Publish Message">>,
            'requestBody' => hoconsc:mk(hoconsc:ref(?MODULE, publish_message)),
            responses => #{
                200 => hoconsc:mk(hoconsc:ref(?MODULE, publish_message_info))
            }
        }
    };
schema("/publish/bulk") ->
    #{
        'operationId' => publish_batch,
        post => #{
            description => <<"Publish Messages">>,
            'requestBody' => hoconsc:mk(hoconsc:array(hoconsc:ref(?MODULE, publish_message)), #{}),
            responses => #{
                200 => hoconsc:mk(hoconsc:array(hoconsc:ref(?MODULE, publish_message_info)), #{})
            }
        }
    }.

fields(message) ->
    [
        {topic,
            hoconsc:mk(binary(), #{
                desc => <<"Topic Name">>,
                required => true,
                example => <<"api/example/topic">>
            })},
        {qos,
            hoconsc:mk(emqx_schema:qos(), #{
                desc => <<"MQTT QoS">>,
                required => false,
                default => 0
            })},
        {clientid,
            hoconsc:mk(binary(), #{
                desc => <<"From client ID">>,
                required => false,
                example => <<"api_example_client">>
            })},
        {payload,
            hoconsc:mk(binary(), #{
                desc => <<"MQTT Payload">>,
                required => true,
                example => <<"hello emqx api">>
            })},
        {retain,
            hoconsc:mk(boolean(), #{
                desc => <<"MQTT Retain Message">>,
                required => false,
                default => false
            })}
    ];
fields(publish_message) ->
    [
        {payload_encoding,
            hoconsc:mk(hoconsc:enum([plain, base64]), #{
                desc => <<"MQTT Payload Encoding, base64 or plain">>,
                required => false,
                default => plain
            })}
    ] ++ fields(message);
fields(publish_message_info) ->
    [
        {id,
            hoconsc:mk(binary(), #{
                desc => <<"Internal Message ID">>
            })}
    ] ++ fields(message).

publish(post, #{body := Body}) ->
    case message(Body) of
        {ok, Message} ->
            _ = emqx_mgmt:publish(Message),
            {200, format_message(Message)};
        {error, R} ->
            {400, 'BAD_REQUEST', to_binary(R)}
    end.

publish_batch(post, #{body := Body}) ->
    case messages(Body) of
        {ok, Messages} ->
            _ = [emqx_mgmt:publish(Message) || Message <- Messages],
            {200, format_message(Messages)};
        {error, R} ->
            {400, 'BAD_REQUEST', to_binary(R)}
    end.

message(Map) ->
    Encoding = maps:get(<<"payload_encoding">>, Map, plain),
    case encode_payload(Encoding, maps:get(<<"payload">>, Map)) of
        {ok, Payload} ->
            From = maps:get(<<"clientid">>, Map, http_api),
            QoS = maps:get(<<"qos">>, Map, 0),
            Topic = maps:get(<<"topic">>, Map),
            Retain = maps:get(<<"retain">>, Map, false),
            {ok, emqx_message:make(From, QoS, Topic, Payload, #{retain => Retain}, #{})};
        {error, R} ->
            {error, R}
    end.

encode_payload(plain, Payload) ->
    {ok, Payload};
encode_payload(base64, Payload) ->
    try
        {ok, base64:decode(Payload)}
    catch
        _:_ ->
            {error, {decode_base64_payload_failed, Payload}}
    end.

messages(List) ->
    messages(List, []).

messages([], Res) ->
    {ok, lists:reverse(Res)};
messages([MessageMap | List], Res) ->
    case message(MessageMap) of
        {ok, Message} ->
            messages(List, [Message | Res]);
        {error, R} ->
            {error, R}
    end.

format_message(Messages) when is_list(Messages) ->
    [format_message(Message) || Message <- Messages];
format_message(#message{
    id = ID, qos = Qos, from = From, topic = Topic, payload = Payload, flags = Flags
}) ->
    #{
        id => emqx_guid:to_hexstr(ID),
        qos => Qos,
        topic => Topic,
        payload => Payload,
        retain => maps:get(retain, Flags, false),
        clientid => to_binary(From)
    }.

to_binary(Data) when is_binary(Data) ->
    Data;
to_binary(Data) ->
    list_to_binary(io_lib:format("~p", [Data])).
