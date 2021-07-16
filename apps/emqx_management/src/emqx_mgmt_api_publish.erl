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
-module(emqx_mgmt_api_publish).
%% API
-include_lib("emqx/include/emqx.hrl").

-behavior(minirest_api).

-export([api_spec/0]).

-export([ publish/2
        , publish_batch/2]).

api_spec() ->
    {
        [publish_api(), publish_batch_api()],
        [message_schema()]
    }.

publish_api() ->
    MeteData = #{
        post => #{
            description => "publish",
            'requestBody' => #{
                content => #{
                    'application/json' => #{
                        schema => #{
                            type => object,
                            properties => maps:with([id], message_properties())}}}},
            responses => #{
                <<"200">> => emqx_mgmt_util:response_schema(<<"publish ok">>, <<"message">>)}}},
    {"/publish", MeteData, publish}.

publish_batch_api() ->
    MeteData = #{
        post => #{
            description => "publish",
            'requestBody' => #{
                content => #{
                    'application/json' => #{
                        schema => #{
                            type => array,
                            items => #{
                                type => object,
                                properties =>  maps:with([id], message_properties())}}}}},
            responses => #{
                <<"200">> => emqx_mgmt_util:response_array_schema(<<"publish ok">>, <<"message">>)}}},
    {"/publish_batch", MeteData, publish_batch}.

message_schema() ->
    #{
        message => #{
            type => object,
            properties => message_properties()
        }
    }.

message_properties() ->
    #{
        id => #{
            type => string,
            description => <<"Message ID">>},
        topic => #{
            type => string,
            description => <<"Topic">>},
        qos => #{
            type => integer,
            enum => [0, 1, 2],
            description => <<"Qos">>},
        payload => #{
            type => string,
            description => <<"Topic">>},
        from => #{
            type => string,
            description => <<"Message from">>},
        flag => #{
            type => <<"object">>,
            description => <<"Message flag">>,
            properties => #{
                sys => #{
                    type => boolean,
                    default => false,
                    description => <<"System message flag, nullable, default false">>},
                dup => #{
                    type => boolean,
                    default => false,
                    description => <<"Dup message flag, nullable, default false">>},
                retain => #{
                    type => boolean,
                    default => false,
                    description => <<"Retain message flag, nullable, default false">>}
            }
        }
    }.

publish(post, Request) ->
    {ok, Body, _} = cowboy_req:read_body(Request),
    Message = message(emqx_json:decode(Body, [return_maps])),
    _ = emqx_mgmt:publish(Message),
    {200, emqx_json:encode(format_message(Message))}.

publish_batch(post, Request) ->
    {ok, Body, _} = cowboy_req:read_body(Request),
    Messages = messages(emqx_json:decode(Body, [return_maps])),
    _ = [emqx_mgmt:publish(Message) || Message <- Messages],
    ResponseBody = emqx_json:encode(format_message(Messages)),
    {200, ResponseBody}.

message(Map) ->
    From    = maps:get(<<"from">>, Map, http_api),
    QoS     = maps:get(<<"qos">>, Map, 0),
    Topic   = maps:get(<<"topic">>, Map),
    Payload = maps:get(<<"payload">>, Map),
    Flags   = flags(Map),
    emqx_message:make(From, QoS, Topic, Payload, Flags, #{}).

flags(Map) ->
    Flags   = maps:get(<<"flags">>, Map, #{}),
    Retain  = maps:get(<<"retain">>, Flags, false),
    Sys     = maps:get(<<"sys">>, Flags, false),
    Dup     = maps:get(<<"dup">>, Flags, false),
    #{
        retain => Retain,
        sys => Sys,
        dup => Dup
    }.

messages(List) ->
    [message(MessageMap) || MessageMap <- List].

format_message(Messages) when is_list(Messages)->
    [format_message(Message) || Message <- Messages];
format_message(#message{id = ID, qos = Qos, from = From, topic = Topic, payload = Payload, flags = Flags}) ->
    #{
        id => emqx_guid:to_hexstr(ID),
        qos => Qos,
        topic => Topic,
        payload => Payload,
        flag => Flags,
        from => to_binary(From)
    }.

to_binary(Data) when is_binary(Data) ->
    Data;
to_binary(Data)  ->
    list_to_binary(io_lib:format("~p", [Data])).
