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
%% API
-include_lib("emqx/include/emqx.hrl").

-behaviour(minirest_api).

-import(emqx_mgmt_util, [ object_schema/1
                        , object_schema/2
                        , object_array_schema/1
                        , object_array_schema/2
                        , properties/1
                        ]).

-export([api_spec/0]).

-export([ publish/2
        , publish_batch/2]).

api_spec() ->
    {[publish_api(), publish_bulk_api()], []}.

publish_api() ->
    MeteData = #{
        post => #{
            description => <<"Publish">>,
            'requestBody' => object_schema(maps:without([id], properties())),
            responses => #{
                <<"200">> => object_schema(properties(), <<"publish ok">>)}}},
    {"/publish", MeteData, publish}.

publish_bulk_api() ->
    MeteData = #{
        post => #{
            description => <<"publish">>,
            'requestBody' => object_array_schema(maps:without([id], properties())),
            responses => #{
                <<"200">> => object_array_schema(properties(), <<"publish ok">>)}}},
    {"/publish/bulk", MeteData, publish_batch}.

properties() ->
    properties([
        {id, string, <<"Message Id">>},
        {topic, string, <<"Topic">>},
        {qos, integer, <<"QoS">>, [0, 1, 2]},
        {payload, string, <<"Topic">>},
        {from, string, <<"Message from">>},
        {retain, boolean, <<"Retain message flag, nullable, default false">>}
    ]).

publish(post, #{body := Body}) ->
    Message = message(Body),
    _ = emqx_mgmt:publish(Message),
    {200, format_message(Message)}.

publish_batch(post, #{body := Body}) ->
    Messages = messages(Body),
    _ = [emqx_mgmt:publish(Message) || Message <- Messages],
    {200, format_message(Messages)}.

message(Map) ->
    From    = maps:get(<<"from">>, Map, http_api),
    QoS     = maps:get(<<"qos">>, Map, 0),
    Topic   = maps:get(<<"topic">>, Map),
    Payload = maps:get(<<"payload">>, Map),
    Retain = maps:get(<<"retain">>, Map, false),
    emqx_message:make(From, QoS, Topic, Payload, #{retain => Retain}, #{}).

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
        retain => maps:get(retain, Flags, false),
        from => to_binary(From)
    }.

to_binary(Data) when is_binary(Data) ->
    Data;
to_binary(Data)  ->
    list_to_binary(io_lib:format("~p", [Data])).
