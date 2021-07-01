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

-include_lib("emqx/include/emqx.hrl").

%% API
-export([rest_schema/0, rest_api/0]).

-export([ handle_publish/1
        , handle_publish_batch/1]).

rest_schema() ->
    MessageDef = #{
        <<"topic">> =>
        #{
            type => <<"string">>
        },
        <<"qos">> =>
        #{
            type => <<"integer">>,
            enum => [0, 1, 2],
            default => 0
        },
        <<"payload">> =>
        #{
            type => <<"string">>
        },
        <<"retain">> =>
        #{
            type => boolean,
            required => false,
            default => false
        }
    },
    [{<<"message">>, MessageDef}].

rest_api() ->
    [publish_api(), publish_batch_api()].

publish_api() ->
    Path = "/publish",
    Metadata = #{
        post =>
            #{tags => ["client"],
            description => "publish message",
            operationId => handle_publish,
            requestBody =>
                #{content => #{'application/json' =>
                    #{schema => cowboy_swagger:schema(<<"message">>)}}},
            responses => #{
                <<"200">> => #{description => <<"publish ok">>}}}},
    {Path, Metadata}.

publish_batch_api() ->
    Path = "/publish_batch",
    Metadata = #{
        post =>
            #{tags => ["client"],
            description => "publish messages",
            operationId => handle_publish_batch,
            requestBody =>
            #{content => #{
                'application/json' => #{
                    schema => #{
                        type => array, items =>  cowboy_swagger:schema(<<"message">>)}}}},
            responses => #{<<"200">> => #{description => <<"publish ok">>}}}},
    {Path, Metadata}.

%%%==============================================================================================
%% parameters trans
handle_publish(Request) ->
    {ok, Body, _} = cowboy_req:read_body(Request),
    Message = emqx_json:decode(Body, [return_maps]),
    Qos = maps:get(<<"qos">>, Message, 0),
    Topic = maps:get(<<"topic">>, Message),
    Payload = maps:get(<<"payload">>, Message),
    Retain = maps:get(retain, Message, false),
    publish(#{qos => Qos, topic => Topic, payload => Payload, retain => Retain}).

handle_publish_batch(Request) ->
    {ok, Body, _} = cowboy_req:read_body(Request),
    Messages0 = emqx_json:decode(Body, [return_maps]),
    Messages =
        [begin
             Topic = maps:get(<<"topic">>, Message0),
             Qos = maps:get(<<"qos">>, Message0, 0),
             Payload = maps:get(<<"payload">>, Message0),
             Retain = maps:get(retain, Message0, false),
             #{topic => Topic, qos => Qos, payload => Payload, retain => Retain}
         end || Message0 <- Messages0],
    publish_batch(#{messages => Messages}).

%%%==============================================================================================
%% api apply
publish(#{qos := Qos, topic := Topic, payload := Payload, retain := Retain}) ->
    case do_publish(Qos, Topic, Payload, Retain) of
        ok ->
            {200};
        {error ,Reason} ->
            Body = emqx_json:encode(#{code => "UN_KNOW_ERROR", reason => io_lib:format("~p", [Reason])}),
            {500, Body}
    end.

publish_batch(#{messages := Messages}) ->
    ArgsList = [[Qos, Topic,  Payload, Retain]
        || #{qos := Qos, topic := Topic, payload := Payload, retain := Retain} <- Messages],
    Data = emqx_mgmt_util:batch_operation(?MODULE, do_publish, ArgsList),
    Body = emqx_json:encode(Data),
    {200, Body}.

%%%==============================================================================================
%% internal function

do_publish(Qos, Topic, Payload, Retain) ->
    Message = emqx_message:make(?MODULE, Qos, Topic, Payload),
    PublishResult = emqx_mgmt:publish(Message#message{flags = #{retain => Retain}}),
    case PublishResult of
        [] ->
            ok;
        [{_, _ , {ok, _}} | _] ->
            ok;
        [{_, _ , {error, Reason}} | _] ->
            {error, Reason}
    end.
