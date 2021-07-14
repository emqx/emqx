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
            parameters => [#{
                name => message,
                in => body,
                required => true,
                schema => minirest:ref(<<"message">>)
            }],
            responses => #{<<"200">> => #{description => <<"publish ok">>}}}},
    {"/publish", MeteData, publish}.

publish_batch_api() ->
    MeteData = #{
        post => #{
            description => "publish",
            parameters => [#{
                name => message,
                in => body,
                required => true,
                schema =>#{
                    type => array,
                    items => minirest:ref(<<"message">>)}
            }],
            responses => #{
                <<"200">> => #{
                    description => <<"publish result">>,
                    schema => emqx_mgmt_util:batch_response_schema(<<"message">>)
                }}}},
    {"/publish_batch", MeteData, publish_batch}.

message_schema() ->
    Def = #{
        <<"topic">> => #{
            type => <<"string">>,
            description => <<"Topic">>},
        <<"qos">> => #{
            type => <<"integer">>,
            enum => [0, 1, 2],
            description => <<"Qos">>},
        <<"retain">> => #{
            type => <<"boolean">>,
            description => <<"Retain message or not">>},
        <<"payload">> => #{
            type => <<"string">>,
            description => <<"Topic">>}
        },
    {<<"message">>, Def}.

publish(post, Request) ->
    {ok, Body, _} = cowboy_req:read_body(Request),
    Message = message(emqx_json:decode(Body, [return_maps])),
    _ = emqx_mgmt:publish(Message),
    {200}.

publish_batch(post, Request) ->
    {ok, Body, _} = cowboy_req:read_body(Request),
    Messages = messages(emqx_json:decode(Body, [return_maps])),
    _ = [emqx_mgmt:publish(Message) || Message <- Messages],
    ResponseBody = emqx_json:encode(#{success => length(Messages), failed => 0, detail => []}),
    {200, ResponseBody}.

message(Map) ->
    Topic   = maps:get(<<"topic">>, Map),
    Payload = maps:get(<<"payload">>, Map),
    Qos     = maps:get(<<"qos">>, Map, 0),
    Retain  = maps:get(<<"retain">>, Map, false),
    Message = emqx_message:make(http_api, Qos, Topic, Payload),
    Message#message{flags = #{retain => Retain}}.

messages(List) ->
    [message(MessageMap) || MessageMap <- List].
