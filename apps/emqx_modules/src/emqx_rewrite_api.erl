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
-module(emqx_rewrite_api).

-behaviour(minirest_api).

-export([api_spec/0]).

-export([topic_rewrite/2]).

-define(MAX_RULES_LIMIT, 20).

-define(EXCEED_LIMIT, 'EXCEED_LIMIT').

api_spec() ->
    {[rewrite_api()], []}.

topic_rewrite_schema() ->
    #{
        type => object,
        properties => #{
            action => #{
                type => string,
                description => <<"Node">>,
                enum => [subscribe, publish]},
            source_topic => #{
                type => string,
                description => <<"Topic">>},
            re => #{
                type => string,
                description => <<"Regular expressions">>},
            dest_topic => #{
                type => string,
                description => <<"Destination topic">>}
        }
    }.

rewrite_api() ->
    Path = "/mqtt/topic_rewrite",
    Metadata = #{
        get => #{
            description => <<"List topic rewrite">>,
            responses => #{
                <<"200">> =>
                    emqx_mgmt_util:response_array_schema(<<"List all rewrite rules">>, topic_rewrite_schema())}},
        post => #{
            description => <<"Update topic rewrite">>,
            'requestBody' => emqx_mgmt_util:request_body_array_schema(topic_rewrite_schema()),
            responses => #{
                <<"200">> =>
                    emqx_mgmt_util:response_schema(<<"Update topic rewrite success">>, topic_rewrite_schema()),
                <<"413">> => emqx_mgmt_util:response_error_schema(<<"Rules count exceed max limit">>, [?EXCEED_LIMIT])}}},
    {Path, Metadata, topic_rewrite}.

topic_rewrite(get, _Request) ->
    {200, emqx_rewrite:list()};

topic_rewrite(post, Request) ->
    {ok, Body, _} = cowboy_req:read_body(Request),
    Params = emqx_json:decode(Body, [return_maps]),
    case length(Params) < ?MAX_RULES_LIMIT of
        true ->
            ok = emqx_rewrite:update(Params),
            {200, emqx_rewrite:list()};
        _ ->
            Message = list_to_binary(io_lib:format("Max rewrite rules count is ~p", [?MAX_RULES_LIMIT])),
            {413, #{code => ?EXCEED_LIMIT, message => Message}}
    end.
