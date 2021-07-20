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

-module(emqx_mgmt_api_routes).

-include_lib("emqx/include/emqx.hrl").

%% API
-behavior(minirest_api).

-export([api_spec/0]).

-export([ routes/2
        , route/2]).

-define(TOPIC_NOT_FOUND, 'TOPIC_NOT_FOUND').

api_spec() ->
    {
        [routes_api(), route_api()],
        [route_schema()]
    }.

route_schema() ->
    #{
        route => #{
            type => object,
            properties => #{
                topic => #{
                    type => string},
                node => #{
                    type => string,
                    example => node()}}}}.

routes_api() ->
    Metadata = #{
        get => #{
            description => "EMQ X routes",
            parameters => [
                #{
                    name => page,
                    in => query,
                    description => <<"Page">>,
                    schema => #{type => integer},
                    default => 1
                },
                #{
                    name => limit,
                    in => query,
                    description => <<"Page size">>,
                    schema => #{type => integer},
                    default => emqx_mgmt:max_row_limit()
                }],
            responses => #{
                <<"200">> =>
                    emqx_mgmt_util:response_array_schema("List route info", <<"route">>)}}},
    {"/routes", Metadata, routes}.

route_api() ->
    Metadata = #{
        get => #{
            description => "EMQ X routes",
            parameters => [#{
                name => topic,
                in => path,
                required => true,
                description => <<"topic">>,
                schema => #{type => string}
            }],
            responses => #{
                <<"200">> =>
                    emqx_mgmt_util:response_schema(<<"Route info">>, <<"route">>),
                <<"404">> =>
                    emqx_mgmt_util:not_found_schema(<<"Topic not found">>, [?TOPIC_NOT_FOUND])
            }}},
    {"/routes/:topic", Metadata, route}.

%%%==============================================================================================
%% parameters trans
routes(get, Request) ->
    Params = cowboy_req:parse_qs(Request),
    list(Params).

route(get, Request) ->
    Topic = cowboy_req:binding(topic, Request),
    lookup(#{topic => Topic}).

%%%==============================================================================================
%% api apply
list(Params) ->
    Data = emqx_mgmt_api:paginate(emqx_route, Params, fun format/1),
    Response = emqx_json:encode(Data),
    {200, Response}.

lookup(#{topic := Topic}) ->
    case emqx_mgmt:lookup_routes(Topic) of
        [] ->
            NotFound = #{code => ?TOPIC_NOT_FOUND, reason => <<"Topic not found">>},
            Response = emqx_json:encode(NotFound),
            {404, Response};
        [Route] ->
            Data = format(Route),
            Response = emqx_json:encode(Data),
            {200, Response}
    end.

%%%==============================================================================================
%% internal
format(#route{topic = Topic, dest = {_, Node}}) ->
    #{topic => Topic, node => Node};
format(#route{topic = Topic, dest = Node}) ->
    #{topic => Topic, node => Node}.
