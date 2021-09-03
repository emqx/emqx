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
-behaviour(minirest_api).

-export([api_spec/0]).

-export([ routes/2
        , route/2]).

-define(TOPIC_NOT_FOUND, 'TOPIC_NOT_FOUND').

-import(emqx_mgmt_util, [ object_schema/2
                        , object_array_schema/2
                        , error_schema/2
                        , properties/1
                        , page_params/0
                        ]).

api_spec() ->
    {[routes_api(), route_api()], []}.

properties() ->
    properties([
        {topic, string},
        {node, string}
    ]).

routes_api() ->
    Metadata = #{
        get => #{
            description => <<"EMQ X routes">>,
            parameters => page_params(),
            responses => #{
                <<"200">> => object_array_schema(properties(), <<"List route info">>)
            }
        }
    },
    {"/routes", Metadata, routes}.

route_api() ->
    Metadata = #{
        get => #{
            description => <<"EMQ X routes">>,
            parameters => [#{
                name => topic,
                in => path,
                required => true,
                description => <<"Topic string, url encoding">>,
                schema => #{type => string}
            }],
            responses => #{
                <<"200">> =>
                    object_schema(properties(), <<"Route info">>),
                <<"404">> =>
                    error_schema(<<"Topic not found">>, [?TOPIC_NOT_FOUND])
            }
        }
    },
    {"/routes/:topic", Metadata, route}.

%%%==============================================================================================
%% parameters trans
routes(get, #{query_string := Qs}) ->
    list(Qs).

route(get, #{bindings := Bindings}) ->
    lookup(Bindings).

%%%==============================================================================================
%% api apply
list(Params) ->
    Response = emqx_mgmt_api:paginate(emqx_route, Params, fun format/1),
    {200, Response}.

lookup(#{topic := Topic}) ->
    case emqx_mgmt:lookup_routes(Topic) of
        [] ->
            {404, #{code => ?TOPIC_NOT_FOUND, message => <<"Topic not found">>}};
        [Route] ->
            {200, format(Route)}
    end.

%%%==============================================================================================
%% internal
format(#route{topic = Topic, dest = {_, Node}}) ->
    #{topic => Topic, node => Node};
format(#route{topic = Topic, dest = Node}) ->
    #{topic => Topic, node => Node}.
