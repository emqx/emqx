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

-module(emqx_mgmt_api_routes).

-include_lib("emqx/include/emqx.hrl").

%% API
-behaviour(minirest_api).

-export([api_spec/0]).

-export([ routes/2
        , route/2]).

-export([query/4]).

-define(TOPIC_NOT_FOUND, 'TOPIC_NOT_FOUND').

-define(ROUTES_QSCHEMA, [{<<"topic">>, binary}, {<<"node">>, atom}]).

-import(emqx_mgmt_util, [ object_schema/2
                        , object_array_schema/2
                        , error_schema/2
                        , properties/1
                        , page_params/0
                        , generate_response/1
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
            description => <<"EMQX routes">>,
            parameters => [topic_param(query) , node_param()] ++ page_params(),
            responses => #{
                <<"200">> => object_array_schema(properties(), <<"List route info">>),
                <<"400">> => error_schema(<<"Invalid parameters">>, ['INVALID_PARAMETER'])
            }
        }
    },
    {"/routes", Metadata, routes}.

route_api() ->
    Metadata = #{
        get => #{
            description => <<"EMQX routes">>,
            parameters => [topic_param(path)],
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
routes(get, #{query_string := QString}) ->
    list(generate_topic(QString)).

route(get, #{bindings := Bindings}) ->
    lookup(generate_topic(Bindings)).

%%%==============================================================================================
%% api apply
list(QString) ->
    Response = emqx_mgmt_api:node_query(
                 node(), QString, emqx_route, ?ROUTES_QSCHEMA, {?MODULE, query}),
    generate_response(Response).

lookup(#{topic := Topic}) ->
    case emqx_mgmt:lookup_routes(Topic) of
        [] ->
            {404, #{code => ?TOPIC_NOT_FOUND, message => <<"Topic not found">>}};
        [Route] ->
            {200, format(Route)}
    end.

%%%==============================================================================================
%% internal
generate_topic(Params = #{<<"topic">> := Topic}) ->
    Params#{<<"topic">> => uri_string:percent_decode(Topic)};
generate_topic(Params = #{topic := Topic}) ->
    Params#{topic => uri_string:percent_decode(Topic)};
generate_topic(Params) -> Params.

query(Tab, {Qs, _}, Continuation, Limit) ->
    Ms = qs2ms(Qs, [{{route, '_', '_'}, [], ['$_']}]),
    emqx_mgmt_api:select_table_with_count(Tab, Ms, Continuation, Limit, fun format/1).

qs2ms([], Res) -> Res;
qs2ms([{topic,'=:=', T} | Qs], [{{route, _, N}, [], ['$_']}]) ->
    qs2ms(Qs, [{{route, T, N}, [], ['$_']}]);
qs2ms([{node,'=:=', N} | Qs], [{{route, T, _}, [], ['$_']}]) ->
    qs2ms(Qs, [{{route, T, N}, [], ['$_']}]).

format(#route{topic = Topic, dest = {_, Node}}) ->
    #{topic => Topic, node => Node};
format(#route{topic = Topic, dest = Node}) ->
    #{topic => Topic, node => Node}.

topic_param(In) ->
    #{
        name => topic,
        in => In,
        required => In == path,
        description => <<"Topic string, url encoding">>,
        schema => #{type => string}
    }.

node_param()->
    #{
        name => node,
        in => query,
        required => false,
        description => <<"Node">>,
        schema => #{type => string}
    }.
