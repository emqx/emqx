%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mgmt_api_topics).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_router.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

%% API
-behaviour(minirest_api).

-export([
    api_spec/0,
    paths/0,
    schema/1,
    fields/1
]).

-export([
    topics/2,
    topic/2
]).

-export([qs2ms/2, format/1]).

-define(TOPIC_NOT_FOUND, 'TOPIC_NOT_FOUND').

-define(TOPICS_QUERY_SCHEMA, [{<<"topic">>, binary}, {<<"node">>, atom}]).
-define(TAGS, [<<"Topics">>]).

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true, translate_body => true}).

paths() ->
    ["/topics", "/topics/:topic"].

schema("/topics") ->
    #{
        'operationId' => topics,
        get => #{
            description => ?DESC(topic_list),
            tags => ?TAGS,
            parameters => [
                topic_param(query),
                node_param(),
                hoconsc:ref(emqx_dashboard_swagger, page),
                hoconsc:ref(emqx_dashboard_swagger, limit)
            ],
            responses => #{
                200 => [
                    {data, hoconsc:mk(hoconsc:array(hoconsc:ref(topic)), #{})},
                    {meta, hoconsc:mk(hoconsc:ref(emqx_dashboard_swagger, meta), #{})}
                ]
            }
        }
    };
schema("/topics/:topic") ->
    #{
        'operationId' => topic,
        get => #{
            description => ?DESC(topic_info_by_name),
            tags => ?TAGS,
            parameters => [topic_param(path)],
            responses => #{
                200 => hoconsc:mk(hoconsc:array(hoconsc:ref(topic)), #{}),
                404 =>
                    emqx_dashboard_swagger:error_codes(['TOPIC_NOT_FOUND'], <<"Topic not found">>)
            }
        }
    }.

fields(topic) ->
    [
        {topic,
            hoconsc:mk(binary(), #{
                desc => <<"Topic Name">>,
                required => true
            })},
        {node,
            hoconsc:mk(binary(), #{
                desc => <<"Node">>,
                required => true
            })}
    ].

%%%==============================================================================================
%% parameters trans
topics(get, #{query_string := Qs}) ->
    do_list(generate_topic(Qs)).

topic(get, #{bindings := Bindings}) ->
    lookup(generate_topic(Bindings)).

%%%==============================================================================================
%% api apply
do_list(Params) ->
    case
        emqx_mgmt_api:node_query(
            node(),
            ?ROUTE_TAB,
            Params,
            ?TOPICS_QUERY_SCHEMA,
            fun ?MODULE:qs2ms/2,
            fun ?MODULE:format/1
        )
    of
        {error, page_limit_invalid} ->
            {400, #{code => <<"INVALID_PARAMETER">>, message => <<"page_limit_invalid">>}};
        {error, Node, Error} ->
            Message = list_to_binary(io_lib:format("bad rpc call ~p, Reason ~p", [Node, Error])),
            {500, #{code => <<"NODE_DOWN">>, message => Message}};
        Response ->
            {200, Response}
    end.

lookup(#{topic := Topic}) ->
    case emqx_router:lookup_routes(Topic) of
        [] ->
            {404, #{code => ?TOPIC_NOT_FOUND, message => <<"Topic not found">>}};
        Routes when is_list(Routes) ->
            Formatted = [format(Route) || Route <- Routes],
            {200, Formatted}
    end.

%%%==============================================================================================
%% internal
generate_topic(Params = #{<<"topic">> := Topic}) ->
    Params#{<<"topic">> => Topic};
generate_topic(Params = #{topic := Topic}) ->
    Params#{topic => Topic};
generate_topic(Params) ->
    Params.

-spec qs2ms(atom(), {list(), list()}) -> emqx_mgmt_api:match_spec_and_filter().
qs2ms(_Tab, {Qs, _}) ->
    #{
        match_spec => gen_match_spec(Qs, [{{route, '_', '_'}, [], ['$_']}]),
        fuzzy_fun => undefined
    }.

gen_match_spec([], Res) ->
    Res;
gen_match_spec([{topic, '=:=', T} | Qs], [{{route, _, N}, [], ['$_']}]) ->
    gen_match_spec(Qs, [{{route, T, N}, [], ['$_']}]);
gen_match_spec([{node, '=:=', N} | Qs], [{{route, T, _}, [], ['$_']}]) ->
    gen_match_spec(Qs, [{{route, T, N}, [], ['$_']}]).

format(#route{topic = Topic, dest = {_, Node}}) ->
    #{topic => Topic, node => Node};
format(#route{topic = Topic, dest = Node}) ->
    #{topic => Topic, node => Node}.

topic_param(In) ->
    {
        topic,
        hoconsc:mk(binary(), #{
            desc => <<"Topic Name">>,
            in => In,
            required => (In == path),
            example => <<"">>
        })
    }.

node_param() ->
    {
        node,
        hoconsc:mk(binary(), #{
            desc => <<"Node Name">>,
            in => query,
            required => false,
            example => node()
        })
    }.
