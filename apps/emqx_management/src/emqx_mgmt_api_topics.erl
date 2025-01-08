%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

%% API
-behaviour(minirest_api).

-export([
    api_spec/0,
    paths/0,
    schema/1,
    fields/1,
    namespace/0
]).

-export([
    topics/2,
    topic/2
]).

-define(TOPIC_NOT_FOUND, 'TOPIC_NOT_FOUND').

-define(TOPICS_QUERY_SCHEMA, [{<<"topic">>, binary}, {<<"node">>, atom}]).
-define(TAGS, [<<"Topics">>]).

namespace() -> undefined.

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
            })},
        {session,
            hoconsc:mk(binary(), #{
                desc => <<"Session ID">>,
                required => false
            })}
    ].

%%%==============================================================================================
%% parameters trans
topics(get, #{query_string := Qs}) ->
    do_list(Qs).

topic(get, #{bindings := Bindings}) ->
    lookup(Bindings).

%%%==============================================================================================
%% api apply
do_list(Params) ->
    try
        Pager = parse_pager_params(Params),
        {_, Query} = emqx_mgmt_api:parse_qstring(Params, ?TOPICS_QUERY_SCHEMA),
        Stream = mk_topic_stream(qs2ms(Query)),
        QResult = eval_topic_query(Stream, Pager, emqx_mgmt_api:init_query_result()),
        {200, format_list_response(Pager, Query, QResult)}
    catch
        throw:{error, page_limit_invalid} ->
            {400, #{code => <<"INVALID_PARAMETER">>, message => <<"page_limit_invalid">>}};
        error:{invalid_topic_filter, _} ->
            {400, #{code => <<"INVALID_PARAMTER">>, message => <<"topic_filter_invalid">>}}
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

parse_pager_params(Params) ->
    try emqx_mgmt_api:parse_pager_params(Params) of
        Pager = #{} ->
            Pager;
        false ->
            throw({error, page_limit_invalid})
    catch
        error:badarg ->
            throw({error, page_limit_invalid})
    end.

-spec qs2ms({list(), list()}) -> tuple().
qs2ms({Qs, _}) ->
    lists:foldl(fun gen_match_spec/2, {'_', '_'}, Qs).

gen_match_spec({topic, '=:=', QTopic}, {_MTopic, MNode}) when is_atom(MNode) ->
    case emqx_topic:parse(QTopic) of
        {#share{group = Group, topic = Topic}, _SubOpts} ->
            {Topic, {Group, MNode}};
        {Topic, _SubOpts} ->
            {Topic, MNode}
    end;
gen_match_spec({node, '=:=', QNode}, {MTopic, _MDest}) ->
    {MTopic, QNode}.

mk_topic_stream(Spec = {MTopic, _MDest = '_'}) ->
    emqx_utils_stream:chain(emqx_router:stream(Spec), mk_persistent_topic_stream(MTopic));
mk_topic_stream(Spec) ->
    %% NOTE: Assuming that no persistent topic ever matches a query with `node` filter.
    emqx_router:stream(Spec).

mk_persistent_topic_stream(Spec) ->
    case emqx_persistent_message:is_persistence_enabled() of
        true ->
            emqx_persistent_session_ds_router:stream(Spec);
        false ->
            emqx_utils_stream:empty()
    end.

eval_count() ->
    emqx_router:stats(n_routes) + eval_persistent_count().

eval_persistent_count() ->
    case emqx_persistent_message:is_persistence_enabled() of
        true ->
            emqx_persistent_session_ds_router:stats(n_routes);
        false ->
            0
    end.

eval_topic_query(Stream, QState = #{limit := Limit}, QResult) ->
    case emqx_utils_stream:consume(Limit, Stream) of
        {Rows, NStream} ->
            case emqx_mgmt_api:accumulate_query_rows(node(), Rows, QState, QResult) of
                {more, NQResult} ->
                    eval_topic_query(NStream, QState, NQResult);
                {enough, NQResult} ->
                    finalize_query(false, NQResult)
            end;
        Rows when is_list(Rows) ->
            {_, NQResult} = emqx_mgmt_api:accumulate_query_rows(node(), Rows, QState, QResult),
            finalize_query(true, NQResult)
    end.

finalize_query(Complete, QResult = #{overflow := Overflow}) ->
    HasNext = Overflow orelse not Complete,
    QResult#{complete => Complete, hasnext => HasNext}.

format_list_response(Meta, Query, QResult = #{rows := RowsAcc}) ->
    #{
        meta => format_response_meta(Meta, Query, QResult),
        data => lists:flatmap(
            fun({_Node, Rows}) -> [format(R) || R <- Rows] end,
            RowsAcc
        )
    }.

format_response_meta(Meta, _Query, #{hasnext := HasNext, complete := true, cursor := Cursor}) ->
    Meta#{hasnext => HasNext, count => Cursor};
format_response_meta(Meta, _Query = {[], []}, #{hasnext := HasNext}) ->
    Meta#{hasnext => HasNext, count => eval_count()};
format_response_meta(Meta, _Query, #{hasnext := HasNext}) ->
    Meta#{hasnext => HasNext}.

format(#route{topic = Topic, dest = {Group, Node}}) ->
    #{
        topic => emqx_topic:maybe_format_share(emqx_topic:make_shared_record(Group, Topic)),
        node => Node
    };
format(#route{topic = Topic, dest = Node}) when is_atom(Node) ->
    #{topic => Topic, node => Node};
format(#route{topic = Topic, dest = SessionId}) when is_binary(SessionId) ->
    #{topic => Topic, session => SessionId}.

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
