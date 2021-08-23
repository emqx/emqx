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
%% TODO: refactor uri path
-module(emqx_topic_metrics_api).

-behavior(minirest_api).

-import(emqx_mgmt_util, [ request_body_schema/1
                        , response_schema/1
                        , response_schema/2
                        , response_array_schema/2
                        , response_error_schema/2
                        ]).

-export([api_spec/0]).

-export([ list_topic/2
        , list_topic_metrics/2
        , operate_topic_metrics/2
        , reset_all_topic_metrics/2
        , reset_topic_metrics/2
        ]).

-define(ERROR_TOPIC, 'ERROR_TOPIC').

-define(EXCEED_LIMIT, 'EXCEED_LIMIT').

-define(BAD_REQUEST, 'BAD_REQUEST').

api_spec() ->
    {
        [
            list_topic_api(),
            list_topic_metrics_api(),
            get_topic_metrics_api(),
            reset_all_topic_metrics_api(),
            reset_topic_metrics_api()
        ],
        [
            topic_metrics_schema()
        ]
    }.

topic_metrics_schema() ->
    #{
        topic_metrics => #{
            type => object,
            properties => #{
                topic => #{type => string},
                create_time => #{
                    type => string,
                    description => <<"Date time, rfc3339">>
                },
                reset_time => #{
                    type => string,
                    description => <<"Nullable. Date time, rfc3339.">>
                },
                metrics => #{
                    type => object,
                    properties => #{
                        'messages.dropped.count'  => #{type => integer},
                        'messages.dropped.rate'   => #{type => number},
                        'messages.in.count'       => #{type => integer},
                        'messages.in.rate'        => #{type => number},
                        'messages.out.count'      => #{type => integer},
                        'messages.out.rate'       => #{type => number},
                        'messages.qos0.in.count'  => #{type => integer},
                        'messages.qos0.in.rate'   => #{type => number},
                        'messages.qos0.out.count' => #{type => integer},
                        'messages.qos0.out.rate'  => #{type => number},
                        'messages.qos1.in.count'  => #{type => integer},
                        'messages.qos1.in.rate'   => #{type => number},
                        'messages.qos1.out.count' => #{type => integer},
                        'messages.qos1.out.rate'  => #{type => number},
                        'messages.qos2.in.count'  => #{type => integer},
                        'messages.qos2.in.rate'   => #{type => number},
                        'messages.qos2.out.count' => #{type => integer},
                        'messages.qos2.out.rate'  => #{type => number}
                    }
                }
            }
        }
    }.

list_topic_api() ->
    Path = "/mqtt/topic_metrics",
    TopicSchema = #{
        type => object,
        properties => #{
            topic => #{
                type => string}}},
    MetaData = #{
        get => #{
            description => <<"List topic">>,
            responses => #{
                <<"200">> =>
                    response_array_schema(<<"List topic">>, TopicSchema)}}},
    {Path, MetaData, list_topic}.

list_topic_metrics_api() ->
    Path = "/mqtt/topic_metrics/metrics",
    MetaData = #{
        get => #{
            description => <<"List topic metrics">>,
            responses => #{
                <<"200">> =>
                    response_array_schema(<<"List topic metrics">>, topic_metrics)}}},
    {Path, MetaData, list_topic_metrics}.

get_topic_metrics_api() ->
    Path = "/mqtt/topic_metrics/metrics/:topic",
    MetaData = #{
        get => #{
            description => <<"List topic metrics">>,
            parameters => [topic_param()],
            responses => #{
                <<"200">> =>
                    response_schema(<<"List topic metrics">>, topic_metrics)}},
        put => #{
            description => <<"Register topic metrics">>,
            parameters => [topic_param()],
            responses => #{
                <<"200">> =>
                    response_schema(<<"Register topic metrics">>),
                <<"409">> =>
                    response_error_schema(<<"Topic metrics max limit">>, [?EXCEED_LIMIT]),
                <<"400">> =>
                    response_error_schema(<<"Topic metrics already exist">>, [?BAD_REQUEST])}},
        delete => #{
            description => <<"Deregister topic metrics">>,
            parameters => [topic_param()],
            responses => #{
                <<"200">> =>
                    response_schema(<<"Deregister topic metrics">>)}}},
    {Path, MetaData, operate_topic_metrics}.

reset_all_topic_metrics_api() ->
    Path = "/mqtt/topic_metrics/reset",
    MetaData = #{
        put => #{
            description => <<"Reset all topic metrics">>,
            responses => #{
                <<"200">> =>
                    response_schema(<<"Reset all topic metrics">>)}}},
    {Path, MetaData, reset_all_topic_metrics}.

reset_topic_metrics_api() ->
    Path = "/mqtt/topic_metrics/reset/:topic",
    MetaData = #{
        put => #{
            description => <<"Reset topic metrics">>,
            parameters => [topic_param()],
            responses => #{
                <<"200">> =>
                    response_schema(<<"Reset topic metrics">>)}}},
    {Path, MetaData, reset_topic_metrics}.

topic_param() ->
    #{
        name => topic,
        in => path,
        required => true,
        schema => #{type => string}
    }.

topic_param(Request) ->
    cowboy_req:binding(topic, Request).

%%--------------------------------------------------------------------
%% api callback
list_topic(get, _) ->
    list_topics().

list_topic_metrics(get, _) ->
    list_metrics().

operate_topic_metrics(Method, Request) ->
    Topic = topic_param(Request),
    case Method of
        get ->
            get_metrics(Topic);
        put ->
            register(Topic);
        delete ->
            deregister(Topic)
    end.

reset_all_topic_metrics(put, _) ->
    reset().

reset_topic_metrics(put, Request) ->
    Topic = topic_param(Request),
    reset(Topic).

%%--------------------------------------------------------------------
%% api apply
list_topics() ->
    {200, emqx_topic_metrics:all_registered_topics()}.

list_metrics() ->
    {200, emqx_topic_metrics:metrics()}.

register(Topic) ->
    case emqx_topic_metrics:register(Topic) of
        {error, quota_exceeded} ->
            Message = list_to_binary(io_lib:format("Max topic metrics count is  ~p",
                                        [emqx_topic_metrics:max_limit()])),
            {409, #{code => ?EXCEED_LIMIT, message => Message}};
        {error, already_existed} ->
            Message = list_to_binary(io_lib:format("Topic ~p already registered", [Topic])),
            {400, #{code => ?BAD_REQUEST, message => Message}};
        ok ->
            {200}
    end.

deregister(Topic) ->
    _ = emqx_topic_metrics:deregister(Topic),
    {200}.

get_metrics(Topic) ->
    case emqx_topic_metrics:metrics(Topic) of
        {error, topic_not_found} ->
            Message = list_to_binary(io_lib:format("Topic ~p not found", [Topic])),
            {404, #{code => ?ERROR_TOPIC, message => Message}};
        Metrics ->
            {200, Metrics}
    end.

reset() ->
    ok = emqx_topic_metrics:reset(),
    {200}.

reset(Topic) ->
    case emqx_topic_metrics:reset(Topic) of
        {error, topic_not_found} ->
            Message = list_to_binary(io_lib:format("Topic ~p not found", [Topic])),
            {404, #{code => ?ERROR_TOPIC, message => Message}};
        ok ->
            {200}
    end.
