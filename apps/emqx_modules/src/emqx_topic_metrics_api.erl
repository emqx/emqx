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

-behaviour(minirest_api).

-import(emqx_mgmt_util, [ properties/1
                        , schema/1
                        , object_schema/1
                        , object_schema/2
                        , object_array_schema/2
                        , error_schema/2
                        ]).

-export([api_spec/0]).

-export([ topic_metrics/2
        , operate_topic_metrics/2
        ]).

-define(ERROR_TOPIC, 'ERROR_TOPIC').

-define(EXCEED_LIMIT, 'EXCEED_LIMIT').

-define(BAD_TOPIC, 'BAD_TOPIC').

-define(BAD_REQUEST, 'BAD_REQUEST').

api_spec() ->
    {[
        topic_metrics_api(),
        operation_topic_metrics_api()
    ],[]}.

properties() ->
    properties([
        {topic, string},
        {create_time, string, <<"Date time, rfc3339">>},
        {reset_time, string, <<"Nullable. Date time, rfc3339.">>},
        {metrics, object, [{'messages.dropped.count', integer},
                           {'messages.dropped.rate', number},
                           {'messages.in.count', integer},
                           {'messages.in.rate', number},
                           {'messages.out.count', integer},
                           {'messages.out.rate', number},
                           {'messages.qos0.in.count', integer},
                           {'messages.qos0.in.rate', number},
                           {'messages.qos0.out.count', integer},
                           {'messages.qos0.out.rate', number},
                           {'messages.qos1.in.count', integer},
                           {'messages.qos1.in.rate', number},
                           {'messages.qos1.out.count', integer},
                           {'messages.qos1.out.rate', number},
                           {'messages.qos2.in.count', integer},
                           {'messages.qos2.in.rate', number},
                           {'messages.qos2.out.count', integer},
                           {'messages.qos2.out.rate', number}]}
    ]).

topic_metrics_api() ->
    MetaData = #{
        get => #{
            description => <<"List topic metrics">>,
            responses => #{
                <<"200">> => object_array_schema(properties(), <<"List topic metrics">>)
            }
        },
        put => #{
            description => <<"Reset topic metrics by topic name, or all">>,
            'requestBody' => object_schema(properties([
                {topic, string, <<"no topic will reset all">>},
                {action, string, <<"Action, default reset">>, [reset]}
            ])),
            responses => #{
                <<"200">> => schema(<<"Reset topic metrics success">>),
                <<"404">> => error_schema(<<"Topic not found">>, [?ERROR_TOPIC])
            }
        },
        post => #{
            description => <<"Create topic metrics">>,
            'requestBody' => object_schema(properties([{topic, string}])),
            responses => #{
                <<"200">> => schema(<<"Create topic metrics success">>),
                <<"409">> => error_schema(<<"Topic metrics max limit">>, [?EXCEED_LIMIT]),
                <<"400">> => error_schema( <<"Topic metrics already exist or bad topic">>
                                         , [?BAD_REQUEST, ?BAD_TOPIC])
            }
        }
    },
    {"/mqtt/topic_metrics", MetaData, topic_metrics}.

operation_topic_metrics_api() ->
    MetaData = #{
        get => #{
            description => <<"Get topic metrics">>,
            parameters => [topic_param()],
            responses => #{
                <<"200">> => object_schema(properties(), <<"Topic metrics">>),
                <<"404">> => error_schema(<<"Topic not found">>, [?ERROR_TOPIC])
            }},
        delete => #{
            description => <<"Deregister topic metrics">>,
            parameters => [topic_param()],
            responses => #{
                <<"204">> => schema(<<"Deregister topic metrics">>),
                <<"404">> => error_schema(<<"Topic not found">>, [?ERROR_TOPIC])
            }
        }
    },
    {"/mqtt/topic_metrics/:topic", MetaData, operate_topic_metrics}.

topic_param() ->
    #{
        name => topic,
        in => path,
        required => true,
        description => <<"Notice: Topic string url must encode">>,
        schema => #{type => string}
    }.

%%--------------------------------------------------------------------
%% api callback
topic_metrics(get, _) ->
    list_metrics();
topic_metrics(put, #{body := #{<<"topic">> := Topic, <<"action">> := <<"reset">>}}) ->
    reset(Topic);
topic_metrics(put, #{body := #{<<"action">> := <<"reset">>}}) ->
    reset();
topic_metrics(post, #{body := #{<<"topic">> := <<>>}}) ->
    {400, 'BAD_REQUEST', <<"Topic can not be empty">>};
topic_metrics(post, #{body := #{<<"topic">> := Topic}}) ->
    register(Topic).

operate_topic_metrics(Method, #{bindings := #{topic := Topic0}}) ->
    Topic = decode_topic(Topic0),
    case Method of
        get ->
            get_metrics(Topic);
        put ->
            register(Topic);
        delete ->
            deregister(Topic)
    end.

decode_topic(Topic) ->
    uri_string:percent_decode(Topic).

%%--------------------------------------------------------------------
%% api apply
list_metrics() ->
    {200, emqx_topic_metrics:metrics()}.

register(Topic) ->
    case emqx_topic_metrics:register(Topic) of
        {error, quota_exceeded} ->
            Message = list_to_binary(io_lib:format("Max topic metrics count is ~p",
                                        [emqx_topic_metrics:max_limit()])),
            {409, #{code => ?EXCEED_LIMIT, message => Message}};
        {error, bad_topic} ->
            Message = list_to_binary(io_lib:format("Bad Topic, topic cannot have wildcard ~p",
                                        [Topic])),
            {400, #{code => ?BAD_TOPIC, message => Message}};
        {error, {quota_exceeded, bad_topic}} ->
            Message = list_to_binary(
                          io_lib:format(
                              "Max topic metrics count is ~p, and topic cannot have wildcard ~p",
                              [emqx_topic_metrics:max_limit(), Topic])),
            {400, #{code => ?BAD_REQUEST, message => Message}};
        {error, already_existed} ->
            Message = list_to_binary(io_lib:format("Topic ~p already registered", [Topic])),
            {400, #{code => ?BAD_TOPIC, message => Message}};
        ok ->
            {200}
    end.

deregister(Topic) ->
    case emqx_topic_metrics:deregister(Topic) of
        {error, topic_not_found} ->
            Message = list_to_binary(io_lib:format("Topic ~p not found", [Topic])),
            {404, #{code => ?ERROR_TOPIC, message => Message}};
        ok ->
            {200}
    end.

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
