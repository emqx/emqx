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

-import(emqx_mgmt_util, [ properties/1
                        , schema/1
                        , object_schema/2
                        , object_array_schema/2
                        , error_schema/2
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
        []
    }.

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


list_topic_api() ->
    Props = properties([{topic, string}]),
    MetaData = #{
        get => #{
            description => <<"List topic">>,
            responses => #{<<"200">> => object_array_schema(Props, <<"List topic">>)}
        }
    },
    {"/mqtt/topic_metrics", MetaData, list_topic}.

list_topic_metrics_api() ->
    MetaData = #{
        get => #{
            description => <<"List topic metrics">>,
            responses => #{
                <<"200">> => object_array_schema(properties(), <<"List topic metrics">>)
            }
        }
    },
    {"/mqtt/topic_metrics/metrics", MetaData, list_topic_metrics}.

get_topic_metrics_api() ->
    MetaData = #{
        get => #{
            description => <<"List topic metrics">>,
            parameters => [topic_param()],
            responses => #{
                <<"200">> => object_schema(properties(), <<"List topic metrics">>)}},
        put => #{
            description => <<"Register topic metrics">>,
            parameters => [topic_param()],
            responses => #{
                <<"200">> => schema(<<"Register topic metrics">>),
                <<"409">> => error_schema(<<"Topic metrics max limit">>, [?EXCEED_LIMIT]),
                <<"400">> => error_schema(<<"Topic metrics already exist">>, [?BAD_REQUEST])
            }
        },
        delete => #{
            description => <<"Deregister topic metrics">>,
            parameters => [topic_param()],
            responses => #{ <<"200">> => schema(<<"Deregister topic metrics">>)}
        }
    },
    {"/mqtt/topic_metrics/metrics/:topic", MetaData, operate_topic_metrics}.

reset_all_topic_metrics_api() ->
    MetaData = #{
        put => #{
            description => <<"Reset all topic metrics">>,
            responses => #{<<"200">> => schema(<<"Reset all topic metrics">>)}
        }
    },
    {"/mqtt/topic_metrics/reset", MetaData, reset_all_topic_metrics}.

reset_topic_metrics_api() ->
    Path = "/mqtt/topic_metrics/reset/:topic",
    MetaData = #{
        put => #{
            description => <<"Reset topic metrics">>,
            parameters => [topic_param()],
            responses => #{<<"200">> => schema(<<"Reset topic metrics">>)}
        }
    },
    {Path, MetaData, reset_topic_metrics}.

topic_param() ->
    #{
        name => topic,
        in => path,
        required => true,
        schema => #{type => string}
    }.

%%--------------------------------------------------------------------
%% api callback
list_topic(get, _) ->
    list_topics().

list_topic_metrics(get, _) ->
    list_metrics().

operate_topic_metrics(Method, #{bindings := #{topic := Topic}}) ->
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

reset_topic_metrics(put, #{bindings := #{topic := Topic}}) ->
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
