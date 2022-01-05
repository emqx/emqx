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
        %% Get all nodes metrics and accumulate all of these
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
%% HTTP Callbacks
%%--------------------------------------------------------------------

topic_metrics(get, _) ->
    case cluster_accumulation_metrics() of
        {error, Reason} ->
            {500, Reason};
        {ok, Metrics} ->
            {200, Metrics}
    end;

topic_metrics(put, #{body := #{<<"topic">> := Topic, <<"action">> := <<"reset">>}}) ->
    case reset(Topic) of
        ok -> {200};
        {error, Reason} -> reason2httpresp(Reason)
    end;
topic_metrics(put, #{body := #{<<"action">> := <<"reset">>}}) ->
    reset(),
    {200};

topic_metrics(post, #{body := #{<<"topic">> := <<>>}}) ->
    {400, 'BAD_REQUEST', <<"Topic can not be empty">>};
topic_metrics(post, #{body := #{<<"topic">> := Topic}}) ->
    case emqx_modules_conf:add_topic_metrics(Topic) of
        {ok, Topic} ->
            {200};
        {error, Reason} ->
            reason2httpresp(Reason)
    end.

operate_topic_metrics(get, #{bindings := #{topic := Topic0}}) ->
    case cluster_accumulation_metrics(emqx_http_lib:uri_decode(Topic0)) of
        {ok, Metrics} ->
            {200, Metrics};
        {error, Reason} ->
            reason2httpresp(Reason)
    end;

operate_topic_metrics(delete, #{bindings := #{topic := Topic0}}) ->
    case emqx_modules_conf:remove_topic_metrics(emqx_http_lib:uri_decode(Topic0)) of
        ok -> {200};
        {error, Reason} -> reason2httpresp(Reason)
    end.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

cluster_accumulation_metrics() ->
    case multicall(emqx_topic_metrics, metrics, []) of
        {SuccResList, []} ->
            {ok, accumulate_nodes_metrics(SuccResList)};
        {_, FailedNodes} ->
            {error, {badrpc, FailedNodes}}
    end.

cluster_accumulation_metrics(Topic) ->
    case multicall(emqx_topic_metrics, metrics, [Topic]) of
        {SuccResList, []} ->
            case lists:filter(fun({error, _}) -> false; (_) -> true
                              end, SuccResList) of
                [] -> {error, topic_not_found};
                TopicMetrics ->
                    NTopicMetrics = [ [T] || T <- TopicMetrics],
                    [AccMetrics] = accumulate_nodes_metrics(NTopicMetrics),
                    {ok, AccMetrics}
            end;
        {_, FailedNodes} ->
            {error, {badrpc, FailedNodes}}
    end.

accumulate_nodes_metrics(NodesTopicMetrics) ->
    AccMap = lists:foldl(fun(TopicMetrics, ExAcc) ->
        MetricsMap = lists:foldl(
                       fun(#{topic := Topic,
                             metrics := Metrics,
                             create_time := CreateTime}, Acc) ->
                               Acc#{Topic => {Metrics, CreateTime}}
                       end, #{}, TopicMetrics),
        accumulate_metrics(MetricsMap, ExAcc)
    end, #{}, NodesTopicMetrics),
    maps:fold(fun(Topic, {Metrics, CreateTime1}, Acc1) ->
        [#{topic => Topic,
           metrics => Metrics,
           create_time => CreateTime1} | Acc1]
    end, [], AccMap).

%% @doc TopicMetricsIn :: #{<<"topic">> := {Metrics, CreateTime}}
accumulate_metrics(TopicMetricsIn, TopicMetricsAcc) ->
    Topics = maps:keys(TopicMetricsIn),
    lists:foldl(fun(Topic, Acc) ->
        {Metrics, CreateTime} = maps:get(Topic, TopicMetricsIn),
        NMetrics = do_accumulation_metrics(
                     Metrics,
                     maps:get(Topic, TopicMetricsAcc, undefined)
                    ),
        maps:put(Topic, {NMetrics, CreateTime}, Acc)
    end, #{}, Topics).

%% @doc MetricsIn :: #{'messages.dropped.rate' :: integer(), ...}
do_accumulation_metrics(MetricsIn, undefined) -> MetricsIn;
do_accumulation_metrics(MetricsIn, MetricsAcc) ->
    Keys = maps:keys(MetricsIn),
    lists:foldl(fun(Key, Acc) ->
        InVal = maps:get(Key, MetricsIn),
        NVal = InVal + maps:get(Key, MetricsAcc, 0),
        maps:put(Key, NVal, Acc)
    end, #{}, Keys).

reset() ->
    _ = multicall(emqx_topic_metrics, reset, []),
    ok.

reset(Topic) ->
    case multicall(emqx_topic_metrics, reset, [Topic]) of
        {SuccResList, []} ->
            case lists:filter(fun({error, _}) -> true; (_) -> false
                              end, SuccResList) of
                [{error, Reason} | _] ->
                    {error, Reason};
                [] ->
                    ok
            end
    end.

%%--------------------------------------------------------------------
%% utils

multicall(M, F, A) ->
    emqx_rpc:multicall(mria_mnesia:running_nodes(), M, F, A).

reason2httpresp(quota_exceeded) ->
    Msg = list_to_binary(
            io_lib:format("Max topic metrics count is ~p",
                          [emqx_topic_metrics:max_limit()])),
    {409, #{code => ?EXCEED_LIMIT, message => Msg}};
reason2httpresp(bad_topic) ->
    Msg = <<"Bad Topic, topic cannot have wildcard">>,
    {400, #{code => ?BAD_TOPIC, message => Msg}};
reason2httpresp({quota_exceeded, bad_topic}) ->
    Msg = list_to_binary(
            io_lib:format(
                "Max topic metrics count is ~p, and topic cannot have wildcard",
                [emqx_topic_metrics:max_limit()])),
    {400, #{code => ?BAD_REQUEST, message => Msg}};
reason2httpresp(already_existed) ->
    Msg = <<"Topic already registered">>,
    {400, #{code => ?BAD_TOPIC, message => Msg}};
reason2httpresp(topic_not_found) ->
    Msg = <<"Topic not found">>,
    {404, #{code => ?ERROR_TOPIC, message => Msg}};
reason2httpresp(not_found) ->
    Msg = <<"Topic not found">>,
    {404, #{code => ?ERROR_TOPIC, message => Msg}}.
