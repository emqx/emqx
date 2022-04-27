%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_prometheus_mria).

-export([
    deregister_cleanup/1,
    collect_mf/2
]).

-include_lib("prometheus/include/prometheus.hrl").

%% Please don't remove this attribute, prometheus uses it to
%% automatically register collectors.
-behaviour(prometheus_collector).

%%====================================================================
%% Macros
%%====================================================================

-define(METRIC_NAME_PREFIX, "emqx_mria_").

%%====================================================================
%% Collector API
%%====================================================================

%% @private
deregister_cleanup(_) -> ok.

%% @private
-spec collect_mf(_Registry, Callback) -> ok when
    _Registry :: prometheus_registry:registry(),
    Callback :: prometheus_collector:callback().
collect_mf(_Registry, Callback) ->
    case mria_rlog:backend() of
        rlog ->
            Metrics = metrics(),
            _ = [add_metric_family(Metric, Callback) || Metric <- Metrics],
            ok;
        mnesia ->
            ok
    end.

add_metric_family({Name, Metrics}, Callback) ->
    Callback(
        prometheus_model_helpers:create_mf(
            ?METRIC_NAME(Name),
            <<"">>,
            gauge,
            catch_all(Metrics)
        )
    ).

%%====================================================================
%% Internal functions
%%====================================================================

metrics() ->
    Metrics =
        case mria_rlog:role() of
            replicant ->
                [lag, bootstrap_time, bootstrap_num_keys, message_queue_len, replayq_len];
            core ->
                [last_intercepted_trans, weight, replicants, server_mql]
        end,
    [{MetricId, fun() -> get_shard_metric(MetricId) end} || MetricId <- Metrics].

get_shard_metric(Metric) ->
    %% TODO: only report shards that are up
    [
        {[{shard, Shard}], get_shard_metric(Metric, Shard)}
     || Shard <- mria_schema:shards(), Shard =/= undefined
    ].

get_shard_metric(replicants, Shard) ->
    length(mria_status:agents(Shard));
get_shard_metric(Metric, Shard) ->
    case mria_status:get_shard_stats(Shard) of
        #{Metric := Value} when is_number(Value) ->
            Value;
        _ ->
            undefined
    end.

catch_all(DataFun) ->
    try
        DataFun()
    catch
        _:_ -> undefined
    end.
