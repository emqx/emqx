%%--------------------------------------------------------------------
%% Copyright (c) 2020-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mgmt_api_metrics).

-behaviour(minirest_api).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hocon_types.hrl").
-include_lib("emqx/include/emqx_metrics.hrl").
-include_lib("emqx/include/logger.hrl").

-import(hoconsc, [mk/2, ref/2]).

%% minirest/dashboard_swagger behaviour callbacks
-export([
    api_spec/0,
    paths/0,
    schema/1,
    namespace/0
]).

-export([
    roots/0,
    fields/1
]).

%% http handlers
-export([metrics/2]).

%%--------------------------------------------------------------------
%% minirest behaviour callbacks
%%--------------------------------------------------------------------

namespace() -> undefined.

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    ["/metrics"].

%%--------------------------------------------------------------------
%% http handlers

metrics(get, #{query_string := Qs}) ->
    case maps:get(<<"aggregate">>, Qs, false) of
        true ->
            {200, emqx_mgmt:get_metrics()};
        false ->
            Data = cluster_metrics(emqx:running_nodes()),
            {200, Data}
    end.

%% if no success (unlikely to happen though), log error level
%% otherwise warning.
badrpc_log_level([]) -> error;
badrpc_log_level(_) -> warning.

cluster_metrics(Nodes) ->
    %% each call has 5 seconds timeout, so it's ok to use infinity here
    L1 = emqx_utils:pmap(
        fun(Node) ->
            case emqx_mgmt:get_metrics(Node) of
                {error, Reason} ->
                    #{node => Node, reason => Reason};
                Result when is_list(Result) ->
                    Result
            end
        end,
        Nodes,
        infinity
    ),
    {OK, Failed} =
        lists:partition(
            fun
                (R) when is_list(R) ->
                    true;
                (E) when is_map(E) ->
                    false
            end,
            L1
        ),
    Failed =/= [] orelse
        ?SLOG(
            badrpc_log_level(OK),
            #{msg => "failed_to_fetch_metrics", errors => Failed},
            #{tag => "MGMT"}
        ),
    lists:map(fun({Node, L}) when is_list(L) -> maps:from_list([{node, Node} | L]) end, OK).

%%--------------------------------------------------------------------
%% swagger defines
%%--------------------------------------------------------------------

schema("/metrics") ->
    #{
        'operationId' => metrics,
        get =>
            #{
                description => ?DESC(emqx_metrics),
                tags => [<<"Metrics">>],
                parameters =>
                    [
                        {aggregate,
                            mk(
                                boolean(),
                                #{
                                    in => query,
                                    required => false,
                                    desc => <<"Whether to aggregate all nodes Metrics">>
                                }
                            )}
                    ],
                responses =>
                    #{
                        200 => hoconsc:union(
                            [
                                ref(?MODULE, aggregated_metrics),
                                hoconsc:array(ref(?MODULE, node_metrics))
                            ]
                        )
                    }
            }
    }.

roots() ->
    [].

fields(aggregated_metrics) ->
    properties();
fields(node_metrics) ->
    [{node, mk(binary(), #{desc => <<"Node name">>})}] ++ properties().

properties() ->
    Metrics = lists:append([
        ?BYTES_METRICS,
        ?PACKET_METRICS,
        ?MESSAGE_METRICS,
        ?DELIVERY_METRICS,
        ?CLIENT_METRICS,
        ?SESSION_METRICS,
        ?STASTS_ACL_METRICS,
        ?STASTS_AUTHN_METRICS,
        ?OLP_METRICS
    ]),
    lists:reverse(
        lists:foldl(
            fun({_Type, MetricName, Desc}, Acc) ->
                [m(MetricName, Desc) | Acc]
            end,
            [],
            Metrics
        )
    ).

m(K, Desc) ->
    {K, mk(non_neg_integer(), #{desc => Desc})}.
