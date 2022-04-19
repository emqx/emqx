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
-module(emqx_mgmt_api_metrics_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_mgmt_api_test_util:init_suite(),
    Config.

end_per_suite(_) ->
    emqx_mgmt_api_test_util:end_suite().

t_metrics_api(_) ->
    {ok, MetricsResponse} = request_helper("metrics?aggregate=true"),
    MetricsFromAPI = emqx_json:decode(MetricsResponse, [return_maps]),
    AggregateMetrics = emqx_mgmt:get_metrics(),
    match_helper(AggregateMetrics, MetricsFromAPI).

t_single_node_metrics_api(_) ->
    {ok, MetricsResponse} = request_helper("metrics"),
    [MetricsFromAPI] = emqx_json:decode(MetricsResponse, [return_maps]),
    LocalNodeMetrics = maps:from_list(
        emqx_mgmt:get_metrics(node()) ++ [{node, to_bin(node())}]
    ),
    match_helper(LocalNodeMetrics, MetricsFromAPI).

match_helper(SystemMetrics, MetricsFromAPI) ->
    length_equal(SystemMetrics, MetricsFromAPI),
    Fun =
        fun(Key, {SysMetrics, APIMetrics}) ->
            Value = maps:get(Key, SysMetrics),
            ?assertEqual(Value, maps:get(to_bin(Key), APIMetrics)),
            {Value, {SysMetrics, APIMetrics}}
        end,
    lists:mapfoldl(Fun, {SystemMetrics, MetricsFromAPI}, maps:keys(SystemMetrics)).

length_equal(SystemMetrics, MetricsFromAPI) ->
    ?assertEqual(erlang:length(maps:keys(SystemMetrics)), erlang:length(maps:keys(MetricsFromAPI))).

request_helper(Path) ->
    MetricsPath = emqx_mgmt_api_test_util:api_path([Path]),
    emqx_mgmt_api_test_util:request_api(get, MetricsPath).

to_bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
to_bin(L) when is_list(L) -> list_to_binary(L);
to_bin(I) when is_integer(I) -> integer_to_binary(I);
to_bin(B) when is_binary(B) -> B.
