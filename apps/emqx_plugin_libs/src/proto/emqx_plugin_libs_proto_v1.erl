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

-module(emqx_plugin_libs_proto_v1).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,

    get_metrics/3
]).

-include_lib("emqx/include/bpapi.hrl").

introduced_in() ->
    "5.0.0".

-spec get_metrics(
    node(),
    emqx_metrics_worker:handler_name(),
    emqx_metrics_worker:metric_id()
) -> emqx_metrics_worker:metrics() | {badrpc, _}.
get_metrics(Node, HandlerName, MetricId) ->
    rpc:call(Node, emqx_metrics_worker, get_metrics, [HandlerName, MetricId]).
