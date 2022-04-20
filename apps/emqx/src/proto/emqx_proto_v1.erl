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

-module(emqx_proto_v1).

-behaviour(emqx_bpapi).

-include("bpapi.hrl").

-export([
    introduced_in/0,

    is_running/1,

    get_alarms/2,
    get_stats/1,
    get_metrics/1,

    deactivate_alarm/2,
    delete_all_deactivated_alarms/1,

    clean_authz_cache/1,
    clean_authz_cache/2,
    clean_pem_cache/1
]).

introduced_in() ->
    "5.0.0".

-spec is_running(node()) -> boolean() | {badrpc, term()}.
is_running(Node) ->
    rpc:call(Node, emqx, is_running, []).

-spec get_alarms(node(), all | activated | deactivated) -> [map()].
get_alarms(Node, Type) ->
    rpc:call(Node, emqx_alarm, get_alarms, [Type]).

-spec get_stats(node()) -> emqx_stats:stats() | {badrpc, _}.
get_stats(Node) ->
    rpc:call(Node, emqx_stats, getstats, []).

-spec get_metrics(node()) -> [{emqx_metrics:metric_name(), non_neg_integer()}] | {badrpc, _}.
get_metrics(Node) ->
    rpc:call(Node, emqx_metrics, all, []).

-spec clean_authz_cache(node(), emqx_types:clientid()) ->
    ok
    | {error, not_found}
    | {badrpc, _}.
clean_authz_cache(Node, ClientId) ->
    rpc:call(Node, emqx_authz_cache, drain_cache, [ClientId]).

-spec clean_authz_cache(node()) -> ok | {badrpc, _}.
clean_authz_cache(Node) ->
    rpc:call(Node, emqx_authz_cache, drain_cache, []).

-spec clean_pem_cache(node()) -> ok | {badrpc, _}.
clean_pem_cache(Node) ->
    rpc:call(Node, ssl_pem_cache, clear, []).

-spec deactivate_alarm(node(), binary() | atom()) ->
    ok | {error, not_found} | {badrpc, _}.
deactivate_alarm(Node, Name) ->
    rpc:call(Node, emqx_alarm, deactivate, [Name]).

-spec delete_all_deactivated_alarms(node()) -> ok | {badrpc, _}.
delete_all_deactivated_alarms(Node) ->
    rpc:call(Node, emqx_alarm, delete_all_deactivated_alarms, []).
