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

-module(emqx_resource_proto_v1).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,
    create/5,
    create_dry_run/2,
    recreate/4,
    remove/1,
    reset_metrics/1
]).

-include_lib("emqx/include/bpapi.hrl").
-include("emqx_resource.hrl").

introduced_in() ->
    "5.0.0".

-spec create(
    resource_id(),
    resource_group(),
    resource_type(),
    resource_config(),
    create_opts()
) ->
    {ok, resource_data() | 'already_created'} | {error, Reason :: term()}.
create(ResId, Group, ResourceType, Config, Opts) ->
    emqx_cluster_rpc:multicall(emqx_resource, create_local, [
        ResId, Group, ResourceType, Config, Opts
    ]).

-spec create_dry_run(
    resource_type(),
    resource_config()
) ->
    ok | {error, Reason :: term()}.
create_dry_run(ResourceType, Config) ->
    emqx_cluster_rpc:multicall(emqx_resource, create_dry_run_local, [ResourceType, Config]).

-spec recreate(
    resource_id(),
    resource_type(),
    resource_config(),
    create_opts()
) ->
    {ok, resource_data()} | {error, Reason :: term()}.
recreate(ResId, ResourceType, Config, Opts) ->
    emqx_cluster_rpc:multicall(emqx_resource, recreate_local, [ResId, ResourceType, Config, Opts]).

-spec remove(resource_id()) -> ok | {error, Reason :: term()}.
remove(ResId) ->
    emqx_cluster_rpc:multicall(emqx_resource, remove_local, [ResId]).

-spec reset_metrics(resource_id()) -> ok | {error, any()}.
reset_metrics(ResId) ->
    emqx_cluster_rpc:multicall(emqx_resource, reset_metrics_local, [ResId]).
