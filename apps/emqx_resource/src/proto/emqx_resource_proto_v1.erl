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

introduced_in() ->
    "5.0.0".

-spec create(
    emqx_resource:instance_id(),
    emqx_resource:resource_group(),
    emqx_resource:resource_type(),
    emqx_resource:resource_config(),
    emqx_resource:create_opts()
) ->
    emqx_cluster_rpc:multicall_return(emqx_resource:resource_data()).
create(InstId, Group, ResourceType, Config, Opts) ->
    emqx_cluster_rpc:multicall(emqx_resource, create_local, [
        InstId, Group, ResourceType, Config, Opts
    ]).

-spec create_dry_run(
    emqx_resource:resource_type(),
    emqx_resource:resource_config()
) ->
    emqx_cluster_rpc:multicall_return(emqx_resource:resource_data()).
create_dry_run(ResourceType, Config) ->
    emqx_cluster_rpc:multicall(emqx_resource, create_dry_run_local, [ResourceType, Config]).

-spec recreate(
    emqx_resource:instance_id(),
    emqx_resource:resource_type(),
    emqx_resource:resource_config(),
    emqx_resource:create_opts()
) ->
    emqx_cluster_rpc:multicall_return(emqx_resource:resource_data()).
recreate(InstId, ResourceType, Config, Opts) ->
    emqx_cluster_rpc:multicall(emqx_resource, recreate_local, [InstId, ResourceType, Config, Opts]).

-spec remove(emqx_resource:instance_id()) ->
    emqx_cluster_rpc:multicall_return(ok).
remove(InstId) ->
    emqx_cluster_rpc:multicall(emqx_resource, remove_local, [InstId]).

-spec reset_metrics(emqx_resource:instance_id()) ->
    emqx_cluster_rpc:multicall_return(ok).
reset_metrics(InstId) ->
    emqx_cluster_rpc:multicall(emqx_resource, reset_metrics_local, [InstId]).
