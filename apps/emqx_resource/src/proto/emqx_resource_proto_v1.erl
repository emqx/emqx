%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_resource_proto_v1).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,
    deprecated_since/0,
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

deprecated_since() ->
    "5.6.0".

-spec create(
    resource_id(),
    resource_group(),
    resource_module(),
    resource_config(),
    creation_opts()
) ->
    {ok, resource_data() | 'already_created'} | {error, Reason :: term()}.
create(ResId, Group, ResourceType, Config, Opts) ->
    emqx_cluster_rpc:multicall(emqx_resource, create_local, [
        ResId, Group, ResourceType, Config, Opts
    ]).

-spec create_dry_run(
    resource_module(),
    resource_config()
) ->
    ok | {error, Reason :: term()}.
create_dry_run(ResourceType, Config) ->
    emqx_cluster_rpc:multicall(emqx_resource, create_dry_run_local, [ResourceType, Config]).

-spec recreate(
    resource_id(),
    resource_module(),
    resource_config(),
    creation_opts()
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
