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

-module(emqx_conf_proto_v3).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,
    strict_update/3,
    remove_config/2,
    reset/2
]).

-include_lib("emqx/include/bpapi.hrl").

-type update_config_key_path() :: [emqx_map_lib:config_key(), ...].
-type tnx_id() :: pos_integer().

introduced_in() ->
    "5.0.13".

-spec strict_update(
    update_config_key_path(),
    emqx_config:update_request(),
    emqx_config:update_opts()
) ->
    {ok, emqx_config:update_result()}
    | {error, emqx_config:update_error()}
    | {partial_failure, #{
        reason := peers_lagging | stopped_nodes,
        nodes := [node()],
        tnx_id := tnx_id(),
        init_result := {ok, emqx_config:update_result()}
    }}.
strict_update(KeyPath, UpdateReq, Opts) ->
    emqx_cluster_rpc:strict_multicall(emqx, update_config, [KeyPath, UpdateReq, Opts]).

-spec remove_config(update_config_key_path(), emqx_config:update_opts()) ->
    {ok, emqx_config:update_result()} | {error, emqx_config:update_error()}.
remove_config(KeyPath, Opts) ->
    emqx_cluster_rpc:multicall(emqx, remove_config, [KeyPath, Opts]).

-spec reset(update_config_key_path(), emqx_config:update_opts()) ->
    {ok, emqx_config:update_result()} | {error, emqx_config:update_error()}.
reset(KeyPath, Opts) ->
    emqx_cluster_rpc:multicall(emqx, reset_config, [KeyPath, Opts]).
