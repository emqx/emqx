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

-module(emqx_authn_jwks_connector).

-behaviour(emqx_resource).

-include_lib("emqx/include/logger.hrl").

%% callbacks of behaviour emqx_resource
-export([
    on_start/2,
    on_stop/2,
    on_query/4,
    on_get_status/2,
    connect/1
]).

-define(DEFAULT_POOL_SIZE, 8).

on_start(InstId, Opts) ->
    PoolName = emqx_plugin_libs_pool:pool_name(InstId),
    PoolOpts = [
        {pool_size, maps:get(pool_size, Opts, ?DEFAULT_POOL_SIZE)},
        {connector_opts, Opts}
    ],
    case emqx_plugin_libs_pool:start_pool(PoolName, ?MODULE, PoolOpts) of
        ok -> {ok, #{pool_name => PoolName}};
        {error, Reason} -> {error, Reason}
    end.

on_stop(_InstId, #{pool_name := PoolName}) ->
    emqx_plugin_libs_pool:stop_pool(PoolName).

on_query(InstId, get_jwks, AfterQuery, #{pool_name := PoolName}) ->
    Result = ecpool:pick_and_do(PoolName, {emqx_authn_jwks_client, get_jwks, []}, no_handover),
    case Result of
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "emqx_authn_jwks_client_query_failed",
                connector => InstId,
                command => get_jwks,
                reason => Reason
            }),
            emqx_resource:query_failed(AfterQuery);
        _ ->
            emqx_resource:query_success(AfterQuery)
    end,
    Result;
on_query(_InstId, {update, Opts}, AfterQuery, #{pool_name := PoolName}) ->
    lists:foreach(
        fun({_, Worker}) ->
            ok = ecpool_worker:exec(Worker, {emqx_authn_jwks_client, update, [Opts]}, infinity)
        end,
        ecpool:workers(PoolName)
    ),
    emqx_resource:query_success(AfterQuery),
    ok.

on_get_status(_InstId, #{pool_name := PoolName}) ->
    Func =
        fun(Conn) ->
            case emqx_authn_jwks_client:get_jwks(Conn) of
                {ok, _} -> true;
                _ -> false
            end
        end,
    case emqx_plugin_libs_pool:health_check_ecpool_workers(PoolName, Func) of
        true -> connected;
        false -> disconnected
    end.

connect(Opts) ->
    ConnectorOpts = proplists:get_value(connector_opts, Opts),
    emqx_authn_jwks_client:start_link(ConnectorOpts).
