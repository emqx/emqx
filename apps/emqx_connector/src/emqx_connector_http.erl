%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_connector_http).

-include("emqx_connector.hrl").

-include_lib("typerefl/include/types.hrl").
-include_lib("emqx_resource/include/emqx_resource_behaviour.hrl").

%% callbacks of behaviour emqx_resource
-export([ on_start/2
        , on_stop/2
        , on_query/4
        , on_health_check/2
        ]).

-export([structs/0, fields/1]).

-type connection_timeout() :: pos_integer() | infinity.
-type pool_type() :: random | hash.

-reflect_type([ connection_timeout/0
              , pool_type/0
              ]).

%%=====================================================================
%% Hocon schema
structs() -> [""].

fields("") ->
    [ {server, fun server/1}
    , {connection_timeout, fun connection_timeout/1}
    , {max_retries, fun max_retries/1}
    , {retry_interval, fun retry_interval/1}
    , {keepalive, fun keepalive/1}
    , {pool_type, fun pool_type/1}
    , {pool_size, fun pool_size/1}
    ] ++ emqx_connector_schema_lib:ssl_fields().

server(type) -> emqx_schema:ip_port();
server(validator) -> [?REQUIRED("the field 'server' is required")];
server(_) -> undefined.

connection_timeout(type) -> connection_timeout();
connection_timeout(default) -> 5000;
connection_timeout(_) -> undefined.

max_retries(type) -> pos_integer();
max_retries(default) -> 5;
max_retries(_) -> undefined.

retry_interval(type) -> pos_integer();
retry_interval(default) -> 1000;
retry_interval(_) -> undefined.

keepalive(type) -> pos_integer();
keepalive(default) -> 5000;
keepalive(_) -> undefined.

pool_type(type) -> pool_type();
pool_type(default) -> random;
pool_type(_) -> undefined.

pool_size(type) -> pos_integer();
pool_size(default) -> 8;
pool_size(_) -> undefined.

%% ===================================================================
on_start(InstId, #{server := {Host, Port},
                   connect_timeout := ConnectTimeout,
                   max_retries := MaxRetries,
                   retry_interval := RetryInterval,
                   keepalive := Keepalive,
                   pool_type := PoolType,
                   pool_size := PoolSize,
                   ssl := SSL} = Config) ->
    logger:info("starting http connector: ~p, config: ~p", [InstId, Config]),
    {Transport, TransportOpts} = case maps:get(enable, SSL) of
                                     false ->
                                         {tcp, []};
                                     true ->
                                         NSSL = emqx_plugin_libs_ssl:save_files_return_opts(SSL, "connectors", InstId),
                                         {tls, NSSL}
                                 end,
    NTransportOpts = emqx_misc:ipv6_probe(TransportOpts),
    PoolOpts = [ {host, Host}
               , {port, Port}
               , {connect_timeout, ConnectTimeout}
               , {retry, MaxRetries}
               , {retry_timeout, RetryInterval}
               , {keepalive, Keepalive}
               , {pool_type, PoolType}
               , {pool_size, PoolSize}
               , {transport, Transport}
               , {transport, NTransportOpts}],
    PoolName = emqx_plugin_libs_pool:pool_name(InstId),
    {ok, _} = ehttpc_sup:start_pool(PoolName, PoolOpts),
    {ok, #{pool_name => PoolName,
           server => {Host, Port}}}.

on_stop(InstId, #{pool_name := PoolName}) ->
    logger:info("stopping http connector: ~p", [InstId]),
    ehttpc_sup:stop_pool(PoolName).

on_query(InstId, {Method, Request}, AfterQuery, State) ->
    on_query(InstId, {undefined, {Method, Request, 5000}}, AfterQuery, State);
on_query(InstId, {Method, Request, Timeout}, AfterQuery, State) ->
    on_query(InstId, {undefined, {Method, Request, Timeout}}, AfterQuery, State);
on_query(InstId, {KeyOrNum, {Method, Request, Timeout}}, AfterQuery, #{pool_name := PoolName} = State) ->
    logger:debug("http connector ~p received request: ~p, at state: ~p", [InstId, Request, State]),
    case Result = ehttpc:request(case KeyOrNum of
                                     undefined -> PoolName;
                                     _ -> {PoolName, KeyOrNum}
                                 end, Method, Request, Timeout) of
        {error, Reason} ->
            logger:debug("http connector ~p do reqeust failed, sql: ~p, reason: ~p", [InstId, Request, Reason]),
            emqx_resource:query_failed(AfterQuery);
        _ ->
            emqx_resource:query_success(AfterQuery)
    end,
    Result.

on_health_check(_InstId, #{server := {Host, Port}} = State) ->
    case gen_tcp:connect(Host, Port, emqx_misc:ipv6_probe([]), 3000) of
        {ok, Sock} ->
            gen_tcp:close(Sock),
            {ok, State};
        {error, _Reason} ->
            {error, test_query_failed, State}
    end.