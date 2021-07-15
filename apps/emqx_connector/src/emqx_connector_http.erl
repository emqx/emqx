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

-export([ structs/0
        , fields/1
        , validations/0]).

-export([ check_ssl_opts/2 ]).

-type connect_timeout() :: non_neg_integer() | infinity.
-type pool_type() :: random | hash.

-reflect_type([ connect_timeout/0
              , pool_type/0
              ]).

%%=====================================================================
%% Hocon schema
structs() -> [""].

fields("") ->
    [{config, #{type => hoconsc:ref(?MODULE, config)}}];

fields(config) ->
    [ {base_url,        fun base_url/1}
    , {connect_timeout, fun connect_timeout/1}
    , {max_retries,     fun max_retries/1}
    , {retry_interval,  fun retry_interval/1}
    , {keepalive,       fun keepalive/1}
    , {pool_type,       fun pool_type/1}
    , {pool_size,       fun pool_size/1}
    , {ssl_opts,        #{type => hoconsc:ref(?MODULE, ssl_opts),
                          default => #{}}}
    ];

fields(ssl_opts) ->
    [ {cacertfile, fun cacertfile/1}
    , {keyfile,    fun keyfile/1}
    , {certfile,   fun certfile/1}
    , {verify,     fun verify/1}
    ].

validations() ->
    [ {check_ssl_opts, fun check_ssl_opts/1} ].

base_url(type) -> binary();
base_url(nullable) -> false;
base_url(validate) -> [fun check_base_url/1];
base_url(_) -> undefined.

connect_timeout(type) -> connect_timeout();
connect_timeout(default) -> 5000;
connect_timeout(_) -> undefined.

max_retries(type) -> non_neg_integer();
max_retries(default) -> 5;
max_retries(_) -> undefined.

retry_interval(type) -> non_neg_integer();
retry_interval(default) -> 1000;
retry_interval(_) -> undefined.

keepalive(type) -> non_neg_integer();
keepalive(default) -> 5000;
keepalive(_) -> undefined.

pool_type(type) -> pool_type();
pool_type(default) -> random;
pool_type(_) -> undefined.

pool_size(type) -> non_neg_integer();
pool_size(default) -> 8;
pool_size(_) -> undefined.

cacertfile(type) -> string();
cacertfile(nullable) -> true;
cacertfile(_) -> undefined.

keyfile(type) -> string();
keyfile(nullable) -> true;
keyfile(_) -> undefined.

%% TODO: certfile is required
certfile(type) -> string();
certfile(nullable) -> true;
certfile(_) -> undefined.

verify(type) -> boolean();
verify(default) -> false;
verify(_) -> undefined.

%% ===================================================================
on_start(InstId, #{url := URL,
                   connect_timeout := ConnectTimeout,
                   max_retries := MaxRetries,
                   retry_interval := RetryInterval,
                   keepalive := Keepalive,
                   pool_type := PoolType,
                   pool_size := PoolSize} = Config) ->
    logger:info("starting http connector: ~p, config: ~p", [InstId, Config]),
    {ok, #{scheme := Scheme,
           host := Host,
           port := Port,
           path := BasePath}} = emqx_http_lib:uri_parse(URL),
    {Transport, TransportOpts} = case Scheme of
                                     http ->
                                         {tcp, []};
                                     https ->
                                         SSLOpts = emqx_plugin_libs_ssl:save_files_return_opts(
                                                    maps:get(ssl_opts, Config), "connectors", InstId),
                                         {tls, SSLOpts}
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
           host => Host,
           port => Port,
           base_path => BasePath}}.

on_stop(InstId, #{pool_name := PoolName}) ->
    logger:info("stopping http connector: ~p", [InstId]),
    ehttpc_sup:stop_pool(PoolName).

on_query(InstId, {Method, Request}, AfterQuery, State) ->
    on_query(InstId, {undefined, Method, Request, 5000}, AfterQuery, State);
on_query(InstId, {Method, Request, Timeout}, AfterQuery, State) ->
    on_query(InstId, {undefined, Method, Request, Timeout}, AfterQuery, State);
on_query(InstId, {KeyOrNum, Method, Request, Timeout}, AfterQuery, #{pool_name := PoolName,
                                                                     base_path := BasePath} = State) ->
    logger:debug("http connector ~p received request: ~p, at state: ~p", [InstId, Request, State]),
    NRequest = update_path(BasePath, Request),
    case Result = ehttpc:request(case KeyOrNum of
                                     undefined -> PoolName;
                                     _ -> {PoolName, KeyOrNum}
                                 end, Method, NRequest, Timeout) of
        {error, Reason} ->
            logger:debug("http connector ~p do reqeust failed, sql: ~p, reason: ~p", [InstId, NRequest, Reason]),
            emqx_resource:query_failed(AfterQuery);
        _ ->
            emqx_resource:query_success(AfterQuery)
    end,
    Result.

on_health_check(_InstId, #{host := Host, port := Port} = State) ->
    case gen_tcp:connect(Host, Port, emqx_misc:ipv6_probe([]), 3000) of
        {ok, Sock} ->
            gen_tcp:close(Sock),
            {ok, State};
        {error, _Reason} ->
            {error, test_query_failed, State}
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

check_base_url(URL) ->
    case emqx_http_lib:uri_parse(URL) of
        {error, _} -> false;
        {ok, #{query := _}} -> false;
        _ -> true
    end.

check_ssl_opts(Conf) ->
    check_ssl_opts("base_url", Conf).

check_ssl_opts(URLFrom, Conf) ->
    URL = hocon_schema:get_value(URLFrom, Conf),
    {ok, #{scheme := Scheme}} = emqx_http_lib:uri_parse(URL),
    SSLOpts = hocon_schema:get_value("ssl_opts", Conf),
    case {Scheme, maps:size(SSLOpts)} of
        {http, 0} -> true;
        {http, _} -> false;
        {https, 0} -> false;
        {https, _} -> true
    end.

update_path(BasePath, {Path, Headers}) ->
    {filename:join(BasePath, Path), Headers};
update_path(BasePath, {Path, Headers, Body}) ->
    {filename:join(BasePath, Path), Headers, Body}.