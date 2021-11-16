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
-include_lib("emqx/include/logger.hrl").

-behaviour(emqx_resource).

%% callbacks of behaviour emqx_resource
-export([ on_start/2
        , on_stop/2
        , on_query/4
        , on_health_check/2
        ]).

-type url() :: emqx_http_lib:uri_map().
-reflect_type([url/0]).
-typerefl_from_string({url/0, emqx_http_lib, uri_parse}).

-export([ roots/0
        , fields/1
        , validations/0]).

-export([ check_ssl_opts/2 ]).

-type connect_timeout() :: emqx_schema:duration() | infinity.
-type pool_type() :: random | hash.

-reflect_type([ connect_timeout/0
              , pool_type/0
              ]).

%%=====================================================================
%% Hocon schema
roots() ->
    [{config, #{type => hoconsc:ref(?MODULE, config)}}].

fields("http_request") ->
    [ {subscribe_local_topic, hoconsc:mk(binary())}
    , {method, hoconsc:mk(method(), #{default => post})}
    , {path, hoconsc:mk(binary(), #{default => <<"">>})}
    , {headers, hoconsc:mk(map(),
        #{default => #{
            <<"accept">> => <<"application/json">>,
            <<"cache-control">> => <<"no-cache">>,
            <<"connection">> => <<"keep-alive">>,
            <<"content-type">> => <<"application/json">>,
            <<"keep-alive">> => <<"timeout=5">>}})
      }
    , {body, hoconsc:mk(binary(), #{default => <<"${payload}">>})}
    , {request_timeout, hoconsc:mk(emqx_schema:duration_ms(), #{default => <<"30s">>})}
    ];

fields(config) ->
    [ {base_url,          fun base_url/1}
    , {connect_timeout,   fun connect_timeout/1}
    , {max_retries,       fun max_retries/1}
    , {retry_interval,    fun retry_interval/1}
    , {pool_type,         fun pool_type/1}
    , {pool_size,         fun pool_size/1}
    , {enable_pipelining, fun enable_pipelining/1}
    ] ++ emqx_connector_schema_lib:ssl_fields().

method() ->
    hoconsc:enum([post, put, get, delete]).

validations() ->
    [ {check_ssl_opts, fun check_ssl_opts/1} ].

base_url(type) -> url();
base_url(nullable) -> false;
base_url(validator) -> fun(#{query := _Query}) ->
                           {error, "There must be no query in the base_url"};
                          (_) -> ok
                       end;
base_url(_) -> undefined.

connect_timeout(type) -> emqx_schema:duration_ms();
connect_timeout(default) -> <<"5s">>;
connect_timeout(_) -> undefined.

max_retries(type) -> non_neg_integer();
max_retries(default) -> 5;
max_retries(_) -> undefined.

retry_interval(type) -> emqx_schema:duration();
retry_interval(default) -> <<"1s">>;
retry_interval(_) -> undefined.

pool_type(type) -> pool_type();
pool_type(default) -> hash;
pool_type(_) -> undefined.

pool_size(type) -> non_neg_integer();
pool_size(default) -> 8;
pool_size(_) -> undefined.

enable_pipelining(type) -> boolean();
enable_pipelining(default) -> true;
enable_pipelining(_) -> undefined.

%% ===================================================================
on_start(InstId, #{base_url := #{scheme := Scheme,
                                 host := Host,
                                 port := Port,
                                 path := BasePath},
                   connect_timeout := ConnectTimeout,
                   max_retries := MaxRetries,
                   retry_interval := RetryInterval,
                   pool_type := PoolType,
                   pool_size := PoolSize} = Config) ->
    ?SLOG(info, #{msg => "starting http connector",
                  connector => InstId, config => Config}),
    {Transport, TransportOpts} = case Scheme of
                                     http ->
                                         {tcp, []};
                                     https ->
                                         SSLOpts = emqx_plugin_libs_ssl:save_files_return_opts(
                                                    maps:get(ssl, Config), "connectors", InstId),
                                         {tls, SSLOpts}
                                 end,
    NTransportOpts = emqx_misc:ipv6_probe(TransportOpts),
    PoolOpts = [ {host, Host}
               , {port, Port}
               , {connect_timeout, ConnectTimeout}
               , {retry, MaxRetries}
               , {retry_timeout, RetryInterval}
               , {keepalive, 5000}
               , {pool_type, PoolType}
               , {pool_size, PoolSize}
               , {transport, Transport}
               , {transport_opts, NTransportOpts}],
    PoolName = emqx_plugin_libs_pool:pool_name(InstId),
    State = #{
        pool_name => PoolName,
        host => Host,
        port => Port,
        base_path => BasePath,
        channels => preproc_channels(InstId, Config)
    },
    case ehttpc_sup:start_pool(PoolName, PoolOpts) of
        {ok, _} -> {ok, State};
        {error, {already_started, _}} -> {ok, State};
        {error, Reason} ->
            {error, Reason}
    end.

on_stop(InstId, #{pool_name := PoolName}) ->
    ?SLOG(info, #{msg => "stopping http connector",
                  connector => InstId}),
    ehttpc_sup:stop_pool(PoolName).

on_query(InstId, {send_message, ChannelId, Msg}, AfterQuery, #{channels := Channels} = State) ->
    case maps:find(ChannelId, Channels) of
        error -> ?SLOG(error, #{msg => "channel not found", channel_id => ChannelId});
        {ok, ChannConf} ->
            #{method := Method, path := Path, body := Body, headers := Headers,
              request_timeout := Timeout} = proc_channel_conf(ChannConf, Msg),
            on_query(InstId, {Method, {Path, Headers, Body}, Timeout}, AfterQuery, State)
    end;
on_query(InstId, {Method, Request}, AfterQuery, State) ->
    on_query(InstId, {undefined, Method, Request, 5000}, AfterQuery, State);
on_query(InstId, {Method, Request, Timeout}, AfterQuery, State) ->
    on_query(InstId, {undefined, Method, Request, Timeout}, AfterQuery, State);
on_query(InstId, {KeyOrNum, Method, Request, Timeout}, AfterQuery,
        #{pool_name := PoolName, base_path := BasePath} = State) ->
    ?SLOG(debug, #{msg => "http connector received request",
                   request => Request, connector => InstId,
                   state => State}),
    NRequest = update_path(BasePath, Request),
    case Result = ehttpc:request(case KeyOrNum of
                                     undefined -> PoolName;
                                     _ -> {PoolName, KeyOrNum}
                                 end, Method, NRequest, Timeout) of
        {error, Reason} ->
            ?SLOG(error, #{msg => "http connector do reqeust failed",
                           request => NRequest, reason => Reason,
                           connector => InstId}),
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

preproc_channels(<<"bridge:", BridgeId/binary>>, Config) ->
    {BridgeType, BridgeName} = emqx_bridge:parse_bridge_id(BridgeId),
    maps:fold(fun(ChannName, ChannConf, Acc) ->
            Acc#{emqx_bridge:channel_id(BridgeType, BridgeName, egress_channels, ChannName) =>
                 preproc_channel_conf(ChannConf)}
        end, #{}, maps:get(egress_channels, Config, #{}));
preproc_channels(_InstId, _Config) ->
    #{}.

preproc_channel_conf(#{
        method := Method,
        path := Path,
        body := Body,
        headers := Headers} = Conf) ->
    Conf#{ method => emqx_plugin_libs_rule:preproc_tmpl(bin(Method))
         , path => emqx_plugin_libs_rule:preproc_tmpl(Path)
         , body => emqx_plugin_libs_rule:preproc_tmpl(Body)
         , headers => preproc_headers(Headers)
         }.

preproc_headers(Headers) ->
    maps:fold(fun(K, V, Acc) ->
            Acc#{emqx_plugin_libs_rule:preproc_tmpl(bin(K)) =>
                 emqx_plugin_libs_rule:preproc_tmpl(bin(V))}
        end, #{}, Headers).

proc_channel_conf(#{
        method := MethodTks,
        path := PathTks,
        body := BodyTks,
        headers := HeadersTks} = Conf, Msg) ->
    Conf#{ method => make_method(emqx_plugin_libs_rule:proc_tmpl(MethodTks, Msg))
         , path => emqx_plugin_libs_rule:proc_tmpl(PathTks, Msg)
         , body => emqx_plugin_libs_rule:proc_tmpl(BodyTks, Msg)
         , headers => maps:to_list(proc_headers(HeadersTks, Msg))
         }.

proc_headers(HeaderTks, Msg) ->
    maps:fold(fun(K, V, Acc) ->
            Acc#{emqx_plugin_libs_rule:proc_tmpl(K, Msg) =>
                 emqx_plugin_libs_rule:proc_tmpl(V, Msg)}
        end, #{}, HeaderTks).

make_method(M) when M == <<"POST">>; M == <<"post">> -> post;
make_method(M) when M == <<"PUT">>; M == <<"put">> -> put;
make_method(M) when M == <<"GET">>; M == <<"get">> -> get;
make_method(M) when M == <<"DELETE">>; M == <<"delete">> -> delete.

check_ssl_opts(Conf) ->
    check_ssl_opts("base_url", Conf).

check_ssl_opts(URLFrom, Conf) ->
    #{schema := Scheme} = hocon_schema:get_value(URLFrom, Conf),
    SSL= hocon_schema:get_value("ssl", Conf),
    case {Scheme, maps:get(enable, SSL, false)} of
        {http, false} -> true;
        {https, true} -> true;
        {_, _} -> false
    end.

update_path(BasePath, {Path, Headers}) ->
    {filename:join(BasePath, Path), Headers};
update_path(BasePath, {Path, Headers, Body}) ->
    {filename:join(BasePath, Path), Headers, Body}.

bin(Bin) when is_binary(Bin) ->
    Bin;
bin(Str) when is_list(Str) ->
    list_to_binary(Str);
bin(Atom) when is_atom(Atom) ->
    atom_to_binary(Atom, utf8).
