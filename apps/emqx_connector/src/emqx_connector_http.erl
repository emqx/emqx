%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
        , desc/1
        , validations/0
        , namespace/0
        ]).

-export([ check_ssl_opts/2
        ]).

-type connect_timeout() :: emqx_schema:duration() | infinity.
-type pool_type() :: random | hash.

-reflect_type([ connect_timeout/0
              , pool_type/0
              ]).

%%=====================================================================
%% Hocon schema

namespace() -> "connector-http".

roots() ->
    fields(config).

fields(config) ->
    [ {base_url,
       sc(url(),
          #{ required => true
           , validator => fun(#{query := _Query}) ->
                            {error, "There must be no query in the base_url"};
                            (_) -> ok
                          end
           , desc => "
The base URL is the URL includes only the scheme, host and port.<br/>
When send an HTTP request, the real URL to be used is the concatenation of the base URL and the
path parameter (passed by the emqx_resource:query/2,3 or provided by the request parameter).<br/>
For example: `http://localhost:9901/`
"
           })}
    , {connect_timeout,
        sc(emqx_schema:duration_ms(),
           #{ default => "15s"
            , desc => "The timeout when connecting to the HTTP server."
            })}
    , {max_retries,
        sc(non_neg_integer(),
           #{ default => 5
            , desc => "Max retry times if error on sending request."
            })}
    , {retry_interval,
        sc(emqx_schema:duration(),
           #{ default => "1s"
            , desc => "Interval between retries."
            })}
    , {pool_type,
        sc(pool_type(),
           #{ default => random
            , desc => "The type of the pool. Can be one of `random`, `hash`."
            })}
    , {pool_size,
        sc(non_neg_integer(),
           #{ default => 8
            , desc => "The pool size."
            })}
    , {enable_pipelining,
        sc(boolean(),
           #{ default => true
            , desc => "Enable the HTTP pipeline."
            })}
    , {request, hoconsc:mk(
        ref("request"),
        #{ default => undefined
         , required => false
         , desc => "
If the request is provided, the caller can send HTTP requests via
<code>emqx_resource:query(ResourceId, {send_message, BridgeId, Message})</code>
"
         })}
    ] ++ emqx_connector_schema_lib:ssl_fields();

fields("request") ->
    [ {method, hoconsc:mk(hoconsc:enum([post, put, get, delete]), #{required => false, desc => "HTTP method."})}
    , {path, hoconsc:mk(binary(), #{required => false, desc => "URL path."})}
    , {body, hoconsc:mk(binary(), #{required => false, desc => "HTTP request body."})}
    , {headers, hoconsc:mk(map(), #{required => false, desc => "List of HTTP headers."})}
    , {request_timeout,
        sc(emqx_schema:duration_ms(),
           #{ required => false
            , desc => "HTTP request timeout."
            })}
    ].

desc(config) ->
    "";
desc("request") ->
    "";
desc(_) ->
    undefined.

validations() ->
    [ {check_ssl_opts, fun check_ssl_opts/1} ].

sc(Type, Meta) -> hoconsc:mk(Type, Meta).
ref(Field) -> hoconsc:ref(?MODULE, Field).

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
    ?SLOG(info, #{msg => "starting_http_connector",
                  connector => InstId, config => Config}),
    {Transport, TransportOpts} =
        case Scheme of
            http ->
                {tcp, []};
            https ->
                SSLOpts = emqx_tls_lib:to_client_opts(maps:get(ssl, Config)),
                {tls, SSLOpts}
        end,
    NTransportOpts = emqx_misc:ipv6_probe(TransportOpts),
    PoolOpts = [ {host, Host}
               , {port, Port}
               , {connect_timeout, ConnectTimeout}
               , {retry, MaxRetries}
               , {retry_timeout, RetryInterval}
               , {keepalive, 30000}
               , {pool_type, PoolType}
               , {pool_size, PoolSize}
               , {transport, Transport}
               , {transport_opts, NTransportOpts}],
    PoolName = emqx_plugin_libs_pool:pool_name(InstId),
    State = #{
        pool_name => PoolName,
        host => Host,
        port => Port,
        connect_timeout => ConnectTimeout,
        base_path => BasePath,
        request => preprocess_request(maps:get(request, Config, undefined))
    },
    case ehttpc_sup:start_pool(PoolName, PoolOpts) of
        {ok, _} -> {ok, State};
        {error, {already_started, _}} -> {ok, State};
        {error, Reason} ->
            {error, Reason}
    end.

on_stop(InstId, #{pool_name := PoolName}) ->
    ?SLOG(info, #{msg => "stopping_http_connector",
                  connector => InstId}),
    ehttpc_sup:stop_pool(PoolName).

on_query(InstId, {send_message, Msg}, AfterQuery, State) ->
    case maps:get(request, State, undefined) of
        undefined -> ?SLOG(error, #{msg => "request_not_found", connector => InstId});
        Request ->
            #{method := Method, path := Path, body := Body, headers := Headers,
              request_timeout := Timeout} = process_request(Request, Msg),
            on_query(InstId, {Method, {Path, Headers, Body}, Timeout}, AfterQuery, State)
    end;
on_query(InstId, {Method, Request}, AfterQuery, State) ->
    on_query(InstId, {undefined, Method, Request, 5000}, AfterQuery, State);
on_query(InstId, {Method, Request, Timeout}, AfterQuery, State) ->
    on_query(InstId, {undefined, Method, Request, Timeout}, AfterQuery, State);
on_query(InstId, {KeyOrNum, Method, Request, Timeout}, AfterQuery,
        #{pool_name := PoolName, base_path := BasePath} = State) ->
    ?TRACE("QUERY", "http_connector_received",
        #{request => Request, connector => InstId, state => State}),
    NRequest = formalize_request(Method, BasePath, Request),
    case Result = ehttpc:request(case KeyOrNum of
                                     undefined -> PoolName;
                                     _ -> {PoolName, KeyOrNum}
                                 end, Method, NRequest, Timeout) of
        {error, Reason} ->
            ?SLOG(error, #{msg => "http_connector_do_reqeust_failed",
                           request => NRequest, reason => Reason,
                           connector => InstId}),
            emqx_resource:query_failed(AfterQuery);
        {ok, StatusCode, _} when StatusCode >= 200 andalso StatusCode < 300 ->
            emqx_resource:query_success(AfterQuery);
        {ok, StatusCode, _, _} when StatusCode >= 200 andalso StatusCode < 300 ->
            emqx_resource:query_success(AfterQuery);
        {ok, StatusCode, _} ->
            ?SLOG(error, #{msg => "http connector do request, received error response",
                           request => NRequest, connector => InstId,
                           status_code => StatusCode}),
            emqx_resource:query_failed(AfterQuery);
        {ok, StatusCode, _, _} ->
            ?SLOG(error, #{msg => "http connector do request, received error response",
                           request => NRequest, connector => InstId,
                           status_code => StatusCode}),
            emqx_resource:query_failed(AfterQuery)
    end,
    Result.

on_health_check(_InstId, #{host := Host, port := Port, connect_timeout := Timeout} = State) ->
    case do_health_check(Host, Port, Timeout) of
        ok -> {ok, State};
        {error, Reason} ->
            {error, {http_health_check_failed, Reason}, State}
    end.

do_health_check(Host, Port, Timeout) ->
    case gen_tcp:connect(Host, Port, emqx_misc:ipv6_probe([]), Timeout) of
        {ok, Sock} -> gen_tcp:close(Sock), ok;
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

preprocess_request(undefined) ->
    undefined;
preprocess_request(Req) when map_size(Req) == 0 ->
    undefined;
preprocess_request(#{
            method := Method,
            path := Path,
            body := Body,
            headers := Headers
        } = Req) ->
    #{ method => emqx_plugin_libs_rule:preproc_tmpl(bin(Method))
     , path => emqx_plugin_libs_rule:preproc_tmpl(Path)
     , body => emqx_plugin_libs_rule:preproc_tmpl(Body)
     , headers => preproc_headers(Headers)
     , request_timeout => maps:get(request_timeout, Req, 30000)
     }.

preproc_headers(Headers) when is_map(Headers) ->
    maps:fold(fun(K, V, Acc) ->
            [{
                emqx_plugin_libs_rule:preproc_tmpl(bin(K)),
                emqx_plugin_libs_rule:preproc_tmpl(bin(V))
            } | Acc]
        end, [], Headers);
preproc_headers(Headers) when is_list(Headers) ->
    lists:map(fun({K, V}) ->
        {
            emqx_plugin_libs_rule:preproc_tmpl(bin(K)),
            emqx_plugin_libs_rule:preproc_tmpl(bin(V))
        }
              end, Headers).

process_request(#{
            method := MethodTks,
            path := PathTks,
            body := BodyTks,
            headers := HeadersTks,
            request_timeout := ReqTimeout
        } = Conf, Msg) ->
    Conf#{ method => make_method(emqx_plugin_libs_rule:proc_tmpl(MethodTks, Msg))
         , path => emqx_plugin_libs_rule:proc_tmpl(PathTks, Msg)
         , body => process_request_body(BodyTks, Msg)
         , headers => proc_headers(HeadersTks, Msg)
         , request_timeout => ReqTimeout
         }.

process_request_body([], Msg) ->
    emqx_json:encode(Msg);
process_request_body(BodyTks, Msg) ->
    emqx_plugin_libs_rule:proc_tmpl(BodyTks, Msg).

proc_headers(HeaderTks, Msg) ->
    lists:map(fun({K, V}) ->
            {
                emqx_plugin_libs_rule:proc_tmpl(K, Msg),
                emqx_plugin_libs_rule:proc_tmpl(V, Msg)
            }
              end, HeaderTks).

make_method(M) when M == <<"POST">>; M == <<"post">> -> post;
make_method(M) when M == <<"PUT">>; M == <<"put">> -> put;
make_method(M) when M == <<"GET">>; M == <<"get">> -> get;
make_method(M) when M == <<"DELETE">>; M == <<"delete">> -> delete.

check_ssl_opts(Conf) ->
    check_ssl_opts("base_url", Conf).

check_ssl_opts(URLFrom, Conf) ->
    #{scheme := Scheme} = hocon_maps:get(URLFrom, Conf),
    SSL= hocon_maps:get("ssl", Conf),
    case {Scheme, maps:get(enable, SSL, false)} of
        {http, false} -> true;
        {https, true} -> true;
        {_, _} -> false
    end.

formalize_request(Method, BasePath, {Path, Headers, _Body})
        when Method =:= get; Method =:= delete ->
    formalize_request(Method, BasePath, {Path, Headers});
formalize_request(_Method, BasePath, {Path, Headers, Body}) ->
    {filename:join(BasePath, Path), Headers, Body};

formalize_request(_Method, BasePath, {Path, Headers}) ->
    {filename:join(BasePath, Path), Headers}.

bin(Bin) when is_binary(Bin) ->
    Bin;
bin(Str) when is_list(Str) ->
    list_to_binary(Str);
bin(Atom) when is_atom(Atom) ->
    atom_to_binary(Atom, utf8).
