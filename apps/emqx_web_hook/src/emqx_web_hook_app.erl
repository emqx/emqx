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

-module(emqx_web_hook_app).

-behaviour(application).

-emqx_plugin(?MODULE).

-include("emqx_web_hook.hrl").

-export([ start/2
        , stop/1
        ]).

start(_StartType, _StartArgs) ->
    translate_env(),
    {ok, Sup} = emqx_web_hook_sup:start_link(),
    {ok, PoolOpts} = application:get_env(?APP, pool_opts),
    {ok, _Pid} = ehttpc_sup:start_pool(?APP, PoolOpts),
    emqx_web_hook:register_metrics(),
    emqx_web_hook:load(),
    {ok, Sup}.

stop(_State) ->
    emqx_web_hook:unload(),
    ehttpc_sup:stop_pool(?APP).

translate_env() ->
    {ok, URL} = application:get_env(?APP, url),
    {ok, #{host := Host0,
           path := Path0,
           port := Port,
           scheme := Scheme}} = emqx_http_lib:uri_parse(URL),
    Path = path(Path0),
    {Inet, Host} = parse_host(Host0),
    PoolSize = application:get_env(?APP, pool_size, 32),
    MoreOpts = case Scheme of
                   http ->
                       [{transport_opts, [Inet]}];
                   https ->
                       CACertFile = application:get_env(?APP, cacertfile, undefined),
                       CertFile = application:get_env(?APP, certfile, undefined),
                       KeyFile = application:get_env(?APP, keyfile, undefined),
                       {ok, Verify} = application:get_env(?APP, verify),
                       VerifyType = case Verify of
                                       true -> verify_peer;
                                       false -> verify_none
                                   end,
                       SNI = case application:get_env(?APP, server_name_indication, undefined) of
                                 "disable" -> disable;
                                 SNI0 -> SNI0
                             end,
                       TLSOpts = lists:filter(fun({_K, V}) ->
                                                V /= <<>> andalso V /= undefined andalso V /= "" andalso true
                                              end, [{keyfile, KeyFile},
                                                    {certfile, CertFile},
                                                    {cacertfile, CACertFile},
                                                    {verify, VerifyType},
                                                    {server_name_indication, SNI}]),
                       NTLSOpts = [ {versions, emqx_tls_lib:default_versions()}
                                  , {ciphers, emqx_tls_lib:default_ciphers()}
                                  | TLSOpts
                                  ],
                       [{transport, ssl}, {transport_opts, [Inet | NTLSOpts]}]
                end,
    PoolOpts = [{host, Host},
                {port, Port},
                {pool_size, PoolSize},
                {pool_type, hash},
                {connect_timeout, 5000},
                {retry, 5},
                {retry_timeout, 1000}] ++ MoreOpts,
    application:set_env(?APP, path, Path),
    application:set_env(?APP, pool_opts, PoolOpts),
    Headers = application:get_env(?APP, headers, []),
    NHeaders = set_content_type(Headers),
    application:set_env(?APP, headers, NHeaders).

path("") ->
    "/";
path(Path) ->
    Path.

set_content_type(Headers) ->
    NHeaders = proplists:delete(<<"Content-Type">>, proplists:delete(<<"content-type">>, Headers)),
    [{<<"content-type">>, <<"application/json">>} | NHeaders].

parse_host(Host) ->
    case inet:parse_address(Host) of
        {ok, Addr} when size(Addr) =:= 4 -> {inet, Addr};
        {ok, Addr} when size(Addr) =:= 8 -> {inet6, Addr};
        {error, einval} ->
            case inet:getaddr(Host, inet6) of
                {ok, _} -> {inet6, Host};
                {error, _} -> {inet, Host}
            end
    end.
