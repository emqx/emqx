%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    ehttpc_sup:start_pool(?APP, PoolOpts),
    emqx_web_hook:register_metrics(),
    emqx_web_hook:load(),
    {ok, Sup}.

stop(_State) ->
    emqx_web_hook:unload(),
    ehttpc_sup:stop_pool(?APP).

add_default_scheme(URL) when is_list(URL) ->
    add_default_scheme(list_to_binary(URL));
add_default_scheme(<<"http://", _/binary>> = URL) ->
    URL;
add_default_scheme(<<"https://", _/binary>> = URL) ->
    URL;
add_default_scheme(URL) ->
    <<"http://", URL/binary>>.

translate_env() ->
    {ok, URL} = application:get_env(?APP, url),
    #{host := Host0,
      port := Port,
      path := Path0,
      scheme := Scheme} = uri_string:parse(binary_to_list(add_default_scheme(URL))),
    Host = get_addr(Host0),
    Path = path(Path0),
    PoolSize = application:get_env(?APP, pool_size, 32),
    IPv6 = case tuple_size(Host) =:= 8 of
               true -> [inet6];
               false -> []
           end,
    MoreOpts = case Scheme of
                   "http" ->
                       [{transport_opts, IPv6}];
                   "https" ->
                       CACertFile = application:get_env(?APP, cacertfile, undefined),
                       CertFile = application:get_env(?APP, certfile, undefined),
                       KeyFile = application:get_env(?APP, keyfile, undefined),
                       {ok, Verify} = application:get_env(?APP, verify),
                       VerifyType = case Verify of
                                       true -> verify_peer;
                                       false -> verify_none
                                   end,
                       TLSOpts = lists:filter(fun({_K, V}) when V =:= <<>> ->
                                                   false;
                                                   (_) ->
                                                   true
                                               end, [{keyfile, KeyFile}, {certfile, CertFile}, {cacertfile, CACertFile}]),
                       TlsVers = ['tlsv1.2','tlsv1.1',tlsv1],
                       NTLSOpts = [{verify, VerifyType},
                                   {versions, TlsVers},
                                   {ciphers, lists:foldl(fun(TlsVer, Ciphers) ->
                                                               Ciphers ++ ssl:cipher_suites(all, TlsVer)
                                                           end, [], TlsVers)} | TLSOpts],
                       [{transport, ssl}, {transport_opts, NTLSOpts ++ IPv6}]
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

get_addr(Hostname) ->
    case inet:parse_address(Hostname) of
        {ok, {_,_,_,_} = Addr} -> Addr;
        {ok, {_,_,_,_,_,_,_,_} = Addr} -> Addr;
        {error, einval} ->
            case inet:getaddr(Hostname, inet) of
                 {error, _} ->
                     {ok, Addr} = inet:getaddr(Hostname, inet6),
                     Addr;
                 {ok, Addr} -> Addr
            end
    end.

path("") ->
    "/";
path(Path) ->
    Path.

set_content_type(Headers) ->
    NHeaders = proplists:delete(<<"Content-Type">>, proplists:delete(<<"content-type">>, Headers)),
    [{<<"content-type">>, <<"application/json">>} | NHeaders].