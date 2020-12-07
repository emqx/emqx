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

-module(emqx_auth_http_app).

-behaviour(application).

-emqx_plugin(auth).

-include("emqx_auth_http.hrl").

-export([ start/2
        , stop/1
        ]).
-export([init/1]).

%%--------------------------------------------------------------------
%% Application Callbacks
%%--------------------------------------------------------------------

start(_StartType, _StartArgs) ->
    case translate_env() of
        ok ->
            {ok, PoolOpts} = application:get_env(?APP, pool_opts),
            {ok, Sup} = emqx_http_client_sup:start_link(?APP, ssl(inet(PoolOpts))),
            with_env(auth_req, fun load_auth_hook/1),
            with_env(acl_req,  fun load_acl_hook/1),
            {ok, Sup};
        {error, Reason} ->
            {error, Reason}
    end.

load_auth_hook(AuthReq) ->
    ok = emqx_auth_http:register_metrics(),
    SuperReq = r(application:get_env(?APP, super_req, undefined)),
    Params = #{auth_req   => AuthReq,
               super_req  => SuperReq,
               pool_name  => ?APP},
    emqx:hook('client.authenticate', {emqx_auth_http, check, [Params]}).

load_acl_hook(AclReq) ->
    ok = emqx_acl_http:register_metrics(),
    Params = #{acl_req   => AclReq,
               pool_name => ?APP},
    emqx:hook('client.check_acl', {emqx_acl_http, check_acl, [Params]}).

stop(_State) ->
    emqx:unhook('client.authenticate', {emqx_auth_http, check}),
    emqx:unhook('client.check_acl', {emqx_acl_http, check_acl}),
    emqx_http_client_sup:stop_pool(?APP).

%%--------------------------------------------------------------------
%% Dummy supervisor
%%--------------------------------------------------------------------

init([]) ->
    {ok, { {one_for_all, 10, 100}, []} }.

%%--------------------------------------------------------------------
%% Internel functions
%%--------------------------------------------------------------------

with_env(Par, Fun) ->
    case application:get_env(?APP, Par) of
        undefined -> ok;
        {ok, Req} -> Fun(r(Req))
    end.

r(undefined) ->
    undefined;
r(Config) ->
    Headers = application:get_env(?APP, headers, []),
    Method = proplists:get_value(method, Config, post),
    Path    = proplists:get_value(path, Config),
    NewHeaders = [{<<"content_type">>, proplists:get_value(content_type, Config, <<"application/x-www-form-urlencoded">>)} | Headers],
    Params = proplists:get_value(params, Config),
    {ok, RequestTimeout} = application:get_env(?APP, request_timeout),
    #http_request{method = Method, path = Path, headers = NewHeaders, params = Params, request_timeout = RequestTimeout}.

inet(PoolOpts) ->
    case proplists:get_value(host, PoolOpts) of
        Host when tuple_size(Host) =:= 8 ->
            TransOpts = proplists:get_value(transport_opts, PoolOpts, []),
            NewPoolOpts = proplists:delete(transport_opts, PoolOpts),
            [{transport_opts, [inet6 | TransOpts]} | NewPoolOpts];
        _ ->
            PoolOpts
    end.

ssl(PoolOpts) ->
    case proplists:get_value(ssl, PoolOpts, []) of
        [] ->
            PoolOpts;
        SSLOpts ->
            TransOpts = proplists:get_value(transport_opts, PoolOpts, []),
            NewPoolOpts = proplists:delete(transport_opts, PoolOpts),
            [{transport_opts, SSLOpts ++ TransOpts}, {transport, ssl} | NewPoolOpts]
    end.

translate_env() ->
    URLs = lists:foldl(fun(Name, Acc) ->
                    case application:get_env(?APP, Name, []) of
                        [] -> Acc;
                        Env ->
                            URL = proplists:get_value(url, Env),
                            #{host := Host0,
                              port := Port,
                              path := Path} = uri_string:parse(list_to_binary(URL)),
                            {ok, Host} = inet:parse_address(binary_to_list(Host0)),
                            [{Name, {Host, Port, binary_to_list(Path)}} | Acc]
                    end
                end, [], [acl_req, auth_req, super_req]),
    case same_host_and_port(URLs) of
        true ->
            [begin
                 {ok, Req} = application:get_env(?APP, Name),
                 application:set_env(?APP, Name, [{path, Path} | Req])
             end || {Name, {_, _, Path}} <- URLs],
            {_, {Host, Port, _}} = lists:last(URLs),
            PoolOpts = application:get_env(?APP, pool_opts, []),
            application:set_env(?APP, pool_opts, [{host, Host}, {port, Port} | PoolOpts]),
            ok;
        false ->
            {error, different_server}
    end.

same_host_and_port([_]) ->
    true;
same_host_and_port([{_, {Host, Port, _}}, {_, {Host, Port, _}}]) ->
    true;
same_host_and_port([{_, {Host, Port, _}}, URL = {_, {Host, Port, _}} | Rest]) ->
    same_host_and_port([URL | Rest]);
same_host_and_port(_) ->
    false.