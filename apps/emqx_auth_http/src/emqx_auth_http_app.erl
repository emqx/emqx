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
-behaviour(supervisor).

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
    with_env(auth_req, fun load_auth_hook/1),
    with_env(acl_req,  fun load_acl_hook/1),
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

load_auth_hook(AuthReq) ->
    ok = emqx_auth_http:register_metrics(),
    SuperReq = r(application:get_env(?APP, super_req, undefined)),
    HttpOpts = application:get_env(?APP, http_opts, []),
    RetryOpts = application:get_env(?APP, retry_opts, []),
    Headers = application:get_env(?APP, headers, []),
    Params = #{auth_req   => AuthReq,
               super_req  => SuperReq,
               http_opts  => HttpOpts,
               retry_opts => maps:from_list(RetryOpts),
               headers    => Headers},
    emqx:hook('client.authenticate', {emqx_auth_http, check, [Params]}).

load_acl_hook(AclReq) ->
    ok = emqx_acl_http:register_metrics(),
    HttpOpts = application:get_env(?APP, http_opts, []),
    RetryOpts = application:get_env(?APP, retry_opts, []),
    Headers = application:get_env(?APP, headers, []),
    Params = #{acl_req    => AclReq,
               http_opts  => HttpOpts,
               retry_opts => maps:from_list(RetryOpts),
               headers    => Headers},
    emqx:hook('client.check_acl', {emqx_acl_http, check_acl, [Params]}).

stop(_State) ->
    emqx:unhook('client.authenticate', {emqx_auth_http, check}),
    emqx:unhook('client.check_acl', {emqx_acl_http, check_acl}).

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
    Method = proplists:get_value(method, Config, post),
    ContentType = proplists:get_value(content_type, Config, 'x-www-form-urlencoded'),
    Url    = proplists:get_value(url, Config),
    Params = proplists:get_value(params, Config),
    #http_request{method = Method, content_type = ContentType, url = Url, params = Params, options = inet(Url)}.

inet(Url) ->
    case uri_string:parse(Url) of
        #{host := Host} ->
            case inet:parse_address(Host) of
                {ok, Ip} when tuple_size(Ip) =:= 8 ->
                    [{ipv6_host_with_brackets, true}, {socket_opts, [{ipfamily, inet6}]}];
                _ -> []
            end;
        _ -> []
    end.
