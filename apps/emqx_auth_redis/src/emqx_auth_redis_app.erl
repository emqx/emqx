%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_auth_redis_app).

-behaviour(application).

-emqx_plugin(auth).

-include("emqx_auth_redis.hrl").

-export([ start/2
        , stop/1
        ]).

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqx_auth_redis_sup:start_link(),
    _ = if_cmd_enabled(auth_cmd, fun load_auth_hook/1),
    _ = if_cmd_enabled(acl_cmd,  fun load_acl_hook/1),
    {ok, Sup}.

stop(_State) ->
    emqx:unhook('client.authenticate', fun emqx_auth_redis:check/3),
    emqx:unhook('client.check_acl', fun emqx_acl_redis:check_acl/5),
    %% Ensure stop cluster pool if the server type is cluster
    eredis_cluster:stop_pool(?APP).

load_auth_hook(AuthCmd) ->
    SuperCmd = application:get_env(?APP, super_cmd, undefined),
    {ok, HashType} = application:get_env(?APP, password_hash),
    {ok, Timeout} = application:get_env(?APP, query_timeout),
    Type = proplists:get_value(type, application:get_env(?APP, server, [])),
    Config = #{auth_cmd => AuthCmd,
               super_cmd => SuperCmd,
               hash_type => HashType,
               timeout => Timeout,
               type => Type,
               pool => ?APP},
    emqx:hook('client.authenticate', fun emqx_auth_redis:check/3, [Config]).

load_acl_hook(AclCmd) ->
    {ok, Timeout} = application:get_env(?APP, query_timeout),
    Type = proplists:get_value(type, application:get_env(?APP, server, [])),
    Config = #{acl_cmd => AclCmd,
               timeout => Timeout,
               type => Type,
               pool => ?APP},
    emqx:hook('client.check_acl', fun emqx_acl_redis:check_acl/5, [Config]).

if_cmd_enabled(Par, Fun) ->
    case application:get_env(?APP, Par) of
        {ok, Cmd} -> Fun(Cmd);
        undefined -> ok
    end.
