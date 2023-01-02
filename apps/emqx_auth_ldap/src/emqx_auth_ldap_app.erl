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

-module(emqx_auth_ldap_app).

-behaviour(application).

-emqx_plugin(auth).

-include("emqx_auth_ldap.hrl").

%% Application callbacks
-export([ start/2
        , prep_stop/1
        , stop/1
        ]).

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqx_auth_ldap_sup:start_link(),
    _ = if_enabled([device_dn, match_objectclass,
                username_attr, password_attr,
                filters, custom_base_dn, bind_as_user],
               fun load_auth_hook/1),
    _ = if_enabled([device_dn, match_objectclass,
                username_attr, password_attr,
                filters, custom_base_dn, bind_as_user],
               fun load_acl_hook/1),
    {ok, Sup}.

prep_stop(State) ->
    emqx:unhook('client.authenticate', fun emqx_auth_ldap:check/3),
    emqx:unhook('client.check_acl', fun emqx_acl_ldap:check_acl/5),
    State.

stop(_State) ->
    ok.

load_auth_hook(DeviceDn) ->
    Params = maps:from_list(DeviceDn),
    emqx:hook('client.authenticate', fun emqx_auth_ldap:check/3, [Params#{pool => ?APP}]).

load_acl_hook(DeviceDn) ->
    Params = maps:from_list(DeviceDn),
    emqx:hook('client.check_acl', fun emqx_acl_ldap:check_acl/5 , [Params#{pool => ?APP}]).

if_enabled(Cfgs, Fun) ->
    case get_env(Cfgs) of
        {ok, []} -> ok;
        {ok, InitArgs} -> Fun(InitArgs)
    end.

get_env(Cfgs) ->
   get_env(Cfgs, []).

get_env([Cfg | LeftCfgs], ENVS) ->
    case application:get_env(?APP, Cfg) of
        {ok, ENV} ->
            get_env(LeftCfgs, [{Cfg, ENV} | ENVS]);
        undefined ->
            get_env(LeftCfgs, ENVS)
    end;
get_env([], ENVS) ->
   {ok, ENVS}.
