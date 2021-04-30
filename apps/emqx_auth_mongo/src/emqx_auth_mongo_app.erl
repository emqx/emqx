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

-module(emqx_auth_mongo_app).

-behaviour(application).

-emqx_plugin(auth).

-include("emqx_auth_mongo.hrl").

-import(proplists, [get_value/3]).

%% Application callbacks
-export([ start/2
        , prep_stop/1
        , stop/1
        ]).

%%--------------------------------------------------------------------
%% Application callbacks
%%--------------------------------------------------------------------

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqx_auth_mongo_sup:start_link(),
    with_env(auth_query, fun reg_authmod/1),
    with_env(acl_query,  fun reg_aclmod/1),
    {ok, Sup}.

prep_stop(State) ->
    ok = emqx:unhook('client.authenticate', fun emqx_auth_mongo:check/3),
    ok = emqx:unhook('client.check_acl', fun emqx_acl_mongo:check_acl/5),
    State.

stop(_State) ->
    ok.

reg_authmod(AuthQuery) ->
    emqx_auth_mongo:register_metrics(),
    SuperQuery = r(super_query, application:get_env(?APP, super_query, undefined)),
    ok = emqx:hook('client.authenticate', fun emqx_auth_mongo:check/3,
                   [#{authquery => AuthQuery, superquery => SuperQuery, pool => ?APP}]).

reg_aclmod(AclQuery) ->
    emqx_acl_mongo:register_metrics(),
    ok = emqx:hook('client.check_acl', fun emqx_acl_mongo:check_acl/5, [#{aclquery => AclQuery, pool => ?APP}]).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

with_env(Name, Fun) ->
    case application:get_env(?APP, Name) of
        undefined    -> ok;
        {ok, Config} -> Fun(r(Name, Config))
    end.

r(super_query, undefined) ->
    undefined;
r(super_query, Config) ->
    #superquery{collection = list_to_binary(get_value(collection, Config, "mqtt_user")),
                field      = list_to_binary(get_value(super_field, Config, "is_superuser")),
                selector   = get_value(selector, Config, ?DEFAULT_SELECTORS)};

r(auth_query, Config) ->
    #authquery{collection = list_to_binary(get_value(collection, Config, "mqtt_user")),
               field      = get_value(password_field, Config, [<<"password">>]),
               hash       = get_value(password_hash, Config, sha256),
               selector   = get_value(selector, Config, ?DEFAULT_SELECTORS)};

r(acl_query, Config) ->
    #aclquery{collection = list_to_binary(get_value(collection, Config, "mqtt_acl")),
              selector   = get_value(selector, Config, [?DEFAULT_SELECTORS])}.

