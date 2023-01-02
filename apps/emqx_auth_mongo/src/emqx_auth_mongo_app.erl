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

-ifdef(TEST).
-export([with_env/2]).
-endif.

%%--------------------------------------------------------------------
%% Application callbacks
%%--------------------------------------------------------------------

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqx_auth_mongo_sup:start_link(),
    ok = safe_start(),
    {ok, Sup}.

prep_stop(State) ->
    ok = unload_hook(),
    _ = stop_pool(),
    State.

stop(_State) ->
    ok.

unload_hook() ->
    ok = emqx:unhook('client.authenticate', fun emqx_auth_mongo:check/3),
    ok = emqx:unhook('client.check_acl', fun emqx_acl_mongo:check_acl/5).

stop_pool() ->
    ecpool:stop_sup_pool(?APP).

safe_start() ->
    try
        ok = with_env(auth_query, fun reg_authmod/1),
        ok = with_env(acl_query,  fun reg_aclmod/1),
        ok
    catch _E:R:_S ->
        unload_hook(),
        _ = stop_pool(),
        {error, R}
    end.

reg_authmod(AuthQuery) ->
    case emqx_auth_mongo:available(?APP, AuthQuery) of
        ok ->
            HookFun = fun emqx_auth_mongo:check/3,
            HookOptions = #{authquery => AuthQuery, superquery => undefined, pool => ?APP},
            case r(super_query, application:get_env(?APP, super_query, undefined)) of
                undefined ->
                    ok = emqx:hook('client.authenticate', HookFun, [HookOptions]);
                SuperQuery ->
                    case emqx_auth_mongo:available(?APP, SuperQuery) of
                        ok ->
                            ok = emqx:hook('client.authenticate', HookFun,
                                     [HookOptions#{superquery => SuperQuery}]);
                        {error, Reason} ->
                            {error, Reason}
                    end
            end;
        {error, Reason} ->
            {error, Reason}
    end.

reg_aclmod(AclQuery) ->
    case emqx_auth_mongo:available(?APP, AclQuery) of
        ok ->
            ok = emqx:hook('client.check_acl', fun emqx_acl_mongo:check_acl/5,
                     [#{aclquery => AclQuery, pool => ?APP}]);
        {error, Reason} ->
            {error, Reason}
    end.

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
