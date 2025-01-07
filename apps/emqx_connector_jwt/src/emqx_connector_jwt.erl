%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_connector_jwt).

-include("emqx_connector_jwt_tables.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("jose/include/jose_jwt.hrl").
-include_lib("jose/include/jose_jws.hrl").

%% API
-export([
    lookup_jwt/1,
    lookup_jwt/2,
    delete_jwt/2,
    ensure_jwt/1
]).

-type jwt() :: binary().
-type wrapped_jwk() :: fun(() -> jose_jwk:key()).
-type jwk() :: jose_jwk:key().
-type duration() :: non_neg_integer().
-type jwt_config() :: #{
    expiration := duration(),
    resource_id := resource_id(),
    table => ets:table(),
    jwk := wrapped_jwk() | jwk(),
    iss := binary(),
    sub := binary(),
    aud := binary(),
    kid := binary(),
    alg := binary()
}.

-export_type([jwt_config/0, jwt/0]).

-spec lookup_jwt(resource_id()) -> {ok, jwt()} | {error, not_found}.
lookup_jwt(ResourceId) ->
    ?MODULE:lookup_jwt(?JWT_TABLE, ResourceId).

-spec lookup_jwt(ets:table(), resource_id()) -> {ok, jwt()} | {error, not_found}.
lookup_jwt(TId, ResourceId) ->
    try
        case ets:lookup(TId, {ResourceId, jwt}) of
            [{{ResourceId, jwt}, JWT}] ->
                {ok, JWT};
            [] ->
                {error, not_found}
        end
    catch
        error:badarg ->
            {error, not_found}
    end.

-spec delete_jwt(ets:table(), resource_id()) -> ok.
delete_jwt(TId, ResourceId) ->
    try
        ets:delete(TId, {ResourceId, jwt}),
        ?tp(connector_jwt_deleted, #{}),
        ok
    catch
        error:badarg ->
            ok
    end.

%% @doc Attempts to retrieve a valid JWT from the cache.  If there is
%% none or if the cached token is expired, generates an caches a fresh
%% one.
-spec ensure_jwt(jwt_config()) -> jwt().
ensure_jwt(JWTConfig) ->
    #{resource_id := ResourceId} = JWTConfig,
    Table = maps:get(table, JWTConfig, ?JWT_TABLE),
    case lookup_jwt(Table, ResourceId) of
        {error, not_found} ->
            JWT = do_generate_jwt(JWTConfig),
            store_jwt(JWTConfig, JWT),
            JWT;
        {ok, JWT0} ->
            case is_about_to_expire(JWT0) of
                true ->
                    JWT = do_generate_jwt(JWTConfig),
                    store_jwt(JWTConfig, JWT),
                    JWT;
                false ->
                    JWT0
            end
    end.

%%-----------------------------------------------------------------------------------------
%% Helper fns
%%-----------------------------------------------------------------------------------------

-spec do_generate_jwt(jwt_config()) -> jwt().
do_generate_jwt(#{
    expiration := ExpirationMS,
    iss := Iss,
    sub := Sub,
    aud := Aud,
    kid := KId,
    alg := Alg,
    jwk := WrappedJWK
}) ->
    JWK = emqx_secret:unwrap(WrappedJWK),
    Headers = #{
        <<"alg">> => Alg,
        <<"kid">> => KId
    },
    Now = erlang:system_time(seconds),
    ExpirationS = erlang:convert_time_unit(ExpirationMS, millisecond, second),
    Claims = #{
        <<"iss">> => Iss,
        <<"sub">> => Sub,
        <<"aud">> => Aud,
        <<"iat">> => Now,
        <<"exp">> => Now + ExpirationS
    },
    JWT0 = jose_jwt:sign(JWK, Headers, Claims),
    {_, JWT} = jose_jws:compact(JWT0),
    JWT.

-spec store_jwt(jwt_config(), jwt()) -> ok.
store_jwt(#{resource_id := ResourceId} = JWTConfig, JWT) ->
    Table = maps:get(table, JWTConfig, ?JWT_TABLE),
    true = ets:insert(Table, {{ResourceId, jwt}, JWT}),
    ?tp(emqx_connector_jwt_token_stored, #{resource_id => ResourceId}),
    ok.

-spec is_about_to_expire(jwt()) -> boolean().
is_about_to_expire(JWT) ->
    #jose_jwt{fields = #{<<"exp">> := Exp}} = jose_jwt:peek(JWT),
    Now = erlang:system_time(seconds),
    GraceExp = Exp - 5,
    Now >= GraceExp.
