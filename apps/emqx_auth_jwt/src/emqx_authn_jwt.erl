%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authn_jwt).

-include_lib("emqx_auth/include/emqx_authn.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_placeholder.hrl").
-include_lib("jose/include/jose_jwk.hrl").

-export([
    create/2,
    update/2,
    authenticate/2,
    destroy/1
]).

-define(ALLOWED_VARS, [
    ?VAR_CLIENTID,
    ?VAR_USERNAME
]).

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

create(_AuthenticatorID, Config) ->
    create(Config).

create(#{verify_claims := VerifyClaims} = Config) ->
    do_create(Config#{verify_claims => handle_verify_claims(VerifyClaims)}).

update(
    #{use_jwks := false} = Config,
    #{jwk_resource := ResourceId}
) ->
    _ = emqx_resource:remove_local(ResourceId),
    create(Config);
update(#{use_jwks := false} = Config, _State) ->
    create(Config);
update(
    #{use_jwks := true} = Config,
    #{jwk_resource := ResourceId} = State
) ->
    case emqx_resource:simple_sync_query(ResourceId, {update, connector_opts(Config)}) of
        ok ->
            case maps:get(verify_claims, Config, undefined) of
                undefined ->
                    {ok, State};
                VerifyClaims ->
                    {ok, State#{verify_claims => handle_verify_claims(VerifyClaims)}}
            end;
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "jwks_client_option_update_failed",
                resource => ResourceId,
                reason => Reason
            })
    end;
update(#{use_jwks := true} = Config, _State) ->
    create(Config).

authenticate(#{auth_method := _}, _) ->
    ignore;
authenticate(
    Credential,
    #{
        verify_claims := VerifyClaims0,
        disconnect_after_expire := DisconnectAfterExpire,
        jwk := JWK,
        acl_claim_name := AclClaimName,
        from := From
    }
) ->
    JWT = maps:get(From, Credential),
    %% XXX: Only supports single public key
    JWKs = [JWK],
    VerifyClaims = render_expected(VerifyClaims0, Credential),
    verify(JWT, JWKs, VerifyClaims, AclClaimName, DisconnectAfterExpire);
authenticate(
    Credential,
    #{
        verify_claims := VerifyClaims0,
        disconnect_after_expire := DisconnectAfterExpire,
        jwk_resource := ResourceId,
        acl_claim_name := AclClaimName,
        from := From
    }
) ->
    case emqx_resource:simple_sync_query(ResourceId, get_jwks) of
        {error, Reason} ->
            ?TRACE_AUTHN_PROVIDER(error, "get_jwks_failed", #{
                resource => ResourceId,
                reason => Reason
            }),
            ignore;
        {ok, JWKs} ->
            JWT = maps:get(From, Credential),
            VerifyClaims = render_expected(VerifyClaims0, Credential),
            verify(JWT, JWKs, VerifyClaims, AclClaimName, DisconnectAfterExpire)
    end.

destroy(#{jwk_resource := ResourceId}) ->
    _ = emqx_resource:remove_local(ResourceId),
    ok;
destroy(_) ->
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

do_create(#{
    use_jwks := false,
    algorithm := 'hmac-based',
    secret := Secret0,
    secret_base64_encoded := Base64Encoded,
    verify_claims := VerifyClaims,
    disconnect_after_expire := DisconnectAfterExpire,
    acl_claim_name := AclClaimName,
    from := From
}) ->
    case may_decode_secret(Base64Encoded, Secret0) of
        {error, Reason} ->
            {error, Reason};
        Secret ->
            JWK = jose_jwk:from_oct(Secret),
            {ok, #{
                jwk => JWK,
                verify_claims => VerifyClaims,
                disconnect_after_expire => DisconnectAfterExpire,
                acl_claim_name => AclClaimName,
                from => From
            }}
    end;
do_create(
    #{
        use_jwks := false,
        algorithm := 'public-key',
        public_key := PublicKey,
        verify_claims := VerifyClaims,
        disconnect_after_expire := DisconnectAfterExpire,
        acl_claim_name := AclClaimName,
        from := From
    } = Config
) ->
    case
        create_jwk_from_public_key(
            maps:get(enable, Config, false),
            PublicKey
        )
    of
        {ok, JWK} ->
            {ok, #{
                jwk => JWK,
                verify_claims => VerifyClaims,
                disconnect_after_expire => DisconnectAfterExpire,
                acl_claim_name => AclClaimName,
                from => From
            }};
        {error, _Reason} = Err ->
            Err
    end;
do_create(
    #{
        use_jwks := true,
        verify_claims := VerifyClaims,
        disconnect_after_expire := DisconnectAfterExpire,
        acl_claim_name := AclClaimName,
        from := From
    } = Config
) ->
    ResourceId = emqx_authn_utils:make_resource_id(?MODULE),
    {ok, _Data} = emqx_resource:create_local(
        ResourceId,
        ?AUTHN_RESOURCE_GROUP,
        emqx_authn_jwks_connector,
        connector_opts(Config),
        #{}
    ),
    {ok, #{
        jwk_resource => ResourceId,
        verify_claims => VerifyClaims,
        disconnect_after_expire => DisconnectAfterExpire,
        acl_claim_name => AclClaimName,
        from => From
    }}.

create_jwk_from_public_key(true, PublicKey) when
    is_binary(PublicKey); is_list(PublicKey)
->
    try do_create_jwk_from_public_key(PublicKey) of
        %% XXX: Only supports single public key
        #jose_jwk{} = Res ->
            {ok, Res};
        _ ->
            {error, invalid_public_key}
    catch
        _:_ ->
            {error, invalid_public_key}
    end;
create_jwk_from_public_key(false, _PublicKey) ->
    {ok, []}.

do_create_jwk_from_public_key(PublicKey) ->
    case filelib:is_file(PublicKey) of
        true ->
            jose_jwk:from_pem_file(PublicKey);
        false ->
            jose_jwk:from_pem(iolist_to_binary(PublicKey))
    end.

connector_opts(#{ssl := #{enable := Enable} = SSL} = Config) ->
    SSLOpts =
        case Enable of
            true -> maps:without([enable], SSL);
            false -> #{}
        end,
    Config#{ssl_opts => SSLOpts}.

may_decode_secret(false, Secret) ->
    Secret;
may_decode_secret(true, Secret) ->
    try
        base64:decode(Secret)
    catch
        error:_ ->
            {error, {invalid_parameter, secret}}
    end.

render_expected([], _Variables) ->
    [];
render_expected([{Name, ExpectedTemplate} | More], Variables) ->
    Expected = emqx_auth_template:render_str(ExpectedTemplate, Variables),
    [{Name, Expected} | render_expected(More, Variables)].

verify(undefined, _, _, _, _) ->
    ignore;
verify(JWT, JWKs, VerifyClaims, AclClaimName, DisconnectAfterExpire) ->
    case do_verify(JWT, JWKs, VerifyClaims) of
        {ok, Extra} ->
            extra_to_auth_data(Extra, JWT, AclClaimName, DisconnectAfterExpire);
        {error, {missing_claim, Claim}} ->
            %% it's a invalid token, so it's ok to log
            ?TRACE_AUTHN_PROVIDER("missing_jwt_claim", #{jwt => JWT, claim => Claim}),
            {error, bad_username_or_password};
        {error, invalid_signature} ->
            %% it's a invalid token, so it's ok to log
            ?TRACE_AUTHN_PROVIDER("invalid_jwt_signature", #{jwks => JWKs, jwt => JWT}),
            ignore;
        {error, {claims, Claims}} ->
            %% it's a invalid token, so it's ok to log
            ?TRACE_AUTHN_PROVIDER("invalid_jwt_claims", #{jwt => JWT, claims => Claims}),
            {error, bad_username_or_password}
    end.

extra_to_auth_data(Extra, JWT, AclClaimName, DisconnectAfterExpire) ->
    IsSuperuser = emqx_authn_utils:is_superuser(Extra),
    Attrs = emqx_authn_utils:client_attrs(Extra),
    ExpireAt = expire_at(DisconnectAfterExpire, Extra),
    try
        ACL = acl(Extra, AclClaimName),
        Result = merge_maps([ExpireAt, IsSuperuser, ACL, Attrs]),
        {ok, Result}
    catch
        throw:{bad_acl_rule, Reason} ->
            %% it's a invalid token, so ok to log
            ?TRACE_AUTHN_PROVIDER("bad_acl_rule", Reason#{jwt => JWT}),
            {error, bad_username_or_password}
    end.

expire_at(false, _Extra) ->
    #{};
expire_at(true, #{<<"exp">> := ExpireTime}) ->
    #{expire_at => erlang:convert_time_unit(ExpireTime, second, millisecond)};
expire_at(true, #{}) ->
    #{}.

acl(Claims, AclClaimName) ->
    case Claims of
        #{AclClaimName := Rules} ->
            #{
                acl => #{
                    rules => parse_rules(Rules),
                    source_for_logging => jwt,
                    expire => maps:get(<<"exp">>, Claims, undefined)
                }
            };
        _ ->
            #{}
    end.

do_verify(_JWT, [], _VerifyClaims) ->
    {error, invalid_signature};
do_verify(JWT, [JWK | More], VerifyClaims) ->
    try jose_jws:verify(JWK, JWT) of
        {true, Payload, _JWT} ->
            Claims0 = emqx_utils_json:decode(Payload, [return_maps]),
            Claims = try_convert_to_num(Claims0, [<<"exp">>, <<"nbf">>]),
            case verify_claims(Claims, VerifyClaims) of
                ok ->
                    {ok, Claims};
                {error, Reason} ->
                    {error, Reason}
            end;
        {false, _, _} ->
            do_verify(JWT, More, VerifyClaims)
    catch
        _:Reason ->
            ?TRACE_AUTHN_PROVIDER("jwt_verify_error", #{jwt => JWT, reason => Reason}),
            do_verify(JWT, More, VerifyClaims)
    end.

verify_claims(Claims, VerifyClaims0) ->
    Now = erlang:system_time(seconds),
    VerifyClaims =
        [
            {<<"exp">>, fun(ExpireTime) ->
                is_number(ExpireTime) andalso Now < ExpireTime
            end},
            {<<"nbf">>, fun(NotBefore) ->
                is_number(NotBefore) andalso NotBefore =< Now
            end}
        ] ++ VerifyClaims0,
    do_verify_claims(Claims, VerifyClaims).

try_convert_to_num(Claims, [Name | Names]) ->
    case Claims of
        #{Name := Value} ->
            case Value of
                Int when is_number(Int) ->
                    try_convert_to_num(Claims#{Name => Int}, Names);
                Bin when is_binary(Bin) ->
                    case binary_to_number(Bin) of
                        {ok, Num} ->
                            try_convert_to_num(Claims#{Name => Num}, Names);
                        _ ->
                            try_convert_to_num(Claims, Names)
                    end;
                _ ->
                    try_convert_to_num(Claims, Names)
            end;
        _ ->
            try_convert_to_num(Claims, Names)
    end;
try_convert_to_num(Claims, []) ->
    Claims.

do_verify_claims(_Claims, []) ->
    ok;
do_verify_claims(Claims, [{Name, Fun} | More]) when is_function(Fun) ->
    case maps:take(Name, Claims) of
        error ->
            do_verify_claims(Claims, More);
        {Value, NClaims} ->
            case Fun(Value) of
                true ->
                    do_verify_claims(NClaims, More);
                _ ->
                    {error, {claims, {Name, Value}}}
            end
    end;
do_verify_claims(Claims, [{Name, Value} | More]) ->
    case maps:take(Name, Claims) of
        error ->
            {error, {missing_claim, Name}};
        {Value, NClaims} ->
            do_verify_claims(NClaims, More);
        {Value0, _} ->
            {error, {claims, {Name, Value0}}}
    end.

handle_verify_claims(VerifyClaims) ->
    handle_verify_claims(VerifyClaims, []).

handle_verify_claims([], Acc) ->
    Acc;
handle_verify_claims([{Name, Expected0} | More], Acc) ->
    {_, Expected1} = emqx_auth_template:parse_str(Expected0, ?ALLOWED_VARS),
    handle_verify_claims(More, [{Name, Expected1} | Acc]).

binary_to_number(Bin) ->
    case string:to_integer(Bin) of
        {Val, <<>>} ->
            {ok, Val};
        _ ->
            case string:to_float(Bin) of
                {Val, <<>>} -> {ok, Val};
                _ -> false
            end
    end.

%% Parse rules which can be in two different formats:
%% 1. #{<<"pub">> => [<<"a/b">>, <<"c/d">>], <<"sub">> => [...], <<"all">> => [...]}
%% 2. [#{<<"permission">> => <<"allow">>, <<"action">> => <<"publish">>, <<"topic">> => <<"a/b">>}, ...]
parse_rules(Rules) when is_map(Rules) ->
    Rules;
parse_rules(Rules) when is_list(Rules) ->
    emqx_authz_rule_raw:parse_and_compile_rules(Rules).

merge_maps([]) -> #{};
merge_maps([Map | Maps]) -> maps:merge(Map, merge_maps(Maps)).
