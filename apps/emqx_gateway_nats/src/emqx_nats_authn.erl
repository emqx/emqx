%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_nats_authn).

-export([
    build_authn_ctx/4,
    is_auth_required/2,
    ensure_nkey_nonce/2,
    maybe_add_nkey_nonce/2,
    authenticate/4
]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-type authn_ctx() :: #{
    token => undefined | binary(),
    nkeys => [binary()],
    jwt => undefined | map(),
    gateway_auth_enabled => boolean()
}.

-type authn_method() :: token | nkey | jwt.

-spec build_authn_ctx(term(), term(), term(), boolean()) -> authn_ctx().
build_authn_ctx(Token0, NKeys0, JWT0, GatewayAuthEnabled) ->
    #{
        token => normalize_token(Token0),
        nkeys => normalize_nkeys(NKeys0),
        jwt => normalize_jwt_config(JWT0),
        gateway_auth_enabled => GatewayAuthEnabled =:= true
    }.

-spec is_auth_required(map(), authn_ctx()) -> boolean().
is_auth_required(#{enable_authn := false}, _Authn) ->
    false;
is_auth_required(#{enable_authn := true}, Authn) ->
    token_auth_enabled(Authn) orelse
        nkey_auth_enabled(Authn) orelse
        jwt_auth_enabled(Authn) orelse
        gateway_auth_enabled(Authn).

-spec ensure_nkey_nonce(map(), authn_ctx()) -> map().
ensure_nkey_nonce(ConnInfo, Authn) ->
    case nkey_auth_enabled(Authn) of
        false ->
            ConnInfo;
        true ->
            case maps:get(nkey_nonce, ConnInfo, undefined) of
                undefined ->
                    ConnInfo#{nkey_nonce => emqx_utils:rand_id(24)};
                _ ->
                    ConnInfo
            end
    end.

-spec maybe_add_nkey_nonce(map(), map()) -> map().
maybe_add_nkey_nonce(MsgContent, ConnInfo) ->
    case maps:get(nkey_nonce, ConnInfo, undefined) of
        undefined -> MsgContent;
        Nonce -> MsgContent#{nonce => Nonce}
    end.

-spec authenticate(map(), map(), map(), authn_ctx()) ->
    {ok, map()} | {continue, map()} | {error, {authn_method(), term()}}.
authenticate(ConnParams, ConnInfo, ClientInfo, Authn) ->
    case maybe_token_auth(ConnParams, ClientInfo, Authn) of
        {ok, NClientInfo} ->
            {ok, NClientInfo};
        {skip, NClientInfo} ->
            authenticate_with_nkey(ConnParams, ConnInfo, NClientInfo, Authn);
        {error, Reason} ->
            {error, {token, Reason}}
    end.

authenticate_with_nkey(ConnParams, ConnInfo, ClientInfo, Authn) ->
    case maybe_nkey_auth(ConnParams, ConnInfo, ClientInfo, Authn) of
        {ok, NClientInfo} ->
            {ok, NClientInfo};
        {skip, NClientInfo} ->
            authenticate_with_jwt(ConnParams, NClientInfo, Authn);
        {error, Reason} ->
            {error, {nkey, Reason}}
    end.

authenticate_with_jwt(ConnParams, ClientInfo, Authn) ->
    case maybe_jwt_auth(ConnParams, ClientInfo, Authn) of
        {ok, NClientInfo} ->
            {ok, NClientInfo};
        {skip, NClientInfo} ->
            {continue, NClientInfo};
        {error, Reason} ->
            {error, {jwt, Reason}}
    end.

maybe_token_auth(ConnParams, ClientInfo, Authn) ->
    case token_auth_enabled(Authn) of
        false ->
            {skip, ClientInfo};
        true ->
            AuthToken = conn_param(ConnParams, <<"auth_token">>),
            case normalize_token(AuthToken) of
                undefined ->
                    case
                        nkey_auth_enabled(Authn) orelse jwt_auth_enabled(Authn) orelse
                            gateway_auth_enabled(Authn)
                    of
                        true -> {skip, ClientInfo};
                        false -> {error, token_required}
                    end;
                Token ->
                    token_authenticate(Token, ClientInfo, Authn)
            end
    end.

maybe_nkey_auth(ConnParams, ConnInfo, ClientInfo, Authn) ->
    case nkey_auth_enabled(Authn) of
        false ->
            {skip, ClientInfo};
        true ->
            NKey = normalize_token(conn_param(ConnParams, <<"nkey">>)),
            Sig = conn_param(ConnParams, <<"sig">>),
            maybe_nkey_auth_params(NKey, Sig, ConnInfo, ClientInfo, Authn)
    end.

maybe_nkey_auth_params(undefined, _Sig, _ConnInfo, ClientInfo, Authn) ->
    case
        token_auth_enabled(Authn) orelse jwt_auth_enabled(Authn) orelse gateway_auth_enabled(Authn)
    of
        true -> {skip, ClientInfo};
        false -> {error, nkey_required}
    end;
maybe_nkey_auth_params(_NKey, undefined, _ConnInfo, _ClientInfo, _Authn) ->
    {error, nkey_sig_required};
maybe_nkey_auth_params(NKey, Sig, ConnInfo, ClientInfo, Authn) ->
    case maps:get(nkey_nonce, ConnInfo, undefined) of
        undefined ->
            {error, nkey_nonce_unavailable};
        Nonce ->
            nkey_authenticate(NKey, Sig, Nonce, ClientInfo, Authn)
    end.

nkey_authenticate(NKey, Sig, Nonce, ClientInfo, Authn) ->
    Allowed = maps:get(nkeys, Authn, []),
    case nkey_allowed(NKey, Allowed) of
        false ->
            {error, invalid_nkey};
        true ->
            case emqx_nats_nkey:verify_signature(NKey, Sig, Nonce) of
                {ok, _PubKey} ->
                    {ok, ClientInfo#{
                        auth_method => nkey,
                        nkey => NKey,
                        auth_expire_at => undefined
                    }};
                {error, Reason} ->
                    {error, Reason}
            end
    end.

maybe_jwt_auth(ConnParams, ClientInfo, Authn) ->
    case jwt_auth_enabled(Authn) of
        false ->
            {skip, ClientInfo};
        true ->
            JWT = normalize_token(conn_param(ConnParams, <<"jwt">>)),
            case JWT of
                undefined ->
                    case gateway_auth_enabled(Authn) of
                        true -> {skip, ClientInfo};
                        false -> {error, jwt_required}
                    end;
                _ ->
                    jwt_authenticate(JWT, ClientInfo, Authn)
            end
    end.

jwt_authenticate(JWT, ClientInfo, Authn) ->
    case decode_jwt_claims(JWT) of
        {ok, Claims} ->
            Opts = jwt_auth_options(Authn),
            case verify_jwt_claims_time(Claims, Opts) of
                ok ->
                    JWTPerms = extract_jwt_permissions(Claims),
                    NClientInfo = maybe_set_clientinfo_from_jwt_claims(ClientInfo, Claims),
                    {ok, NClientInfo#{
                        auth_method => jwt,
                        jwt_claims => Claims,
                        jwt_permissions => JWTPerms,
                        auth_expire_at => jwt_claim_expire_at(Claims)
                    }};
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

maybe_set_clientinfo_from_jwt_claims(ClientInfo, Claims) ->
    case normalize_token(map_get(Claims, <<"sub">>, undefined)) of
        undefined ->
            ClientInfo;
        Username ->
            ClientInfo#{username => Username}
    end.

decode_jwt_claims(JWT) ->
    case binary:split(JWT, <<".">>, [global]) of
        [_Header, PayloadB64, SignatureB64] when
            PayloadB64 =/= <<>>,
            SignatureB64 =/= <<>>
        ->
            case base64url_decode(PayloadB64) of
                {ok, Payload} ->
                    case emqx_utils_json:safe_decode(Payload, [return_maps]) of
                        {ok, Claims} when is_map(Claims) ->
                            {ok, Claims};
                        _ ->
                            {error, invalid_jwt_claims}
                    end;
                {error, _} = Error ->
                    Error
            end;
        _ ->
            {error, invalid_jwt_format}
    end.

base64url_decode(Value) ->
    try base64:decode(Value, #{mode => urlsafe, padding => false}) of
        Bin -> {ok, Bin}
    catch
        _:_ -> {error, invalid_jwt_base64}
    end.

verify_jwt_claims_time(Claims, Opts) ->
    Now = erlang:system_time(second),
    case verify_jwt_exp(Claims, maps:get(verify_exp, Opts), Now) of
        ok ->
            verify_jwt_nbf(Claims, maps:get(verify_nbf, Opts), Now);
        {error, _} = Error ->
            Error
    end.

verify_jwt_exp(_Claims, false, _Now) ->
    ok;
verify_jwt_exp(Claims, true, Now) ->
    case map_get(Claims, <<"exp">>, undefined) of
        undefined ->
            ok;
        Exp when is_integer(Exp), Now < Exp ->
            ok;
        Exp when is_float(Exp), Now < trunc(Exp) ->
            ok;
        _ ->
            {error, jwt_expired}
    end.

verify_jwt_nbf(_Claims, false, _Now) ->
    ok;
verify_jwt_nbf(Claims, true, Now) ->
    case map_get(Claims, <<"nbf">>, undefined) of
        undefined ->
            ok;
        Nbf when is_integer(Nbf), Nbf =< Now ->
            ok;
        Nbf when is_float(Nbf), trunc(Nbf) =< Now ->
            ok;
        _ ->
            {error, jwt_not_before}
    end.

jwt_claim_expire_at(Claims) ->
    case map_get(Claims, <<"exp">>, undefined) of
        Exp when is_integer(Exp), Exp > 0 ->
            erlang:convert_time_unit(Exp, second, millisecond);
        Exp when is_float(Exp), Exp > 0 ->
            erlang:convert_time_unit(trunc(Exp), second, millisecond);
        _ ->
            undefined
    end.

extract_jwt_permissions(Claims) ->
    Perms0 = map_get(Claims, <<"permissions">>, #{}),
    Perms = normalize_map(Perms0),
    #{
        publish => extract_jwt_action_permissions(Perms, publish),
        subscribe => extract_jwt_action_permissions(Perms, subscribe)
    }.

extract_jwt_action_permissions(Perms, publish) ->
    extract_allow_deny(map_get_any(Perms, [<<"pub">>, <<"publish">>, pub, publish], #{}));
extract_jwt_action_permissions(Perms, subscribe) ->
    extract_allow_deny(map_get_any(Perms, [<<"sub">>, <<"subscribe">>, sub, subscribe], #{})).

extract_allow_deny(Claim) ->
    ClaimMap = normalize_map(Claim),
    #{
        allow => normalize_subject_list(map_get_any(ClaimMap, [<<"allow">>, allow], [])),
        deny => normalize_subject_list(map_get_any(ClaimMap, [<<"deny">>, deny], []))
    }.

normalize_subject_list(Values) when is_list(Values) ->
    lists:filtermap(
        fun(Value) ->
            case normalize_token(Value) of
                undefined -> false;
                Bin -> {true, Bin}
            end
        end,
        Values
    );
normalize_subject_list(_) ->
    [].

token_authenticate(Token, ClientInfo, Authn) ->
    case maps:get(token, Authn, undefined) of
        undefined ->
            {error, token_disabled};
        ConfigToken ->
            Type = token_type(ConfigToken),
            case check_token(Type, ConfigToken, Token) of
                true ->
                    {ok, ClientInfo#{
                        auth_method => token,
                        token_type => Type,
                        auth_expire_at => undefined
                    }};
                false ->
                    {error, invalid_token}
            end
    end.

check_token(plain, ConfigToken, Token) ->
    emqx_passwd:compare_secure(ConfigToken, Token);
check_token(bcrypt, ConfigToken, Token) ->
    ensure_bcrypt_started(),
    emqx_passwd:check_pass({bcrypt, ConfigToken}, ConfigToken, Token).

ensure_bcrypt_started() ->
    _ = application:ensure_all_started(bcrypt),
    ok.

token_type(Token) ->
    case is_bcrypt_token(Token) of
        true -> bcrypt;
        false -> plain
    end.

is_bcrypt_token(<<"$2a$", _/binary>>) ->
    true;
is_bcrypt_token(<<"$2b$", _/binary>>) ->
    true;
is_bcrypt_token(<<"$2y$", _/binary>>) ->
    true;
is_bcrypt_token(_) ->
    false.

normalize_jwt_config(undefined) ->
    undefined;
normalize_jwt_config(#{enable := false}) ->
    undefined;
normalize_jwt_config(#{<<"enable">> := false}) ->
    undefined;
normalize_jwt_config(Config) when is_map(Config) ->
    Config;
normalize_jwt_config(_) ->
    undefined.

jwt_auth_options(Authn) ->
    Config = maps:get(jwt, Authn, #{}),
    #{
        verify_exp => to_bool(map_get_any(Config, [verify_exp, <<"verify_exp">>], true)),
        verify_nbf => to_bool(map_get_any(Config, [verify_nbf, <<"verify_nbf">>], true))
    }.

jwt_has_trusted_operators(Config) ->
    Operators = map_get_any(Config, [trusted_operators, <<"trusted_operators">>], []),
    case Operators of
        [_ | _] ->
            true;
        _ ->
            false
    end.

token_auth_enabled(Authn) ->
    maps:get(token, Authn, undefined) =/= undefined.

nkey_auth_enabled(Authn) ->
    maps:get(nkeys, Authn, []) =/= [].

jwt_auth_enabled(Authn) ->
    case maps:get(jwt, Authn, undefined) of
        undefined ->
            false;
        Config ->
            jwt_has_trusted_operators(Config)
    end.

gateway_auth_enabled(Authn) ->
    maps:get(gateway_auth_enabled, Authn, false) =:= true.

nkey_allowed(NKey, Allowed) ->
    lists:member(emqx_nats_nkey:normalize(NKey), Allowed).

normalize_nkeys(NKeys) when is_list(NKeys) ->
    lists:map(fun emqx_nats_nkey:normalize/1, NKeys);
normalize_nkeys(_) ->
    [].

conn_param(ConnParams, Key) ->
    maps:get(Key, ConnParams, undefined).

normalize_token(undefined) ->
    undefined;
normalize_token(<<>>) ->
    undefined;
normalize_token(Token) when is_binary(Token) ->
    Token;
normalize_token(Token) ->
    emqx_utils_conv:bin(Token).

map_get(Map, Key, Default) when is_map(Map) ->
    maps:get(Key, Map, Default).

map_get_any(Map, [Key | More], Default) when is_map(Map) ->
    case maps:find(Key, Map) of
        {ok, Value} ->
            Value;
        error ->
            map_get_any(Map, More, Default)
    end;
map_get_any(_, [], Default) ->
    Default.

normalize_map(Map) when is_map(Map) ->
    Map;
normalize_map(_) ->
    #{}.

to_bool(true) ->
    true;
to_bool(false) ->
    false;
to_bool(<<"true">>) ->
    true;
to_bool(<<"false">>) ->
    false;
to_bool("true") ->
    true;
to_bool("false") ->
    false;
to_bool(_) ->
    false.
