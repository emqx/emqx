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
    case nonce_auth_enabled(Authn) of
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
            authenticate_with_jwt(ConnParams, ConnInfo, NClientInfo, Authn);
        {error, Reason} ->
            {error, {nkey, Reason}}
    end.

authenticate_with_jwt(ConnParams, ConnInfo, ClientInfo, Authn) ->
    case maybe_jwt_auth(ConnParams, ConnInfo, ClientInfo, Authn) of
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
            CanonicalNKey = emqx_nats_nkey:normalize(NKey),
            case emqx_nats_nkey:verify_signature(CanonicalNKey, Sig, Nonce) of
                {ok, _PubKey} ->
                    {ok, ClientInfo#{
                        username => CanonicalNKey,
                        password => undefined,
                        auth_method => nkey,
                        nkey => CanonicalNKey,
                        auth_expire_at => undefined
                    }};
                {error, Reason} ->
                    {error, Reason}
            end
    end.

maybe_jwt_auth(ConnParams, ConnInfo, ClientInfo, Authn) ->
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
                    NKey = normalize_token(conn_param(ConnParams, <<"nkey">>)),
                    Sig = normalize_token(conn_param(ConnParams, <<"sig">>)),
                    jwt_authenticate(JWT, NKey, Sig, ConnInfo, ClientInfo, Authn)
            end
    end.

jwt_authenticate(JWT, NKey, Sig, ConnInfo, ClientInfo, Authn) ->
    maybe
        {ok, JWTToken = #{claims := Claims}} ?= decode_jwt(JWT),
        ok ?= verify_jwt_signature_chain(JWTToken, Authn),
        Opts = jwt_auth_options(Authn),
        ok ?= verify_jwt_claims_time(Claims, Opts),
        {ok, Username} ?= verify_jwt_nonce_signature(Claims, NKey, Sig, ConnInfo),
        JWTPerms = extract_jwt_permissions(Claims),
        AuthExpireAt = jwt_claim_expire_at(Claims, Opts),
        {ok, ClientInfo#{
            username => Username,
            password => undefined,
            auth_method => jwt,
            nkey => Username,
            jwt_claims => Claims,
            jwt_permissions => JWTPerms,
            auth_expire_at => AuthExpireAt
        }}
    end.

verify_jwt_nonce_signature(Claims, NKey, Sig, ConnInfo) ->
    maybe
        {ok, Signature} ?= require_jwt_signature(Sig),
        {ok, Nonce} ?= get_nkey_nonce(ConnInfo),
        {ok, SubjectNKey} ?= jwt_claim_sub(Claims),
        ok ?= verify_jwt_connect_nkey(SubjectNKey, NKey),
        {ok, _PubKey} ?= emqx_nats_nkey:verify_signature(SubjectNKey, Signature, Nonce),
        {ok, SubjectNKey}
    end.

require_jwt_signature(undefined) ->
    {error, jwt_sig_required};
require_jwt_signature(Signature) ->
    {ok, Signature}.

get_nkey_nonce(ConnInfo) ->
    case maps:get(nkey_nonce, ConnInfo, undefined) of
        undefined ->
            {error, nkey_nonce_unavailable};
        Nonce ->
            {ok, Nonce}
    end.

verify_jwt_connect_nkey(SubjectNKey, NKey) ->
    case resolve_jwt_connect_nkey(SubjectNKey, NKey) of
        {ok, SubjectNKey} ->
            ok;
        {ok, _OtherNKey} ->
            {error, jwt_nkey_mismatch};
        {error, _} = Error ->
            Error
    end.

resolve_jwt_connect_nkey(SubjectNKey, undefined) ->
    {ok, SubjectNKey};
resolve_jwt_connect_nkey(_SubjectNKey, NKey) ->
    case emqx_nats_nkey:decode_public(NKey) of
        {ok, _PubKey} ->
            {ok, emqx_nats_nkey:normalize(NKey)};
        {error, _} = Error ->
            Error
    end.

decode_jwt(JWT) ->
    case binary:split(JWT, <<".">>, [global]) of
        [HeaderB64, PayloadB64, SignatureB64] when
            HeaderB64 =/= <<>>,
            PayloadB64 =/= <<>>,
            SignatureB64 =/= <<>>
        ->
            maybe
                {ok, HeaderJSON} ?= base64url_decode(HeaderB64),
                {ok, Header} ?= decode_jwt_json(HeaderJSON, invalid_jwt_header),
                {ok, PayloadJSON} ?= base64url_decode(PayloadB64),
                {ok, Claims} ?= decode_jwt_json(PayloadJSON, invalid_jwt_claims),
                {ok, Signature} ?= base64url_decode(SignatureB64),
                {ok, #{
                    header => Header,
                    claims => Claims,
                    signing_input => <<HeaderB64/binary, ".", PayloadB64/binary>>,
                    signature => Signature
                }}
            end;
        _ ->
            {error, invalid_jwt_format}
    end.

decode_jwt_json(JSONBin, ErrReason) ->
    case emqx_utils_json:safe_decode(JSONBin, [return_maps]) of
        {ok, Map} when is_map(Map) ->
            {ok, Map};
        _ ->
            {error, ErrReason}
    end.

base64url_decode(Value) ->
    try base64:decode(Value, #{mode => urlsafe, padding => false}) of
        Bin -> {ok, Bin}
    catch
        _:_ -> {error, invalid_jwt_base64}
    end.

verify_jwt_signature_chain(#{claims := Claims} = UserToken, Authn) ->
    Config = maps:get(jwt, Authn, #{}),
    maybe
        ok ?= ensure_jwt_trusted_operators(Config),
        ok ?= verify_jwt_token_alg(UserToken),
        {ok, UserIssuer} ?= jwt_claim_issuer(Claims),
        AccountPubKey = jwt_claim_issuer_account(Claims, UserIssuer),
        {ok, AccountJWT} ?= jwt_account_jwt(Config, AccountPubKey),
        verify_jwt_account_chain(
            UserToken,
            UserIssuer,
            AccountPubKey,
            AccountJWT,
            Config
        )
    end.

verify_jwt_account_chain(UserToken, UserIssuer, AccountPubKey, AccountJWT, Config) ->
    maybe
        {ok, AccountToken = #{claims := AccountClaims}} ?= decode_jwt_account(AccountJWT),
        ok ?= verify_jwt_account_token_alg(AccountToken),
        ok ?= verify_jwt_account_subject(AccountClaims, AccountPubKey),
        {ok, OperatorPubKey} ?= jwt_claim_account_issuer(AccountClaims),
        ok ?= verify_jwt_operator_trusted(OperatorPubKey, Config),
        ok ?= verify_jwt_token_signature(AccountToken, OperatorPubKey),
        ok ?= verify_jwt_user_issuer_allowed(UserIssuer, AccountPubKey, AccountClaims),
        verify_jwt_token_signature(UserToken, UserIssuer)
    end.

ensure_jwt_trusted_operators(Config) ->
    case jwt_trusted_operators(Config) of
        [] ->
            {error, jwt_trusted_operators_required};
        _ ->
            ok
    end.

decode_jwt_account(AccountJWT) ->
    case decode_jwt(AccountJWT) of
        {ok, _} = OK ->
            OK;
        {error, _} ->
            {error, invalid_jwt_account}
    end.

verify_jwt_account_token_alg(AccountToken) ->
    case verify_jwt_token_alg(AccountToken) of
        ok ->
            ok;
        {error, _} ->
            {error, invalid_jwt_account}
    end.

verify_jwt_account_subject(AccountClaims, AccountPubKey) ->
    case jwt_claim_sub(AccountClaims) of
        {ok, AccountPubKey} ->
            ok;
        {ok, _OtherPubKey} ->
            {error, invalid_jwt_account};
        {error, _} ->
            {error, invalid_jwt_account}
    end.

jwt_claim_account_issuer(AccountClaims) ->
    case jwt_claim_issuer(AccountClaims) of
        {ok, _} = OK ->
            OK;
        {error, _} ->
            {error, invalid_jwt_account}
    end.

verify_jwt_operator_trusted(OperatorPubKey, Config) ->
    case lists:member(OperatorPubKey, jwt_trusted_operators(Config)) of
        true ->
            ok;
        false ->
            {error, jwt_untrusted_operator}
    end.

verify_jwt_token_alg(#{header := Header}) ->
    case normalize_token(map_get_any(Header, [<<"alg">>, alg], undefined)) of
        <<"ed25519-nkey">> ->
            ok;
        _ ->
            {error, invalid_jwt_alg}
    end.

verify_jwt_token_signature(#{signing_input := Input, signature := Signature}, IssuerPubKey) ->
    case emqx_nats_nkey:decode_public_any(IssuerPubKey) of
        {ok, PubKey} ->
            try crypto:verify(eddsa, none, Input, Signature, [PubKey, ed25519]) of
                true ->
                    ok;
                false ->
                    {error, invalid_jwt_signature}
            catch
                _:_ ->
                    {error, invalid_jwt_signature}
            end;
        {error, _} ->
            {error, invalid_jwt_issuer}
    end.

verify_jwt_user_issuer_allowed(UserIssuer, AccountPubKey, AccountClaims) ->
    case UserIssuer =:= AccountPubKey of
        true ->
            ok;
        false ->
            NATSClaims = normalize_map(map_get_any(AccountClaims, [<<"nats">>, nats], #{})),
            SigningKeys = normalize_nkey_list(
                map_get_any(NATSClaims, [<<"signing_keys">>, signing_keys], [])
            ),
            case lists:member(UserIssuer, SigningKeys) of
                true ->
                    ok;
                false ->
                    {error, jwt_untrusted_signing_key}
            end
    end.

jwt_claim_issuer(Claims) ->
    case jwt_normalize_nkey_claim(map_get_any(Claims, [<<"iss">>, iss], undefined)) of
        undefined ->
            {error, invalid_jwt_issuer};
        Issuer ->
            {ok, Issuer}
    end.

jwt_claim_issuer_account(Claims, DefaultIssuer) ->
    case
        jwt_normalize_nkey_claim(
            map_get_any(Claims, [<<"issuer_account">>, issuer_account], undefined)
        )
    of
        undefined ->
            DefaultIssuer;
        Account ->
            Account
    end.

jwt_claim_sub(Claims) ->
    case jwt_normalize_nkey_claim(map_get_any(Claims, [<<"sub">>, sub], undefined)) of
        undefined ->
            {error, invalid_jwt_subject};
        Subject ->
            {ok, Subject}
    end.

jwt_normalize_nkey_claim(Value) ->
    case normalize_token(Value) of
        undefined ->
            undefined;
        Bin ->
            emqx_nats_nkey:normalize(Bin)
    end.

jwt_account_jwt(Config, AccountPubKey) ->
    Accounts = jwt_resolver_accounts(Config),
    case maps:get(AccountPubKey, Accounts, undefined) of
        undefined ->
            {error, jwt_account_not_found};
        AccountJWT ->
            {ok, AccountJWT}
    end.

jwt_resolver_accounts(Config) ->
    Resolver = map_get_any(Config, [resolver, <<"resolver">>], undefined),
    Preload0 =
        case is_map(Resolver) of
            true ->
                map_get_any(Resolver, [resolver_preload, <<"resolver_preload">>], []);
            false ->
                map_get_any(Config, [resolver_preload, <<"resolver_preload">>], [])
        end,
    resolver_preload_entries(Preload0, #{}).

resolver_preload_entries([Entry | Rest], Acc) ->
    case normalize_resolver_preload_entry(Entry) of
        {ok, {PubKey, JWT}} ->
            resolver_preload_entries(Rest, Acc#{PubKey => JWT});
        error ->
            resolver_preload_entries(Rest, Acc)
    end;
resolver_preload_entries([], Acc) ->
    Acc;
resolver_preload_entries(_, Acc) ->
    Acc.

normalize_resolver_preload_entry(Entry0) ->
    Entry = normalize_map(Entry0),
    PubKey = jwt_normalize_nkey_claim(map_get_any(Entry, [pubkey, <<"pubkey">>], undefined)),
    JWT = normalize_token(map_get_any(Entry, [jwt, <<"jwt">>], undefined)),
    case {PubKey, JWT} of
        {undefined, _} ->
            error;
        {_, undefined} ->
            error;
        _ ->
            {ok, {PubKey, JWT}}
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

jwt_claim_expire_at(_Claims, #{verify_exp := false}) ->
    undefined;
jwt_claim_expire_at(Claims, _Opts) ->
    jwt_claim_expire_at(Claims).

extract_jwt_permissions(Claims) ->
    NATSClaims = normalize_map(map_get_any(Claims, [<<"nats">>, nats], #{})),
    LegacyPerms = normalize_map(map_get_any(Claims, [<<"permissions">>, permissions], #{})),
    #{
        publish => extract_jwt_action_permissions(NATSClaims, LegacyPerms, publish),
        subscribe => extract_jwt_action_permissions(NATSClaims, LegacyPerms, subscribe)
    }.

extract_jwt_action_permissions(NATSClaims, LegacyPerms, publish) ->
    extract_jwt_action_permissions(
        map_get_any(NATSClaims, [<<"pub">>, <<"publish">>, pub, publish], undefined),
        map_get_any(LegacyPerms, [<<"pub">>, <<"publish">>, pub, publish], #{})
    );
extract_jwt_action_permissions(NATSClaims, LegacyPerms, subscribe) ->
    extract_jwt_action_permissions(
        map_get_any(NATSClaims, [<<"sub">>, <<"subscribe">>, sub, subscribe], undefined),
        map_get_any(LegacyPerms, [<<"sub">>, <<"subscribe">>, sub, subscribe], #{})
    ).

extract_jwt_action_permissions(undefined, LegacyClaim) ->
    extract_allow_deny(LegacyClaim);
extract_jwt_action_permissions(NATSClaim, _LegacyClaim) ->
    extract_allow_deny(NATSClaim).

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
                        username => <<"token">>,
                        password => undefined,
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

jwt_trusted_operators(Config) ->
    normalize_nkey_list(map_get_any(Config, [trusted_operators, <<"trusted_operators">>], [])).

jwt_has_trusted_operators(Config) ->
    jwt_trusted_operators(Config) =/= [].

jwt_has_resolver_accounts(Config) ->
    maps:size(jwt_resolver_accounts(Config)) > 0.

token_auth_enabled(Authn) ->
    maps:get(token, Authn, undefined) =/= undefined.

nkey_auth_enabled(Authn) ->
    maps:get(nkeys, Authn, []) =/= [].

jwt_auth_enabled(Authn) ->
    case maps:get(jwt, Authn, undefined) of
        undefined ->
            false;
        Config ->
            jwt_has_trusted_operators(Config) orelse jwt_has_resolver_accounts(Config)
    end.

nonce_auth_enabled(Authn) ->
    nkey_auth_enabled(Authn) orelse jwt_auth_enabled(Authn).

gateway_auth_enabled(Authn) ->
    maps:get(gateway_auth_enabled, Authn, false) =:= true.

nkey_allowed(NKey, Allowed) ->
    lists:member(emqx_nats_nkey:normalize(NKey), Allowed).

normalize_nkeys(NKeys) when is_list(NKeys) ->
    normalize_nkey_list(NKeys);
normalize_nkeys(_) ->
    [].

normalize_nkey_list(Values) when is_list(Values) ->
    lists:filtermap(
        fun(Value) ->
            case normalize_token(Value) of
                undefined ->
                    false;
                Bin ->
                    {true, emqx_nats_nkey:normalize(Bin)}
            end
        end,
        Values
    );
normalize_nkey_list(_) ->
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
