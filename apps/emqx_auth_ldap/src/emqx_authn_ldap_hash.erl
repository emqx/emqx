%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_ldap_hash).

-include_lib("emqx_auth/include/emqx_authn.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("eldap/include/eldap.hrl").

%% a compatible attribute for version 4.x
-define(ISENABLED_ATTR, "isEnabled").
-define(VALID_ALGORITHMS, [md5, ssha, sha, sha256, sha384, sha512]).
%% TODO
%% 1. Support more salt algorithms, SMD5 SSHA 256/384/512
%% 2. Support https://datatracker.ietf.org/doc/html/rfc3112

-export([
    authenticate/2
]).

-import(proplists, [get_value/2, get_value/3]).

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------
authenticate(
    #{password := Password} = Credential,
    #{
        method := #{
            password_attribute := PasswordAttr,
            is_superuser_attribute := IsSuperuserAttr,
            clientid_override_attribute := ClientIdOverrideAttr
        },
        query_timeout := Timeout,
        resource_id := ResourceId,
        cache_key_template := CacheKeyTemplate,
        base_dn_template := BaseDNTemplate,
        filter_template := FilterTemplate
    } = State
) ->
    CacheKey = emqx_auth_template:cache_key(Credential, CacheKeyTemplate),
    Query = fun() ->
        BaseDN = emqx_auth_ldap_utils:render_base_dn(BaseDNTemplate, Credential),
        Filter = emqx_auth_ldap_utils:render_filter(FilterTemplate, Credential),
        AclAttributes = emqx_auth_ldap_acl:acl_attributes(State),
        Attributes = [
            PasswordAttr,
            IsSuperuserAttr,
            ClientIdOverrideAttr,
            ?ISENABLED_ATTR
            | AclAttributes
        ],
        {query, BaseDN, Filter, [{attributes, Attributes}, {timeout, Timeout}]}
    end,
    Result = emqx_authn_utils:cached_simple_sync_query(CacheKey, ResourceId, Query),
    case Result of
        {ok, []} ->
            ignore;
        {ok, [Entry]} ->
            do_authenticate(Password, Entry, State);
        {error, Reason} ->
            ?TRACE_AUTHN_PROVIDER(error, "ldap_query_failed", #{
                resource => ResourceId,
                timeout => Timeout,
                reason => Reason
            }),
            ignore
    end.

do_authenticate(Password, Entry, #{resource_id := ResourceId} = State) ->
    maybe
        %% To be compatible with v4.x
        ok ?= verify_user_enabled(Entry),
        ok ?= ensure_password(Password, Entry, State),
        {ok, AclFields} ?= emqx_auth_ldap_acl:acl_from_entry(State, Entry),
        {ok, maps:merge(AclFields, authn_result(Entry, State))}
    else
        {error, Reason} ->
            ?TRACE_AUTHN_PROVIDER(error, "ldap_authentication_failed", #{
                resource => ResourceId,
                reason => Reason
            }),
            {error, bad_username_or_password}
    end.

authn_result(Entry, State) ->
    Res0 = is_superuser(Entry, State),
    Res1 = clientid_override(Entry, State),
    maps:merge(Res0, Res1).

ensure_password(
    Password,
    #eldap_entry{attributes = Attributes},
    #{method := #{password_attribute := PasswordAttr}}
) ->
    case get_value(PasswordAttr, Attributes) of
        undefined ->
            {error, no_password};
        [LDAPPassword | _] ->
            extract_hash_algorithm(LDAPPassword, Password, fun try_decode_password/2)
    end.

%% RFC 2307 format password
%% https://datatracker.ietf.org/doc/html/rfc2307
extract_hash_algorithm(LDAPPassword, Password, OnFail) ->
    case
        re:run(
            LDAPPassword,
            "{([^{}]+)}(.+)",
            [{capture, all_but_first, list}, global]
        )
    of
        {match, [[HashTypeStr, PasswordHashStr]]} ->
            case emqx_utils:safe_to_existing_atom(string:to_lower(HashTypeStr)) of
                {ok, HashType} ->
                    PasswordHash = to_binary(PasswordHashStr),
                    is_valid_algorithm(HashType, PasswordHash, Password);
                _Error ->
                    {error, invalid_hash_type}
            end;
        _ ->
            OnFail(LDAPPassword, Password)
    end.

is_valid_algorithm(HashType, PasswordHash, Password) ->
    case lists:member(HashType, ?VALID_ALGORITHMS) of
        true ->
            verify_password(HashType, PasswordHash, Password);
        _ ->
            {error, {invalid_hash_type, HashType}}
    end.

%% this password is in LDIF format which is base64 encoding
try_decode_password(LDAPPassword, Password) ->
    case safe_base64_decode(LDAPPassword) of
        {ok, Decode} ->
            extract_hash_algorithm(
                Decode,
                Password,
                fun(_, _, _, _) ->
                    {error, invalid_password}
                end
            );
        {error, Reason} ->
            {error, {invalid_password, Reason}}
    end.

%% sha with salt
%% https://www.openldap.org/faq/data/cache/347.html
verify_password(ssha, PasswordData, Password) ->
    case safe_base64_decode(PasswordData) of
        {ok, <<PasswordHash:20/binary, Salt/binary>>} ->
            verify_password(sha, hash, PasswordHash, Salt, suffix, Password);
        {ok, _} ->
            {error, invalid_ssha_password};
        {error, Reason} ->
            {error, {invalid_password, Reason}}
    end;
verify_password(
    Algorithm,
    Base64HashData,
    Password
) ->
    verify_password(Algorithm, base64, Base64HashData, <<>>, disable, Password).

verify_password(Algorithm, LDAPPasswordType, LDAPPassword, Salt, Position, Password) ->
    PasswordHash = hash_password(Algorithm, Salt, Position, Password),
    case compare_password(LDAPPasswordType, LDAPPassword, PasswordHash) of
        true ->
            ok;
        _ ->
            {error, bad_username_or_password}
    end.

is_superuser(Entry, #{method := #{is_superuser_attribute := Attr}} = _State) ->
    IsSuperuser = emqx_auth_ldap_utils:get_bool_attribute(Attr, Entry, false),
    #{is_superuser => IsSuperuser}.

clientid_override(Entry, #{method := #{clientid_override_attribute := Attr}} = _State) ->
    case emqx_auth_ldap_utils:get_bin_attribute(Attr, Entry, undefined) of
        ClientIdOverride when is_binary(ClientIdOverride), ClientIdOverride /= <<"">> ->
            #{clientid_override => ClientIdOverride};
        _ ->
            #{}
    end.

safe_base64_decode(Data) ->
    try
        {ok, base64:decode(Data)}
    catch
        _:Reason ->
            {error, {invalid_base64_data, Reason}}
    end.

to_binary(Value) ->
    erlang:list_to_binary(Value).

hash_password(Algorithm, _Salt, disable, Password) ->
    hash_password(Algorithm, Password);
hash_password(Algorithm, Salt, suffix, Password) ->
    hash_password(Algorithm, <<Password/binary, Salt/binary>>).

hash_password(Algorithm, Data) ->
    crypto:hash(Algorithm, Data).

compare_password(hash, LDAPPasswordHash, PasswordHash) ->
    emqx_passwd:compare_secure(LDAPPasswordHash, PasswordHash);
compare_password(base64, Base64HashData, PasswordHash) ->
    emqx_passwd:compare_secure(Base64HashData, base64:encode(PasswordHash)).

verify_user_enabled(Entry) ->
    case emqx_auth_ldap_utils:get_bool_attribute(?ISENABLED_ATTR, Entry, true) of
        true ->
            ok;
        _ ->
            {error, user_disabled}
    end.
