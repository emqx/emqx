%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_ldap_bind).

-include_lib("emqx_auth/include/emqx_authn.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("eldap/include/eldap.hrl").

-export([
    authenticate/2
]).

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

authenticate(
    Credential,
    #{cache_key_template := CacheKeyTemplate} = State
) ->
    CacheKey = emqx_auth_template:cache_key(Credential, CacheKeyTemplate),
    emqx_auth_cache:with_cache(?AUTHN_CACHE, CacheKey, fun() ->
        case do_authenticate(Credential, State) of
            {error, _} = Error ->
                {nocache, Error};
            Result ->
                {cache, Result}
        end
    end).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

do_authenticate(
    Credential,
    #{
        query_timeout := Timeout,
        resource_id := ResourceId,
        password_template := PasswordTemplate,
        base_dn_template := BaseDNTemplate,
        filter_template := FilterTemplate,
        method := #{
            is_superuser_attribute := IsSuperuserAttribute,
            clientid_override_attribute := ClientIdOverrideAttribute
        }
    } = State
) ->
    BaseDN = emqx_auth_ldap_utils:render_base_dn(BaseDNTemplate, Credential),
    Filter = emqx_auth_ldap_utils:render_filter(FilterTemplate, Credential),
    AclAttributes = emqx_auth_ldap_acl:acl_attributes(State),
    ClientAttrsAttributes = client_attrs_attributes(State),
    Result = emqx_resource:simple_sync_query(
        ResourceId,
        {query, BaseDN, Filter, [
            {attributes, [
                IsSuperuserAttribute,
                ClientIdOverrideAttribute
                | AclAttributes ++ ClientAttrsAttributes
            ]},
            {timeout, Timeout}
        ]}
    ),
    case Result of
        {ok, []} ->
            ignore;
        {ok, [#eldap_entry{object_name = ObjectName} = Entry]} ->
            Password = emqx_auth_ldap_utils:render_password(PasswordTemplate, Credential),
            case
                emqx_resource:simple_sync_query(
                    ResourceId,
                    {bind, ObjectName, Password}
                )
            of
                {ok, #{result := ok}} ->
                    format_authentication_result(State, Entry);
                {ok, #{result := 'invalidCredentials'}} ->
                    ?TRACE_AUTHN_PROVIDER(info, "ldap_bind_failed", #{
                        resource => ResourceId,
                        reason => 'invalidCredentials'
                    }),
                    {error, bad_username_or_password};
                {error, Reason} ->
                    ?TRACE_AUTHN_PROVIDER(error, "ldap_bind_failed", #{
                        resource => ResourceId,
                        reason => Reason
                    }),
                    {error, bad_username_or_password}
            end;
        {error, Reason} ->
            ?TRACE_AUTHN_PROVIDER(error, "ldap_query_failed", #{
                resource => ResourceId,
                timeout => Timeout,
                reason => Reason
            }),
            emqx_authn_utils:backend_failure_result()
    end.

format_authentication_result(
    #{
        resource_id := ResourceId,
        method := #{
            is_superuser_attribute := IsSuperuserAttribute
        }
    } = State,
    Entry
) ->
    IsSuperuser = emqx_auth_ldap_utils:get_bool_attribute(
        IsSuperuserAttribute, Entry, false
    ),
    maybe
        {ok, AclFields} ?= acl_from_entry(State, Entry),
        {ok, ClientAttrs} ?= emqx_authn_ldap_client_attrs:from_entry(Entry, State),
        AuthResult0 = maps:merge(AclFields#{is_superuser => IsSuperuser}, ClientAttrs),
        {ok, maps:merge(AuthResult0, clientid_override(Entry, State))}
    else
        {error, no_client_attrs} ->
            ?TRACE_AUTHN_PROVIDER(info, "ldap_no_client_attrs_matched", #{
                resource => ResourceId
            }),
            {error, not_authorized};
        {error, {invalid_acl_rules, Reason}} ->
            ?TRACE_AUTHN_PROVIDER(error, "ldap_bind_invalid_acl_rules", #{
                resource => ResourceId,
                reason => Reason
            }),
            {error, bad_username_or_password}
    end.

acl_from_entry(State, Entry) ->
    case emqx_auth_ldap_acl:acl_from_entry(State, Entry) of
        {ok, _} = Ok -> Ok;
        {error, Reason} -> {error, {invalid_acl_rules, Reason}}
    end.

client_attrs_attributes(#{client_attrs := ClientAttrs}) ->
    emqx_authn_ldap_client_attrs:attributes(ClientAttrs).

clientid_override(Entry, #{method := #{clientid_override_attribute := Attr}} = _State) ->
    case emqx_auth_ldap_utils:get_bin_attribute(Attr, Entry, undefined) of
        ClientIdOverride when is_binary(ClientIdOverride), ClientIdOverride /= <<"">> ->
            #{clientid_override => ClientIdOverride};
        _ ->
            #{}
    end.
