%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
            is_superuser_attribute := IsSuperuserAttribute
        }
    } = _State
) ->
    BaseDN = emqx_auth_ldap_utils:render_base_dn(BaseDNTemplate, Credential),
    Filter = emqx_auth_ldap_utils:render_filter(FilterTemplate, Credential),
    Result = emqx_resource:simple_sync_query(
        ResourceId,
        {query, BaseDN, Filter, [{attributes, [IsSuperuserAttribute]}, {timeout, Timeout}]}
    ),
    case Result of
        {ok, []} ->
            ignore;
        {ok, [Entry]} ->
            Password = emqx_auth_ldap_utils:render_password(PasswordTemplate, Credential),
            case
                emqx_resource:simple_sync_query(
                    ResourceId,
                    {bind, Entry#eldap_entry.object_name, Password}
                )
            of
                {ok, #{result := ok}} ->
                    IsSuperuser = emqx_auth_ldap_utils:get_bool_attribute(
                        IsSuperuserAttribute, Entry, false
                    ),
                    {ok, #{is_superuser => IsSuperuser}};
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
            ignore
    end.
