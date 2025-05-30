%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_ldap).

-behaviour(emqx_authn_provider).

-export([
    create/2,
    update/2,
    authenticate/2,
    destroy/1
]).

-include_lib("emqx_auth/include/emqx_authn.hrl").
-include("emqx_auth_ldap.hrl").

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

create(_AuthenticatorID, Config) ->
    maybe
        ResourceId = emqx_authn_utils:make_resource_id(?AUTHN_BACKEND_BIN),
        {ok, State} ?= parse_config(Config),
        {ok, _Data} ?=
            emqx_authn_utils:create_resource(
                ResourceId, emqx_ldap_connector, Config, ?AUTHN_MECHANISM_BIN, ?AUTHN_BACKEND_BIN
            ),
        {ok, State#{resource_id => ResourceId}}
    end.

update(Config, #{resource_id := ResourceId} = _State) ->
    maybe
        {ok, NState} ?= parse_config(Config),
        {ok, _} ?=
            emqx_authn_utils:update_resource(
                emqx_ldap_connector, Config, ResourceId, ?AUTHN_MECHANISM_BIN, ?AUTHN_BACKEND_BIN
            ),
        {ok, NState#{resource_id => ResourceId}}
    end.

destroy(#{resource_id := ResourceId}) ->
    _ = emqx_resource:remove_local(ResourceId),
    ok.

authenticate(#{auth_method := _}, _) ->
    ignore;
authenticate(#{password := undefined}, _) ->
    {error, bad_username_or_password};
authenticate(Credential, #{method := #{type := hash}} = State) ->
    emqx_authn_ldap_hash:authenticate(Credential, State);
authenticate(Credential, #{method := #{type := bind}} = State) ->
    emqx_authn_ldap_bind:authenticate(Credential, State).

parse_config(
    #{base_dn := BaseDN, filter := Filter, query_timeout := QueryTimeout, method := Method} = Config
) ->
    maybe
        {PasswordVars, PasswordTemplate} =
            case Method of
                #{bind_password := Password} ->
                    emqx_auth_template:parse_str(Password, ?AUTHN_DEFAULT_ALLOWED_VARS);
                _ ->
                    {[], <<>>}
            end,
        {ok, BaseDNTemplate, BaseDNVars} ?=
            emqx_auth_ldap_utils:parse_dn(BaseDN, ?AUTHN_DEFAULT_ALLOWED_VARS),
        {ok, FilterTemplate, FilterVars} ?=
            emqx_auth_ldap_utils:parse_filter(Filter, ?AUTHN_DEFAULT_ALLOWED_VARS),
        CacheKeyTemplate = emqx_auth_template:cache_key_template(
            lists:uniq(BaseDNVars ++ FilterVars ++ PasswordVars)
        ),
        ACLAttributeNames = maps:with(
            [
                publish_attribute,
                subscribe_attribute,
                all_attribute,
                acl_rule_attribute,
                acl_ttl_attribute
            ],
            Config
        ),
        {ok,
            maps:merge(
                ACLAttributeNames,
                #{
                    query_timeout => QueryTimeout,
                    method => Method,
                    cache_key_template => CacheKeyTemplate,
                    base_dn_template => BaseDNTemplate,
                    filter_template => FilterTemplate,
                    password_template => PasswordTemplate
                }
            )}
    end.
