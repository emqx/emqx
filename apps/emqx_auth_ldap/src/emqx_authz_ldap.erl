%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz_ldap).
-behaviour(emqx_authz_source).

%% AuthZ Callbacks
-export([
    create/1,
    update/2,
    destroy/1,
    authorize/4
]).

-include_lib("emqx/include/emqx_placeholder.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("eldap/include/eldap.hrl").
-include("emqx_auth_ldap.hrl").

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-define(ALLOWED_VARS, [
    ?VAR_USERNAME,
    ?VAR_CLIENTID,
    ?VAR_PEERHOST,
    ?VAR_CERT_CN_NAME,
    ?VAR_CERT_SUBJECT,
    ?VAR_ZONE,
    ?VAR_NS_CLIENT_ATTRS,
    ?VAR_LISTENER
]).

%%------------------------------------------------------------------------------
%% AuthZ Callbacks
%%------------------------------------------------------------------------------

create(Source) ->
    ResourceId = emqx_authz_utils:make_resource_id(?AUTHZ_TYPE),
    State = new_state(ResourceId, Source),
    ok = emqx_authz_utils:create_resource(emqx_ldap_connector, State),
    State.

update(#{resource_id := ResourceId} = _State, Source) ->
    State = new_state(ResourceId, Source),
    ok = emqx_authz_utils:update_resource(emqx_ldap_connector, State),
    State.

destroy(#{resource_id := ResourceId}) ->
    emqx_authz_utils:remove_resource(ResourceId).

authorize(
    Client,
    Action,
    Topic,
    #{
        query_timeout := QueryTimeout,
        resource_id := ResourceId,
        cache_key_template := CacheKeyTemplate,
        base_dn_template := BaseDNTemplate,
        filter_template := FilterTemplate
    } = State
) ->
    AclAttrs = emqx_auth_ldap_acl:acl_attributes(State),
    CacheKey = emqx_auth_template:cache_key(Client, CacheKeyTemplate),
    Query = fun() ->
        BaseDN = emqx_auth_ldap_utils:render_base_dn(BaseDNTemplate, Client),
        Filter = emqx_auth_ldap_utils:render_filter(FilterTemplate, Client),
        {query, BaseDN, Filter, [{attributes, AclAttrs}, {timeout, QueryTimeout}]}
    end,
    Result = emqx_authz_utils:cached_simple_sync_query(
        CacheKey, ResourceId, Query
    ),
    case Result of
        {ok, []} ->
            nomatch;
        {ok, [Entry]} ->
            case emqx_auth_ldap_acl:entry_rules(State, Entry) of
                {ok, AclRules} ->
                    emqx_authz_rule:matches(Client, Action, Topic, AclRules);
                {error, Reason} ->
                    ?SLOG(error, #{
                        msg => "invalid_acl_rules",
                        reason => Reason,
                        ldap_entry => Entry
                    }),
                    nomatch
            end;
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "ldap_query_failed",
                reason => emqx_utils:redact(Reason),
                resource_id => ResourceId
            }),
            nomatch
    end.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

new_state(ResourceId, #{base_dn := BaseDN, filter := Filter} = Source) ->
    maybe
        {ok, BaseDNTemplate, BaseDNVars} ?= emqx_auth_ldap_utils:parse_dn(BaseDN, ?ALLOWED_VARS),
        {ok, FilterTemplate, FilterVars} ?=
            emqx_auth_ldap_utils:parse_filter(Filter, ?ALLOWED_VARS),
        CacheKeyTemplate = emqx_auth_template:cache_key_template(BaseDNVars ++ FilterVars),
        ResourceConfig = emqx_authz_utils:resource_config(
            [
                query_timeout,
                publish_attribute,
                subscribe_attribute,
                all_attribute,
                acl_rule_attribute,
                base_dn,
                filter
            ],
            Source
        ),
        AttrNames = maps:with(
            [
                query_timeout,
                publish_attribute,
                subscribe_attribute,
                all_attribute,
                acl_rule_attribute
            ],
            Source
        ),
        emqx_authz_utils:init_state(Source, AttrNames#{
            cache_key_template => CacheKeyTemplate,
            base_dn_template => BaseDNTemplate,
            filter_template => FilterTemplate,
            resource_config => ResourceConfig,
            resource_id => ResourceId
        })
    else
        {error, Reason} ->
            error({load_config_error, Reason})
    end.
