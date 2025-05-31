%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz_ldap).
-behaviour(emqx_authz_source).

%% AuthZ Callbacks
-export([
    create/1,
    update/1,
    destroy/1,
    authorize/4
]).

-export([
    entry_rules/2
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
    Annotations = new_annotations(#{id => ResourceId}, Source),
    case emqx_authz_utils:create_resource(ResourceId, emqx_ldap_connector, Source, ?AUTHZ_TYPE) of
        {ok, _} ->
            Source#{annotations => Annotations};
        {error, Reason} ->
            error({load_config_error, Reason})
    end.

update(#{annotations := #{id := ResourceId}} = Source) ->
    Annotations = new_annotations(#{id => ResourceId}, Source),
    case emqx_authz_utils:update_resource(emqx_ldap_connector, Source, ?AUTHZ_TYPE) of
        {ok, _ResourceId} ->
            Source#{annotations => Annotations};
        {error, Reason} ->
            error({load_config_error, Reason})
    end.

destroy(#{annotations := #{id := Id}}) ->
    emqx_authz_utils:remove_resource(Id).

authorize(
    Client,
    Action,
    Topic,
    #{
        query_timeout := QueryTimeout,
        annotations := #{
            id := ResourceId,
            cache_key_template := CacheKeyTemplate,
            base_dn_template := BaseDNTemplate,
            filter_template := FilterTemplate
        } = Annotations
    }
) ->
    ACLAttrs = acl_attributes(Annotations),
    CacheKey = emqx_auth_template:cache_key(Client, CacheKeyTemplate),
    Query = fun() ->
        BaseDN = emqx_auth_ldap_utils:render_base_dn(BaseDNTemplate, Client),
        Filter = emqx_auth_ldap_utils:render_filter(FilterTemplate, Client),
        {query, BaseDN, Filter, [{attributes, ACLAttrs}, {timeout, QueryTimeout}]}
    end,
    Result = emqx_authz_utils:cached_simple_sync_query(
        CacheKey, ResourceId, Query
    ),
    case Result of
        {ok, []} ->
            nomatch;
        {ok, [Entry]} ->
            case entry_rules(Annotations, Entry) of
                {ok, ACLRules} ->
                    emqx_authz_rule:matches(Client, Action, Topic, ACLRules);
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
%% API
%%------------------------------------------------------------------------------

entry_rules(
    #{
        acl_rule_attribute := ACLRuleAttr,
        all_attribute := AllAttr,
        publish_attribute := PublishAttr,
        subscribe_attribute := SubscribeAttr
    },
    Entry
) ->
    %% Legacy rules
    RawRulesPubSub = raw_whitelist_rules(<<"all">>, get_attr_values(AllAttr, Entry)),
    RawRulesPublish = raw_whitelist_rules(<<"pub">>, get_attr_values(PublishAttr, Entry)),
    RawRulesSubscribe = raw_whitelist_rules(<<"sub">>, get_attr_values(SubscribeAttr, Entry)),
    maybe
        %% JSON-encoded raw rules
        {ok, RawRules} ?= decode_acl_rules(get_attr_values(ACLRuleAttr, Entry)),
        RawRulesAll = lists:concat([RawRulesPubSub, RawRulesPublish, RawRulesSubscribe, RawRules]),
        {ok, ACLRules} ?= parse_and_compile_acl_rules(RawRulesAll),
        {ok, ACLRules}
    else
        {error, _} = Error ->
            Error
    end.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

new_annotations(Init, #{base_dn := BaseDN, filter := Filter} = Source) ->
    maybe
        {ok, BaseDNTemplate, BaseDNVars} ?= emqx_auth_ldap_utils:parse_dn(BaseDN, ?ALLOWED_VARS),
        {ok, FilterTemplate, FilterVars} ?=
            emqx_auth_ldap_utils:parse_filter(Filter, ?ALLOWED_VARS),
        CacheKeyTemplate = emqx_auth_template:cache_key_template(BaseDNVars ++ FilterVars),
        State = maps:with(
            [
                query_timeout,
                publish_attribute,
                subscribe_attribute,
                all_attribute,
                acl_rule_attribute
            ],
            Source
        ),
        maps:merge(Init, State#{
            cache_key_template => CacheKeyTemplate,
            base_dn_template => BaseDNTemplate,
            filter_template => FilterTemplate
        })
    else
        {error, Reason} ->
            error({load_config_error, Reason})
    end.

acl_attributes(#{
    acl_rule_attribute := ACLRuleAttr,
    all_attribute := AllAttr,
    publish_attribute := PublishAttr,
    subscribe_attribute := SubscribeAttr
}) ->
    [ACLRuleAttr, AllAttr, PublishAttr, SubscribeAttr].

raw_whitelist_rules(_Action, []) ->
    [];
raw_whitelist_rules(Action, Topics) ->
    [
        #{
            <<"permission">> => <<"allow">>,
            <<"action">> => Action,
            <<"topics">> => [list_to_binary(Topic) || Topic <- Topics]
        }
    ].

get_attr_values(AttrName, #eldap_entry{attributes = Attrs}) ->
    proplists:get_value(AttrName, Attrs, []).

parse_and_compile_acl_rules(ACLRulesRaw) ->
    try emqx_authz_rule_raw:parse_and_compile_rules(ACLRulesRaw) of
        Rules -> {ok, Rules}
    catch
        throw:Reason ->
            ?SLOG(warning, #{
                msg => "invalid_acl_rules_raw",
                rules => ACLRulesRaw,
                reason => Reason
            }),
            {error, Reason}
    end.

decode_acl_rules(JSONs) ->
    decode_acl_rules(JSONs, []).

decode_acl_rules([], Acc) ->
    {ok, lists:concat(lists:reverse(Acc))};
decode_acl_rules([JSON | JSONRest], Acc) ->
    case emqx_utils_json:safe_decode(JSON) of
        {ok, ACLRuleRaw} ->
            decode_acl_rules(JSONRest, [wrap_as_list(ACLRuleRaw) | Acc]);
        {error, Reason} = Error ->
            ?SLOG(warning, #{
                msg => "invalid_acl_rule_json",
                json => JSON,
                reason => Reason
            }),
            Error
    end.

wrap_as_list(L) when is_list(L) ->
    L;
wrap_as_list(L) ->
    [L].
