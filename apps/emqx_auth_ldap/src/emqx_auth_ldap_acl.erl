%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_auth_ldap_acl).

-moduledoc """
    This module is used to extract ACL rules from LDAP entry.
""".

-include_lib("eldap/include/eldap.hrl").
-include_lib("emqx/include/logger.hrl").

-export([
    acl_attributes/1,
    acl_from_entry/2,
    entry_rules/2
]).

-type attribute_config() :: #{
    acl_rule_attribute => attribute_name(),
    all_attribute => attribute_name(),
    publish_attribute => attribute_name(),
    subscribe_attribute => attribute_name(),
    acl_ttl_attribute => attribute_name()
}.

-type ldap_entry() :: #eldap_entry{}.
-type attribute_name() :: string().

-type acl_with_ttl() :: #{
    acl => #{
        source_for_logging => binary(),
        rules => [emqx_authz_rule:rule()]
    },
    expire_at => integer()
}.

-type acl_without_ttl() :: #{
    acl => #{
        source_for_logging => binary(),
        rules => [emqx_authz_rule:rule()]
    }
}.

-type client_info_acl() :: acl_with_ttl() | acl_without_ttl().

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

-spec acl_attributes(attribute_config()) -> [attribute_name()].
acl_attributes(Config) ->
    maps:values(
        maps:with(
            [acl_rule_attribute, all_attribute, publish_attribute, subscribe_attribute],
            Config
        )
    ).

-spec acl_from_entry(attribute_config(), ldap_entry()) -> {ok, client_info_acl()} | {error, any()}.
acl_from_entry(Config, Entry) ->
    AclTtlAttrName = maps:get(acl_ttl_attribute, Config, undefined),
    maybe
        {ok, AclTtl} ?= acl_ttl(AclTtlAttrName, Entry),
        {ok, AclRules} ?= entry_rules(Config, Entry),
        AclFields =
            case {AclRules, AclTtl} of
                {[], _} ->
                    #{};
                {_, undefined} ->
                    #{
                        acl => #{
                            source_for_logging => <<"ldap">>,
                            rules => AclRules
                        }
                    };
                _ ->
                    #{
                        %% For emqx_channel to drop the connection after the expiration
                        expire_at => expire_at_ms(AclTtl),
                        acl => #{
                            source_for_logging => <<"ldap">>,
                            rules => AclRules,
                            %% For emqx_authz_client_info
                            expire => expire_at(AclTtl)
                        }
                    }
            end,
        {ok, AclFields}
    end.

-spec entry_rules(attribute_config(), ldap_entry()) ->
    {ok, [emqx_authz_rule:rule()]} | {error, any()}.
entry_rules(Config, Entry) ->
    PubSubAttrName = maps:get(all_attribute, Config, undefined),
    PublishAttrName = maps:get(publish_attribute, Config, undefined),
    SubscribeAttrName = maps:get(subscribe_attribute, Config, undefined),
    ACLRuleAttrName = maps:get(acl_rule_attribute, Config, undefined),

    %% Legacy rules with whitelist topics as attribute values
    RawRulesPubSub = raw_whitelist_rules(<<"all">>, get_attr_values(PubSubAttrName, Entry)),
    RawRulesPublish = raw_whitelist_rules(<<"pub">>, get_attr_values(PublishAttrName, Entry)),
    RawRulesSubscribe = raw_whitelist_rules(<<"sub">>, get_attr_values(SubscribeAttrName, Entry)),
    maybe
        %% JSON-encoded raw rules (`emqx_authz_rule_raw`) as attribute values
        {ok, RawRules} ?= decode_acl_rules(get_attr_values(ACLRuleAttrName, Entry)),
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

expire_at(TtlSec) ->
    erlang:system_time(second) + TtlSec.

expire_at_ms(TtlSec) ->
    erlang:convert_time_unit(expire_at(TtlSec), second, millisecond).

acl_ttl(undefined, _Entry) ->
    {ok, undefined};
acl_ttl(AclTtlAttr, Entry) ->
    case emqx_auth_ldap_utils:get_bin_attribute(AclTtlAttr, Entry, undefined) of
        undefined ->
            {ok, undefined};
        AclTtlBin ->
            emqx_schema:to_timeout_duration_s(AclTtlBin)
    end.

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

get_attr_values(undefined, _Entry) ->
    [];
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
