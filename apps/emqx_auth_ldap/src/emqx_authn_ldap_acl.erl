%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_ldap_acl).

-moduledoc """
    This module is used to extract ACL rules from LDAP entry during authentication.
    These rules stored in the client info and are used by `emqx_authz_client_info`
    authorizer to authorize the client's actions without external queries.
""".

-export([
    acl_attributes/1,
    acl_from_entry/2
]).

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

acl_attributes(#{
    acl_rule_attribute := ACLRuleAttr,
    all_attribute := AllAttr,
    publish_attribute := PublishAttr,
    subscribe_attribute := SubscribeAttr
}) ->
    [ACLRuleAttr, AllAttr, PublishAttr, SubscribeAttr].

acl_from_entry(#{acl_ttl_attribute := AclTtlAttr} = Attributes, Entry) ->
    maybe
        {ok, AclTtl} ?= acl_ttl(AclTtlAttr, Entry),
        {ok, AclRules} ?= emqx_authz_ldap:entry_rules(Attributes, Entry),
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

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

expire_at(TtlSec) ->
    erlang:system_time(second) + TtlSec.

expire_at_ms(TtlSec) ->
    erlang:convert_time_unit(expire_at(TtlSec), second, millisecond).

acl_ttl(AclTtlAttr, Entry) ->
    case emqx_auth_ldap_utils:get_bin_attribute(AclTtlAttr, Entry, undefined) of
        undefined ->
            {ok, undefined};
        AclTtlBin ->
            emqx_schema:to_timeout_duration_s(AclTtlBin)
    end.
