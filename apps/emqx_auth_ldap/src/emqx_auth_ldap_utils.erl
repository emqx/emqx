%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_auth_ldap_utils).

-include_lib("eldap/include/eldap.hrl").

-export([
    render_base_dn/2,
    render_filter/2,
    render_password/2,
    parse_filter/2,
    parse_dn/2,
    get_bool_attribute/3,
    get_bin_attribute/3
]).

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

render_base_dn(BaseDNTemplate, Credential) ->
    emqx_ldap_dn:map_values(
        fun(Value) ->
            iodata_to_str(emqx_auth_template:render_str_for_raw(Value, Credential))
        end,
        BaseDNTemplate
    ).

render_filter(FilterTemplate, Credential) ->
    emqx_ldap_filter:map_values(
        fun(Value) ->
            iodata_to_str(emqx_auth_template:render_str_for_raw(Value, Credential))
        end,
        FilterTemplate
    ).

render_password(PasswordTemplate, Credential) ->
    iodata_to_str(emqx_auth_template:render_str_for_raw(PasswordTemplate, Credential)).

parse_filter(Filter, AllowedVars) ->
    maybe
        {ok, ParsedFilter} ?= emqx_ldap_filter:parse(Filter),
        {FilterTemplate, AllUsedVars} = emqx_ldap_filter:mapfold_values(
            fun(Value, UsedVarsAcc) ->
                {UsedVars, ValueTemplate} = emqx_auth_template:parse_str(Value, AllowedVars),
                {ValueTemplate, UsedVars ++ UsedVarsAcc}
            end,
            [],
            ParsedFilter
        ),
        {ok, FilterTemplate, AllUsedVars}
    end.

parse_dn(DN, AllowedVars) ->
    maybe
        {ok, ParsedDN} ?= emqx_ldap_dn:parse(DN),
        {DNTemplate, AllUsedVars} = emqx_ldap_dn:mapfold_values(
            fun(Value, UsedVarsAcc) ->
                {UsedVars, ValueTemplate} = emqx_auth_template:parse_str(Value, AllowedVars),
                {ValueTemplate, UsedVars ++ UsedVarsAcc}
            end,
            [],
            ParsedDN
        ),
        {ok, DNTemplate, AllUsedVars}
    end.

get_bool_attribute(Attribute, #eldap_entry{attributes = Attributes}, Default) ->
    case get_attribute_value(Attribute, Attributes) of
        undefined ->
            Default;
        Value ->
            BinValue = list_to_binary(string:to_lower(Value)),
            emqx_authn_utils:to_bool(BinValue)
    end.

get_bin_attribute(Attribute, #eldap_entry{attributes = Attributes}, Default) ->
    case get_attribute_value(Attribute, Attributes) of
        undefined ->
            Default;
        Value ->
            iolist_to_binary(Value)
    end.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

get_attribute_value(Key, Proplists) ->
    case proplists:get_value(Key, Proplists, undefined) of
        [Value | _] ->
            Value;
        undefined ->
            undefined
    end.

iodata_to_str(Iodata) ->
    binary_to_list(iolist_to_binary(Iodata)).
