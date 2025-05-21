%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_auth_ldap_utils).

-export([
    render_base_dn/2,
    render_filter/2,
    render_password/2,
    parse_filter/2,
    parse_dn/2
]).

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

render_base_dn(BaseDNTemplate, Credential) ->
    {DN, undefined} = emqx_ldap_dn:mapfold_values(
        fun(Value, Acc) ->
            {iodata_to_str(emqx_auth_template:render_str_for_raw(Value, Credential)), Acc}
        end,
        undefined,
        BaseDNTemplate
    ),
    DN.

render_filter(FilterTemplate, Credential) ->
    {Filter, undefined} = emqx_ldap_filter:mapfold_values(
        fun(Value, Acc) ->
            {iodata_to_str(emqx_auth_template:render_str_for_raw(Value, Credential)), Acc}
        end,
        undefined,
        FilterTemplate
    ),
    Filter.

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

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

iodata_to_str(Iodata) ->
    binary_to_list(iolist_to_binary(Iodata)).
