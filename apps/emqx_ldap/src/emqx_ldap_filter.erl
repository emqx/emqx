%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ldap_filter).

-moduledoc """
Module for parsing, transformation and formatting LDAP filters.

The LDAP filter format is described in RFC 4515:
https://www.rfc-editor.org/rfc/rfc4515

We implement a more loose filter format, more like used by openldap's ldapsearch tool.
""".

-include("emqx_ldap.hrl").

-export([
    parse/1,
    mapfold_values/3,
    map_values/2,
    to_eldap/1
]).

-type ldap_search_filter(ValueType) :: #ldap_search_filter{
    filter :: filter(ValueType)
}.

-type ldap_search_filter() :: ldap_search_filter(string()).

-type filter(ValueType) ::
    simple(ValueType)
    | present()
    | substring(ValueType)
    | extensible(ValueType)
    | {'and', [filter(ValueType)]}
    | {'or', [filter(ValueType)]}
    | {'not', filter(ValueType)}.

-type attribute() :: string().

-type simple(ValueType) ::
    {equal, attribute(), ValueType}
    | {approx, attribute(), ValueType}
    | {'greaterOrEqual', attribute(), ValueType}
    | {'lessOrEqual', attribute(), ValueType}.

-type present() :: {present, attribute()}.

-type substring(ValueType) ::
    {substring, attribute(), [{initial, ValueType} | {any, ValueType} | {final, ValueType}]}.

-type extensible(ValueType) ::
    {extensible, [{type, attribute()} | {'dnAttributes', boolean()} | {'matchingRule', string()}],
        ValueType}.

-doc """
Term that may be used as filter in the `eldap:search/2` call.
""".
-type eldap_filter() :: term().

-export_type([
    ldap_search_filter/0, ldap_search_filter/1
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec parse(string() | binary()) -> {ok, ldap_search_filter()} | {error, term()}.
parse(Filter) when is_binary(Filter) ->
    parse(binary_to_list(Filter));
parse(Filter) ->
    maybe
        {ok, ParsedFilter} ?= emqx_ldap_filter_parser:scan_and_parse(Filter),
        {ok, #ldap_search_filter{filter = ParsedFilter}}
    end.

-spec to_eldap(ldap_search_filter()) -> eldap_filter().
to_eldap(#ldap_search_filter{filter = Filter}) ->
    do_to_eldap(Filter).

%% NOTE
%% Ignoring dialyzer warnings due to an eldap bug:
%% https://github.com/erlang/otp/pull/9859
-dialyzer({nowarn_function, do_to_eldap/1}).
do_to_eldap({extensible, ExtensibleOpts, Value}) ->
    eldap:'extensibleMatch'(Value, ExtensibleOpts);
do_to_eldap({substring, Attribute, SubstringOpts}) ->
    eldap:substrings(Attribute, SubstringOpts);
do_to_eldap({equal, Attribute, Value}) ->
    eldap:'equalityMatch'(Attribute, Value);
do_to_eldap({approx, Attribute, Value}) ->
    eldap:'approxMatch'(Attribute, Value);
do_to_eldap({'greaterOrEqual', Attribute, Value}) ->
    eldap:'greaterOrEqual'(Attribute, Value);
do_to_eldap({'lessOrEqual', Attribute, Value}) ->
    eldap:'lessOrEqual'(Attribute, Value);
do_to_eldap({present, Attribute}) ->
    eldap:present(Attribute);
do_to_eldap({'and', Filters}) ->
    eldap:'and'([do_to_eldap(Filter) || Filter <- Filters]);
do_to_eldap({'or', Filters}) ->
    eldap:'or'([do_to_eldap(Filter) || Filter <- Filters]);
do_to_eldap({'not', Filter}) ->
    eldap:'not'(do_to_eldap(Filter)).

-spec mapfold_values(
    fun((ValueType, Acc) -> {NewValueType, Acc}),
    Acc,
    ldap_search_filter(ValueType)
) ->
    {ldap_search_filter(NewValueType), Acc}.
mapfold_values(Fun, Acc0, #ldap_search_filter{filter = Filter0}) ->
    {Filter1, Acc1} = do_mapfold_values(Fun, Acc0, Filter0),
    {#ldap_search_filter{filter = Filter1}, Acc1}.

-spec map_values(
    fun((ValueType) -> NewValueType),
    ldap_search_filter(ValueType)
) ->
    ldap_search_filter(NewValueType).
map_values(Fun, LDAPSearchFilter0) ->
    {LDAPSearchFilter, undefined} = mapfold_values(
        fun(Value, Acc) ->
            {Fun(Value), Acc}
        end,
        undefined,
        LDAPSearchFilter0
    ),
    LDAPSearchFilter.

do_mapfold_values(Fun, Acc0, {extensible, ExtensibleOpts, Value0}) ->
    {Value1, Acc1} = Fun(Value0, Acc0),
    {{extensible, ExtensibleOpts, Value1}, Acc1};
do_mapfold_values(Fun, Acc0, {substring, Attribute, SubstringOpts}) ->
    {NewOpts, Acc1} = lists:mapfoldl(
        fun({Type, Value}, Acc) ->
            {NewValue, NewAcc} = Fun(Value, Acc),
            {{Type, NewValue}, NewAcc}
        end,
        Acc0,
        SubstringOpts
    ),
    {{substring, Attribute, NewOpts}, Acc1};
do_mapfold_values(Fun, Acc0, {equal, Attribute, Value0}) ->
    {Value1, Acc1} = Fun(Value0, Acc0),
    {{equal, Attribute, Value1}, Acc1};
do_mapfold_values(Fun, Acc0, {approx, Attribute, Value0}) ->
    {Value1, Acc1} = Fun(Value0, Acc0),
    {{approx, Attribute, Value1}, Acc1};
do_mapfold_values(Fun, Acc0, {'greaterOrEqual', Attribute, Value0}) ->
    {Value1, Acc1} = Fun(Value0, Acc0),
    {{'greaterOrEqual', Attribute, Value1}, Acc1};
do_mapfold_values(Fun, Acc0, {'lessOrEqual', Attribute, Value0}) ->
    {Value1, Acc1} = Fun(Value0, Acc0),
    {{'lessOrEqual', Attribute, Value1}, Acc1};
do_mapfold_values(_Fun, Acc0, {present, Attribute}) ->
    {present, Attribute, Acc0};
do_mapfold_values(Fun, Acc0, {'and', Filters0}) ->
    {Filters1, Acc1} = lists:mapfoldl(
        fun(Filter, Acc) ->
            do_mapfold_values(Fun, Acc, Filter)
        end,
        Acc0,
        Filters0
    ),
    {{'and', Filters1}, Acc1};
do_mapfold_values(Fun, Acc0, {'or', Filters0}) ->
    {Filters1, Acc1} = lists:mapfoldl(
        fun(Filter, Acc) ->
            do_mapfold_values(Fun, Acc, Filter)
        end,
        Acc0,
        Filters0
    ),
    {{'or', Filters1}, Acc1};
do_mapfold_values(Fun, Acc0, {'not', Filter0}) ->
    {Filter1, Acc1} = do_mapfold_values(Fun, Acc0, Filter0),
    {{'not', Filter1}, Acc1}.
