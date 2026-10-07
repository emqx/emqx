%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_ldap_client_attrs).

-moduledoc """
Sets client attributes from the attributes of the LDAP entry that authenticated the client.

Each mapping entry reads one directory attribute. It selects one value of that attribute
with an ordered list of regular expressions, and sets one client attribute to a component
of the selected value or to a configured literal.

A value is selected as follows. The patterns are tried in the configured order. For each
pattern, the values are tried in the order the directory returned them. The first value
that matches the current pattern is selected, and the remaining patterns are not tried.
When no value matches any pattern, the entry sets no attribute.
""".

-include_lib("eldap/include/eldap.hrl").
-include_lib("emqx_ldap/include/emqx_ldap.hrl").

-export([
    compile/1,
    validate_patterns/1,
    attributes/1,
    from_entry/2
]).

-define(RE_OPTS, [unicode]).

-type extract() :: cn | value | literal.
-type compiled() :: #{
    attribute := string(),
    set_as_attr := binary(),
    select := [re:mp()],
    extract := extract(),
    literal := binary() | undefined
}.

-type ldap_entry() :: #eldap_entry{}.

-export_type([compiled/0]).

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

-doc "Compiles the `select` patterns of each mapping entry from the checked config.".
-spec compile([map()]) -> {ok, [compiled()]} | {error, term()}.
compile(Entries) ->
    try
        {ok, lists:map(fun compile_entry/1, Entries)}
    catch
        throw:Reason ->
            {error, Reason}
    end.

-doc "Schema validator for the `select` field: every pattern must compile.".
-spec validate_patterns([binary()]) -> ok | {error, term()}.
validate_patterns([]) ->
    {error, <<"select must contain at least one regular expression">>};
validate_patterns(Patterns) ->
    try
        lists:foreach(fun compile_pattern/1, Patterns)
    catch
        throw:Reason ->
            {error, Reason}
    end.

-doc "Returns the directory attributes that the LDAP query must request.".
-spec attributes([compiled()]) -> [string()].
attributes(Compiled) ->
    lists:usort([Attr || #{attribute := Attr} <- Compiled]).

-doc """
Returns the client attributes for an LDAP entry, in the shape of an authentication result.
Returns `{error, no_client_attrs}` when `require_client_attrs` is set and no mapping entry
set an attribute.
""".
-spec from_entry(ldap_entry(), map()) ->
    {ok, #{client_attrs => map()}} | {error, no_client_attrs}.
from_entry(Entry, #{client_attrs := Compiled, require_client_attrs := Require}) ->
    Attrs = lists:foldl(
        fun(#{set_as_attr := Name} = Mapping, Acc) ->
            case map_entry(Entry, Mapping) of
                {ok, Value} -> Acc#{Name => Value};
                none -> Acc
            end
        end,
        #{},
        Compiled
    ),
    case emqx_authn_utils:maybe_client_attrs(#{<<"client_attrs">> => Attrs}) of
        Result when map_size(Result) =:= 0 andalso Require ->
            {error, no_client_attrs};
        Result ->
            {ok, Result}
    end.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

compile_entry(#{attribute := Attr, set_as_attr := Name, select := Patterns} = Entry) ->
    Extract = maps:get(extract, Entry, value),
    Literal = maps:get(literal, Entry, undefined),
    ok = check_literal(Name, Extract, Literal),
    #{
        attribute => binary_to_list(Attr),
        set_as_attr => iolist_to_binary(Name),
        select => lists:map(fun compile_pattern/1, Patterns),
        extract => Extract,
        literal => Literal
    }.

check_literal(_Name, literal, Literal) when is_binary(Literal), Literal =/= <<>> ->
    ok;
check_literal(Name, literal, _Literal) ->
    throw(#{
        reason => missing_literal,
        set_as_attr => Name,
        explain => <<"extract is literal, but literal is not set or is empty">>
    });
check_literal(_Name, _Extract, undefined) ->
    ok;
check_literal(Name, Extract, _Literal) ->
    throw(#{
        reason => unexpected_literal,
        set_as_attr => Name,
        extract => Extract,
        explain => <<"literal is set, but extract is not literal">>
    }).

compile_pattern(Pattern) ->
    case re:compile(Pattern, ?RE_OPTS) of
        {ok, MP} ->
            MP;
        {error, {ErrString, Position}} ->
            throw(#{
                reason => invalid_regular_expression,
                pattern => iolist_to_binary(Pattern),
                error => iolist_to_binary(ErrString),
                position => Position
            })
    end.

map_entry(Entry, #{attribute := Attr, select := MPs, extract := Extract, literal := Literal}) ->
    maybe
        {ok, Value} ?= select(MPs, attribute_values(Attr, Entry)),
        extract(Extract, Value, Literal)
    end.

select([], _Values) ->
    none;
select([MP | MPs], Values) ->
    case lists:search(fun(Value) -> is_match(Value, MP) end, Values) of
        {value, Value} -> {ok, Value};
        false -> select(MPs, Values)
    end.

%% A value that is not valid UTF-8 cannot be matched by a pattern compiled
%% with the `unicode` option, and re:run/3 raises badarg for it.
is_match(Value, MP) ->
    try
        match =:= re:run(Value, MP, [{capture, none}])
    catch
        error:badarg -> false
    end.

extract(literal, _Value, Literal) ->
    {ok, Literal};
extract(value, Value, _Literal) ->
    non_empty(Value);
extract(cn, Value, _Literal) ->
    case emqx_ldap_dn:parse(Value) of
        {ok, #ldap_dn{dn = [FirstRDN | _]}} ->
            first_cn(FirstRDN);
        _ ->
            none
    end.

first_cn([]) ->
    none;
first_cn([{Type, CN} | Rest]) when is_binary(CN) ->
    case string:lowercase(Type) of
        <<"cn">> -> non_empty(CN);
        _ -> first_cn(Rest)
    end;
first_cn([_HexString | Rest]) ->
    first_cn(Rest).

non_empty(<<>>) -> none;
non_empty(Value) -> {ok, Value}.

%% LDAP attribute descriptions are case-insensitive, and the directory may
%% return a different casing than the configured one.
attribute_values(Attr, #eldap_entry{attributes = Attributes}) ->
    Lower = string:lowercase(Attr),
    lists:append([
        [iolist_to_binary(V) || V <- Values]
     || {Name, Values} <- Attributes, string:lowercase(Name) =:= Lower
    ]).
