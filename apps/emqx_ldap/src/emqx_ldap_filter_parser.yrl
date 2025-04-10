Header
"%%--------------------------------------------------------------------\n"
"%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.\n"
"%%--------------------------------------------------------------------".

Nonterminals
filter filtercomp filterlist item simple present substring initial any final extensible attr value type dnattrs matchingrule dnvalue complexValue.

Terminals
lparen rparen 'and' 'or' 'not' equal approx greaterOrEqual lessOrEqual asterisk colon dn string comma.

Rootsymbol filter.
Left 100 present.
Left 500 substring.

filter ->
    lparen filtercomp rparen : '$2'.

filtercomp ->
    'and' filterlist: 'and'('$2').
filtercomp ->
    'or' filterlist: 'or'('$2').
filtercomp ->
    'not' filter: 'not'('$2').
filtercomp ->
    item: '$1'.

filterlist ->
    filter: ['$1'].
filterlist ->
    filter filterlist: ['$1' | '$2'].

item ->
    simple: '$1'.
item ->
    present: '$1'.
item ->
    substring: '$1'.
item->
    extensible: '$1'.

simple ->
    attr equal complexValue: equal('$1', '$3').
simple ->
    attr approx value: approx('$1', '$3').
simple ->
    attr greaterOrEqual value: greaterOrEqual('$1', '$3').
simple ->
    attr lessOrEqual value: lessOrEqual('$1', '$3').

present ->
    attr equal asterisk: present('$1').

substring ->
    attr equal initial asterisk any final: substrings('$1', ['$3', '$5', '$6']).
substring ->
    attr equal asterisk any final: substrings('$1', ['$4', '$5']).
substring ->
    attr equal initial asterisk any: substrings('$1', ['$3', '$5']).
substring ->
    attr equal asterisk any: substrings('$1', ['$4']).

initial ->
    value: {initial, '$1'}.

final ->
    value: {final, '$1'}.

any -> any value asterisk: 'any'('$1', '$2').
any -> '$empty': [].

extensible ->
    type dnattrs matchingrule colon equal complexValue : extensible('$6', ['$1', '$2', '$3']).
extensible ->
    type dnattrs colon equal complexValue: extensible('$5', ['$1', '$2']).
extensible ->
    type matchingrule colon equal complexValue: extensible('$5', ['$1', '$2']).
extensible ->
    type colon equal complexValue: extensible('$4', ['$1']).

extensible ->
    dnattrs matchingrule colon equal complexValue: extensible('$5', ['$1', '$2']).
extensible ->
    matchingrule colon equal complexValue: extensible('$4', ['$1']).

attr ->
    string: get_value('$1').

value ->
    string: get_value('$1').

dnvalue ->
    string equal string comma dnvalue: make_dn_value('$1', '$3', '$5').
dnvalue ->
    string equal string: make_dn_value('$1', '$3').

complexValue ->
    value: '$1'.
complexValue ->
    dnvalue: '$1'.

type ->
    value: {type, '$1'}.

dnattrs ->
    dn: {dnAttributes, true}.

matchingrule ->
    colon value: {matchingRule, '$2'}.

Erlang code.
-export([scan_and_parse/1]).
-ignore_xref({return_error, 2}).

'and'(Value) ->
    eldap:'and'(Value).

'or'(Value) ->
    eldap:'or'(Value).

'not'(Value) ->
    eldap:'not'(Value).

equal(Attr, Value) ->
    eldap:equalityMatch(Attr, Value).

approx(Attr, Value) ->
    eldap:approxMatch(Attr, Value).

greaterOrEqual(Attr, Value) ->
    eldap:greaterOrEqual(Attr, Value).

lessOrEqual(Attr, Value) ->
    eldap:lessOrEqual(Attr, Value).

present(Value) ->
    eldap:present(Value).

substrings(Attr, List) ->
    eldap:substrings(Attr, flatten(List)).

'any'(List, Item) ->
    [List, {any, Item}].

extensible(Value, Opts) -> eldap:extensibleMatch(Value, Opts).

flatten(List) -> lists:flatten(List).

get_value({_Token, _Line, Value}) ->
    Value.

make_dn_value(Attr, Value) ->
    Attr1 = get_value(Attr),
    Value1 = get_value(Value),
    Attr1 ++ "=" ++ Value1.

make_dn_value(Attr, Value, Next) ->
    Prefix = make_dn_value(Attr, Value),
    Prefix ++ "," ++ Next.

scan_and_parse(Bin) when is_binary(Bin) ->
    scan_and_parse(erlang:binary_to_list(Bin));
scan_and_parse(String) ->
    case emqx_ldap_filter_lexer:string(String) of
        {ok, Tokens, _} ->
            parse(Tokens);
        {error, Reason, _} ->
            {error, Reason}
    end.
