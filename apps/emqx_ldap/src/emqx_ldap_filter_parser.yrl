%% See for more details:
%% https://www.rfc-editor.org/rfc/rfc4515
%% https://ldap.com/ldap-filters/

Header
"%%--------------------------------------------------------------------\n"
"%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.\n"
"%%--------------------------------------------------------------------".

Nonterminals
filter filtercomp filterlist item simple presentMatch substringMatch extensibleMatch extensibleSubject.

Terminals
extensible equal approx greaterOrEqual lessOrEqual present substring lparen rparen 'and' 'or' 'not' attr dnAttrs matchingRule.

Rootsymbol filter.

filter ->
    lparen filtercomp rparen : '$2'.

filtercomp ->
    'and' filterlist: {'and', '$2'}.
filtercomp ->
    'or' filterlist: {'or', '$2'}.
filtercomp ->
    'not' filter: {'not', '$2'}.
filtercomp ->
    item: '$1'.

filterlist ->
    filter: ['$1'].
filterlist ->
    filter filterlist: ['$1' | '$2'].

item ->
    simple: '$1'.
item ->
    presentMatch: '$1'.
item ->
    substringMatch: '$1'.
item->
    extensibleMatch: '$1'.

simple ->
    attr equal: {equal, value('$1'), trim_unescape_value('$2')}.
simple ->
    attr approx: {approx, value('$1'), trim_unescape_value('$2')}.
simple ->
    attr greaterOrEqual: {greaterOrEqual, value('$1'), trim_unescape_value('$2')}.
simple ->
    attr lessOrEqual: {lessOrEqual, value('$1'), trim_unescape_value('$2')}.

presentMatch ->
    attr present: {present, value('$1')}.

substringMatch ->
    attr substring: {substring, value('$1'), parse_substring(trim(value('$2')))}.

extensibleMatch ->
    extensibleSubject extensible: {extensible, '$1', trim_unescape_value('$2')}.

extensibleSubject ->
    attr: [{type, value('$1')}].
extensibleSubject ->
    attr dnAttrs: [{type, value('$1')}, {dnAttributes, true}].
extensibleSubject ->
    attr matchingRule: [{type, value('$1')}, {matchingRule, trim_matching_rule('$2')}].
extensibleSubject ->
    attr dnAttrs matchingRule: [{type, value('$1')}, {dnAttributes, true}, {matchingRule, trim_matching_rule('$3')}].
extensibleSubject ->
    dnAttrs matchingRule: [{dnAttributes, true}, {matchingRule, trim_matching_rule('$2')}].
extensibleSubject ->
    matchingRule: [{matchingRule, trim_matching_rule('$1')}].


Erlang code.

-export([scan_and_parse/1]).
-ignore_xref({return_error, 2}).

-define(IS_HEX_CHAR(C), (C >= $0 andalso C =< $9) orelse (C >= $a andalso C =< $f) orelse (C >= $A andalso C =< $F)).

value({_, Value}) -> Value.

trim_unescape_value(Token) ->
    unescape(trim(value(Token))).

trim([$= | Rest]) ->
    Rest;
trim([$~, $= | Rest]) ->
    Rest;
trim([$<, $= | Rest]) ->
    Rest;
trim([$>, $= | Rest]) ->
    Rest;
trim([$:, $= | Rest]) ->
    Rest;
trim(Other) ->
    Other.

%% No need to unescape
trim_matching_rule({matchingRule, [$: | Rest]}) ->  Rest.

hex_char_to_int(HexChar) when HexChar >= $0 andalso HexChar =< $9 -> HexChar - $0;
hex_char_to_int(HexChar) when HexChar >= $a andalso HexChar =< $f -> HexChar - $a + 10;
hex_char_to_int(HexChar) when HexChar >= $A andalso HexChar =< $F -> HexChar - $A + 10.

unescape([$\\, HexChar1, HexChar2 | Rest]) when ?IS_HEX_CHAR(HexChar1) andalso ?IS_HEX_CHAR(HexChar2) ->
    [hex_char_to_int(HexChar1) * 16 + hex_char_to_int(HexChar2) | unescape(Rest)];
unescape([$\\, Char | Rest]) ->
    [Char | unescape(Rest)];
unescape([Other | Rest]) ->
    [Other | unescape(Rest)];
unescape([]) ->
    [].

parse_substring(String) ->
    Segments = [unescape(Segment) || Segment <- emqx_ldap_utils:split(String, $*)],
    parse_substring_initial(Segments, []).

parse_substring_initial(["" | Rest], Acc) ->
    parse_substring_any_final(Rest, Acc);
parse_substring_initial([Segment | Rest], Acc) ->
    parse_substring_any_final(Rest, [{initial, Segment} | Acc]).

parse_substring_any_final([""], Acc) ->
    lists:reverse(Acc);
parse_substring_any_final([Segment], Acc) ->
    lists:reverse([{final, Segment} | Acc]);
parse_substring_any_final([Segment | Rest], Acc) ->
    parse_substring_any_final(Rest, [{any, Segment} | Acc]).

scan_and_parse(Bin) when is_binary(Bin) ->
    scan_and_parse(erlang:binary_to_list(Bin));
scan_and_parse(String) ->
    case emqx_ldap_filter_lexer:string(String) of
        {ok, Tokens, _} ->
            parse(Tokens);
        {error, Reason, _} ->
            {error, Reason}
    end.
