%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ldap_dn).

-moduledoc """
Module for parsing, transformation and formatting LDAP Distinguished Name (DN) strings.

The DN string format is described in RFC 4514:
https://www.rfc-editor.org/rfc/rfc4514

Although the DN is passed as a string in LDAP protocol, we parse it:
* to make early validation of the DN string
* to provide consistent transformation of the value components, e.g. templating.
""".

-include("emqx_ldap.hrl").

-export([
    parse/1,
    mapfold_values/3,
    to_string/1
]).

% distinguishedName = [ relativeDistinguishedName
% *( COMMA relativeDistinguishedName ) ]
% relativeDistinguishedName = attributeTypeAndValue
% *( PLUS attributeTypeAndValue )
% attributeTypeAndValue = attributeType EQUALS attributeValue
% attributeType = descr / numericoid
% attributeValue = string / hexstring

% ; The following characters are to be escaped when they appear
% ; in the value to be encoded: ESC, one of <escaped>, leading
% ; SHARP or SPACE, trailing SPACE, and NULL.
% string =   [ ( leadchar / pair ) [ *( stringchar / pair ) ( trailchar / pair ) ] ]

% leadchar = LUTF1 / UTFMB
% LUTF1 = %x01-1F / %x21 / %x24-2A / %x2D-3A / %x3D / %x3F-5B / %x5D-7F

% trailchar  = TUTF1 / UTFMB
% TUTF1 = %x01-1F / %x21 / %x23-2A / %x2D-3A / %x3D / %x3F-5B / %x5D-7F

% stringchar = SUTF1 / UTFMB
% SUTF1 = %x01-21 / %x23-2A / %x2D-3A / %x3D / %x3F-5B / %x5D-7F

% pair = ESC ( ESC / special / hexpair )
% special = escaped / SPACE / SHARP / EQUALS
% escaped = DQUOTE / PLUS / COMMA / SEMI / LANGLE / RANGLE
% hexstring = SHARP 1*hexpair
% hexpair = HEX HEX

-type ldap_dn(ValueType) :: #ldap_dn{dn :: dn(ValueType)}.
-type ldap_dn() :: ldap_dn(string()).

-type attribute() :: string().
-type attribute_value(ValueType) :: ValueType | {hexstring, string()}.
-type attribute_and_value(ValueType) :: {attribute(), attribute_value(ValueType)}.

-type dn(ValueType) ::
    [[attribute_and_value(ValueType)]].

-export_type([ldap_dn/0, ldap_dn/1]).

-define(IS_STRING_CHAR(CH),
    ((CH >= 16#00 andalso CH =< 16#21) orelse
        (CH >= 16#24 andalso CH =< 16#2A) orelse
        (CH >= 16#2D andalso CH =< 16#3A) orelse
        (CH >= 16#3D andalso CH =< 16#5B) orelse
        (CH >= 16#5D andalso CH =< 16#7F))
).

-define(IS_EXT_STRING_CHAR(CH), (?IS_STRING_CHAR(CH) orelse CH =:= 16#20)).

-define(IS_ESCAPE_CHAR(CH),
    ((CH =:= $\\) orelse (CH =:= 16#20) orelse (CH =:= $#) orelse
        (CH =:= $=) orelse (CH =:= $") orelse (CH =:= $+) orelse
        (CH =:= $,) orelse (CH =:= $;) orelse (CH =:= $<) orelse
        (CH =:= $>))
).

-define(IS_HEX_CHAR(CH),
    ((CH >= $0 andalso CH =< $9) orelse
        (CH >= $A andalso CH =< $F) orelse
        (CH >= $a andalso CH =< $f))
).

-define(ATTR_RE, """
    ^
    # OID
    (?:
        # Numeric OID
        \d+(?:\.\d+)*
        |
        # Alpha OID
        [a-zA-Z][a-zA-Z0-9\-]*
    )
    # Descr terms (optional)
    (?:;[a-zA-Z0-9\-]+)*
    $
""").

%%--------------------------------------------------------------------
%% API functions
%%--------------------------------------------------------------------

-spec parse(string()) -> {ok, ldap_dn()} | {error, term()}.
parse(DN) ->
    try
        {ok, #ldap_dn{dn = parse_dn(DN)}}
    catch
        throw:Reason ->
            {error, Reason}
    end.

-spec to_string(ldap_dn()) -> iodata().
to_string(#ldap_dn{dn = DN}) ->
    lists:join(",", lists:map(fun rdn_to_string/1, DN)).

-spec mapfold_values(fun((ValueType, Acc) -> {NewValueType, Acc}), Acc, ldap_dn(ValueType)) ->
    {ldap_dn(NewValueType), Acc}.
mapfold_values(Fun, Acc0, #ldap_dn{dn = DN}) ->
    {NewDN, NewAcc} = lists:mapfoldl(
        fun(RDN, Acc1) ->
            lists:mapfoldl(
                fun(AttrValuePair, Acc2) ->
                    case AttrValuePair of
                        {_, {hexstring, _}} ->
                            {AttrValuePair, Acc2};
                        {Attr, Value0} ->
                            {Value, Acc3} = Fun(Value0, Acc2),
                            {{Attr, Value}, Acc3}
                    end
                end,
                Acc1,
                RDN
            )
        end,
        Acc0,
        DN
    ),
    {#ldap_dn{dn = NewDN}, NewAcc}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

parse_dn(DN) ->
    RDNs = emqx_ldap_utils:split(DN, $,),
    lists:map(fun parse_rdn/1, RDNs).

parse_rdn(RDN) ->
    AttrValuePairs = emqx_ldap_utils:split(RDN, $+),
    lists:map(fun parse_attr_value_pair/1, AttrValuePairs).

parse_attr_value_pair(AttrValuePair) ->
    case emqx_ldap_utils:split(AttrValuePair, $=) of
        [Attr, Value] ->
            {parse_attr(Attr), parse_value(Value)};
        _ ->
            throw({invalid_attr_value_pair, AttrValuePair})
    end.

parse_attr(Attr0) ->
    Attr = string:trim(Attr0),
    {ok, RE} = re:compile(?ATTR_RE, [extended]),
    case re:run(Attr, RE) of
        nomatch ->
            throw({invalid_attr, Attr});
        _ ->
            Attr
    end.

parse_value(Value0) ->
    Value = string:trim(Value0),
    case Value of
        "" ->
            throw(empty_value);
        [$#] ->
            throw(empty_hexstring);
        [$# | Rest] ->
            ok = validate_hexstring(Rest),
            {hexstring, Rest};
        _ ->
            parse_string(Value, [])
    end.

validate_hexstring([]) ->
    ok;
validate_hexstring([CH1, CH2 | Rest]) when ?IS_HEX_CHAR(CH1) andalso ?IS_HEX_CHAR(CH2) ->
    validate_hexstring(Rest);
validate_hexstring(_) ->
    throw(invalid_hexstring).

parse_string([], Acc) ->
    lists:reverse(Acc);
parse_string([$\\, HexChar1, HexChar2 | Rest], Acc) when
    ?IS_HEX_CHAR(HexChar1) andalso ?IS_HEX_CHAR(HexChar2)
->
    parse_string(Rest, [hex_char_to_int(HexChar1) * 16 + hex_char_to_int(HexChar2) | Acc]);
parse_string([$\\, Char | Rest], Acc) when ?IS_ESCAPE_CHAR(Char) ->
    parse_string(Rest, [Char | Acc]);
parse_string([Char | Rest], Acc) when ?IS_EXT_STRING_CHAR(Char) ->
    parse_string(Rest, [Char | Acc]);
parse_string([Char | _Rest], _Acc) ->
    throw({invalid_string_char, Char}).

hex_char_to_int(HexChar) when HexChar >= $0 andalso HexChar =< $9 ->
    HexChar - $0;
hex_char_to_int(HexChar) when HexChar >= $a andalso HexChar =< $f ->
    HexChar - $a + 10;
hex_char_to_int(HexChar) when HexChar >= $A andalso HexChar =< $F ->
    HexChar - $A + 10.

rdn_to_string(RDN) ->
    lists:join("+", lists:map(fun attr_value_to_string/1, RDN)).

attr_value_to_string({Attr, Value}) ->
    [Attr, "=", value_to_string(Value)].

value_to_string({hexstring, HexString}) ->
    [$# | HexString];
value_to_string(" " ++ Rest) ->
    [$\\, " " | do_value_to_string(Rest)];
value_to_string(Value) ->
    do_value_to_string(Value).

do_value_to_string([]) ->
    [];
%% do not escape space in the middle of a value
do_value_to_string([16#20, Char | Rest]) ->
    [16#20 | do_value_to_string([Char | Rest])];
do_value_to_string([Char | Rest]) when ?IS_ESCAPE_CHAR(Char) ->
    [$\\, Char | do_value_to_string(Rest)];
do_value_to_string([Char | Rest]) when ?IS_STRING_CHAR(Char) ->
    [Char | do_value_to_string(Rest)];
do_value_to_string([Char | Rest]) when Char >= 0 andalso Char =< 16#FF ->
    [$\\, to_hex_char(Char div 16), to_hex_char(Char rem 16) | do_value_to_string(Rest)];
do_value_to_string([Char | _Rest]) ->
    throw({invalid_string_char, Char}).

to_hex_char(In) when In >= 0 andalso In =< 9 ->
    $0 + In;
to_hex_char(In) when In >= 10 andalso In =< 15 ->
    $a + (In - 10).
