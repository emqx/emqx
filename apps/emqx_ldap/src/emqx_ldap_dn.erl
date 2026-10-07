%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    map_values/2,
    to_string/1,
    is_attribute_description/1
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
-type ldap_dn() :: ldap_dn(binary()).

-type attribute() :: binary().
-type attribute_value(ValueType) :: ValueType | {hexstring, binary()}.
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

-define(IS_WS(CH), (CH =:= $\s orelse CH =:= $\t orelse CH =:= $\r orelse CH =:= $\n)).

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

-doc "Parses a DN. A list is taken as a list of bytes, which is how `eldap` returns values.".
-spec parse(binary() | [byte()]) -> {ok, ldap_dn()} | {error, term()}.
parse(DN) when is_list(DN) ->
    parse(list_to_binary(DN));
parse(DN) when is_binary(DN) ->
    try
        {ok, #ldap_dn{dn = parse_dn(DN)}}
    catch
        throw:Reason ->
            {error, Reason}
    end.

-doc """
Returns `true` when the string is an LDAP attribute description (RFC 4512, section 2.5):
a name or a numeric OID, optionally followed by options such as `;binary`.
""".
-spec is_attribute_description(string() | binary()) -> boolean().
is_attribute_description(String) ->
    {ok, RE} = re:compile(?ATTR_RE, [extended, dollar_endonly]),
    match =:= re:run(String, RE, [{capture, none}]).

-doc "Formats a DN as the string `eldap` takes. A value may be a binary or a string.".
-spec to_string(ldap_dn(iodata())) -> string().
to_string(#ldap_dn{dn = DN}) ->
    binary_to_list(iolist_to_binary(lists:join(",", lists:map(fun rdn_to_string/1, DN)))).

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

-spec map_values(
    fun((ValueType) -> NewValueType),
    ldap_dn(ValueType)
) ->
    ldap_dn(NewValueType).
map_values(Fun, LDAPDN0) ->
    {LDAPDN, undefined} = mapfold_values(
        fun(Value, Acc) ->
            {Fun(Value), Acc}
        end,
        undefined,
        LDAPDN0
    ),
    LDAPDN.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

parse_dn(DN) ->
    lists:map(fun parse_rdn/1, split(DN, $,)).

parse_rdn(RDN) ->
    lists:map(fun parse_attr_value_pair/1, split(RDN, $+)).

parse_attr_value_pair(AttrValuePair) ->
    case split(AttrValuePair, $=) of
        [Attr, Value] ->
            {parse_attr(Attr), parse_value(Value)};
        _ ->
            throw({invalid_attr_value_pair, AttrValuePair})
    end.

%% Splits on the separator, except where a backslash escapes it.
split(Bin, Sep) ->
    split(Bin, Sep, <<>>, []).

split(<<Sep, Rest/binary>>, Sep, Cur, Acc) ->
    split(Rest, Sep, <<>>, [Cur | Acc]);
split(<<$\\, Char, Rest/binary>>, Sep, Cur, Acc) ->
    split(Rest, Sep, <<Cur/binary, $\\, Char>>, Acc);
split(<<Char, Rest/binary>>, Sep, Cur, Acc) ->
    split(Rest, Sep, <<Cur/binary, Char>>, Acc);
split(<<>>, _Sep, Cur, Acc) ->
    lists:reverse([Cur | Acc]).

parse_attr(Attr0) ->
    Attr = trim(Attr0),
    {ok, RE} = re:compile(?ATTR_RE, [extended]),
    case re:run(Attr, RE, [{capture, none}]) of
        nomatch ->
            throw({invalid_attr, Attr});
        match ->
            Attr
    end.

parse_value(Value0) ->
    case trim_value(Value0) of
        <<>> ->
            throw(empty_value);
        <<$#>> ->
            throw(empty_hexstring);
        <<$#, HexString/binary>> ->
            ok = validate_hexstring(HexString),
            {hexstring, HexString};
        Value ->
            parse_string(Value, <<>>)
    end.

%% Strips ASCII whitespace around a value. A trailing space that a backslash
%% escapes is part of the value and is kept.
trim_value(Value0) ->
    Value1 = trim_leading(Value0),
    Value = trim_trailing(Value1),
    case byte_size(Value) < byte_size(Value1) andalso ends_with_escape(Value) of
        true -> <<Value/binary, $\s>>;
        false -> Value
    end.

trim(Bin) ->
    trim_trailing(trim_leading(Bin)).

trim_leading(<<Char, Rest/binary>>) when ?IS_WS(Char) ->
    trim_leading(Rest);
trim_leading(Bin) ->
    Bin.

trim_trailing(Bin) ->
    binary:part(Bin, 0, size_without_trailing_ws(Bin, byte_size(Bin))).

size_without_trailing_ws(Bin, Size) when Size > 0 ->
    case binary:at(Bin, Size - 1) of
        Char when ?IS_WS(Char) -> size_without_trailing_ws(Bin, Size - 1);
        _ -> Size
    end;
size_without_trailing_ws(_Bin, 0) ->
    0.

%% True when the value ends with a backslash that escapes the byte after it.
%% A backslash and the byte it escapes are consumed as a pair, so an escaped
%% backslash does not count.
ends_with_escape(<<>>) ->
    false;
ends_with_escape(<<$\\>>) ->
    true;
ends_with_escape(<<$\\, _, Rest/binary>>) ->
    ends_with_escape(Rest);
ends_with_escape(<<_, Rest/binary>>) ->
    ends_with_escape(Rest).

validate_hexstring(<<>>) ->
    ok;
validate_hexstring(<<CH1, CH2, Rest/binary>>) when ?IS_HEX_CHAR(CH1) andalso ?IS_HEX_CHAR(CH2) ->
    validate_hexstring(Rest);
validate_hexstring(_) ->
    throw(invalid_hexstring).

parse_string(<<>>, Acc) ->
    Acc;
parse_string(<<$\\, HexChar1, HexChar2, Rest/binary>>, Acc) when
    ?IS_HEX_CHAR(HexChar1) andalso ?IS_HEX_CHAR(HexChar2)
->
    Byte = hex_char_to_int(HexChar1) * 16 + hex_char_to_int(HexChar2),
    parse_string(Rest, <<Acc/binary, Byte>>);
parse_string(<<$\\, Char, Rest/binary>>, Acc) when ?IS_ESCAPE_CHAR(Char) ->
    parse_string(Rest, <<Acc/binary, Char>>);
parse_string(<<Char, Rest/binary>>, Acc) when ?IS_EXT_STRING_CHAR(Char) ->
    parse_string(Rest, <<Acc/binary, Char>>);
%% UTFMB: a multi-byte UTF-8 character. The match fails on a malformed sequence.
parse_string(<<Char/utf8, Rest/binary>>, Acc) when Char >= 16#80 ->
    parse_string(Rest, <<Acc/binary, Char/utf8>>);
parse_string(<<Char, _Rest/binary>>, _Acc) ->
    throw({invalid_string_char, Char}).

hex_char_to_int(HexChar) when HexChar >= $0 andalso HexChar =< $9 ->
    HexChar - $0;
hex_char_to_int(HexChar) when HexChar >= $a andalso HexChar =< $f ->
    HexChar - $a + 10;
hex_char_to_int(HexChar) when HexChar >= $A andalso HexChar =< $F ->
    HexChar - $A + 10.

rdn_to_string(RDN) ->
    lists:join("+", lists:map(fun attr_value_to_string/1, RDN)).

attr_value_to_string({Attr, {hexstring, HexString}}) ->
    [Attr, "=#", HexString];
attr_value_to_string({Attr, Value}) ->
    [Attr, "=", escape_value(iolist_to_binary(Value))].

%% A leading space, a trailing space, the special characters, and every byte
%% outside the string character set are escaped.
escape_value(<<$\s, Rest/binary>>) ->
    [$\\, $\s | escape_chars(Rest)];
escape_value(Value) ->
    escape_chars(Value).

escape_chars(<<>>) ->
    [];
escape_chars(<<$\s, Rest/binary>>) when Rest =/= <<>> ->
    [$\s | escape_chars(Rest)];
escape_chars(<<Char, Rest/binary>>) when ?IS_ESCAPE_CHAR(Char) ->
    [$\\, Char | escape_chars(Rest)];
escape_chars(<<Char, Rest/binary>>) when ?IS_STRING_CHAR(Char) ->
    [Char | escape_chars(Rest)];
escape_chars(<<Char, Rest/binary>>) ->
    [$\\, to_hex_char(Char div 16), to_hex_char(Char rem 16) | escape_chars(Rest)].

to_hex_char(In) when In >= 0 andalso In =< 9 ->
    $0 + In;
to_hex_char(In) when In >= 10 andalso In =< 15 ->
    $a + (In - 10).
