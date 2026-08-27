%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% This lexer recognizes the restricted ClickHouse INSERT template token set.
%% Leex selects the longest prefix. Rule order resolves equal-length matches.

Definitions.

%% ClickHouse ASCII whitespace scanning:
%% https://github.com/ClickHouse/ClickHouse/blob/8dfb1700858195fa704221e360fa0798ac6ee9ed/src/Parsers/Lexer.cpp#L107-L118
WS              = [\s\t\r\n\v\f]+
%% ClickHouse BareWord bytes:
%% https://github.com/ClickHouse/ClickHouse/blob/8dfb1700858195fa704221e360fa0798ac6ee9ed/src/Common/StringUtils/StringUtils.h#L75-L120
%% https://github.com/ClickHouse/ClickHouse/blob/8dfb1700858195fa704221e360fa0798ac6ee9ed/src/Parsers/Lexer.cpp#L425-L465
ID_START        = [A-Za-z_\$]
ID_CONTINUE     = [A-Za-z0-9_\$]
IDENTIFIER      = {ID_START}{ID_CONTINUE}*
%% emqx_template placeholder envelope:
%% See emqx_template:parse/1 in apps/emqx_utils/src/emqx_template.erl.
PLACEHOLDER     = \$\{[A-Za-z0-9_.]*\}
%% ClickHouse decimal token scanning:
%% https://github.com/ClickHouse/ClickHouse/blob/8dfb1700858195fa704221e360fa0798ac6ee9ed/src/Parsers/Lexer.cpp#L120-L223
%% https://github.com/ClickHouse/ClickHouse/blob/8dfb1700858195fa704221e360fa0798ac6ee9ed/src/Parsers/Lexer.cpp#L250-L287
NUMBER          = (([0-9]+(\.[0-9]*)?)|(\.[0-9]+))([eE][+-]?[0-9]+)?
%% A doubled delimiter stays in the token. A backslash consumes the next byte:
%% https://github.com/ClickHouse/ClickHouse/blob/8dfb1700858195fa704221e360fa0798ac6ee9ed/src/Parsers/Lexer.cpp#L13-L46
SQ_STRING       = '([^'\\]|\\[\000-\377]|'')*'
DQ_STRING       = "([^"\\]|\\[\000-\377]|"")*"
BT_IDENTIFIER   = `([^`\\]|\\[\000-\377]|``)*`

Rules.

{WS}            : skip_token.
%% Reject exact ClickHouse comment openers instead of scanning comment bodies:
%% https://github.com/ClickHouse/ClickHouse/blob/8dfb1700858195fa704221e360fa0798ac6ee9ed/src/Parsers/Lexer.cpp#L292-L360
--               : {error, {comments_not_allowed, TokenLine}}.
\/\*             : {error, {comments_not_allowed, TokenLine}}.
\/\/             : {error, {comments_not_allowed, TokenLine}}.
\#[\s!]          : {error, {comments_not_allowed, TokenLine}}.
{PLACEHOLDER}   : placeholder(TokenChars, TokenLine).
{SQ_STRING}     : {token, {sq_string, TokenLine, to_binary(TokenChars)}}.
{DQ_STRING}     : {token, {dq_string, TokenLine, to_binary(TokenChars)}}.
{BT_IDENTIFIER} : {token, {bt_identifier, TokenLine, to_binary(TokenChars)}}.
{NUMBER}        : {token, {number, TokenLine, to_binary(TokenChars)}}.
{IDENTIFIER}    : identifier(TokenChars, TokenLine).
%% ClickHouse source for the supported one-byte punctuation and operator forms:
%% https://github.com/ClickHouse/ClickHouse/blob/8dfb1700858195fa704221e360fa0798ac6ee9ed/src/Parsers/Lexer.cpp#L233-L287
%% https://github.com/ClickHouse/ClickHouse/blob/8dfb1700858195fa704221e360fa0798ac6ee9ed/src/Parsers/Lexer.cpp#L290-L363
%% https://github.com/ClickHouse/ClickHouse/blob/8dfb1700858195fa704221e360fa0798ac6ee9ed/src/Parsers/Lexer.cpp#L396-L401
\(              : {token, {'(', TokenLine}}.
\)              : {token, {')', TokenLine}}.
\[              : {token, {'[', TokenLine}}.
\]              : {token, {']', TokenLine}}.
\{              : {token, {'{', TokenLine}}.
\}              : {token, {'}', TokenLine}}.
,               : {token, {',', TokenLine}}.
:               : {token, {':', TokenLine}}.
\.              : {token, {'.', TokenLine}}.
;               : {token, {';', TokenLine}}.
>=              : {token, {'>=', TokenLine}}.
<=              : {token, {'<=', TokenLine}}.
<>              : {token, {'<>', TokenLine}}.
!=              : {token, {'!=', TokenLine}}.
==              : {token, {'==', TokenLine}}.
=               : {token, {'=', TokenLine}}.
>               : {token, {'>', TokenLine}}.
<               : {token, {'<', TokenLine}}.
\+              : {token, {'+', TokenLine}}.
-               : {token, {'-', TokenLine}}.
\*              : {token, {'*', TokenLine}}.
\/              : {token, {'/', TokenLine}}.
\%              : {token, {'%', TokenLine}}.
'               : {error, {unterminated_string, TokenLine}}.
"               : {error, {unterminated_quoted_identifier, TokenLine}}.
`               : {error, {unterminated_backtick_identifier, TokenLine}}.
\$              : {error, {invalid_placeholder, TokenLine}}.
.               : {error, {unsupported_token, TokenLine, to_binary(TokenChars)}}.

Erlang code.

to_binary(Chars) ->
    list_to_binary(Chars).

placeholder(Chars, Line) ->
    Bin = to_binary(Chars),
    case emqx_bridge_clickhouse_sql:parse_placeholder(Bin) of
        {ok, Placeholder} -> {token, {placeholder, Line, Placeholder}};
        {error, _} -> {error, {invalid_placeholder, Line, Bin}}
    end.

identifier(Chars, Line) ->
    %% ClickHouse classifies a keyword after scanning the complete BareWord:
    %% https://github.com/ClickHouse/ClickHouse/blob/8dfb1700858195fa704221e360fa0798ac6ee9ed/src/Parsers/CommonParsers.cpp#L7-L39
    Bin = to_binary(Chars),
    case string:lowercase(Bin) of
        <<"insert">> -> {token, {insert, Line}};
        <<"into">> -> {token, {into, Line}};
        <<"table">> -> {token, {table, Line}};
        <<"values">> -> {token, {values, Line}};
        <<"format">> -> {token, {format, Line}};
        <<"except">> -> {token, {except, Line}};
        <<"null">> -> {token, {null, Line}};
        <<"true">> -> {token, {true, Line}};
        <<"false">> -> {token, {false, Line}};
        <<"case">> -> {token, {case_kw, Line}};
        <<"when">> -> {token, {when_kw, Line}};
        <<"then">> -> {token, {then_kw, Line}};
        <<"else">> -> {token, {else_kw, Line}};
        <<"end">> -> {token, {end_kw, Line}};
        <<"is">> -> {token, {is_kw, Line}};
        <<"and">> -> {token, {and_kw, Line}};
        <<"or">> -> {token, {or_kw, Line}};
        <<"not">> -> {token, {not_kw, Line}};
        _ -> {token, {identifier, Line, Bin}}
    end.
