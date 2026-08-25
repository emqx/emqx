%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% This lexer recognizes the restricted MySQL INSERT INTO VALUES token set.

Definitions.

WS              = [\s\t\r\n\f]+
ID_START        = [A-Za-z_\$\200-\377]
ID_CONTINUE     = [A-Za-z0-9_\$\200-\377]
IDENTIFIER      = {ID_START}{ID_CONTINUE}*
PLACEHOLDER     = \$\{[A-Za-z0-9_.]*\}
NUMBER          = (([0-9]+(\.[0-9]*)?)|(\.[0-9]+))([eE][+-]?[0-9]+)?
HEX_NUMBER      = 0[xX][0-9A-Fa-f]+
HEX_STRING      = [xX]'([0-9A-Fa-f][0-9A-Fa-f])*'
SQ_COMMON       = '([^'\\]|\\[^']|'')*'
SQ_BACKSLASH    = '([^'\\]|\\.|'')*'
SQ_PLAIN        = '([^']|'')*'
DQ_COMMON       = "([^"\\]|\\[^"]|"")*"
DQ_BACKSLASH    = "([^"\\]|\\.|"")*"
DQ_PLAIN        = "([^"]|"")*"
BT_IDENTIFIER   = `([^`]|``)*`

Rules.

{WS}            : skip_token.
--               : {error, {comments_not_allowed, TokenLine}}.
\/\*             : {error, {comments_not_allowed, TokenLine}}.
\#               : {error, {comments_not_allowed, TokenLine}}.
{PLACEHOLDER}   : placeholder(TokenChars, TokenLine).
{HEX_STRING}    : {token, {hex_string, TokenLine, to_binary(TokenChars)}}.
{HEX_NUMBER}    : {token, {hex_number, TokenLine, to_binary(TokenChars)}}.
{SQ_COMMON}     : {token, {sq_string, TokenLine, to_binary(TokenChars)}}.
{SQ_BACKSLASH}  : {error, {ambiguous_single_quoted_string, TokenLine}}.
{SQ_PLAIN}      : {error, {ambiguous_single_quoted_string, TokenLine}}.
{DQ_COMMON}     : {token, {dq_opaque, TokenLine, to_binary(TokenChars)}}.
{DQ_BACKSLASH}  : {error, {ambiguous_double_quoted_token, TokenLine}}.
{DQ_PLAIN}      : {error, {ambiguous_double_quoted_token, TokenLine}}.
{BT_IDENTIFIER} : {token, {bt_identifier, TokenLine, to_binary(TokenChars)}}.
{NUMBER}        : {token, {number, TokenLine, to_binary(TokenChars)}}.
{IDENTIFIER}    : identifier(TokenChars, TokenLine).
\(              : {token, {'(', TokenLine}}.
\)              : {token, {')', TokenLine}}.
,               : {token, {',', TokenLine}}.
\.              : {token, {'.', TokenLine}}.
;               : {token, {';', TokenLine}}.
=               : {token, {'=', TokenLine}}.
\+              : {token, {'+', TokenLine}}.
-               : {token, {'-', TokenLine}}.
\*              : {token, {'*', TokenLine}}.
\/              : {token, {'/', TokenLine}}.
\%              : {token, {'%', TokenLine}}.
'               : {error, {unterminated_single_quoted_string, TokenLine}}.
"               : {error, {unterminated_double_quoted_token, TokenLine}}.
`               : {error, {unterminated_backtick_identifier, TokenLine}}.
\$              : {error, {invalid_placeholder, TokenLine}}.
.               : {error, {unsupported_token, TokenLine, to_binary(TokenChars)}}.

Erlang code.

to_binary(Chars) ->
    list_to_binary(Chars).

placeholder(Chars, Line) ->
    Bin = to_binary(Chars),
    case emqx_mysql_sql:parse_placeholder(Bin) of
        {ok, Placeholder} -> {token, {placeholder, Line, Placeholder}};
        {error, _} -> {error, {invalid_placeholder, Line, Bin}}
    end.

identifier(Chars, Line) ->
    Bin = to_binary(Chars),
    case string:lowercase(Bin) of
        <<"insert">> -> {token, {insert, Line}};
        <<"into">> -> {token, {into, Line}};
        <<"values">> -> {token, {values, Line, Bin}};
        <<"on">> -> {token, {on, Line}};
        <<"duplicate">> -> {token, {duplicate, Line}};
        <<"key">> -> {token, {key, Line, Bin}};
        <<"update">> -> {token, {update, Line, Bin}};
        <<"as">> -> {token, {as, Line}};
        <<"null">> -> {token, {null, Line}};
        <<"true">> -> {token, {true, Line}};
        <<"false">> -> {token, {false, Line}};
        <<"default">> -> {token, {default, Line}};
        _ -> {token, {identifier, Line, Bin}}
    end.
