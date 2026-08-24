%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% This lexer recognizes the restricted TDengine multi-table INSERT template token set.

Definitions.

WS              = [\s\t\r\n\f]+
ID_START        = [A-Za-z_\200-\377]
ID_CONTINUE     = [A-Za-z0-9_\200-\377]
IDENTIFIER      = {ID_START}{ID_CONTINUE}*
PLACEHOLDER     = \$\{[A-Za-z0-9_.]*\}
IDENTIFIER_TEMPLATE = ({IDENTIFIER}{PLACEHOLDER}|{PLACEHOLDER}({ID_CONTINUE}|{PLACEHOLDER}))({ID_CONTINUE}|{PLACEHOLDER})*
NUMBER          = (([0-9]+(\.[0-9]*)?)|(\.[0-9]+))([eE][+-]?[0-9]+)?
DURATION        = [0-9]+[buasmhdnywBUASMHDNYW]
SQ_STRING       = '([^'\\]|\\.|'')*'
DQ_STRING       = "([^"\\]|\\.|"")*"
BT_IDENTIFIER   = `([^`]|``)*`

Rules.

{WS}            : skip_token.
--              : {error, {comments_not_allowed, TokenLine}}.
\/\*            : {error, {comments_not_allowed, TokenLine}}.
{IDENTIFIER_TEMPLATE} : identifier_template(TokenChars, TokenLine).
{PLACEHOLDER}   : placeholder(TokenChars, TokenLine).
{SQ_STRING}     : {token, {sq_string, TokenLine, to_binary(TokenChars)}}.
{DQ_STRING}     : {token, {dq_string, TokenLine, to_binary(TokenChars)}}.
{BT_IDENTIFIER} : backtick_identifier(TokenChars, TokenLine).
{DURATION}      : {token, {duration, TokenLine, to_binary(TokenChars)}}.
{NUMBER}        : {token, {number, TokenLine, to_binary(TokenChars)}}.
{IDENTIFIER}    : identifier(to_binary(TokenChars), TokenLine).
\(              : {token, {'(', TokenLine}}.
\)              : {token, {')', TokenLine}}.
,               : {token, {',', TokenLine}}.
\.              : {token, {'.', TokenLine}}.
;               : {token, {';', TokenLine}}.
\+              : {token, {'+', TokenLine}}.
-               : {token, {'-', TokenLine}}.
'               : {error, {unterminated_string, TokenLine}}.
"               : {error, {unterminated_double_string, TokenLine}}.
`               : {error, {unterminated_backtick_identifier, TokenLine}}.
\$              : {error, {invalid_placeholder, TokenLine}}.
.               : {error, {unsupported_token, TokenLine, to_binary(TokenChars)}}.

Erlang code.

to_binary(Chars) ->
    list_to_binary(Chars).

placeholder(Chars, Line) ->
    Bin = to_binary(Chars),
    case emqx_bridge_tdengine_sql:parse_placeholder(Bin) of
        {ok, Placeholder} -> {token, {placeholder, Line, Placeholder}};
        {error, _} -> {error, {invalid_placeholder, Line, Bin}}
    end.

backtick_identifier(Chars, Line) ->
    Bin = to_binary(Chars),
    Body = binary:part(Bin, 1, byte_size(Bin) - 2),
    case emqx_bridge_tdengine_sql:parse_identifier_parts(Body, backtick) of
        {ok, Parts} -> {token, {bt_identifier, Line, Parts}};
        {error, Reason} -> {error, {invalid_tdengine_identifier, Line, Reason}}
    end.

identifier_template(Chars, Line) ->
    Bin = to_binary(Chars),
    case emqx_bridge_tdengine_sql:parse_identifier_parts(Bin, bare) of
        {ok, Parts} -> {token, {identifier_template, Line, Parts}};
        {error, Reason} -> {error, {invalid_tdengine_identifier, Line, Reason}}
    end.

identifier(Bin, Line) ->
    case string:lowercase(Bin) of
        <<"insert">> -> {token, {insert, Line}};
        <<"into">> -> {token, {into, Line}};
        <<"using">> -> {token, {using, Line}};
        <<"tags">> -> {token, {tags, Line}};
        <<"values">> -> {token, {values, Line}};
        <<"null">> -> {token, {null, Line}};
        <<"true">> -> {token, {true, Line}};
        <<"false">> -> {token, {false, Line}};
        <<"now">> -> {token, {now, Line}};
        <<"today">> -> {token, {today, Line}};
        _ -> {token, {identifier, Line, Bin}}
    end.
