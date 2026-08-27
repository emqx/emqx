%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% This lexer recognizes the restricted TDengine multi-table INSERT template token set.
%% Leex selects the longest prefix. Rule order resolves equal-length matches.
%% compile/1 rejects NUL bytes before invoking this lexer.

Definitions.

%% TDengine whitespace scanning:
%% https://github.com/taosdata/TDengine/blob/4bde7ac8fbebc3aa9143124bfcbd08645c46a037/source/libs/parser/src/parTokenizer.c#L327-L339
WS              = [\s\t\r\n\v\f]+
%% TDengine bare identifier bytes:
%% https://github.com/taosdata/TDengine/blob/4bde7ac8fbebc3aa9143124bfcbd08645c46a037/source/libs/parser/src/parTokenizer.c#L272-L282
%% https://github.com/taosdata/TDengine/blob/4bde7ac8fbebc3aa9143124bfcbd08645c46a037/source/libs/parser/src/parTokenizer.c#L594-L615
ID_START        = [A-Za-z_]
ID_CONTINUE     = [A-Za-z0-9_]
IDENTIFIER      = {ID_START}{ID_CONTINUE}*
%% emqx_template placeholder envelope; parse_placeholder/1 applies local validation:
%% See emqx_template:parse/1 in apps/emqx_utils/src/emqx_template.erl.
PLACEHOLDER     = \$\{[A-Za-z0-9_.]*\}
%% Match an identifier template that contains at least one placeholder.
%% It may start with an identifier, or with a placeholder followed by an identifier byte
%% or another placeholder. The tail may mix identifier bytes and placeholders.
%% A lone placeholder does not match, so the PLACEHOLDER rule handles it.
IDENTIFIER_TEMPLATE = ({IDENTIFIER}{PLACEHOLDER}|{PLACEHOLDER}({ID_CONTINUE}|{PLACEHOLDER}))({ID_CONTINUE}|{PLACEHOLDER})*
%% A decimal point joins a TDengine number only when a digit follows it:
%% https://github.com/taosdata/TDengine/blob/4bde7ac8fbebc3aa9143124bfcbd08645c46a037/source/libs/parser/src/parTokenizer.c#L487-L510
%% https://github.com/taosdata/TDengine/blob/4bde7ac8fbebc3aa9143124bfcbd08645c46a037/source/libs/parser/src/parTokenizer.c#L538-L586
NUMBER          = (([0-9]+(\.[0-9]+)?)|(\.[0-9]+))([eE][+-]?[0-9]+)?
%% TDengine recognizes a duration unit only at an identifier boundary:
%% https://github.com/taosdata/TDengine/blob/4bde7ac8fbebc3aa9143124bfcbd08645c46a037/source/libs/parser/src/parTokenizer.c#L547-L586
DURATION        = [0-9]+[buasmhdnywBUASMHDNYW]
DURATION_CONTINUATION = {DURATION}[A-Za-z0-9_]+
%% Strings consume backslash escapes or doubled quotes. Backticks double only:
%% https://github.com/taosdata/TDengine/blob/4bde7ac8fbebc3aa9143124bfcbd08645c46a037/source/libs/parser/src/parTokenizer.c#L457-L485
SQ_STRING       = '([^'\\]|\\(.|\n)|'')*'
%% '
DQ_STRING       = "([^"\\]|\\(.|\n)|"")*"
%% "
BT_IDENTIFIER   = `([^`]|``)*`

Rules.

{WS}            : skip_token.
%% Reject TDengine comment openers instead of scanning comment bodies:
%% https://github.com/taosdata/TDengine/blob/4bde7ac8fbebc3aa9143124bfcbd08645c46a037/source/libs/parser/src/parTokenizer.c#L344-L386
--              : {error, {comments_not_allowed, TokenLine}}.
\/\*            : {error, {comments_not_allowed, TokenLine}}.
{IDENTIFIER_TEMPLATE} : identifier_template(TokenChars, TokenLine).
{PLACEHOLDER}   : placeholder(TokenChars, TokenLine).
{SQ_STRING}     : {token, {sq_string, TokenLine, to_binary(TokenChars)}}.
{DQ_STRING}     : {token, {dq_string, TokenLine, to_binary(TokenChars)}}.
{BT_IDENTIFIER} : backtick_identifier(TokenChars, TokenLine).
{DURATION_CONTINUATION} : {error, {ambiguous_duration_boundary, TokenLine}}.
{DURATION}      : {token, {duration, TokenLine, to_binary(TokenChars)}}.
{NUMBER}        : {token, {number, TokenLine, to_binary(TokenChars)}}.
{IDENTIFIER}    : identifier(to_binary(TokenChars), TokenLine).
%% TDengine source for the supported one-byte punctuation and operator forms:
%% https://github.com/taosdata/TDengine/blob/4bde7ac8fbebc3aa9143124bfcbd08645c46a037/source/libs/parser/src/parTokenizer.c#L344-L443
%% https://github.com/taosdata/TDengine/blob/4bde7ac8fbebc3aa9143124bfcbd08645c46a037/source/libs/parser/src/parTokenizer.c#L487-L510
\(              : {token, {'(', TokenLine}}.
\)              : {token, {')', TokenLine}}.
,               : {token, {',', TokenLine}}.
\.              : {token, {'.', TokenLine}}.
;               : {token, {';', TokenLine}}.
\+              : {token, {'+', TokenLine}}.
-               : {token, {'-', TokenLine}}.
'               : {error, {unterminated_string, TokenLine}}.
%% '
"               : {error, {unterminated_double_string, TokenLine}}.
%% "
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
    %% TDengine performs keyword lookup after scanning the complete identifier:
    %% https://github.com/taosdata/TDengine/blob/4bde7ac8fbebc3aa9143124bfcbd08645c46a037/source/libs/parser/src/parTokenizer.c#L594-L615
    %% https://github.com/taosdata/TDengine/blob/4bde7ac8fbebc3aa9143124bfcbd08645c46a037/source/libs/parser/src/parTokenizer.c#L299-L320
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
