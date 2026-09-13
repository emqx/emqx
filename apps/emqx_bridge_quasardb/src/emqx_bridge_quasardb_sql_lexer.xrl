%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% This lexer recognizes the restricted QuasarDB 3.14.1 INSERT token set.
%% QuasarDB does not publish its lexer. The linked grammar defines each token
%% category. Byte boundaries were verified against the pinned 3.14.1 ODBC driver.

Definitions.

%% INSERT grammar and examples:
%% https://doc.quasar.ai/3.14.1/queries/insert.html
%% Raw ODBC accepts horizontal and vertical tab, form feed, CR, LF, and space.
WS              = [\x{9}-\x{D}\x{20}]+
%% Table and column names are alphanumeric, cannot start with a number, and in
%% practice also accept underscores:
%% https://doc.quasar.ai/3.14.1/queries/create_table.html
%% https://doc.quasar.ai/3.14.1/queries/alter_table.html
ID_START        = [A-Za-z_]
ID_CONTINUE     = [A-Za-z0-9_]
IDENTIFIER      = {ID_START}{ID_CONTINUE}*
%% Double-quoted identifiers use backslash escapes in QuasarDB 3.14.1.
%% Identifier roles are defined by the CREATE TABLE and INSERT grammars:
%% https://doc.quasar.ai/3.14.1/queries/create_table.html
%% https://doc.quasar.ai/3.14.1/queries/insert.html
DQ_IDENTIFIER   = "(\\[\x{0}-\x{FF}]|[^"\\])*"
%% "
%% INSERT examples use single-quoted BLOB and STRING values. QuasarDB 3.14.1
%% uses a backslash to escape the next byte and rejects doubled apostrophes:
%% https://doc.quasar.ai/3.14.1/queries/insert.html
SQ_STRING       = '(\\[\x{0}-\x{FF}]|[^'\\])*'
%% '
%% Absolute timestamp forms and precision:
%% https://doc.quasar.ai/3.14.1/queries/timestamps.html#absolute
DIGIT2          = [0-9][0-9]
DIGIT4          = [0-9][0-9][0-9][0-9]
DATE            = {DIGIT4}-{DIGIT2}-{DIGIT2}
%% Empirically deduced, see t_lexer_conformance in emqx_bridge_quasardb_SUITE
FRACTION        = \.[0-9][0-9]?[0-9]?[0-9]?[0-9]?[0-9]?[0-9]?[0-9]?[0-9]?
TIME            = [Tt]{DIGIT2}:{DIGIT2}(:{DIGIT2}({FRACTION})?)?[Zz]?
TIMESTAMP       = {DATE}({TIME})?
%% INSERT examples define integer and double values:
%% https://doc.quasar.ai/3.14.1/queries/insert.html
NUMBER          = [+-]?(([0-9]+(\.[0-9]*)?)|(\.[0-9]+))([eE][+-]?[0-9]+)?
%% EMQX placeholder envelope. parse_placeholder/1 applies path validation:
%% apps/emqx_utils/src/emqx_template.erl
PLACEHOLDER     = \$\{[A-Za-z0-9_.]*\}

Rules.

{WS}            : skip_token.
%% Comments are outside the published INSERT grammar and are rejected instead
%% of being copied across placeholder boundaries:
%% https://doc.quasar.ai/3.14.1/queries/insert.html#synopsis
--              : {error, {comments_not_allowed, TokenLine}}.
\/\*            : {error, {comments_not_allowed, TokenLine}}.
\#              : {error, {comments_not_allowed, TokenLine}}.
{PLACEHOLDER}   : placeholder(TokenChars, TokenLine).
{SQ_STRING}     : {token, {sq_string, TokenLine, to_binary(TokenChars)}}.
{DQ_IDENTIFIER} : {token, {dq_identifier, TokenLine, to_binary(TokenChars)}}.
{TIMESTAMP}     : {token, {timestamp, TokenLine, to_binary(TokenChars)}}.
{NUMBER}        : {token, {number, TokenLine, to_binary(TokenChars)}}.
\$timestamp     : {token, {timestamp_column, TokenLine, <<"$timestamp">>}}.
{IDENTIFIER}    : identifier(TokenChars, TokenLine).
%% INSERT punctuation:
%% https://doc.quasar.ai/3.14.1/queries/insert.html#synopsis
\(              : {token, {'(', TokenLine}}.
\)              : {token, {')', TokenLine}}.
,               : {token, {',', TokenLine}}.
;               : {token, {';', TokenLine}}.
'               : {error, {unterminated_string, TokenLine}}.
%% '
"               : {error, {unterminated_quoted_identifier, TokenLine}}.
%% "
\$              : {error, {invalid_placeholder, TokenLine}}.
.               : {error, {unsupported_token, TokenLine, to_binary(TokenChars)}}.

Erlang code.

to_binary(Chars) ->
    list_to_binary(Chars).

placeholder(Chars, Line) ->
    Bin = to_binary(Chars),
    case emqx_sql_plan:parse_placeholder(Bin) of
        {ok, Placeholder} -> {token, {placeholder, Line, Placeholder}};
        {error, _} -> {error, {invalid_placeholder, Line, Bin}}
    end.

identifier(Chars, Line) ->
    Bin = to_binary(Chars),
    case string:lowercase(Bin) of
        %% Keywords and special timestamp values are case-insensitive in 3.14.1.
        %% https://doc.quasar.ai/3.14.1/queries/insert.html
        %% https://doc.quasar.ai/3.14.1/queries/timestamps.html#absolute
        <<"insert">> -> {token, {insert, Line, Bin}};
        <<"into">> -> {token, {into, Line, Bin}};
        <<"values">> -> {token, {values, Line, Bin}};
        <<"null">> -> {token, {null, Line, Bin}};
        <<"now">> -> {token, {relative_timestamp, Line, Bin}};
        <<"today">> -> {token, {relative_timestamp, Line, Bin}};
        <<"yesterday">> -> {token, {relative_timestamp, Line, Bin}};
        <<"tomorrow">> -> {token, {relative_timestamp, Line, Bin}};
        <<"epoch">> -> {token, {timestamp_constant, Line, Bin}};
        <<"end_of_time">> -> {token, {timestamp_constant, Line, Bin}};
        _ -> {token, {identifier, Line, Bin}}
    end.
