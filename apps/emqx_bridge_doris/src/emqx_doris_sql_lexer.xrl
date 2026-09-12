%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
%% Restricted lexer based on Doris 2.1.9. Not a full ANTLR translation.
%% https://github.com/apache/doris/blob/3390475e02a359380b98cc99c965b65f77827054/fe/fe-core/src/main/antlr4/org/apache/doris/nereids/DorisLexer.g4
%% Input is bytes (0..255), with no Unicode validation. Leex uses longest match,
%% then rule order. Numeric predicates are not translated; see README.md.

Definitions.

%% Quoted strings and identifiers. Strings use byte input and ASCII case-insensitive R.
%% https://github.com/apache/doris/blob/3390475e02a359380b98cc99c965b65f77827054/fe/fe-core/src/main/antlr4/org/apache/doris/nereids/DorisLexer.g4#L599-L604
%% https://github.com/apache/doris/blob/3390475e02a359380b98cc99c965b65f77827054/fe/fe-core/src/main/antlr4/org/apache/doris/nereids/DorisLexer.g4#L647-L649
STRING_LITERAL = ('(\\[\x{0}-\x{FF}]|''|[^'\\])*'|"(\\[\x{0}-\x{FF}]|""|[^"\\])*"|[Rr]'[^']*'|[Rr]"[^"]*")
%% "
BACKQUOTED_IDENTIFIER = (`([^`]|``)*`)
%% Character and numeric definitions. LETTER uses bytes rather than Unicode code points.
%% EXPONENT uses ASCII case-insensitive E.
%% https://github.com/apache/doris/blob/3390475e02a359380b98cc99c965b65f77827054/fe/fe-core/src/main/antlr4/org/apache/doris/nereids/DorisLexer.g4#L651-L668
%% https://github.com/apache/doris/blob/3390475e02a359380b98cc99c965b65f77827054/fe/fe-core/src/main/antlr4/org/apache/doris/nereids/DorisLexer.g4#L625-L627
LETTER = ([$A-Z_a-z]|[\x{80}-\x{FF}])
DIGIT = [0-9]
DECIMAL_DIGITS = ([0-9]+\.[0-9]*|\.[0-9]+)
EXPONENT = ([Ee][+\-]?[0-9]+)
INTEGER_VALUE = ([0-9]+)
%% WS:
%% https://github.com/apache/doris/blob/3390475e02a359380b98cc99c965b65f77827054/fe/fe-core/src/main/antlr4/org/apache/doris/nereids/DorisLexer.g4#L682-L684
WS = ([\x{9}-\x{A}\x{D}\x{20}]+)
%% Reject digit-starting bare identifiers because they are ambiguous with numeric literals.
%% Local restriction of IDENTIFIER, which allows a leading digit upstream.
%% https://github.com/apache/doris/blob/3390475e02a359380b98cc99c965b65f77827054/fe/fe-core/src/main/antlr4/org/apache/doris/nereids/DorisLexer.g4#L643-L645
IDENTIFIER = {LETTER}({LETTER}|{DIGIT})*
%% EMQX ${...} placeholders have no upstream counterpart; Doris PLACEHOLDER is '?'.
%% https://github.com/apache/doris/blob/3390475e02a359380b98cc99c965b65f77827054/fe/fe-core/src/main/antlr4/org/apache/doris/nereids/DorisLexer.g4#L405
PLACEHOLDER = \$\{[A-Za-z0-9_.]*\}
%% Combine INTEGER_VALUE, EXPONENT_VALUE and DECIMAL_VALUE without Java predicates.
%% https://github.com/apache/doris/blob/3390475e02a359380b98cc99c965b65f77827054/fe/fe-core/src/main/antlr4/org/apache/doris/nereids/DorisLexer.g4#L625-L636
NUMBER = ({INTEGER_VALUE}|{DECIMAL_DIGITS}){EXPONENT}?
%% Reject numeric prefixes followed by identifier characters. This enforces the
%% local digit-starting identifier restriction and rejects unsupported suffixes.
%% NUMBER precedes BAD_NUMBER so valid exponents win equal-length matches.
%% https://github.com/apache/doris/blob/3390475e02a359380b98cc99c965b65f77827054/fe/fe-core/src/main/antlr4/org/apache/doris/nereids/DorisLexer.g4#L29-L48
BAD_NUMBER = ({INTEGER_VALUE}|{DECIMAL_DIGITS}){EXPONENT}?{LETTER}({LETTER}|{DIGIT})*

Rules.

%% WS: skip rather than emit on the hidden channel.
%% https://github.com/apache/doris/blob/3390475e02a359380b98cc99c965b65f77827054/fe/fe-core/src/main/antlr4/org/apache/doris/nereids/DorisLexer.g4#L682-L684
{WS} : skip_token.
%% Reject SIMPLE_COMMENT and BRACKETED_COMMENT prefixes instead of consuming comments.
%% https://github.com/apache/doris/blob/3390475e02a359380b98cc99c965b65f77827054/fe/fe-core/src/main/antlr4/org/apache/doris/nereids/DorisLexer.g4#L670-L676
-- : {error, comments_not_allowed}.
\/\* : {error, comments_not_allowed}.
%% Emit local tokens using the definitions and source references above.
%% NUMBER merges INTEGER_VALUE, EXPONENT_VALUE and DECIMAL_VALUE.
%% BAD_NUMBER rejects unsupported boundaries; IDENTIFIER classifies keywords below.
{PLACEHOLDER} : placeholder(TokenChars, TokenLine).
{STRING_LITERAL} : {token, {string, TokenLine, to_binary(TokenChars)}}.
{BACKQUOTED_IDENTIFIER} : {token, {bt_identifier, TokenLine, to_binary(TokenChars)}}.
{NUMBER} : {token, {number, TokenLine, to_binary(TokenChars)}}.
{BAD_NUMBER} : {error, unsupported_number}.
{IDENTIFIER} : identifier(TokenChars, TokenLine).
%% Punctuation.
%% https://github.com/apache/doris/blob/3390475e02a359380b98cc99c965b65f77827054/fe/fe-core/src/main/antlr4/org/apache/doris/nereids/DorisLexer.g4#L61-L66
; : {token, {';', TokenLine}}.
\( : {token, {'(', TokenLine}}.
\) : {token, {')', TokenLine}}.
, : {token, {',', TokenLine}}.
\. : {token, {'.', TokenLine}}.
%% Comparison and arithmetic operators.
%% https://github.com/apache/doris/blob/3390475e02a359380b98cc99c965b65f77827054/fe/fe-core/src/main/antlr4/org/apache/doris/nereids/DorisLexer.g4#L571-L583
(=|==) : {token, {'=', TokenLine}}.
<=> : {token, {'<=>', TokenLine}}.
(<>|!=) : {token, {'<>', TokenLine}}.
< : {token, {'<', TokenLine}}.
(<=|!>) : {token, {'<=', TokenLine}}.
> : {token, {'>', TokenLine}}.
(>=|!<) : {token, {'>=', TokenLine}}.
\+ : {token, {'+', TokenLine}}.
- : {token, {'-', TokenLine}}.
\* : {token, {'*', TokenLine}}.
/ : {token, {'/', TokenLine}}.
\% : {token, {'%', TokenLine}}.
%% Reject unsupported input rather than emit UNRECOGNIZED; Leex dot excludes LF.
%% https://github.com/apache/doris/blob/3390475e02a359380b98cc99c965b65f77827054/fe/fe-core/src/main/antlr4/org/apache/doris/nereids/DorisLexer.g4#L686-L691
. : {error, {unsupported_token, to_binary(TokenChars)}}.

Erlang code.

to_binary(Chars) -> list_to_binary(Chars).

placeholder(Chars, Line) ->
    case emqx_sql_plan:parse_placeholder(to_binary(Chars)) of
        {ok, Var} -> {token, {placeholder, Line, Var}};
        {error, Reason} -> {error, Reason}
    end.

identifier(Chars, Line) ->
    Bin = to_binary(Chars),
    Lower = list_to_binary(string:lowercase(Chars)),
    case Lower of
        %% Selected Doris keywords; other names remain identifiers.
        %% https://github.com/apache/doris/blob/3390475e02a359380b98cc99c965b65f77827054/fe/fe-core/src/main/antlr4/org/apache/doris/nereids/DorisLexer.g4#L93-L558
        <<"insert">> -> {token, {insert, Line}};
        <<"into">> -> {token, {into, Line}};
        <<"values">> -> {token, {values, Line}};
        <<"null">> -> {token, {null, Line}};
        <<"true">> -> {token, {true, Line}};
        <<"false">> -> {token, {false, Line}};
        <<"default">> -> {token, {default, Line}};
        <<"current_date">> -> {token, {builtin_expression, Line, <<"CURRENT_DATE">>}};
        <<"current_time">> -> {token, {builtin_expression, Line, <<"CURRENT_TIME">>}};
        <<"current_timestamp">> -> {token, {builtin_expression, Line, <<"CURRENT_TIMESTAMP">>}};
        <<"localtime">> -> {token, {builtin_expression, Line, <<"LOCALTIME">>}};
        <<"localtimestamp">> -> {token, {builtin_expression, Line, <<"LOCALTIMESTAMP">>}};
        <<"current_user">> -> {token, {builtin_expression, Line, <<"CURRENT_USER">>}};
        <<"session_user">> -> {token, {builtin_expression, Line, <<"SESSION_USER">>}};
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
