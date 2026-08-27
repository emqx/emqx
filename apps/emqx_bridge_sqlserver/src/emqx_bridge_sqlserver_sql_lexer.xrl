%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% This lexer recognizes the restricted SQL Server INSERT template token set.
%% Leex selects the longest prefix. Rule order resolves equal-length matches.

Definitions.

%% SqlScriptDOM defines SQL whitespace separately from identifier characters:
%% https://github.com/microsoft/SqlScriptDOM/blob/01aa17bfa32f25f1b1084b72c2e6a1a92b44633a/SqlScriptDom/Parser/TSql/TSql160.g#L33997-L34107
WS              = [\s\t\r\n\v\f]+
%% Use the ASCII subset of SQL Server regular identifier categories:
%% https://learn.microsoft.com/en-us/sql/relational-databases/databases/database-identifiers?view=sql-server-ver16#rules-for-regular-identifiers
ID_START        = [A-Za-z_@\#]
ID_CONTINUE     = [A-Za-z0-9_@\#\$]
IDENTIFIER      = {ID_START}{ID_CONTINUE}*
%% emqx_template placeholder envelope; parse_placeholder/1 applies local validation:
%% See emqx_template:parse/1 in apps/emqx_utils/src/emqx_template.erl.
PLACEHOLDER     = \$\{[A-Za-z0-9_.]*\}
%% Accept only complete forms. ScriptDOM also tokenizes incomplete forms:
%% https://github.com/microsoft/SqlScriptDOM/blob/01aa17bfa32f25f1b1084b72c2e6a1a92b44633a/SqlScriptDom/Parser/TSql/TSql160.g#L34147-L34198
NUMBER          = (([0-9]+(\.[0-9]*)?)|(\.[0-9]+))([eE][+-]?[0-9]+)?
HEX             = 0[xX][0-9A-Fa-f]+
SQ_STRING       = '([^']|'')*'
%% '
NQ_STRING       = [nN]'([^']|'')*'
%% '
%% SqlScriptDOM scans doubled double quotes and brackets as one token:
%% https://github.com/microsoft/SqlScriptDOM/blob/01aa17bfa32f25f1b1084b72c2e6a1a92b44633a/SqlScriptDom/Parser/TSql/TSql160.g#L34308-L34342
DQ_IDENTIFIER   = "([^"]|"")*"
%% "
BR_IDENTIFIER   = \[([^\]]|\]\])+\]

Rules.

{WS}            : skip_token.
%% Reject SQL Server comment openers instead of implementing complete comments:
%% https://github.com/microsoft/SqlScriptDOM/blob/01aa17bfa32f25f1b1084b72c2e6a1a92b44633a/SqlScriptDom/Parser/TSql/TSql160.g#L34351-L34392
--               : {error, {comments_not_allowed, TokenLine}}.
\/\*             : {error, {comments_not_allowed, TokenLine}}.
{PLACEHOLDER}   : placeholder(TokenChars, TokenLine).
{NQ_STRING}     : {token, {nq_string, TokenLine, to_binary(TokenChars)}}.
{SQ_STRING}     : {token, {sq_string, TokenLine, to_binary(TokenChars)}}.
{DQ_IDENTIFIER} : {token, {dq_identifier, TokenLine, to_binary(TokenChars)}}.
{BR_IDENTIFIER} : {token, {br_identifier, TokenLine, to_binary(TokenChars)}}.
{HEX}           : {token, {hex, TokenLine, to_binary(TokenChars)}}.
{NUMBER}        : {token, {number, TokenLine, to_binary(TokenChars)}}.
{IDENTIFIER}    : identifier(TokenChars, TokenLine).
%% TSql160 defines these punctuation and arithmetic tokens directly.
%% Its Number rule decides whether a dot starts a decimal:
%% https://github.com/microsoft/SqlScriptDOM/blob/01aa17bfa32f25f1b1084b72c2e6a1a92b44633a/SqlScriptDom/Parser/TSql/TSql160.g#L33934-L33971
%% https://github.com/microsoft/SqlScriptDOM/blob/01aa17bfa32f25f1b1084b72c2e6a1a92b44633a/SqlScriptDom/Parser/TSql/TSql160.g#L34147-L34193
\(              : {token, {'(', TokenLine}}.
\)              : {token, {')', TokenLine}}.
,               : {token, {',', TokenLine}}.
\.              : {token, {'.', TokenLine}}.
;               : {token, {';', TokenLine}}.
>=              : {token, {'>=', TokenLine}}.
<=              : {token, {'<=', TokenLine}}.
<>              : {token, {'<>', TokenLine}}.
!=              : {token, {'!=', TokenLine}}.
=               : {token, {'=', TokenLine}}.
>               : {token, {'>', TokenLine}}.
<               : {token, {'<', TokenLine}}.
\+              : {token, {'+', TokenLine}}.
-               : {token, {'-', TokenLine}}.
\*              : {token, {'*', TokenLine}}.
\/              : {token, {'/', TokenLine}}.
\%              : {token, {'%', TokenLine}}.
'               : {error, {unterminated_string, TokenLine}}.
%% '
"               : {error, {unterminated_quoted_identifier, TokenLine}}.
%% "
\[              : {error, {unterminated_bracket_identifier, TokenLine}}.
\$              : {error, {invalid_placeholder, TokenLine}}.
.               : {error, {unsupported_token, TokenLine, to_binary(TokenChars)}}.

Erlang code.

to_binary(Chars) ->
    list_to_binary(Chars).

placeholder(Chars, Line) ->
    Bin = to_binary(Chars),
    case emqx_bridge_sqlserver_sql:parse_placeholder(Bin) of
        {ok, Placeholder} -> {token, {placeholder, Line, Placeholder}};
        {error, _} -> {error, {invalid_placeholder, Line, Bin}}
    end.

identifier(Chars, Line) ->
    Bin = to_binary(Chars),
    case string:lowercase(Bin) of
        <<"insert">> -> {token, {insert, Line}};
        <<"into">> -> {token, {into, Line}};
        <<"values">> -> {token, {values, Line}};
        <<"null">> -> {token, {null, Line}};
        <<"default">> -> {token, {default, Line}};
        <<"current_timestamp">> -> {token, {current_timestamp, Line}};
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
