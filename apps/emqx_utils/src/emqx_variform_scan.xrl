Definitions.
%% Define regular expressions for tokens
IDENTIFIER  = [a-zA-Z][a-zA-Z0-9_.]*
SQ_STRING   = \'[^\']*\'
DQ_STRING   = \"[^\"]*\"
INTEGER     = [+-]?[0-9]+
FLOAT       = [+-]?\\d+\\.\\d+
LPAREN      = \(
RPAREN      = \)
LBRACKET    = \[
RBRACKET    = \]
COMMA       = ,
WHITESPACE  = [\s\t\n]+

Rules.
{WHITESPACE} : skip_token.
{IDENTIFIER} : {token, {identifier, TokenLine, TokenChars}}.
{SQ_STRING}  : {token, {string, TokenLine, unquote(TokenChars, $')}}.
{DQ_STRING}  : {token, {string, TokenLine, unquote(TokenChars, $")}}.
{INTEGER}    : {token, {integer, TokenLine, list_to_integer(TokenChars)}}.
{FLOAT}      : {token, {float, TokenLine, list_to_float(TokenChars)}}.
{LPAREN}     : {token, {'(', TokenLine}}.
{RPAREN}     : {token, {')', TokenLine}}.
{LBRACKET}   : {token, {'[', TokenLine}}.
{RBRACKET}   : {token, {']', TokenLine}}.
{COMMA}      : {token, {',', TokenLine}}.

Erlang code.

unquote(String, Char) ->
    string:trim(String, both, [Char]).
