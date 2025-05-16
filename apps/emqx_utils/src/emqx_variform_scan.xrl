Definitions.
%% Define regular expressions for tokens
BOOLEAN     = true|false
IDENTIFIER  = [a-zA-Z][-a-zA-Z0-9_.]*
SQ_STRING   = \'[^\']*\'
DQ_STRING   = \"[^\"]*\"
INTEGER     = [+-]?[0-9]+
FLOAT       = [+-]?([0-9]+\.[0-9]+)(e[+-]?[0-9]+)?
LPAREN      = \(
RPAREN      = \)
LBRACKET    = \[
RBRACKET    = \]
COMMA       = ,
WHITESPACE  = [\s\t\n]+

Rules.
{WHITESPACE} : skip_token.
{BOOLEAN}    : {token, {boolean, TokenLine, list_to_atom(TokenChars)}}.
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
