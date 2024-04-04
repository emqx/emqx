Definitions.
%% Define regular expressions for tokens
IDENTIFIER  = [a-zA-Z][a-zA-Z0-9_.]*
SQ_STRING   = \'[^\']*\'
DQ_STRING   = \"[^\"]*\"
NUMBER      = [+-]?(\\d+\\.\\d+|[0-9]+)
LPAREN      = \(
RPAREN      = \)
LBRACKET    = \[
RBRACKET    = \]
COMMA       = ,
WHITESPACE  = [\s\t\n]+

Rules.
%% Match function names, variable names (with ${}), strings, numbers, and structural characters
{WHITESPACE} : skip_token.
{IDENTIFIER} : {token, {identifier, TokenLine, TokenChars}}.
{SQ_STRING}  : {token, {string, TokenLine, unquote(TokenChars, $')}}.
{DQ_STRING}  : {token, {string, TokenLine, unquote(TokenChars, $")}}.
{NUMBER}     : {token, {number, TokenLine, TokenChars}}.
{LPAREN}     : {token, {'(', TokenLine}}.
{RPAREN}     : {token, {')', TokenLine}}.
{LBRACKET}   : {token, {'[', TokenLine}}.
{RBRACKET}   : {token, {']', TokenLine}}.
{COMMA}      : {token, {',', TokenLine}}.

Erlang code.

unquote(String, Char) ->
    string:trim(String, both, [Char]).
