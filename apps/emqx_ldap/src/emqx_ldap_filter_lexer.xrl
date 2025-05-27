%% See for more details:
%% https://www.rfc-editor.org/rfc/rfc4515
%% https://ldap.com/ldap-filters/

Definitions.

%% Char classes

%% ASCII characters without 0x00, '(', ')', '*', and '\'
StringChars = [\x01-\x27\x2B-\x5B\x5D-\x7F]
WhiteSpaceChars = [\s\t\n\r]
HexChars = [0-9a-fA-F]
AlphaChars = [a-zA-Z]
AttrChars = [a-zA-Z0-9\-]

%% Token elements
String = ({StringChars}|\\.|\\{HexChars}{2})*
NonEmptyString = ({StringChars}|\\.|\\{HexChars}{2})+
NumOid = ([0-9]+(\.[0-9]+)*)
AlphaOid = {AlphaChars}{AttrChars}*
Oid = ({NumOid}|{AlphaOid})
Attr = ({Oid})(;{AttrChars}+)*
DnAttrs = :[dD][nN]
MatchingRule = :{Oid}
WhiteSpace = {WhiteSpaceChars}+

%% Substring variants
SSWithInitial = {NonEmptyString}\*({NonEmptyString}\*)*{String}?
SSWithFinal = {String}?\*({NonEmptyString}\*)*{NonEmptyString}
SSWithAny = {String}?\*({NonEmptyString}\*)+{String}?

Rules.

\:={String} : {token, {extensible, TokenChars}}.
%% NOTE: RFC's substring syntax is:
%%
%% substring      = attr EQUALS [initial] any [final]
%% initial        = assertionvalue
%% any            = ASTERISK *(assertionvalue ASTERISK)
%% final          = assertionvalue
%% assertionvalue = valueencoding
%% valueencoding  = 0*(normal / escaped)
%%
%% It obviously allows empty 'any' parts, e.g. (a=b**c), but e.g.
%% openldap's ldapsearch does not. We also choose to not support
%% empty 'any' parts, because it is useless and confusing.
=({SSWithInitial}|{SSWithFinal}|{SSWithAny}) : {token, {substring, TokenChars}}.
={String} : {token, {equal, TokenChars}}.
\~={String} : {token, {approx, TokenChars}}.
\>={String} : {token, {greaterOrEqual, TokenChars}}.
\<={String} : {token, {lessOrEqual, TokenChars}}.
=\* : {token, {present, TokenChars}}.
\( : {token, {lparen, TokenChars}}.
\) : {token, {rparen, TokenChars}}.
\& : {token, {'and', TokenChars}}.
\| : {token, {'or', TokenChars}}.
\! : {token, {'not', TokenChars}}.
{Attr} : {token, {attr, TokenChars}}.
{DnAttrs} : {token, {dnAttrs, TokenChars}}.
{MatchingRule} : {token, {matchingRule, TokenChars}}.
%% RFC allows spaces only in the strings, but
%% openldap's ldapsearch allows spaces between some control structures.
%% Here we make the parser even a bit more permissive, not only
%% allowing filters like `(  a:dn:caseIgnoreMatch:=foo)`
%% but also `( a  :dn :caseIgnoreMatch :=foo)`
{WhiteSpace} : skip_token.
. : {token, {error, TokenChars}}.

Erlang code.

%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

