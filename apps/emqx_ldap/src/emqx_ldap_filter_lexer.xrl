Definitions.

Control = [()&|!=~><:*]
White = [\s\t\n\r]+
StringChars = [^()&|!=~><:*\t\n\r]
Escape = \\{Control}|\\{White}
String = ({Escape}|{StringChars})+

Rules.

\( : {token, {lparen, TokenLine}}.
\) : {token, {rparen, TokenLine}}.
\& : {token, {'and', TokenLine}}.
\| : {token, {'or', TokenLine}}.
\! : {token, {'not', TokenLine}}.
= : {token, {equal, TokenLine}}.
~= : {token, {approx, TokenLine}}.
>= : {token, {greaterOrEqual, TokenLine}}.
<= : {token, {lessOrEqual, TokenLine}}.
\* : {token, {asterisk, TokenLine}}.
\:dn : {token, {dn, TokenLine}}.
\: : {token, {colon, TokenLine}}.
{White} : skip_token.
{String} : {token, {string, TokenLine, to_string(TokenChars)}}.
%% Leex will hang if a composite operation is missing a character
{Control} : {error, lists:flatten(io_lib:format("Unexpected Tokens:~ts", [TokenChars]))}.

Erlang code.

%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
%% eldap does not support neither the '\28value\29' nor '\(value\)'
%% so after the tokenization we should remove all escape character
to_string(TokenChars) ->
    String = string:trim(TokenChars),
    lists:flatten(string:replace(String, "\\", "", all)).
