Definitions.

Control = [()&|!=~><:*]
White = [\s\t\n\r]+
NonString = [^()&|!=~><:*\s\t\n\r]
String = {NonString}+

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
\: : {token, {colon, TokenLine}}.
dn : {token, {dn, TokenLine}}.
{White} : skip_token.
{String} : {token, {string, TokenLine, TokenChars}}.
%% Leex will hang if a composite operation is missing a character
{Control} : {error, lists:flatten(io_lib:format("Unexpected Tokens:~ts", [TokenChars]))}.

Erlang code.

%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
