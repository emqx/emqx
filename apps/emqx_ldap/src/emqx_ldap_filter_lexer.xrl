Definitions.

Control = [()&|!=~><:*,]
White = [\s\t\n\r]+
StringChars = [^()&|!=~><:*,\t\n\r\\]
Escape = \\\\|\\{Control}|\\{White}
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
, : {token, {comma, TokenLine}}.
{White} : skip_token.
{String} : {token, {string, TokenLine, to_string(TokenChars)}}.
%% Leex will hang if a composite operation is missing a character
{Control} : {error, format("Unexpected Tokens:~ts", [TokenChars])}.

Erlang code.

%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
%% eldap does not support neither the '\28value\29' nor '\(value\)'
%% so after the tokenization we should remove all escape character
to_string(TokenChars) ->
    String = string:trim(TokenChars),
    trim_escape(String).

%% because of the below situation, we can't directly use the `replace` to trim the escape character
%%trim_escape([$\\, $\\ | T]) ->
%%    [$\\ | trim_escape(T)];
trim_escape([$\\, Char | T]) ->
    [Char | trim_escape(T)];
%% the underneath is impossible to occur because it is not valid in the lexer
%% trim_escape([$\\])
trim_escape([Char | T]) ->
    [Char | trim_escape(T)];
trim_escape([]) ->
    [].

format(Fmt, Args) ->
    lists:flatten(io_lib:format(Fmt, Args)).
