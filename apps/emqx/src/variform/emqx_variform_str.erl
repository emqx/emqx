%%--------------------------------------------------------------------
%% Copyright (c) 2020-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

%% Predefined functions string templating
-module(emqx_variform_str).

%% String Funcs
-export([
    coalesce/1,
    coalesce/2,
    lower/1,
    ltrim/1,
    ltrim/2,
    reverse/1,
    rtrim/1,
    rtrim/2,
    strlen/1,
    substr/2,
    substr/3,
    trim/1,
    trim/2,
    upper/1,
    split/2,
    split/3,
    concat/1,
    concat/2,
    tokens/2,
    tokens/3,
    sprintf_s/2,
    pad/2,
    pad/3,
    pad/4,
    replace/3,
    replace/4,
    regex_match/2,
    regex_replace/3,
    ascii/1,
    find/2,
    find/3,
    join_to_string/1,
    join_to_string/2,
    unescape/1
]).

-define(IS_EMPTY(X), (X =:= <<>> orelse X =:= "" orelse X =:= undefined)).

%%------------------------------------------------------------------------------
%% String Funcs
%%------------------------------------------------------------------------------

%% @doc Return the first non-empty string
coalesce(A, B) when ?IS_EMPTY(A) andalso ?IS_EMPTY(B) ->
    <<>>;
coalesce(A, _) when is_binary(A) ->
    A;
coalesce(_, B) ->
    B.

%% @doc Return the first non-empty string
coalesce([]) ->
    <<>>;
coalesce([H | T]) ->
    coalesce(H, coalesce(T)).

lower(S) when is_binary(S) ->
    string:lowercase(S).

ltrim(S) when is_binary(S) ->
    string:trim(S, leading).

ltrim(S, Chars) ->
    string:trim(S, leading, Chars).

reverse(S) when is_binary(S) ->
    iolist_to_binary(string:reverse(S)).

rtrim(S) when is_binary(S) ->
    string:trim(S, trailing).

rtrim(S, Chars) when is_binary(S) ->
    string:trim(S, trailing, Chars).

strlen(S) when is_binary(S) ->
    string:length(S).

substr(S, Start) when is_binary(S), is_integer(Start) ->
    string:slice(S, Start).

substr(S, Start, Length) when
    is_binary(S),
    is_integer(Start),
    is_integer(Length)
->
    string:slice(S, Start, Length).

trim(S) when is_binary(S) ->
    string:trim(S).

trim(S, Chars) when is_binary(S) ->
    string:trim(S, both, Chars).

upper(S) when is_binary(S) ->
    string:uppercase(S).

split(S, P) when is_binary(S), is_binary(P) ->
    [R || R <- string:split(S, P, all), R =/= <<>> andalso R =/= ""].

split(S, P, <<"notrim">>) ->
    string:split(S, P, all);
split(S, P, <<"leading_notrim">>) ->
    string:split(S, P, leading);
split(S, P, <<"leading">>) when is_binary(S), is_binary(P) ->
    [R || R <- string:split(S, P, leading), R =/= <<>> andalso R =/= ""];
split(S, P, <<"trailing_notrim">>) ->
    string:split(S, P, trailing);
split(S, P, <<"trailing">>) when is_binary(S), is_binary(P) ->
    [R || R <- string:split(S, P, trailing), R =/= <<>> andalso R =/= ""].

tokens(S, Separators) ->
    [list_to_binary(R) || R <- string:lexemes(binary_to_list(S), binary_to_list(Separators))].

tokens(S, Separators, <<"nocrlf">>) ->
    [
        list_to_binary(R)
     || R <- string:lexemes(binary_to_list(S), binary_to_list(Separators) ++ [$\r, $\n, [$\r, $\n]])
    ].

%% implicit convert args to strings, and then do concatenation
concat(S1, S2) ->
    concat([S1, S2], unicode).

concat(List) ->
    unicode:characters_to_binary(lists:map(fun str/1, List), unicode).

sprintf_s(Format, Args) when is_list(Args) ->
    erlang:iolist_to_binary(io_lib:format(binary_to_list(Format), Args)).

pad(S, Len) when is_binary(S), is_integer(Len) ->
    iolist_to_binary(string:pad(S, Len, trailing)).

pad(S, Len, <<"trailing">>) when is_binary(S), is_integer(Len) ->
    iolist_to_binary(string:pad(S, Len, trailing));
pad(S, Len, <<"both">>) when is_binary(S), is_integer(Len) ->
    iolist_to_binary(string:pad(S, Len, both));
pad(S, Len, <<"leading">>) when is_binary(S), is_integer(Len) ->
    iolist_to_binary(string:pad(S, Len, leading)).

pad(S, Len, <<"trailing">>, Char) when is_binary(S), is_integer(Len), is_binary(Char) ->
    Chars = unicode:characters_to_list(Char, utf8),
    iolist_to_binary(string:pad(S, Len, trailing, Chars));
pad(S, Len, <<"both">>, Char) when is_binary(S), is_integer(Len), is_binary(Char) ->
    Chars = unicode:characters_to_list(Char, utf8),
    iolist_to_binary(string:pad(S, Len, both, Chars));
pad(S, Len, <<"leading">>, Char) when is_binary(S), is_integer(Len), is_binary(Char) ->
    Chars = unicode:characters_to_list(Char, utf8),
    iolist_to_binary(string:pad(S, Len, leading, Chars)).

replace(SrcStr, P, RepStr) when is_binary(SrcStr), is_binary(P), is_binary(RepStr) ->
    iolist_to_binary(string:replace(SrcStr, P, RepStr, all)).

replace(SrcStr, P, RepStr, <<"all">>) when is_binary(SrcStr), is_binary(P), is_binary(RepStr) ->
    iolist_to_binary(string:replace(SrcStr, P, RepStr, all));
replace(SrcStr, P, RepStr, <<"trailing">>) when
    is_binary(SrcStr), is_binary(P), is_binary(RepStr)
->
    iolist_to_binary(string:replace(SrcStr, P, RepStr, trailing));
replace(SrcStr, P, RepStr, <<"leading">>) when is_binary(SrcStr), is_binary(P), is_binary(RepStr) ->
    iolist_to_binary(string:replace(SrcStr, P, RepStr, leading)).

regex_match(Str, RE) ->
    case re:run(Str, RE, [global, {capture, none}]) of
        match -> true;
        nomatch -> false
    end.

regex_replace(SrcStr, RE, RepStr) ->
    re:replace(SrcStr, RE, RepStr, [global, {return, binary}]).

ascii(Char) when is_binary(Char) ->
    [FirstC | _] = binary_to_list(Char),
    FirstC.

find(S, P) when is_binary(S), is_binary(P) ->
    find_s(S, P, leading).

find(S, P, <<"trailing">>) when is_binary(S), is_binary(P) ->
    find_s(S, P, trailing);
find(S, P, <<"leading">>) when is_binary(S), is_binary(P) ->
    find_s(S, P, leading).

find_s(S, P, Dir) ->
    case string:find(S, P, Dir) of
        nomatch -> <<"">>;
        SubStr -> SubStr
    end.

join_to_string(List) when is_list(List) ->
    join_to_string(<<", ">>, List).

join_to_string(Sep, List) when is_list(List), is_binary(Sep) ->
    iolist_to_binary(lists:join(Sep, [str(Item) || Item <- List])).

unescape(Bin) when is_binary(Bin) ->
    UnicodeList = unicode:characters_to_list(Bin, utf8),
    UnescapedUnicodeList = unescape_string(UnicodeList),
    UnescapedUTF8Bin = unicode:characters_to_binary(UnescapedUnicodeList, utf32, utf8),
    case UnescapedUTF8Bin of
        Out when is_binary(Out) ->
            Out;
        Error ->
            throw({invalid_unicode_character, Error})
    end.

unescape_string(Input) -> unescape_string(Input, []).

unescape_string([], Acc) ->
    lists:reverse(Acc);
unescape_string([$\\, $\\ | Rest], Acc) ->
    unescape_string(Rest, [$\\ | Acc]);
unescape_string([$\\, $n | Rest], Acc) ->
    unescape_string(Rest, [$\n | Acc]);
unescape_string([$\\, $t | Rest], Acc) ->
    unescape_string(Rest, [$\t | Acc]);
unescape_string([$\\, $r | Rest], Acc) ->
    unescape_string(Rest, [$\r | Acc]);
unescape_string([$\\, $b | Rest], Acc) ->
    unescape_string(Rest, [$\b | Acc]);
unescape_string([$\\, $f | Rest], Acc) ->
    unescape_string(Rest, [$\f | Acc]);
unescape_string([$\\, $v | Rest], Acc) ->
    unescape_string(Rest, [$\v | Acc]);
unescape_string([$\\, $' | Rest], Acc) ->
    unescape_string(Rest, [$\' | Acc]);
unescape_string([$\\, $" | Rest], Acc) ->
    unescape_string(Rest, [$\" | Acc]);
unescape_string([$\\, $? | Rest], Acc) ->
    unescape_string(Rest, [$\? | Acc]);
unescape_string([$\\, $a | Rest], Acc) ->
    unescape_string(Rest, [$\a | Acc]);
%% Start of HEX escape code
unescape_string([$\\, $x | [$0 | _] = HexStringStart], Acc) ->
    unescape_handle_hex_string(HexStringStart, Acc);
unescape_string([$\\, $x | [$1 | _] = HexStringStart], Acc) ->
    unescape_handle_hex_string(HexStringStart, Acc);
unescape_string([$\\, $x | [$2 | _] = HexStringStart], Acc) ->
    unescape_handle_hex_string(HexStringStart, Acc);
unescape_string([$\\, $x | [$3 | _] = HexStringStart], Acc) ->
    unescape_handle_hex_string(HexStringStart, Acc);
unescape_string([$\\, $x | [$4 | _] = HexStringStart], Acc) ->
    unescape_handle_hex_string(HexStringStart, Acc);
unescape_string([$\\, $x | [$5 | _] = HexStringStart], Acc) ->
    unescape_handle_hex_string(HexStringStart, Acc);
unescape_string([$\\, $x | [$6 | _] = HexStringStart], Acc) ->
    unescape_handle_hex_string(HexStringStart, Acc);
unescape_string([$\\, $x | [$7 | _] = HexStringStart], Acc) ->
    unescape_handle_hex_string(HexStringStart, Acc);
unescape_string([$\\, $x | [$8 | _] = HexStringStart], Acc) ->
    unescape_handle_hex_string(HexStringStart, Acc);
unescape_string([$\\, $x | [$9 | _] = HexStringStart], Acc) ->
    unescape_handle_hex_string(HexStringStart, Acc);
unescape_string([$\\, $x | [$A | _] = HexStringStart], Acc) ->
    unescape_handle_hex_string(HexStringStart, Acc);
unescape_string([$\\, $x | [$B | _] = HexStringStart], Acc) ->
    unescape_handle_hex_string(HexStringStart, Acc);
unescape_string([$\\, $x | [$C | _] = HexStringStart], Acc) ->
    unescape_handle_hex_string(HexStringStart, Acc);
unescape_string([$\\, $x | [$D | _] = HexStringStart], Acc) ->
    unescape_handle_hex_string(HexStringStart, Acc);
unescape_string([$\\, $x | [$E | _] = HexStringStart], Acc) ->
    unescape_handle_hex_string(HexStringStart, Acc);
unescape_string([$\\, $x | [$F | _] = HexStringStart], Acc) ->
    unescape_handle_hex_string(HexStringStart, Acc);
unescape_string([$\\, $x | [$a | _] = HexStringStart], Acc) ->
    unescape_handle_hex_string(HexStringStart, Acc);
unescape_string([$\\, $x | [$b | _] = HexStringStart], Acc) ->
    unescape_handle_hex_string(HexStringStart, Acc);
unescape_string([$\\, $x | [$c | _] = HexStringStart], Acc) ->
    unescape_handle_hex_string(HexStringStart, Acc);
unescape_string([$\\, $x | [$d | _] = HexStringStart], Acc) ->
    unescape_handle_hex_string(HexStringStart, Acc);
unescape_string([$\\, $x | [$e | _] = HexStringStart], Acc) ->
    unescape_handle_hex_string(HexStringStart, Acc);
unescape_string([$\\, $x | [$f | _] = HexStringStart], Acc) ->
    unescape_handle_hex_string(HexStringStart, Acc);
%% We treat all other escape sequences as not valid input to leave room for
%% extending the function to support more escape codes
unescape_string([$\\, X | _Rest], _Acc) ->
    erlang:throw({unrecognized_escape_sequence, list_to_binary([$\\, X])});
unescape_string([First | Rest], Acc) ->
    unescape_string(Rest, [First | Acc]).

unescape_handle_hex_string(HexStringStart, Acc) ->
    {RemainingString, Num} = parse_hex_string(HexStringStart),
    unescape_string(RemainingString, [Num | Acc]).

parse_hex_string(SeqStartingWithHexDigit) ->
    parse_hex_string(SeqStartingWithHexDigit, []).

parse_hex_string([], Acc) ->
    ReversedAcc = lists:reverse(Acc),
    {[], list_to_integer(ReversedAcc, 16)};
parse_hex_string([First | Rest] = String, Acc) ->
    case is_hex_digit(First) of
        true ->
            parse_hex_string(Rest, [First | Acc]);
        false ->
            ReversedAcc = lists:reverse(Acc),
            {String, list_to_integer(ReversedAcc, 16)}
    end.

is_hex_digit($0) -> true;
is_hex_digit($1) -> true;
is_hex_digit($2) -> true;
is_hex_digit($3) -> true;
is_hex_digit($4) -> true;
is_hex_digit($5) -> true;
is_hex_digit($6) -> true;
is_hex_digit($7) -> true;
is_hex_digit($8) -> true;
is_hex_digit($9) -> true;
is_hex_digit($A) -> true;
is_hex_digit($B) -> true;
is_hex_digit($C) -> true;
is_hex_digit($D) -> true;
is_hex_digit($E) -> true;
is_hex_digit($F) -> true;
is_hex_digit($a) -> true;
is_hex_digit($b) -> true;
is_hex_digit($c) -> true;
is_hex_digit($d) -> true;
is_hex_digit($e) -> true;
is_hex_digit($f) -> true;
is_hex_digit(_) -> false.

%%------------------------------------------------------------------------------
%% Data Type Conversion Funcs
%%------------------------------------------------------------------------------

str(Data) ->
    emqx_utils_conv:bin(Data).
