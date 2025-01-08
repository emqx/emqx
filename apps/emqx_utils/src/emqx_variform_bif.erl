%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% Predefined functions for variform expressions.
-module(emqx_variform_bif).

%% String Funcs
-export([
    lower/1,
    ltrim/1,
    ltrim/2,
    reverse/1,
    rtrim/1,
    rtrim/2,
    rm_prefix/2,
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
    regex_extract/2,
    ascii/1,
    find/2,
    find/3,
    join_to_string/1,
    join_to_string/2,
    unescape/1,
    any_to_str/1,
    is_empty_val/1,
    'not'/1
]).

%% Array functions
-export([nth/2]).

%% Random functions
-export([rand_str/1, rand_int/1]).

%% Schema-less encod/decode
-export([
    bin2hexstr/1,
    hexstr2bin/1,
    int2hexstr/1,
    base64_encode/1,
    base64_decode/1
]).

%% Hash functions
-export([hash/2, hash_to_range/3, map_to_range/3]).

%% String compare functions
-export([str_comp/2, str_eq/2, str_neq/2, str_lt/2, str_lte/2, str_gt/2, str_gte/2]).

%% Number compare functions
-export([num_comp/2, num_eq/2, num_neq/2, num_lt/2, num_lte/2, num_gt/2, num_gte/2]).

%% System
-export([getenv/1]).

-define(CACHE(Key), {?MODULE, Key}).
-define(ENV_CACHE(Env), ?CACHE({env, Env})).

%%------------------------------------------------------------------------------
%% String Funcs
%%------------------------------------------------------------------------------

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

%% @doc Remove the prefix of a string if there is a match.
%% The original stirng is returned if there is no match.
rm_prefix(S, Prefix) ->
    Size = size(Prefix),
    case S of
        <<P:Size/binary, Rem/binary>> when P =:= Prefix ->
            Rem;
        _ ->
            S
    end.

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
    concat([S1, S2]).

%% @doc Concatenate a list of strings.
%% NOTE: it converts non-string elements to Erlang term literals for backward compatibility
concat(List) ->
    unicode:characters_to_binary(lists:map(fun any_to_str/1, List), unicode).

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

%% @doc Non-global search for specified regular expression pattern in the given string.
%% If matches are found, it returns a list of all captured groups from these matches.
%% If no matches are found or there are no groups captured, it returns an empty list.
%% This function can be used to extract parts of a string based on a regular expression,
%% excluding the complete match itself.
%%
%% Examples:
%%  ("Number: 12345", "(\\d+)") -> [<<"12345">>]
%%  ("Hello, world!", "(\\w+).*\s(\\w+)") -> [<<"Hello">>, <<"world">>]
%%  ("No numbers here!", "(\\d+)") -> []
%%  ("Date: 2021-05-20", "(\\d{4})-(\\d{2})-(\\d{2})") -> [<<"2021">>, <<"05">>, <<"20">>]
-spec regex_extract(string() | binary(), string() | binary()) -> [binary()].
regex_extract(Str, Regexp) ->
    case re:run(Str, Regexp, [{capture, all_but_first, binary}]) of
        {match, CapturedGroups} ->
            CapturedGroups;
        _ ->
            []
    end.

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
    iolist_to_binary(lists:join(Sep, [any_to_str(Item) || Item <- List])).

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

nth(N, List) when (is_list(N) orelse is_binary(N)) andalso is_list(List) ->
    try binary_to_integer(iolist_to_binary(N)) of
        N1 ->
            nth(N1, List)
    catch
        _:_ ->
            throw(#{reason => invalid_argument, func => nth, index => N})
    end;
nth(N, List) when is_integer(N) andalso is_list(List) ->
    case length(List) of
        L when L < N -> <<>>;
        _ -> lists:nth(N, List)
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
    %% Terminal bell
    unescape_string(Rest, [7 | Acc]);
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

any_to_str(Data) ->
    emqx_utils_conv:bin(Data).

%%------------------------------------------------------------------------------
%% Random functions
%%------------------------------------------------------------------------------

%% @doc Make a random string with urlsafe-base62 charset.
rand_str(Length) when is_integer(Length) andalso Length > 0 ->
    emqx_utils:rand_id(Length);
rand_str(_) ->
    throw(#{reason => badarg, function => ?FUNCTION_NAME}).

%% @doc Make a random integer in the range `[1, N]`.
rand_int(N) when is_integer(N) andalso N >= 1 ->
    rand:uniform(N);
rand_int(N) ->
    throw(#{reason => badarg, function => ?FUNCTION_NAME, expected => "positive integer", got => N}).

%%------------------------------------------------------------------------------
%% Data encoding
%%------------------------------------------------------------------------------

%% @doc Encode an integer to hex string. e.g. 15 as 'f'
int2hexstr(Int) ->
    erlang:integer_to_binary(Int, 16).

%% @doc Encode bytes in hex string format.
bin2hexstr(Bin) when is_binary(Bin) ->
    emqx_utils:bin_to_hexstr(Bin, upper);
%% If Bin is a bitstring which is not divisible by 8, we pad it and then do the
%% conversion
bin2hexstr(Bin) when is_bitstring(Bin), (8 - (bit_size(Bin) rem 8)) >= 4 ->
    PadSize = 8 - (bit_size(Bin) rem 8),
    Padding = <<0:PadSize>>,
    BinToConvert = <<Padding/bitstring, Bin/bitstring>>,
    <<_FirstByte:8, HexStr/binary>> = emqx_utils:bin_to_hexstr(BinToConvert, upper),
    HexStr;
bin2hexstr(Bin) when is_bitstring(Bin) ->
    PadSize = 8 - (bit_size(Bin) rem 8),
    Padding = <<0:PadSize>>,
    BinToConvert = <<Padding/bitstring, Bin/bitstring>>,
    emqx_utils:bin_to_hexstr(BinToConvert, upper).

%% @doc Decode hex string into its original bytes.
hexstr2bin(Str) when is_binary(Str) ->
    emqx_utils:hexstr_to_bin(Str).

%% @doc Encode any bytes to base64.
base64_encode(Bin) ->
    base64:encode(Bin).

%% @doc Decode base64 encoded string.
base64_decode(Bin) ->
    base64:decode(Bin).

%%------------------------------------------------------------------------------
%% Hash functions
%%------------------------------------------------------------------------------

%% @doc Hash with all available algorithm provided by crypto module.
%% Return hex format string.
%% - md4 | md5
%% - sha (sha1)
%% - sha224 | sha256 | sha384 | sha512
%% - sha3_224 | sha3_256 | sha3_384 | sha3_512
%% - shake128 | shake256
%% - blake2b | blake2s
hash(<<"sha1">>, Bin) ->
    hash(sha, Bin);
hash(Algorithm, Bin) when is_binary(Algorithm) ->
    Type =
        try
            binary_to_existing_atom(Algorithm)
        catch
            _:_ ->
                throw(#{
                    reason => unknown_hash_algorithm,
                    algorithm => Algorithm
                })
        end,
    hash(Type, Bin);
hash(Type, Bin) when is_atom(Type) ->
    %% lower is for backward compatibility
    emqx_utils:bin_to_hexstr(crypto:hash(Type, Bin), lower).

%% @doc Hash binary data to an integer within a specified range [Min, Max]
hash_to_range(Bin, Min, Max) when
    is_binary(Bin) andalso
        size(Bin) > 0 andalso
        is_integer(Min) andalso
        is_integer(Max) andalso
        Min =< Max
->
    Hash = hash(sha256, Bin),
    HashNum = binary_to_integer(Hash, 16),
    map_to_range(HashNum, Min, Max);
hash_to_range(_, _, _) ->
    throw(#{reason => badarg, function => ?FUNCTION_NAME}).

map_to_range(Bin, Min, Max) when is_binary(Bin) andalso size(Bin) > 0 ->
    HashNum = binary:decode_unsigned(Bin),
    map_to_range(HashNum, Min, Max);
map_to_range(Int, Min, Max) when
    is_integer(Int) andalso
        is_integer(Min) andalso
        is_integer(Max) andalso
        Min =< Max
->
    Range = Max - Min + 1,
    Min + (Int rem Range);
map_to_range(_, _, _) ->
    throw(#{reason => badarg, function => ?FUNCTION_NAME}).

compare(A, A) -> eq;
compare(A, B) when A < B -> lt;
compare(_A, _B) -> gt.

%% @doc Compare two strings, returns
%% - 'eq' if they are the same.
%% - 'lt' if arg-1 is ordered before arg-2
%% - `gt` if arg-1 is ordered after arg-2
str_comp(A0, B0) ->
    A = any_to_str(A0),
    B = any_to_str(B0),
    compare(A, B).

%% @doc Return 'true' if two strings are the same, otherwise 'false'.
str_eq(A, B) -> eq =:= str_comp(A, B).

%% @doc Return 'true' if two string are not the same.
str_neq(A, B) -> eq =/= str_comp(A, B).

%% @doc Return 'true' if arg-1 is ordered before arg-2, otherwise 'false'.
str_lt(A, B) -> lt =:= str_comp(A, B).

%% @doc Return 'true' if arg-1 is ordered after arg-2, otherwise 'false'.
str_gt(A, B) -> gt =:= str_comp(A, B).

%% @doc Return 'true' if arg-1 is not ordered after arg-2, otherwise 'false'.
str_lte(A, B) ->
    R = str_comp(A, B),
    R =:= lt orelse R =:= eq.

%% @doc Return 'true' if arg-1 is not ordered bfore arg-2, otherwise 'false'.
str_gte(A, B) ->
    R = str_comp(A, B),
    R =:= gt orelse R =:= eq.

num_comp(A, B) when is_number(A) andalso is_number(B) ->
    compare(A, B).

%% @doc Return 'true' if two numbers are the same, otherwise 'false'.
num_eq(A, B) -> eq =:= num_comp(A, B).

%% @doc Return 'true' if two numbers are not the same, otherwise 'false'.
num_neq(A, B) -> eq =/= num_comp(A, B).

%% @doc Return 'true' if arg-1 is ordered before arg-2, otherwise 'false'.
num_lt(A, B) -> lt =:= num_comp(A, B).

%% @doc Return 'true' if arg-1 is ordered after arg-2, otherwise 'false'.
num_gt(A, B) -> gt =:= num_comp(A, B).

%% @doc Return 'true' if arg-1 is not ordered after arg-2, otherwise 'false'.
num_lte(A, B) ->
    R = num_comp(A, B),
    R =:= lt orelse R =:= eq.

%% @doc Return 'true' if arg-1 is not ordered bfore arg-2, otherwise 'false'.
num_gte(A, B) ->
    R = num_comp(A, B),
    R =:= gt orelse R =:= eq.

%% @doc Return 'true' if the argument is `undefined`, `null` or empty string, or empty array.
is_empty_val(undefined) -> true;
is_empty_val(null) -> true;
is_empty_val(<<>>) -> true;
is_empty_val([]) -> true;
is_empty_val(_) -> false.

%% @doc The 'not' operation for boolean values and strings.
'not'(true) -> false;
'not'(false) -> true;
'not'(<<"true">>) -> <<"false">>;
'not'(<<"false">>) -> <<"true">>.

%%------------------------------------------------------------------------------
%% System
%%------------------------------------------------------------------------------
getenv(Bin) when is_binary(Bin) ->
    EnvKey = ?ENV_CACHE(Bin),
    case persistent_term:get(EnvKey, undefined) of
        undefined ->
            Name = "EMQXVAR_" ++ erlang:binary_to_list(Bin),
            Result =
                case os:getenv(Name) of
                    false ->
                        <<>>;
                    Value ->
                        erlang:list_to_binary(Value)
                end,
            persistent_term:put(EnvKey, Result),
            Result;
        Result ->
            Result
    end.
