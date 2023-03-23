%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_ft_fs_util).

-include_lib("snabbkaffe/include/trace.hrl").

-export([is_filename_safe/1]).
-export([escape_filename/1]).
-export([unescape_filename/1]).

-export([read_decode_file/2]).

-export([fold/4]).

-type glob() :: ['*' | globfun()].
-type globfun() ::
    fun((_Filename :: file:name()) -> boolean()).
-type foldfun(Acc) ::
    fun(
        (
            _Filepath :: file:name(),
            _Info :: file:file_info() | {error, _IoError},
            _Stack :: [file:name()],
            Acc
        ) -> Acc
    ).

-define(IS_UNSAFE(C),
    ((C) =:= $% orelse
        (C) =:= $: orelse
        (C) =:= $\\ orelse
        (C) =:= $/)
).

-define(IS_PRINTABLE(C),
    % NOTE: See `io_lib:printable_unicode_list/1`
    (((C) >= 32 andalso (C) =< 126) orelse
        ((C) >= 16#A0 andalso (C) < 16#D800) orelse
        ((C) > 16#DFFF andalso (C) < 16#FFFE) orelse
        ((C) > 16#FFFF andalso (C) =< 16#10FFFF))
).

%%

-spec is_filename_safe(file:filename_all()) -> ok | {error, atom()}.
is_filename_safe(FN) when is_binary(FN) ->
    is_filename_safe(unicode:characters_to_list(FN));
is_filename_safe("") ->
    {error, empty};
is_filename_safe(FN) when FN == "." orelse FN == ".." ->
    {error, special};
is_filename_safe(FN) ->
    verify_filename_safe(FN).

verify_filename_safe([$% | Rest]) ->
    verify_filename_safe(Rest);
verify_filename_safe([C | _]) when ?IS_UNSAFE(C) ->
    {error, unsafe};
verify_filename_safe([C | _]) when not ?IS_PRINTABLE(C) ->
    {error, nonprintable};
verify_filename_safe([_ | Rest]) ->
    verify_filename_safe(Rest);
verify_filename_safe([]) ->
    ok.

-spec escape_filename(binary()) -> file:name().
escape_filename(Name) when Name == <<".">> orelse Name == <<"..">> ->
    lists:reverse(percent_encode(Name, ""));
escape_filename(Name) ->
    escape(Name, "").

escape(<<C/utf8, Rest/binary>>, Acc) when ?IS_UNSAFE(C) ->
    escape(Rest, percent_encode(<<C/utf8>>, Acc));
escape(<<C/utf8, Rest/binary>>, Acc) when not ?IS_PRINTABLE(C) ->
    escape(Rest, percent_encode(<<C/utf8>>, Acc));
escape(<<C/utf8, Rest/binary>>, Acc) ->
    escape(Rest, [C | Acc]);
escape(<<>>, Acc) ->
    lists:reverse(Acc).

-spec unescape_filename(file:name()) -> binary().
unescape_filename(Name) ->
    unescape(Name, <<>>).

unescape([$%, A, B | Rest], Acc) ->
    unescape(Rest, percent_decode(A, B, Acc));
unescape([C | Rest], Acc) ->
    unescape(Rest, <<Acc/binary, C/utf8>>);
unescape([], Acc) ->
    Acc.

percent_encode(<<A:4, B:4, Rest/binary>>, Acc) ->
    percent_encode(Rest, [dec2hex(B), dec2hex(A), $% | Acc]);
percent_encode(<<>>, Acc) ->
    Acc.

percent_decode(A, B, Acc) ->
    <<Acc/binary, (hex2dec(A) * 16 + hex2dec(B))>>.

dec2hex(X) when (X >= 0) andalso (X =< 9) -> X + $0;
dec2hex(X) when (X >= 10) andalso (X =< 15) -> X + $A - 10.

hex2dec(X) when (X >= $0) andalso (X =< $9) -> X - $0;
hex2dec(X) when (X >= $A) andalso (X =< $F) -> X - $A + 10;
hex2dec(X) when (X >= $a) andalso (X =< $f) -> X - $a + 10;
hex2dec(_) -> error(badarg).

%%

-spec read_decode_file(file:name(), fun((binary()) -> Value)) ->
    {ok, Value} | {error, _IoError}.
read_decode_file(Filepath, DecodeFun) ->
    case file:read_file(Filepath) of
        {ok, Content} ->
            safe_decode(Content, DecodeFun);
        {error, _} = Error ->
            Error
    end.

safe_decode(Content, DecodeFun) ->
    try
        {ok, DecodeFun(Content)}
    catch
        C:E:Stacktrace ->
            ?tp(warning, "safe_decode_failed", #{
                class => C,
                exception => E,
                stacktrace => Stacktrace
            }),
            {error, corrupted}
    end.

-spec fold(foldfun(Acc), Acc, _Root :: file:name(), glob()) ->
    Acc.
fold(Fun, Acc, Root, Glob) ->
    fold(Fun, Acc, [], Root, Glob, []).

fold(Fun, AccIn, Path, Root, [Glob | Rest], Stack) when Glob == '*' orelse is_function(Glob) ->
    case file:list_dir(filename:join(Root, Path)) of
        {ok, Filenames} ->
            lists:foldl(
                fun(FN, Acc) ->
                    case matches_glob(Glob, FN) of
                        true when Path == [] ->
                            fold(Fun, Acc, FN, Root, Rest, [FN | Stack]);
                        true ->
                            fold(Fun, Acc, filename:join(Path, FN), Root, Rest, [FN | Stack]);
                        false ->
                            Acc
                    end
                end,
                AccIn,
                Filenames
            );
        {error, enotdir} ->
            AccIn;
        {error, Reason} ->
            Fun(Path, {error, Reason}, Stack, AccIn)
    end;
fold(Fun, AccIn, Filepath, Root, [], Stack) ->
    case file:read_link_info(filename:join(Root, Filepath), [{time, posix}, raw]) of
        {ok, Info} ->
            Fun(Filepath, Info, Stack, AccIn);
        {error, Reason} ->
            Fun(Filepath, {error, Reason}, Stack, AccIn)
    end.

matches_glob('*', _) ->
    true;
matches_glob(FilterFun, Filename) when is_function(FilterFun) ->
    FilterFun(Filename).
