%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include_lib("kernel/include/file.hrl").

-export([is_filename_safe/1]).
-export([escape_filename/1]).
-export([unescape_filename/1]).

-export([read_decode_file/2]).
-export([read_info/1]).
-export([list_dir/1]).

-export([fold/4]).

-export([mk_temp_filename/1]).

-type foldfun(Acc) ::
    fun(
        (
            _Filepath :: file:name(),
            _Info :: file:file_info() | {error, file:posix()},
            _Stack :: emqx_ft_fs_iterator:pathstack(),
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

-spec read_info(file:name_all()) ->
    {ok, file:file_info()} | {error, file:posix() | badarg}.
read_info(AbsPath) ->
    % NOTE
    % Be aware that this function is occasionally mocked in `emqx_ft_fs_util_SUITE`.
    file:read_link_info(AbsPath, [{time, posix}, raw]).

-spec list_dir(file:name_all()) ->
    {ok, [file:name()]} | {error, file:posix() | badarg}.
list_dir(AbsPath) ->
    case ?MODULE:read_info(AbsPath) of
        {ok, #file_info{type = directory}} ->
            file:list_dir(AbsPath);
        {ok, #file_info{}} ->
            {error, enotdir};
        {error, Reason} ->
            {error, Reason}
    end.

-spec fold(foldfun(Acc), Acc, _Root :: file:name(), emqx_ft_fs_iterator:glob()) ->
    Acc.
fold(FoldFun, Acc, Root, Glob) ->
    fold(FoldFun, Acc, emqx_ft_fs_iterator:new(Root, Glob)).

fold(FoldFun, Acc, It) ->
    case emqx_ft_fs_iterator:next(It) of
        {{node, _Path, {error, enotdir}, _PathStack}, ItNext} ->
            fold(FoldFun, Acc, ItNext);
        {{_Type, Path, Info, PathStack}, ItNext} ->
            AccNext = FoldFun(Path, Info, PathStack, Acc),
            fold(FoldFun, AccNext, ItNext);
        none ->
            Acc
    end.

-spec mk_temp_filename(file:filename()) ->
    file:filename().
mk_temp_filename(Filename) ->
    % NOTE
    % Using only the first 200 characters of the filename to avoid making filenames
    % exceeding 255 bytes in UTF-8. It's actually too conservative, `Suffix` can be
    % at most 16 bytes.
    Unique = erlang:unique_integer([positive]),
    Suffix = binary:encode_hex(<<Unique:64>>),
    mk_filename([string:slice(Filename, 0, 200), ".", Suffix]).

mk_filename(Comps) ->
    lists:append(lists:map(fun mk_filename_component/1, Comps)).

mk_filename_component(A) when is_atom(A) ->
    atom_to_list(A);
mk_filename_component(B) when is_binary(B) ->
    unicode:characters_to_list(B);
mk_filename_component(S) when is_list(S) ->
    S.
