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
