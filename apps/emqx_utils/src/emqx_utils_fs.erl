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

-module(emqx_utils_fs).

-include_lib("kernel/include/file.hrl").

-export([traverse_dir/3]).
-export([read_info/1]).
-export([find_relpath/2]).
-export([canonicalize/1]).

-type fileinfo() :: #file_info{}.
-type foldfun(Acc) ::
    fun((_Filepath :: file:name(), fileinfo() | {error, file:posix()}, Acc) -> Acc).

%% @doc Traverse a directory recursively and apply a fold function to each file.
%%
%% This is a safer version of `filelib:fold_files/5` which does not follow symlinks
%% and reports errors to the fold function, giving the user more control over the
%% traversal.
%% It's not an error if `Dirpath` is not a directory, in which case the fold function
%% will be called once with the file info of `Dirpath`.
-spec traverse_dir(foldfun(Acc), Acc, _Dirpath :: file:name()) ->
    Acc.
traverse_dir(FoldFun, Acc, Dirpath) ->
    traverse_dir(FoldFun, Acc, Dirpath, read_info(Dirpath)).

traverse_dir(FoldFun, AccIn, DirPath, {ok, #file_info{type = directory}}) ->
    case file:list_dir(DirPath) of
        {ok, Filenames} ->
            lists:foldl(
                fun(Filename, Acc) ->
                    AbsPath = filename:join(DirPath, Filename),
                    traverse_dir(FoldFun, Acc, AbsPath)
                end,
                AccIn,
                Filenames
            );
        {error, Reason} ->
            FoldFun(DirPath, {error, Reason}, AccIn)
    end;
traverse_dir(FoldFun, Acc, AbsPath, {ok, Info}) ->
    FoldFun(AbsPath, Info, Acc);
traverse_dir(FoldFun, Acc, AbsPath, {error, Reason}) ->
    FoldFun(AbsPath, {error, Reason}, Acc).

-spec read_info(file:name()) ->
    {ok, fileinfo()} | {error, file:posix() | badarg}.
read_info(AbsPath) ->
    file:read_link_info(AbsPath, [{time, posix}, raw]).

-spec find_relpath(file:name(), file:name()) ->
    file:name().
find_relpath(Path, RelativeTo) ->
    case
        filename:pathtype(Path) =:= filename:pathtype(RelativeTo) andalso
            drop_path_prefix(filename:split(Path), filename:split(RelativeTo))
    of
        false ->
            Path;
        [] ->
            ".";
        RelativePath ->
            filename:join(RelativePath)
    end.

drop_path_prefix([Name | T1], [Name | T2]) ->
    drop_path_prefix(T1, T2);
drop_path_prefix(Path, []) ->
    Path;
drop_path_prefix(_Path, _To) ->
    false.

%% @doc Canonicalize a file path.
%% Removes stray slashes and converts to a string.
-spec canonicalize(file:name()) ->
    string().
canonicalize(Filename) ->
    case filename:split(str(Filename)) of
        Components = [_ | _] ->
            filename:join(Components);
        [] ->
            ""
    end.

str(Value) ->
    case unicode:characters_to_list(Value, unicode) of
        Str when is_list(Str) ->
            Str;
        {error, _, _} ->
            erlang:error(badarg, [Value])
    end.
