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

-module(emqx_ft_fs_iterator).

-export([new/2]).
-export([next/1]).
-export([next_leaf/1]).

-export([seek/3]).

-export([fold/3]).
-export([fold_n/4]).

-export_type([t/0]).
-export_type([glob/0]).
-export_type([pathstack/0]).

-type root() :: file:name().
-type glob() :: ['*' | globfun()].
-type globfun() ::
    fun((_Filename :: file:name()) -> boolean())
    | fun((_Filename :: file:name(), pathstack()) -> boolean()).

% A path stack is a list of path components, in reverse order.
-type pathstack() :: [file:name(), ...].

-opaque t() :: #{
    root := root(),
    queue := [_PathStack :: [file:name()]],
    head := glob(),
    stack := [{[pathstack()], glob()}]
}.

-type entry() :: entry_leaf() | entry_node().
-type entry_leaf() ::
    {leaf, file:name(), file:file_info() | {error, file:posix()}, pathstack()}.
-type entry_node() ::
    {node, file:name(), {error, file:posix()}, pathstack()}.

-spec new(root(), glob()) ->
    t().
new(Root, Glob) ->
    #{
        root => Root,
        queue => [[]],
        head => Glob,
        stack => []
    }.

-spec next(t()) ->
    {entry(), t()} | none.
next(It = #{queue := [PathStack | Rest], head := []}) ->
    {emit(PathStack, It), It#{queue => Rest}};
next(It = #{queue := [PathStack | Rest], head := [Pat | _], root := Root}) ->
    Filepath = mk_filepath(PathStack),
    case emqx_ft_fs_util:list_dir(filename:join(Root, Filepath)) of
        {ok, Filenames} ->
            Sorted = lists:sort(Filenames),
            Matches = [[Fn | PathStack] || Fn <- Sorted, matches_glob(Pat, Fn, [Fn | PathStack])],
            ItNext = windup(It),
            next(ItNext#{queue => Matches});
        {error, _} = Error ->
            {{node, Filepath, Error, PathStack}, It#{queue => Rest}}
    end;
next(It = #{queue := []}) ->
    unwind(It).

windup(It = #{queue := [_ | Rest], head := [Pat | Glob], stack := Stack}) ->
    % NOTE
    % Preserve unfinished paths and glob in the stack, so that we can resume traversal
    % when the lower levels of the tree are exhausted.
    It#{
        head => Glob,
        stack => [{Rest, [Pat | Glob]} | Stack]
    }.

unwind(It = #{stack := [{Queue, Glob} | StackRest]}) ->
    % NOTE
    % Resume traversal of unfinished paths from the upper levels of the tree.
    next(It#{
        queue => Queue,
        head => Glob,
        stack => StackRest
    });
unwind(#{stack := []}) ->
    none.

emit(PathStack, #{root := Root}) ->
    Filepath = mk_filepath(PathStack),
    case emqx_ft_fs_util:read_info(filename:join(Root, Filepath)) of
        {ok, Fileinfo} ->
            {leaf, Filepath, Fileinfo, PathStack};
        {error, _} = Error ->
            {leaf, Filepath, Error, PathStack}
    end.

mk_filepath([]) ->
    "";
mk_filepath(PathStack) ->
    filename:join(lists:reverse(PathStack)).

matches_glob('*', _, _) ->
    true;
matches_glob(FilterFun, Filename, _PathStack) when is_function(FilterFun, 1) ->
    FilterFun(Filename);
matches_glob(FilterFun, Filename, PathStack) when is_function(FilterFun, 2) ->
    FilterFun(Filename, PathStack).

%%

-spec next_leaf(t()) ->
    {entry_leaf(), t()} | none.
next_leaf(It) ->
    case next(It) of
        {{leaf, _, _, _} = Leaf, ItNext} ->
            {Leaf, ItNext};
        {{node, _Filename, _Error, _PathStack}, ItNext} ->
            % NOTE
            % Intentionally skipping intermediate traversal errors here, for simplicity.
            next_leaf(ItNext);
        none ->
            none
    end.

%%

-spec seek([file:name()], root(), glob()) ->
    t().
seek(PathSeek, Root, Glob) ->
    SeekGlob = mk_seek_glob(PathSeek, Glob),
    SeekStack = lists:reverse(PathSeek),
    case next_leaf(new(Root, SeekGlob)) of
        {{leaf, _Filepath, _Info, SeekStack}, It} ->
            fixup_glob(Glob, It);
        {{leaf, _Filepath, _Info, Successor}, It = #{queue := Queue}} ->
            fixup_glob(Glob, It#{queue => [Successor | Queue]});
        none ->
            none(Root)
    end.

mk_seek_glob(PathSeek, Glob) ->
    % NOTE
    % The seek glob is a glob that skips all the nodes / leaves that are lexicographically
    % smaller than the seek path. For example, if the seek path is ["a", "b", "c"], and
    % the glob is ['*', '*', '*', '*'], then the seek glob is:
    % [ fun(Path) -> Path >= ["a"] end,
    %   fun(Path) -> Path >= ["a", "b"] end,
    %   fun(Path) -> Path >= ["a", "b", "c"] end,
    %   '*'
    % ]
    L = min(length(PathSeek), length(Glob)),
    merge_glob([mk_seek_pat(lists:sublist(PathSeek, N)) || N <- lists:seq(1, L)], Glob).

mk_seek_pat(PathSeek) ->
    % NOTE
    % The `PathStack` and `PathSeek` are of the same length here.
    fun(_Filename, PathStack) -> lists:reverse(PathStack) >= PathSeek end.

merge_glob([Pat | SeekRest], [PatOrig | Rest]) ->
    [merge_pat(Pat, PatOrig) | merge_glob(SeekRest, Rest)];
merge_glob([], [PatOrig | Rest]) ->
    [PatOrig | merge_glob([], Rest)];
merge_glob([], []) ->
    [].

merge_pat(Pat, PatOrig) ->
    fun(Filename, PathStack) ->
        Pat(Filename, PathStack) andalso matches_glob(PatOrig, Filename, PathStack)
    end.

fixup_glob(Glob, It = #{head := [], stack := Stack}) ->
    % NOTE
    % Restoring original glob through the stack. Strictly speaking, this is not usually
    % necessary, it's a kind of optimization.
    fixup_glob(Glob, lists:reverse(Stack), It#{stack => []}).

fixup_glob(Glob = [_ | Rest], [{Queue, _} | StackRest], It = #{stack := Stack}) ->
    fixup_glob(Rest, StackRest, It#{stack => [{Queue, Glob} | Stack]});
fixup_glob(Rest, [], It) ->
    It#{head => Rest}.

%%

-spec fold(fun((entry(), Acc) -> Acc), Acc, t()) ->
    Acc.
fold(FoldFun, Acc, It) ->
    case next(It) of
        {Entry, ItNext} ->
            fold(FoldFun, FoldFun(Entry, Acc), ItNext);
        none ->
            Acc
    end.

%% NOTE
%% Passing negative `N` is allowed, in which case the iterator will be exhausted
%% completely, like in `fold/3`.
-spec fold_n(fun((entry(), Acc) -> Acc), Acc, t(), _N :: integer()) ->
    {Acc, {more, t()} | none}.
fold_n(_FoldFun, Acc, It, 0) ->
    {Acc, {more, It}};
fold_n(FoldFun, Acc, It, N) ->
    case next(It) of
        {Entry, ItNext} ->
            fold_n(FoldFun, FoldFun(Entry, Acc), ItNext, N - 1);
        none ->
            {Acc, none}
    end.

%%

-spec none(root()) ->
    t().
none(Root) ->
    % NOTE
    % The _none_ iterator is a valid iterator, but it will never yield any entries.
    #{
        root => Root,
        queue => [],
        head => [],
        stack => []
    }.
