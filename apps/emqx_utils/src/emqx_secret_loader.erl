%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_secret_loader).

-moduledoc """
Loads the value of a secret from its source.

The only source is a file: a secret that is set to `file://<path>` reads its value
from that file each time it is unwrapped. This module decides which files such a
secret can read:

- The file must match a pattern in `node.secret_file_allow_list`. The setting is
  read-only at runtime. `emqx_config` puts it in place before it checks the config
  at boot. Before that, every path is allowed.
- The file that `node.cookie` points to is never read. Only the boot script reads
  it, before the node starts. The script passes its path to the node in the
  environment variable `EMQX_NODE_COOKIE_FILE`.

Both rules apply to the file that is opened: the symbolic links in the path are
resolved first, and that file is the one that is read.

When the file is not allowed, the secret is an empty binary and an error log names
the path. The caller gets no exception: a secret is unwrapped in many places, and
most of them cannot handle one.

A caller that can report an error, for example a dry run, calls `check/1` before
it uses the secret.
""".

%% API
-export([load/1, check/1]).
-export([file/1]).

%% Internal exports
-export([is_allowed/2]).

-export_type([source/0, denied/0]).

-type source() :: {file, string() | binary()}.
-type denied() :: #{
    path := binary(),
    %% The file that the path leads to, when it is not `path` itself.
    real_path => binary(),
    reason := node_cookie_file | not_in_allow_list,
    hint := binary()
}.

-include_lib("kernel/include/file.hrl").

%% The pattern that allows every path.
-define(ALLOW_ALL, <<"**">>).
-define(COOKIE_FILE_ENV, "EMQX_NODE_COOKIE_FILE").
%% The number of symbolic links that one path can go through, as on Linux.
-define(MAX_LINKS, 40).
%% The length of the longest path that can be opened, as on Linux.
-define(MAX_PATH, 4096).

-spec load(source()) -> binary() | no_return().
load({file, <<"file://", Path/binary>>}) ->
    file(Path);
load({file, "file://" ++ Path}) ->
    file(Path).

-doc """
Tell whether `load/1` reads the file of `Source`.

The file is not read. The error has the fields that `load/1` logs when it does not
read the file.
""".
-spec check(source()) -> ok | {error, denied()}.
check({file, <<"file://", Path/binary>>}) ->
    check_result(check_file(interpolate(Path)));
check({file, "file://" ++ Path}) ->
    check_result(check_file(interpolate(Path))).

check_result({ok, _Filename}) -> ok;
check_result({error, _} = Error) -> Error.

-spec file(file:filename_all()) -> binary() | no_return().
file(Filename0) ->
    Filename = interpolate(Filename0),
    case check_file(Filename) of
        {ok, RealName} ->
            read(RealName, Filename);
        {error, Denied} ->
            logger:log(error, Denied#{msg => "secret_file_read_denied"}),
            <<>>
    end.

interpolate(Filename) ->
    emqx_utils_schema:naive_env_interpolation(Filename).

%% An error names the path as it is in the config.
read(RealName, Filename) ->
    case file:read_file(RealName) of
        {ok, Secret} ->
            string:trim(Secret, trailing);
        {error, Reason} ->
            throw(#{
                msg => failed_to_read_secret_file,
                path => Filename,
                reason => emqx_utils:explain_posix(Reason)
            })
    end.

%% Return the name of the file to read. It is the file that the path leads to,
%% so the file that is checked is the file that is read.
check_file(Filename) when is_list(Filename) ->
    AllowAll = lists:member(?ALLOW_ALL, allow_list()),
    case {AllowAll, cookie_file()} of
        {true, false} ->
            %% No rule can deny the file, so there is no need to resolve the path.
            {ok, Filename};
        _ ->
            check_real_file(Filename, AllowAll)
    end;
check_file(Filename) ->
    %% Not a valid file name. Reading it fails with the usual error.
    {ok, Filename}.

check_real_file(Filename, AllowAll) ->
    Components = components(Filename),
    Real = real_components(Components),
    case denied_reason(Filename, Real, AllowAll) of
        false ->
            {ok, real_name(Filename, Real)};
        Reason ->
            {error, with_real_path(denied(Filename, Reason), Components, Real)}
    end.

denied(Filename, Reason) ->
    #{
        path => unicode:characters_to_binary(Filename),
        reason => Reason,
        hint => hint(Reason)
    }.

denied_reason(Filename, Real, AllowAll) ->
    case is_node_cookie_file(Filename, Real) of
        true ->
            node_cookie_file;
        false ->
            case AllowAll orelse is_real_allowed(Real, allow_list()) of
                true -> false;
                false -> not_in_allow_list
            end
    end.

real_name(_Filename, {ok, Real}) -> join(Real);
%% The path cannot be resolved. Reading it fails with the usual error.
real_name(Filename, error) -> Filename.

with_real_path(Report, Components, {ok, Real}) when Real =/= Components ->
    Report#{real_path => unicode:characters_to_binary(join(Real))};
with_real_path(Report, _Components, _Real) ->
    Report.

hint(node_cookie_file) ->
    <<"The file that 'node.cookie' points to cannot be used as a secret.">>;
hint(not_in_allow_list) ->
    <<
        "No pattern in 'node.secret_file_allow_list' matches the file. "
        "A pattern is compared with the real path of the file, in which the symbolic "
        "links are resolved. "
        "Move the file, or add a pattern for it in emqx.conf and restart the node."
    >>.

%% The fallback applies until `emqx_config` has put the setting in place. It does
%% that before it checks the config at boot.
allow_list() ->
    emqx_config:get([node, secret_file_allow_list], [?ALLOW_ALL]).

cookie_file() ->
    case os:getenv(?COOKIE_FILE_ENV) of
        false -> false;
        "" -> false;
        CookieFile -> CookieFile
    end.

%% The cookie file is recognized as a file, not as a name: a symbolic link or a hard
%% link to it is the cookie file too.
is_node_cookie_file(Filename, Real) ->
    case cookie_file() of
        false ->
            false;
        CookieFile ->
            is_same_file(Filename, CookieFile) orelse
                (Real =/= error andalso Real =:= real_components(components(CookieFile)))
    end.

is_same_file(Filename1, Filename2) ->
    case {file_id(Filename1), file_id(Filename2)} of
        {{ok, Id}, {ok, Id}} -> true;
        _ -> false
    end.

%% The file system and the inode number identify a file. Links are followed.
file_id(Filename) ->
    case file:read_file_info(Filename, [raw]) of
        {ok, #file_info{major_device = Device, inode = Inode}} -> {ok, {Device, Inode}};
        {error, _} = Error -> Error
    end.

-doc """
Return `true` when a pattern in `Patterns` matches the file that `Path` leads to.

A pattern is an absolute path in which:

- `*` matches any number of characters inside one path component;
- `?` matches one character inside one path component;
- `**`, as a whole component, matches any number of components, including none.

`Path` is made absolute, and its symbolic links and `..` components are resolved,
before it is compared. So a pattern is compared with the real path of the file,
and a symbolic link in an allowed directory does not give access to a file
outside of it. The directory at the start of a pattern, up to its first wildcard,
can have symbolic links too: the pattern is also tried with them resolved.
A path that is too long to be opened matches no pattern.

A file in a mounted secret volume is often a symbolic link to a file in a
subdirectory of the volume. The pattern `<volume>/**` matches such a file, and
`<volume>/*` does not.

The pattern `**` on its own allows every path.
""".
-spec is_allowed(file:filename_all(), [binary() | string()]) -> boolean().
is_allowed(Path, Patterns) ->
    lists:member(?ALLOW_ALL, Patterns) orelse
        is_real_allowed(real_components(components(Path)), Patterns).

is_real_allowed({ok, Real}, Patterns) ->
    lists:any(fun(Pattern) -> match_pattern(pattern_components(Pattern), Real) end, Patterns);
is_real_allowed(error, _Patterns) ->
    false.

match_pattern(Pattern, Real) ->
    match_components(Pattern, Real) orelse match_real_pattern(Pattern, Real).

match_real_pattern(Pattern, Real) ->
    {Dir, Wildcards} = lists:splitwith(fun is_literal/1, Pattern),
    case real_components(Dir) of
        {ok, Dir} -> false;
        {ok, RealDir} -> match_components(RealDir ++ Wildcards, Real);
        error -> false
    end.

is_literal(Component) ->
    not lists:any(fun(Char) -> Char =:= $* orelse Char =:= $? end, Component).

%%--------------------------------------------------------------------
%% Paths
%%--------------------------------------------------------------------

%% The components of the absolute form of the path, without the `.` components.
components(Path) ->
    [C || C <- split(filename:absname(to_list(Path))), C =/= "."].

pattern_components(Pattern) ->
    split(to_list(Pattern)).

%% Split at each `/`, as the operating system does. The `string` module is not
%% used here: it does not split at a `/` that a combining character follows.
split(Path) ->
    case filename:split(Path) of
        ["/" | Components] -> Components;
        Components -> Components
    end.

%% `filename:join/1` is not used here: its cost grows with the square of the
%% number of components.
join([]) ->
    "/";
join(Components) ->
    lists:append([[$/ | Component] || Component <- Components]).

%% Resolve the symbolic links and the `..` components of a path, one component at
%% a time, as the operating system does when it opens the file. A component that
%% does not exist is kept: the path can still be compared, and reading it fails.
%% The result is `error` when the path cannot lead to a file.
real_components(Components) ->
    %% The path can come from an API request. The limit keeps the cost of a check
    %% small: each component takes one call to the file server.
    case path_length(Components) =< ?MAX_PATH of
        true -> real_components(Components, [], ?MAX_LINKS);
        false -> error
    end.

path_length(Components) ->
    lists:foldl(fun(Component, Length) -> Length + length(Component) + 1 end, 0, Components).

real_components([], Acc, _Links) ->
    {ok, lists:reverse(Acc)};
real_components([".." | Components], Acc, Links) ->
    real_components(Components, parent(Acc), Links);
real_components([Component | Components], Acc, Links) ->
    case file:read_link_all(join(lists:reverse([Component | Acc]))) of
        {ok, Target} when Links > 0 ->
            real_link_target(Target, Components, Acc, Links - 1);
        {ok, _Target} ->
            %% Too many links: a loop.
            error;
        {error, _NotALink} ->
            real_components(Components, [Component | Acc], Links)
    end.

real_link_target(Target, Components, Acc, Links) when is_list(Target) ->
    case [C || C <- filename:split(Target), C =/= "."] of
        ["/" | Absolute] -> real_components(Absolute ++ Components, [], Links);
        Relative -> real_components(Relative ++ Components, Acc, Links)
    end;
real_link_target(_RawName, _Components, _Acc, _Links) ->
    %% The target is not a valid file name in the encoding of the node.
    error.

parent([]) -> [];
parent([_ | Acc]) -> Acc.

to_list(Path) when is_binary(Path) -> unicode:characters_to_list(Path);
to_list(Path) when is_list(Path) -> Path.

%%--------------------------------------------------------------------
%% Pattern matching
%%--------------------------------------------------------------------

%% Both matchers below go back only to the last wildcard when a comparison fails.
%% This is enough to find a match when there is one, and it keeps the cost
%% proportional to the pattern length times the path length. The path can come
%% from an API request, so the cost must not grow faster than that.

match_components(Patterns, Components) ->
    match_components(Patterns, Components, none).

match_components(["**" | Patterns], Components, _Last) ->
    match_components(Patterns, Components, {Patterns, Components});
match_components([Pattern | Patterns], [Component | Components], Last) ->
    case match_name(Pattern, Component) of
        true -> match_components(Patterns, Components, Last);
        false -> retry_components(Last)
    end;
match_components([], [], _Last) ->
    true;
match_components(_Patterns, _Components, Last) ->
    retry_components(Last).

%% Let the last `**` take one more component.
retry_components({Patterns, [_ | Components]}) ->
    match_components(Patterns, Components, {Patterns, Components});
retry_components(_) ->
    false.

match_name(Pattern, Name) ->
    match_name(Pattern, Name, none).

match_name([$* | Pattern], Name, _Last) ->
    match_name(Pattern, Name, {Pattern, Name});
match_name([$? | Pattern], [_ | Name], Last) ->
    match_name(Pattern, Name, Last);
match_name([Char | Pattern], [Char | Name], Last) ->
    match_name(Pattern, Name, Last);
match_name([], [], _Last) ->
    true;
match_name(_Pattern, _Name, Last) ->
    retry_name(Last).

%% Let the last `*` take one more character.
retry_name({Pattern, [_ | Name]}) ->
    match_name(Pattern, Name, {Pattern, Name});
retry_name(_) ->
    false.
