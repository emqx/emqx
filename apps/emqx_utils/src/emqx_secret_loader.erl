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

- The path must match a pattern in `node.secret_file_allow_list`. The setting is
  read-only at runtime. It reaches this module as the application environment
  variable `secret_file_allow_list` of `emqx_utils`.
- The file that `node.cookie` points to is never read. Only the boot script reads
  it, before the node starts. The script passes its path to the node in the
  environment variable `EMQX_NODE_COOKIE_FILE`.

When the file is not allowed, the secret is an empty binary and an error log names
the path. The caller gets no exception: a secret is unwrapped in many places, and
most of them cannot handle one.
""".

%% API
-export([load/1]).
-export([file/1]).

%% Internal exports
-export([is_allowed/2]).

-export_type([source/0]).

-type source() :: {file, string() | binary()}.

%% The pattern that allows every path.
-define(ALLOW_ALL, <<"**">>).
-define(COOKIE_FILE_ENV, "EMQX_NODE_COOKIE_FILE").

-spec load(source()) -> binary() | no_return().
load({file, <<"file://", Path/binary>>}) ->
    file(Path);
load({file, "file://" ++ Path}) ->
    file(Path).

-spec file(file:filename_all()) -> binary() | no_return().
file(Filename0) ->
    Filename = emqx_utils_schema:naive_env_interpolation(Filename0),
    case check(Filename) of
        ok ->
            read(Filename);
        {error, Reason} ->
            logger:log(error, #{
                msg => "secret_file_read_denied",
                path => unicode:characters_to_binary(Filename),
                reason => Reason,
                hint => hint(Reason)
            }),
            <<>>
    end.

read(Filename) ->
    case file:read_file(Filename) of
        {ok, Secret} ->
            string:trim(Secret, trailing);
        {error, Reason} ->
            throw(#{
                msg => failed_to_read_secret_file,
                path => Filename,
                reason => emqx_utils:explain_posix(Reason)
            })
    end.

check(Filename) when is_list(Filename) ->
    case is_node_cookie_file(Filename) of
        true ->
            {error, node_cookie_file};
        false ->
            case is_allowed(Filename, allow_list()) of
                true -> ok;
                false -> {error, not_in_allow_list}
            end
    end;
check(_Filename) ->
    %% Not a valid file name. Reading it fails with the usual error.
    ok.

hint(node_cookie_file) ->
    <<"The file that 'node.cookie' points to cannot be used as a secret.">>;
hint(not_in_allow_list) ->
    <<
        "No pattern in 'node.secret_file_allow_list' matches the path. "
        "Move the file, or add a pattern for it in emqx.conf and restart the node."
    >>.

allow_list() ->
    application:get_env(emqx_utils, secret_file_allow_list, [?ALLOW_ALL]).

is_node_cookie_file(Filename) ->
    case os:getenv(?COOKIE_FILE_ENV) of
        false -> false;
        "" -> false;
        CookieFile -> resolve(components(Filename)) =:= resolve(components(CookieFile))
    end.

-doc """
Return `true` when a pattern in `Patterns` matches `Path`.

A pattern is an absolute path in which:

- `*` matches any number of characters inside one path component;
- `?` matches one character inside one path component;
- `**`, as a whole component, matches any number of components, including none.

`Path` is made absolute before it is compared. Symbolic links are not followed.
A path that has a `..` component matches no pattern, because the file that it
names depends on the symbolic links on the way. The one exception is the pattern
`**` on its own, which allows every path.
""".
-spec is_allowed(file:filename_all(), [binary() | string()]) -> boolean().
is_allowed(Path, Patterns) ->
    lists:member(?ALLOW_ALL, Patterns) orelse is_matched(components(Path), Patterns).

is_matched(Components, Patterns) ->
    (not lists:member("..", Components)) andalso
        lists:any(
            fun(Pattern) -> match_components(pattern_components(Pattern), Components) end,
            Patterns
        ).

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

%% Remove the `..` components by name, as a shell does for `cd ..`.
resolve(Components) ->
    lists:reverse(lists:foldl(fun resolve/2, [], Components)).

resolve("..", []) -> [];
resolve("..", [_ | Acc]) -> Acc;
resolve(Component, Acc) -> [Component | Acc].

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
