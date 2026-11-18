%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_secret_loader_tests).

-include_lib("eunit/include/eunit.hrl").

%% Logger handler callback
-export([log/2]).

-define(ALLOW_LIST, secret_file_allow_list).
-define(COOKIE_FILE_ENV, "EMQX_NODE_COOKIE_FILE").

%%--------------------------------------------------------------------
%% Patterns
%%--------------------------------------------------------------------

allowed(Pattern, Path) ->
    emqx_secret_loader:is_allowed(Path, [Pattern]).

one_component_test() ->
    P = <<"/run/secrets/*">>,
    ?assert(allowed(P, <<"/run/secrets/db">>)),
    ?assert(allowed(P, "/run/secrets/db")),
    ?assertNot(allowed(P, <<"/run/secrets/db/password">>)),
    ?assertNot(allowed(P, <<"/run/secrets">>)),
    ?assertNot(allowed(P, <<"/run/secrets2/db">>)),
    ?assertNot(allowed(P, <<"/etc/passwd">>)).

any_depth_test() ->
    P = <<"/run/secrets/**">>,
    ?assert(allowed(P, <<"/run/secrets/db">>)),
    ?assert(allowed(P, <<"/run/secrets/db/password">>)),
    ?assert(allowed(P, <<"/run/secrets">>)),
    ?assertNot(allowed(P, <<"/run/secrets2/db">>)),
    ?assertNot(allowed(P, <<"/run">>)),
    ?assertNot(allowed(P, <<"/etc/run/secrets/db">>)).

any_depth_in_the_middle_test() ->
    P = <<"/a/**/b/**/c">>,
    ?assert(allowed(P, <<"/a/b/c">>)),
    ?assert(allowed(P, <<"/a/x/b/y/z/c">>)),
    ?assert(allowed(P, <<"/a/x/b/y/b/c/c">>)),
    ?assertNot(allowed(P, <<"/a/x/c">>)),
    ?assertNot(allowed(P, <<"/a/b/c/d">>)),
    ?assert(allowed(<<"/**/*.pem">>, <<"/etc/emqx/certs/key.pem">>)),
    ?assert(allowed(<<"/**/*.pem">>, <<"/key.pem">>)),
    ?assertNot(allowed(<<"/**/*.pem">>, <<"/etc/key.pem.bak">>)),
    ?assert(allowed(<<"**/tls/*.key">>, <<"/etc/emqx/tls/a.key">>)),
    ?assertNot(allowed(<<"**/tls/*.key">>, <<"/etc/emqx/tls/x/a.key">>)).

inside_a_component_test() ->
    ?assert(allowed(<<"/run/*/db/*.pw">>, <<"/run/x/db/a.pw">>)),
    ?assertNot(allowed(<<"/run/*/db/*.pw">>, <<"/run/x/y/db/a.pw">>)),
    ?assertNot(allowed(<<"/run/*/db/*.pw">>, <<"/run/x/db/a.pwd">>)),
    ?assert(allowed(<<"/run/s?cret">>, <<"/run/secret">>)),
    ?assertNot(allowed(<<"/run/s?cret">>, <<"/run/scret">>)),
    ?assertNot(allowed(<<"/run/s?cret">>, <<"/run/s/cret">>)),
    ?assert(allowed(<<"/*a*b*">>, <<"/xaxbx">>)),
    ?assert(allowed(<<"/*a*b*">>, <<"/ab">>)),
    ?assertNot(allowed(<<"/*a*b*">>, <<"/ba">>)),
    ?assert(allowed(<<"/*ab">>, <<"/aaab">>)),
    ?assert(allowed(<<"/*ab">>, <<"/abab">>)),
    ?assertNot(allowed(<<"/*ab">>, <<"/aba">>)),
    %% `**' is a wildcard for components only when it is a whole component.
    ?assert(allowed(<<"/run/a**">>, <<"/run/abc">>)),
    ?assertNot(allowed(<<"/run/a**">>, <<"/run/abc/d">>)).

literal_test() ->
    P = <<"/run/secrets/db">>,
    ?assert(allowed(P, <<"/run/secrets/db">>)),
    ?assert(allowed(P, <<"/run/secrets/./db">>)),
    ?assert(allowed(P, <<"/run//secrets/db">>)),
    ?assertNot(allowed(P, <<"/run/secrets/DB">>)),
    ?assert(allowed(<<"/secrets/密码/*"/utf8>>, <<"/secrets/密码/db"/utf8>>)).

dot_dot_test() ->
    ?assertNot(allowed(<<"/run/**">>, <<"/run/secrets/../db">>)),
    ?assertNot(allowed(<<"/run/**">>, <<"/run/secrets/../../etc/passwd">>)),
    ?assertNot(allowed(<<"/**">>, <<"/run/secrets/../db">>)),
    %% A name that only starts with dots is a normal name.
    ?assert(allowed(<<"/run/**">>, <<"/run/secrets/..data/db">>)),
    ?assert(allowed(<<"**">>, <<"/run/secrets/../db">>)).

%% A `/' is a separator also when a combining character follows it.
combining_character_test() ->
    ?assertNot(allowed(<<"/run/secrets/*">>, <<"/run/secrets/a/", 16#301/utf8, "b">>)),
    ?assert(allowed(<<"/run/secrets/*/*">>, <<"/run/secrets/a/", 16#301/utf8, "b">>)),
    ?assertNot(allowed(<<"/run/**">>, <<"/run/secrets/../", 16#301/utf8, "b">>)).

relative_path_test() ->
    {ok, Cwd} = file:get_cwd(),
    ?assert(emqx_secret_loader:is_allowed(<<"secrets/db">>, [Cwd ++ "/secrets/*"])),
    ?assertNot(allowed(<<"/secrets/*">>, <<"secrets/db">>)).

pattern_list_test() ->
    Path = <<"/run/secrets/db">>,
    ?assertNot(emqx_secret_loader:is_allowed(Path, [])),
    ?assert(emqx_secret_loader:is_allowed(Path, [<<"/etc/*">>, <<"/run/secrets/*">>])),
    ?assert(emqx_secret_loader:is_allowed(Path, [<<"/etc/*">>, <<"**">>])),
    ?assertNot(emqx_secret_loader:is_allowed(Path, [<<"/etc/*">>, <<"/run/*">>])).

%% The path can come from an API request, so a long one must not be slow to check.
long_path_test_() ->
    Name = lists:duplicate(200_000, $a),
    Deep = lists:append(lists:duplicate(50_000, "/a")),
    {timeout, 30, fun() ->
        ?assertNot(allowed(<<"/*a*a*a*a*b">>, "/" ++ Name)),
        ?assert(allowed(<<"/*a*a*a*a*">>, "/" ++ Name)),
        ?assertNot(allowed(<<"/**/a/**/a/**/a/**/b">>, Deep)),
        ?assert(allowed(<<"/**/a/**/a/**/a/**">>, Deep))
    end}.

%%--------------------------------------------------------------------
%% Loading
%%--------------------------------------------------------------------

load_test_() ->
    %% `local': the log handler sends the reports to the process that runs `setup/0'.
    {foreach, local, fun setup/0, fun teardown/1, [
        fun(#{file := File}) ->
            {"every path is allowed when the setting is not set", fun() ->
                ?assertEqual(<<"secret">>, load(File)),
                ?assertEqual([], denied_logs())
            end}
        end,
        fun(#{dir := Dir, file := File}) ->
            {"a path that matches a pattern", fun() ->
                set_allow_list([<<"/no/such/dir/*">>, bin([Dir, "/*"])]),
                ?assertEqual(<<"secret">>, load(File)),
                ?assertEqual(<<"secret">>, emqx_secret_loader:file(bin(File))),
                ?assertEqual([], denied_logs())
            end}
        end,
        fun(#{file := File}) ->
            {"a path that matches no pattern", fun() ->
                set_allow_list([<<"/no/such/dir/*">>]),
                ?assertEqual(<<>>, load(File)),
                ?assertMatch(
                    [#{path := Path, reason := not_in_allow_list}] when Path =:= File,
                    denied_logs()
                )
            end}
        end,
        fun(#{file := File}) ->
            {"an empty list allows no file", fun() ->
                set_allow_list([]),
                ?assertEqual(<<>>, load(File)),
                ?assertMatch([#{reason := not_in_allow_list}], denied_logs())
            end}
        end,
        fun(#{dir := Dir}) ->
            {"a file that does not exist", fun() ->
                %% Not allowed: no error about the file itself.
                set_allow_list([<<"/no/such/dir/*">>]),
                ?assertEqual(<<>>, load(Dir ++ "/no-such-file")),
                %% Allowed: the error is the same as before.
                set_allow_list([<<"**">>]),
                ?assertThrow(
                    #{msg := failed_to_read_secret_file},
                    load(Dir ++ "/no-such-file")
                )
            end}
        end,
        fun(#{dir := Dir, file := File}) ->
            {"the path is checked after the environment variable is replaced", fun() ->
                os:putenv("EMQX_SECRET_LOADER_TEST_DIR", Dir),
                try
                    set_allow_list([bin([Dir, "/*"])]),
                    ?assertEqual(
                        <<"secret">>,
                        load("${EMQX_SECRET_LOADER_TEST_DIR}/" ++ filename:basename(File))
                    ),
                    set_allow_list([<<"/no/such/dir/*">>]),
                    ?assertEqual(
                        <<>>,
                        load("${EMQX_SECRET_LOADER_TEST_DIR}/" ++ filename:basename(File))
                    ),
                    ?assertMatch([#{path := Path}] when Path =:= File, denied_logs())
                after
                    os:unsetenv("EMQX_SECRET_LOADER_TEST_DIR")
                end
            end}
        end,
        fun(#{dir := Dir, file := File}) ->
            {"the node cookie file is never read", fun() ->
                Cookie = Dir ++ "/cookie",
                ok = file:write_file(Cookie, <<"cookie">>),
                ?assertEqual(<<"cookie">>, load(Cookie)),
                os:putenv(?COOKIE_FILE_ENV, Cookie),
                ?assertEqual(<<>>, load(Cookie)),
                ?assertEqual(<<>>, load(Dir ++ "/./cookie")),
                ?assertEqual(<<>>, load(Dir ++ "/sub/../cookie")),
                ?assertMatch(
                    [#{reason := node_cookie_file}, _, _],
                    denied_logs()
                ),
                %% The pattern that allows the directory does not allow the cookie file.
                set_allow_list([bin([Dir, "/*"])]),
                ?assertEqual(<<>>, load(Cookie)),
                ?assertEqual(<<"secret">>, load(File))
            end}
        end,
        fun(#{dir := Dir, file := File}) ->
            {"check/1 gives the result of a load, and it logs nothing", fun() ->
                ?assertEqual(ok, check(File)),
                set_allow_list([<<"/no/such/dir/*">>]),
                FileBin = bin(File),
                ?assertMatch(
                    {error, #{path := FileBin, reason := not_in_allow_list, hint := <<_/binary>>}},
                    check(File)
                ),
                ?assertMatch(
                    {error, #{reason := not_in_allow_list}},
                    emqx_secret_loader:check({file, bin(["file://", File])})
                ),
                %% The file is not read: a file that does not exist is allowed as well.
                set_allow_list([bin([Dir, "/*"])]),
                ?assertEqual(ok, check(Dir ++ "/no-such-file")),
                os:putenv(?COOKIE_FILE_ENV, File),
                ?assertMatch({error, #{reason := node_cookie_file}}, check(File)),
                ?assertEqual([], denied_logs())
            end}
        end
    ]}.

load(Path) ->
    emqx_secret:unwrap(emqx_secret:wrap_load({file, "file://" ++ Path})).

check(Path) ->
    emqx_secret_loader:check({file, "file://" ++ Path}).

set_allow_list(Patterns) ->
    application:set_env(emqx_utils, ?ALLOW_LIST, Patterns).

setup() ->
    Unique = integer_to_list(erlang:unique_integer([positive])),
    Dir = filename:join("/tmp", ?MODULE_STRING ++ "-" ++ Unique),
    File = filename:join(Dir, "secret"),
    ok = filelib:ensure_dir(File),
    ok = file:write_file(File, <<"secret\n">>),
    application:unset_env(emqx_utils, ?ALLOW_LIST),
    os:unsetenv(?COOKIE_FILE_ENV),
    ok = logger:add_handler(?MODULE, ?MODULE, #{config => #{pid => self()}}),
    #{dir => Dir, file => File}.

teardown(#{dir := Dir}) ->
    ok = logger:remove_handler(?MODULE),
    %% Leave no report in the mailbox of the process that runs the tests.
    _ = denied_logs(),
    application:unset_env(emqx_utils, ?ALLOW_LIST),
    os:unsetenv(?COOKIE_FILE_ENV),
    ok = file:del_dir_r(Dir).

log(#{msg := {report, #{msg := "secret_file_read_denied"} = Report}, level := error}, #{
    config := #{pid := Pid}
}) ->
    Pid ! {denied_log, Report},
    ok;
log(_Event, _Config) ->
    ok.

%% The reports of the denied reads so far, with the path as a string.
denied_logs() ->
    receive
        {denied_log, #{path := Path} = Report} ->
            [Report#{path := unicode:characters_to_list(Path)} | denied_logs()]
    after 0 ->
        []
    end.

bin(IoData) ->
    iolist_to_binary(IoData).
