%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_secret_loader_tests).

-include_lib("eunit/include/eunit.hrl").

%% Logger handler callback
-export([log/2]).

-define(ALLOW_LIST, [node, secret_file_allow_list]).
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

%% A `..' component is resolved before the path is compared.
dot_dot_test() ->
    ?assert(allowed(<<"/run/**">>, <<"/run/secrets/../db">>)),
    ?assertNot(allowed(<<"/run/secrets/**">>, <<"/run/secrets/../db">>)),
    ?assertNot(allowed(<<"/run/**">>, <<"/run/secrets/../../etc/passwd">>)),
    ?assert(allowed(<<"/etc/*">>, <<"/run/secrets/../../etc/passwd">>)),
    ?assertNot(allowed(<<"/run/**">>, <<"/run/../../../etc/passwd">>)),
    %% A name that only starts with dots is a normal name.
    ?assert(allowed(<<"/run/**">>, <<"/run/secrets/..data/db">>)),
    ?assert(allowed(<<"**">>, <<"/run/secrets/../db">>)).

%% A `/' is a separator also when a combining character follows it.
combining_character_test() ->
    ?assertNot(allowed(<<"/run/secrets/*">>, <<"/run/secrets/a/", 16#301/utf8, "b">>)),
    ?assert(allowed(<<"/run/secrets/*/*">>, <<"/run/secrets/a/", 16#301/utf8, "b">>)),
    ?assertNot(allowed(<<"/run/secrets/**">>, <<"/run/secrets/../", 16#301/utf8, "b">>)).

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
    %% The longest paths that can be opened.
    MaxName = lists:duplicate(4_000, $a),
    MaxDeep = lists:append(lists:duplicate(2_000, "/a")),
    {timeout, 30, fun() ->
        %% A path that is too long to be opened matches no pattern.
        ?assertNot(allowed(<<"/*a*a*a*a*">>, "/" ++ Name)),
        ?assertNot(allowed(<<"/**/a/**/a/**/a/**">>, Deep)),
        ?assert(allowed(<<"**">>, Deep)),
        ?assertNot(allowed(<<"/*a*a*a*a*b">>, "/" ++ MaxName)),
        ?assert(allowed(<<"/*a*a*a*a*">>, "/" ++ MaxName)),
        ?assertNot(allowed(<<"/**/a/**/a/**/a/**/b">>, MaxDeep)),
        ?assert(allowed(<<"/**/a/**/a/**/a/**">>, MaxDeep))
    end}.

%%--------------------------------------------------------------------
%% Loading
%%--------------------------------------------------------------------

load_test_() ->
    %% `local': the log handler sends the reports to the process that runs `setup/0'.
    {foreach, local, fun setup/0, fun teardown/1, [
        fun(#{file := File}) ->
            {"every path is allowed before the config is loaded", fun() ->
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
        end,

        fun(#{dir := Dir}) ->
            {"a file in a mounted secret volume is a link that stays in the volume", fun() ->
                Volume = mounted_volume(Dir, "password", <<"mounted\n">>),
                %% Each rule is on the file that is opened, which is two levels down.
                set_allow_list([bin([Volume, "/**"])]),
                ?assertEqual(<<"mounted">>, load(Volume ++ "/password")),
                ?assertEqual([], denied_logs()),
                set_allow_list([bin([Volume, "/password"])]),
                ?assertEqual(<<"mounted">>, load(Volume ++ "/password")),
                set_allow_list([bin([Volume, "/*"])]),
                ?assertEqual([], read_files(fun() -> <<>> = load(Volume ++ "/password") end)),
                RealPath = bin([Volume, "/..2026_11_19/password"]),
                ?assertMatch(
                    [#{reason := not_in_allow_list, real_path := RealPath}],
                    denied_logs()
                )
            end}
        end,
        fun(#{dir := Dir, file := File}) ->
            {"a link in an allowed directory to a file outside of it", fun() ->
                Allowed = Dir ++ "/allowed",
                ok = filelib:ensure_dir(Allowed ++ "/x"),
                ok = file:make_symlink(File, Allowed ++ "/link"),
                ok = file:make_symlink("..", Allowed ++ "/up"),
                set_allow_list([bin([Allowed, "/**"])]),
                ?assertEqual(
                    [],
                    read_files(fun() ->
                        <<>> = load(Allowed ++ "/link"),
                        <<>> = load(Allowed ++ "/up/secret"),
                        <<>> = load(Allowed ++ "/../secret")
                    end)
                ),
                FileBin = bin(File),
                ?assertMatch(
                    [
                        #{reason := not_in_allow_list, real_path := FileBin},
                        #{reason := not_in_allow_list, real_path := FileBin},
                        #{reason := not_in_allow_list, real_path := FileBin}
                    ],
                    denied_logs()
                ),
                ?assertMatch({error, #{real_path := FileBin}}, check(Allowed ++ "/link"))
            end}
        end,
        fun(#{dir := Dir, file := File}) ->
            {"a link outside of an allowed directory to a file in it", fun() ->
                ok = file:make_symlink(File, Dir ++ "/../" ++ filename:basename(Dir) ++ ".link"),
                Link = Dir ++ ".link",
                try
                    set_allow_list([bin([Dir, "/*"])]),
                    ?assertEqual([File], read_files(fun() -> <<"secret">> = load(Link) end)),
                    ?assertEqual([], denied_logs())
                after
                    file:delete(Link)
                end
            end}
        end,
        fun(#{dir := Dir, file := File}) ->
            {"the directory of a pattern can be a link", fun() ->
                ok = file:make_symlink(Dir, Dir ++ "/alias"),
                set_allow_list([bin([Dir, "/alias/*"])]),
                ?assertEqual(<<"secret">>, load(Dir ++ "/alias/secret")),
                ?assertEqual(<<"secret">>, load(File)),
                ?assertEqual([], denied_logs())
            end}
        end,
        fun(#{dir := Dir}) ->
            {"a loop of links leads to no file", fun() ->
                ok = file:make_symlink("loop", Dir ++ "/loop"),
                set_allow_list([bin([Dir, "/**"])]),
                ?assertEqual(<<>>, load(Dir ++ "/loop")),
                ?assertMatch([#{reason := not_in_allow_list}], denied_logs())
            end}
        end,
        fun(#{dir := Dir}) ->
            {"a link to the node cookie file is the node cookie file", fun() ->
                Cookie = Dir ++ "/cookie",
                ok = file:write_file(Cookie, <<"cookie">>),
                ok = file:make_symlink(Cookie, Dir ++ "/symlink"),
                ok = file:make_symlink("symlink", Dir ++ "/symlink2"),
                ok = file:make_link(Cookie, Dir ++ "/hardlink"),
                os:putenv(?COOKIE_FILE_ENV, Dir ++ "/symlink"),
                ?assertEqual(
                    [],
                    read_files(fun() ->
                        <<>> = load(Cookie),
                        <<>> = load(Dir ++ "/symlink"),
                        <<>> = load(Dir ++ "/symlink2"),
                        <<>> = load(Dir ++ "/hardlink")
                    end)
                ),
                ?assertMatch(
                    [
                        #{reason := node_cookie_file},
                        #{reason := node_cookie_file},
                        #{reason := node_cookie_file},
                        #{reason := node_cookie_file}
                    ],
                    denied_logs()
                ),
                %% A cookie file that does not exist is compared by its path.
                os:putenv(?COOKIE_FILE_ENV, Dir ++ "/no-such-cookie"),
                ?assertMatch(
                    {error, #{reason := node_cookie_file}},
                    check(Dir ++ "/./no-such-cookie")
                ),
                ?assertEqual(<<"cookie">>, load(Cookie))
            end}
        end
    ]}.

load(Path) ->
    emqx_secret:unwrap(emqx_secret:wrap_load({file, "file://" ++ Path})).

check(Path) ->
    emqx_secret_loader:check({file, "file://" ++ Path}).

set_allow_list(Patterns) ->
    emqx_config:put(?ALLOW_LIST, Patterns).

setup() ->
    Unique = integer_to_list(erlang:unique_integer([positive])),
    Dir = filename:join("/tmp", ?MODULE_STRING ++ "-" ++ Unique),
    File = filename:join(Dir, "secret"),
    ok = filelib:ensure_dir(File),
    ok = file:write_file(File, <<"secret\n">>),
    %% Start with no `node' config, as before the node loads its config.
    NodeConf = emqx_config:get([node], undefined),
    ok = emqx_config:erase(node),
    os:unsetenv(?COOKIE_FILE_ENV),
    ok = logger:add_handler(?MODULE, ?MODULE, #{config => #{pid => self()}}),
    #{dir => Dir, file => File, node_conf => NodeConf}.

teardown(#{dir := Dir, node_conf := NodeConf}) ->
    ok = logger:remove_handler(?MODULE),
    %% Leave no report in the mailbox of the process that runs the tests.
    _ = denied_logs(),
    ok = emqx_config:erase(node),
    NodeConf =:= undefined orelse emqx_config:put([node], NodeConf),
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

%% A directory as a container platform mounts a secret volume: the file is a link
%% to a file in a hidden directory, through a second link.
mounted_volume(Dir, Name, Content) ->
    Volume = Dir ++ "/volume",
    Data = Volume ++ "/..2026_11_19",
    ok = filelib:ensure_dir(Data ++ "/x"),
    ok = file:write_file(Data ++ "/" ++ Name, Content),
    ok = file:make_symlink("..2026_11_19", Volume ++ "/..data"),
    ok = file:make_symlink("..data/" ++ Name, Volume ++ "/" ++ Name),
    Volume.

%% Run `Fun' and return the names of the files that it reads.
read_files(Fun) ->
    Self = self(),
    Tracer = spawn_link(fun() -> read_files_tracer(Self, []) end),
    _ = erlang:trace_pattern({file, read_file, 1}, true, []),
    1 = erlang:trace(Self, true, [call, {tracer, Tracer}]),
    try
        Fun()
    after
        1 = erlang:trace(Self, false, [call]),
        _ = erlang:trace_pattern({file, read_file, 1}, false, [])
    end,
    Ref = erlang:trace_delivered(Self),
    receive
        {trace_delivered, Self, Ref} -> ok
    end,
    Tracer ! {stop, Self},
    receive
        {read_files, Files} -> Files
    end.

read_files_tracer(Owner, Acc) ->
    receive
        {trace, Owner, call, {file, read_file, [File]}} ->
            read_files_tracer(Owner, [unicode:characters_to_list(File) | Acc]);
        {stop, Owner} ->
            Owner ! {read_files, lists:reverse(Acc)}
    end.

bin(IoData) ->
    iolist_to_binary(IoData).
