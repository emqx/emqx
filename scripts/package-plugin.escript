#!/usr/bin/env escript
%%! -pa _build/emqx-enterprise/lib/jsone/ebin
%% -*- mode: erlang -*-

%% Package an EMQX monorepo plugin as `<name>-<vsn>.tar.gz`, reading the
%% plugin's already-compiled beams from the umbrella build tree
%% (`_build/$PROFILE/lib/<name>/`).
%%
%% This replicates the layout produced by `rebar3 emqx_plugrel tar` but
%% without requiring the plugin to have its own rebar3 project with a
%% dep closure. The plugin is compiled once by the root `make`; this
%% script just packages the output.
%%
%% JSON encoding uses `jsone` loaded from the umbrella build via the
%% `-pa` directive above. The escript must run with CWD=repo root so
%% that relative path resolves — `scripts/build-plugin.sh` handles that.
%%
%% Usage: package-plugin.escript <plugin-name>
%% Env: PROFILE (default: emqx-enterprise), ROOT_DIR (default: script parent)

-mode(compile).

-define(METADATA_VSN, <<"0.2.0">>).

main([App]) ->
    try
        do(App)
    catch
        error:Reason:St ->
            io:format(standard_error, "package-plugin: ~p~n~p~n", [Reason, St]),
            halt(1)
    end;
main(_) ->
    io:format(standard_error, "Usage: package-plugin.escript <plugin-name>~n", []),
    halt(1).

do(AppStr) ->
    Root = root_dir(),
    Profile = os_env("PROFILE", "emqx-enterprise"),
    PluginDir = filename:join([Root, "plugins", AppStr]),
    LibDir = filename:join([Root, "_build", Profile, "lib", AppStr]),
    assert_dir(PluginDir, "plugin source"),
    assert_dir(LibDir, "umbrella compiled beams (run 'make' at repo root first)"),
    AppFile = filename:join([LibDir, "ebin", AppStr ++ ".app"]),
    AppVsn = read_app_vsn(AppFile),
    RelVsn = read_file_trim(filename:join(PluginDir, "VERSION")),
    Rebar = read_rebar_config(filename:join(PluginDir, "rebar.config")),
    Metadata = proplists:get_value(emqx_plugrel, Rebar, []),
    NameWithRelVsn = AppStr ++ "-" ++ RelVsn,
    StageRoot = filename:join([Root, "_build", "plugins", "stage-" ++ AppStr]),
    StageDir = filename:join(StageRoot, NameWithRelVsn),
    AppCopyDir = filename:join(StageDir, AppStr ++ "-" ++ AppVsn),
    rm_rf(StageRoot),
    ok = filelib:ensure_dir(filename:join(StageDir, ".")),
    ok = copy_dir(LibDir, AppCopyDir, ["ebin", "priv"]),
    %% TODO: if the plugin declares third-party deps in its rebar.config
    %% that are not already shipped with EMQX, copy them into the stage
    %% tree under `<dep>-<vsn>/ebin`. The dep list is the plugin .app's
    %% `applications` property minus:
    %%   - OTP apps (kernel, stdlib, sasl, ssl, crypto, ...),
    %%   - apps already in the built release's `_build/$PROFILE/rel/.../lib/`.
    %% `rebar3 emqx_plugrel tar` does this; no current plugin needs it.
    ok = copy_extra_deps(Root, Profile, LibDir, StageDir, AppStr),
    %% Include README.md at the top level of the tarball if present.
    copy_if_exists(filename:join(PluginDir, "README.md"), filename:join(StageDir, "README.md")),
    Info = build_info(Metadata, AppStr, RelVsn, AppVsn, PluginDir),
    ok = file:write_file(filename:join(StageDir, "release.json"), encode_json(Info)),
    OutDir = filename:join([Root, "_build", "plugins"]),
    ok = filelib:ensure_dir(filename:join(OutDir, ".")),
    TarPath = filename:join(OutDir, NameWithRelVsn ++ ".tar.gz"),
    ShaPath = filename:join(OutDir, NameWithRelVsn ++ ".sha256"),
    ok = create_tar(StageRoot, NameWithRelVsn, TarPath),
    {ok, TarBin} = file:read_file(TarPath),
    ok = file:write_file(ShaPath, binary:encode_hex(crypto:hash(sha256, TarBin), lowercase)),
    rm_rf(StageRoot),
    io:format("Packaged ~ts~n", [TarPath]),
    ok.

root_dir() ->
    case os:getenv("ROOT_DIR") of
        false ->
            ScriptDir = filename:dirname(escript:script_name()),
            filename:absname(filename:join(ScriptDir, ".."));
        V ->
            V
    end.

os_env(Name, Default) ->
    case os:getenv(Name) of
        false -> Default;
        "" -> Default;
        V -> V
    end.

assert_dir(Dir, What) ->
    case filelib:is_dir(Dir) of
        true ->
            ok;
        false ->
            io:format(standard_error, "Error: missing ~s: ~ts~n", [What, Dir]),
            halt(1)
    end.

read_file_trim(Path) ->
    {ok, Bin} = file:read_file(Path),
    string:trim(binary_to_list(Bin)).

read_app_vsn(AppFile) ->
    {ok, [{application, _, Props}]} = file:consult(AppFile),
    {vsn, Vsn} = lists:keyfind(vsn, 1, Props),
    Vsn.

read_rebar_config(Path) ->
    {ok, Terms} = file:consult(Path),
    Terms.

rm_rf(Path) ->
    case filelib:is_dir(Path) of
        true ->
            {ok, Entries} = file:list_dir(Path),
            [rm_rf(filename:join(Path, E)) || E <- Entries],
            ok = file:del_dir(Path);
        false ->
            case filelib:is_file(Path) of
                true -> ok = file:delete(Path);
                false -> ok
            end
    end.

copy_dir(From, To) -> copy_dir(From, To, all).

%% Only top-level filter: copy only `Filter` entries from `From`, then
%% copy recursively from there. `all` means copy everything.
copy_dir(From, To, Filter) ->
    ok = filelib:ensure_dir(filename:join(To, ".")),
    {ok, Entries} = file:list_dir(From),
    Selected =
        case Filter of
            all -> Entries;
            L when is_list(L) -> [E || E <- Entries, lists:member(E, L)]
        end,
    lists:foreach(
        fun(E) ->
            Src = filename:join(From, E),
            Dst = filename:join(To, E),
            case filelib:is_dir(Src) of
                true ->
                    copy_dir(Src, Dst);
                false ->
                    {ok, _} = file:copy(Src, Dst),
                    ok
            end
        end,
        Selected
    ),
    ok.

copy_if_exists(Src, Dst) ->
    case filelib:is_regular(Src) of
        true ->
            {ok, _} = file:copy(Src, Dst),
            ok;
        false ->
            ok
    end.

%% Placeholder: no current plugin ships extra deps. When one does,
%% compute the list of apps to bundle from the plugin `.app`'s
%% `applications` property, filter out OTP apps and apps already in
%% the EMQX release, and copy each dep's compiled beams to
%% `<StageDir>/<dep>-<vsn>/ebin`.
copy_extra_deps(_Root, _Profile, _LibDir, _StageDir, _AppStr) ->
    ok.

build_info(Metadata, AppStr, RelVsn, AppVsn, PluginDir) ->
    Base = maps:from_list([{K, normalize(V)} || {K, V} <- Metadata]),
    Extra = #{
        name => list_to_binary(AppStr),
        rel_vsn => list_to_binary(RelVsn),
        rel_apps => [list_to_binary(AppStr ++ "-" ++ AppVsn)],
        git_ref => git_ref(PluginDir),
        git_commit_or_build_date => build_date(PluginDir),
        metadata_vsn => ?METADATA_VSN,
        built_on_otp_release => list_to_binary(erlang:system_info(otp_release)),
        with_config_schema =>
            filelib:is_regular(filename:join([PluginDir, "priv", "config_schema.avsc"])),
        hidden => proplists:get_value(hidden, Metadata, false)
    },
    maps:merge(Base, Extra).

git_ref(Dir) ->
    case sh(Dir, "git rev-parse HEAD") of
        {ok, S} -> list_to_binary(S);
        error -> <<"unknown">>
    end.

build_date(Dir) ->
    case sh(Dir, "git log -1 --pretty=format:%cd --date=format:%Y-%m-%d") of
        {ok, S} ->
            list_to_binary(S);
        error ->
            {Y, M, D} = erlang:date(),
            iolist_to_binary(io_lib:format("~4..0b-~2..0b-~2..0b", [Y, M, D]))
    end.

sh(Dir, Cmd) ->
    Full = "cd " ++ Dir ++ " && " ++ Cmd ++ " 2>/dev/null",
    case os:cmd(Full) of
        "" -> error;
        Out -> {ok, string:trim(Out, trailing, "\n ")}
    end.

%% Convert Erlang strings to binaries; leave atoms, maps, and everything
%% else for jsone to encode natively. Nested proplists are kept as-is;
%% jsone treats `[{K, V} | _]` as a JSON object.
normalize(L) when is_list(L) ->
    case io_lib:printable_unicode_list(L) of
        true -> list_to_binary(L);
        false -> [normalize(E) || E <- L]
    end;
normalize({K, V}) ->
    {K, normalize(V)};
normalize(V) ->
    V.

encode_json(Term) ->
    jsone:encode(Term, [
        native_forward_slash,
        {indent, 4},
        {space, 1},
        {object_key_type, scalar}
    ]).

create_tar(StageRoot, NameWithRelVsn, TarPath) ->
    {ok, OldCwd} = file:get_cwd(),
    try
        ok = file:set_cwd(StageRoot),
        Files = filelib:wildcard(NameWithRelVsn ++ "/*"),
        erl_tar:create(TarPath, Files, [compressed])
    after
        file:set_cwd(OldCwd)
    end.
