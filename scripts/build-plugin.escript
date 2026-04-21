#!/usr/bin/env escript
%% -*- mode: erlang -*-
%%! -noshell

%% Package an EMQX monorepo plugin as `<name>-<vsn>.tar.gz`, reading the
%% plugin's already-compiled beams from the umbrella build tree
%% (`_build/$PROFILE/lib/<name>/`).
%%
%% This replicates the layout produced by `rebar3 emqx_plugrel tar` but
%% without requiring the plugin to have its own rebar3 project with a
%% dep closure. The plugin is compiled once by the root `make`; this
%% script just packages the output.
%%
%% Usage: build-plugin.escript <plugin-name>
%% Env: PROFILE (default: emqx-enterprise), ROOT_DIR (default: script parent)

-mode(compile).

-define(METADATA_VSN, <<"0.2.0">>).
-define(EMQX_PLUGREL_VSN, <<"0.5.1">>).

main([App]) ->
    try
        do(App)
    catch
        error:Reason:St ->
            io:format(standard_error, "build-plugin: ~p~n~p~n", [Reason, St]),
            halt(1)
    end;
main(_) ->
    io:format(standard_error, "Usage: build-plugin.escript <plugin-name>~n", []),
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
    %% Include README.md at the top level of the tarball if present.
    copy_if_exists(filename:join(PluginDir, "README.md"), filename:join(StageDir, "README.md")),
    Info = build_info(Metadata, AppStr, RelVsn, AppVsn, PluginDir),
    ok = file:write_file(filename:join(StageDir, "release.json"), encode_json(Info, 0)),
    OutDir = filename:join([Root, "_build", "plugins"]),
    ok = filelib:ensure_dir(filename:join(OutDir, ".")),
    TarPath = filename:join(OutDir, NameWithRelVsn ++ ".tar.gz"),
    ShaPath = filename:join(OutDir, NameWithRelVsn ++ ".sha256"),
    ok = create_tar(StageRoot, NameWithRelVsn, TarPath),
    {ok, TarBin} = file:read_file(TarPath),
    ok = file:write_file(ShaPath, hexdigest(sha256, TarBin)),
    rm_rf(StageRoot),
    io:format("Packaged ~ts~n", [TarPath]),
    ok.

root_dir() ->
    case os:getenv("ROOT_DIR") of
        false ->
            ScriptDir = filename:dirname(escript:script_name()),
            filename:absname(filename:join(ScriptDir, ".."));
        V -> V
    end.

os_env(Name, Default) ->
    case os:getenv(Name) of
        false -> Default;
        "" -> Default;
        V -> V
    end.

assert_dir(Dir, What) ->
    case filelib:is_dir(Dir) of
        true -> ok;
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
                true -> copy_dir(Src, Dst);
                false ->
                    {ok, _} = file:copy(Src, Dst),
                    ok
            end
        end, Selected),
    ok.

copy_if_exists(Src, Dst) ->
    case filelib:is_regular(Src) of
        true -> {ok, _} = file:copy(Src, Dst), ok;
        false -> ok
    end.

build_info(Metadata, AppStr, RelVsn, AppVsn, PluginDir) ->
    BaseInfo = lists:foldl(
        fun({K, V}, Acc) -> [{atom_to_binary(K, utf8), erl_to_json(V)} | Acc] end,
        [], Metadata),
    MoreInfo = [
        {<<"name">>, list_to_binary(AppStr)},
        {<<"rel_vsn">>, list_to_binary(RelVsn)},
        {<<"rel_apps">>, [list_to_binary(AppStr ++ "-" ++ AppVsn)]},
        {<<"git_ref">>, git_ref(PluginDir)},
        {<<"git_commit_or_build_date">>, build_date(PluginDir)},
        {<<"metadata_vsn">>, ?METADATA_VSN},
        {<<"built_on_otp_release">>, list_to_binary(erlang:system_info(otp_release))},
        {<<"with_config_schema">>, filelib:is_regular(filename:join([PluginDir, "priv", "config_schema.avsc"]))},
        {<<"emqx_plugrel_vsn">>, ?EMQX_PLUGREL_VSN},
        {<<"hidden">>, proplists:get_value(hidden, Metadata, false)}
    ],
    Merged = lists:foldl(
        fun({K, _} = Kv, Acc) -> lists:keystore(K, 1, Acc, Kv) end,
        BaseInfo, MoreInfo),
    lists:reverse(Merged).

git_ref(Dir) ->
    case sh(Dir, "git rev-parse HEAD") of
        {ok, S} -> list_to_binary(S);
        error -> <<"unknown">>
    end.

build_date(Dir) ->
    case sh(Dir, "git log -1 --pretty=format:%cd --date=format:%Y-%m-%d") of
        {ok, S} -> list_to_binary(S);
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

erl_to_json(true) -> true;
erl_to_json(false) -> false;
erl_to_json(V) when is_atom(V) -> atom_to_binary(V, utf8);
erl_to_json(V) when is_binary(V) -> V;
erl_to_json(V) when is_integer(V); is_float(V) -> V;
erl_to_json([{_, _} | _] = Pl) ->
    case is_proplist(Pl) of
        true -> [{atom_or_bin(K), erl_to_json(V)} || {K, V} <- Pl];
        false -> [erl_to_json(E) || E <- Pl]
    end;
erl_to_json(V) when is_list(V) ->
    case io_lib:printable_unicode_list(V) of
        true -> list_to_binary(V);
        false -> [erl_to_json(E) || E <- V]
    end;
erl_to_json({K, Val}) -> [{atom_or_bin(K), erl_to_json(Val)}];
erl_to_json(V) -> V.

is_proplist(L) ->
    lists:all(fun({K, _}) when is_atom(K); is_binary(K); is_list(K) -> true;
                 (_) -> false end, L).

atom_or_bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
atom_or_bin(B) when is_binary(B) -> B;
atom_or_bin(L) when is_list(L) -> list_to_binary(L).

encode_json(V, Indent) ->
    iolist_to_binary(encode(V, Indent)).

encode(null, _) -> <<"null">>;
encode(true, _) -> <<"true">>;
encode(false, _) -> <<"false">>;
encode(V, _) when is_integer(V) -> integer_to_binary(V);
encode(V, _) when is_float(V) -> float_to_binary(V, [{decimals, 6}, compact]);
encode(V, _) when is_binary(V) -> [$", escape(V), $"];
encode([{_, _} | _] = Obj, I) ->
    Pad = spaces(I + 4),
    EndPad = spaces(I),
    Items = [[Pad, encode(atom_or_bin(K), I + 4), <<": ">>, encode(V, I + 4)]
             || {K, V} <- Obj],
    [<<"{\n">>, join(Items, <<",\n">>), <<"\n">>, EndPad, <<"}">>];
encode([], _) -> <<"[]">>;
encode(L, I) when is_list(L) ->
    Pad = spaces(I + 4),
    EndPad = spaces(I),
    Items = [[Pad, encode(V, I + 4)] || V <- L],
    [<<"[\n">>, join(Items, <<",\n">>), <<"\n">>, EndPad, <<"]">>];
encode(A, I) when is_atom(A) -> encode(atom_to_binary(A, utf8), I).

escape(Bin) ->
    binary:replace(binary:replace(Bin, <<"\\">>, <<"\\\\">>, [global]),
                   <<"\"">>, <<"\\\"">>, [global]).

spaces(N) -> lists:duplicate(N, $\s).

join([], _Sep) -> [];
join([H], _Sep) -> [H];
join([H | T], Sep) -> [H, Sep | join(T, Sep)].

create_tar(StageRoot, NameWithRelVsn, TarPath) ->
    {ok, OldCwd} = file:get_cwd(),
    try
        ok = file:set_cwd(StageRoot),
        Files = filelib:wildcard(NameWithRelVsn ++ "/*"),
        erl_tar:create(TarPath, Files, [compressed])
    after
        file:set_cwd(OldCwd)
    end.

hexdigest(Alg, Bin) ->
    Digest = crypto:hash(Alg, Bin),
    [io_lib:format("~2.16.0b", [B]) || <<B:8>> <= Digest].
