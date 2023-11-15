#!/usr/bin/env escript
%%! -noshell -noinput
%% -*- mode: erlang;erlang-indent-level: 4;indent-tabs-mode: nil -*-
%% ex: ft=erlang ts=4 sw=4 et

-define(TIMEOUT, 300000).
-define(INFO(Fmt, Args), io:format(standard_io, Fmt ++ "~n", Args)).
-define(ERROR(Fmt, Args), io:format(standard_error, "ERROR: " ++ Fmt ++ "~n", Args)).
-define(SEMVER_RE,
    <<"^(0|[1-9]\\d*)\\.(0|[1-9]\\d*)\\.(0|[1-9]\\d*)(-[a-zA-Z\\d][-a-zA-Z.\\d]*)?(\\+[a-zA-Z\\d][-a-zA-Z.\\d]*)?$">>
).

-mode(compile).

main([Command0, DistInfoStr | CommandArgs]) ->
    %% convert the distribution info arguments string to an erlang term
    {ok, Tokens, _} = erl_scan:string(DistInfoStr ++ "."),
    {ok, DistInfo} = erl_parse:parse_term(Tokens),
    %% convert arguments into a proplist
    Opts = parse_arguments(CommandArgs),
    %% invoke the command passed as argument
    F =
        case Command0 of
            "install" -> fun(A, B) -> install(A, B) end;
            "unpack" -> fun(A, B) -> unpack(A, B) end;
            "upgrade" -> fun(A, B) -> upgrade(A, B) end;
            "downgrade" -> fun(A, B) -> downgrade(A, B) end;
            "uninstall" -> fun(A, B) -> uninstall(A, B) end;
            "versions" -> fun(A, B) -> versions(A, B) end
        end,
    F(DistInfo, Opts);
main(Args) ->
    ?INFO("unknown args: ~p", [Args]),
    erlang:halt(1).

unpack({RelName, NameTypeArg, NodeName, Cookie}, Opts) ->
    TargetNode = start_distribution(NodeName, NameTypeArg, Cookie),
    Version = proplists:get_value(version, Opts),
    case unpack_release(RelName, TargetNode, Version, Opts) of
        {ok, Vsn} ->
            ?INFO("Unpacked successfully: ~p", [Vsn]);
        old ->
            %% no need to unpack, has been installed previously
            ?INFO("Release ~s is marked old.", [Version]);
        unpacked ->
            ?INFO("Release ~s is already unpacked.", [Version]);
        current ->
            ?INFO("Release ~s is already installed and current.", [Version]);
        permanent ->
            ?INFO("Release ~s is already installed and set permanent.", [Version]);
        {error, Reason} ->
            ?INFO("Unpack failed: ~p.", [Reason]),
            print_existing_versions(TargetNode),
            erlang:halt(2)
    end;
unpack(_, Args) ->
    ?INFO("unpack: unknown args ~p", [Args]).

install({RelName, NameTypeArg, NodeName, Cookie}, Opts) ->
    TargetNode = start_distribution(NodeName, NameTypeArg, Cookie),
    Version = proplists:get_value(version, Opts),
    validate_target_version(Version, TargetNode),
    case unpack_release(RelName, TargetNode, Version, Opts) of
        {ok, Vsn} ->
            ?INFO("Unpacked successfully: ~p.", [Vsn]),
            check_and_install(TargetNode, Vsn),
            maybe_permafy(TargetNode, RelName, Vsn, Opts);
        old ->
            %% no need to unpack, has been installed previously
            ?INFO("Release ~s is marked old, switching to it.", [Version]),
            check_and_install(TargetNode, Version),
            maybe_permafy(TargetNode, RelName, Version, Opts);
        unpacked ->
            ?INFO("Release ~s is already unpacked, now installing.", [Version]),
            check_and_install(TargetNode, Version),
            maybe_permafy(TargetNode, RelName, Version, Opts);
        current ->
            case proplists:get_value(permanent, Opts, true) of
                true ->
                    ?INFO(
                        "Release ~s is already installed and current, making permanent.",
                        [Version]
                    ),
                    permafy(TargetNode, RelName, Version);
                false ->
                    ?INFO(
                        "Release ~s is already installed and current.",
                        [Version]
                    )
            end;
        permanent ->
            %% this release is marked permanent, however it might not the
            %% one currently running
            case current_release_version(TargetNode) of
                Version ->
                    ?INFO(
                        "Release ~s is already installed, running and set permanent.",
                        [Version]
                    );
                CurrentVersion ->
                    ?INFO(
                        "Release ~s is the currently running version.",
                        [CurrentVersion]
                    ),
                    check_and_install(TargetNode, Version),
                    maybe_permafy(TargetNode, RelName, Version, Opts)
            end;
        {error, Reason} ->
            ?INFO("Unpack failed: ~p", [Reason]),
            print_existing_versions(TargetNode),
            erlang:halt(2)
    end;
install(_, Args) ->
    ?INFO("install: unknown args ~p", [Args]).

upgrade(DistInfo, Args) ->
    install(DistInfo, Args).

downgrade(DistInfo, Args) ->
    install(DistInfo, Args).

uninstall({_RelName, NameTypeArg, NodeName, Cookie}, Opts) ->
    TargetNode = start_distribution(NodeName, NameTypeArg, Cookie),
    WhichReleases = which_releases(TargetNode),
    Version = proplists:get_value(version, Opts),
    case proplists:get_value(Version, WhichReleases) of
        undefined ->
            ?INFO("Release ~s is already uninstalled.", [Version]);
        old ->
            ?INFO("Release ~s is marked old, uninstalling it.", [Version]),
            remove_release(TargetNode, Version);
        unpacked ->
            ?INFO(
                "Release ~s is marked unpacked, uninstalling it",
                [Version]
            ),
            remove_release(TargetNode, Version);
        current ->
            ?INFO("Uninstall failed: Release ~s is marked current.", [Version]),
            erlang:halt(2);
        permanent ->
            ?INFO("Uninstall failed: Release ~s is running.", [Version]),
            erlang:halt(2)
    end;
uninstall(_, Args) ->
    ?INFO("uninstall: unknown args ~p", [Args]).

versions({_RelName, NameTypeArg, NodeName, Cookie}, _Opts) ->
    TargetNode = start_distribution(NodeName, NameTypeArg, Cookie),
    print_existing_versions(TargetNode).

parse_arguments(Args) ->
    IsEnterprise = os:getenv("IS_ENTERPRISE") == "yes",
    parse_arguments(Args, [{is_enterprise, IsEnterprise}]).

parse_arguments([], Acc) ->
    Acc;
parse_arguments(["--no-permanent" | Rest], Acc) ->
    parse_arguments(Rest, [{permanent, false}] ++ Acc);
parse_arguments([VersionStr | Rest], Acc) ->
    Version = parse_version(VersionStr),
    parse_arguments(Rest, [{version, Version}] ++ Acc).

unpack_release(RelName, TargetNode, Version, Opts) ->
    StartScriptExists = filelib:is_regular(filename:join(["releases", Version, "start.boot"])),
    WhichReleases = which_releases(TargetNode),
    IsEnterprise = proplists:get_value(is_enterprise, Opts),
    case proplists:get_value(Version, WhichReleases) of
        Res when Res =:= undefined; (Res =:= unpacked andalso not StartScriptExists) ->
            %% not installed, so unpack tarball:
            %% look for a release package with the intended version in the following order:
            %%      releases/<relname>-<version>.tar.gz
            %%      releases/<version>/<relname>-<version>.tar.gz
            %%      releases/<version>/<relname>.tar.gz
            case find_and_link_release_package(Version, RelName, IsEnterprise) of
                {_, undefined} ->
                    {error, release_package_not_found};
                {ReleasePackage, ReleasePackageLink} ->
                    ?INFO(
                        "Release ~s not found, attempting to unpack ~s",
                        [Version, ReleasePackage]
                    ),
                    case
                        rpc:call(
                            TargetNode,
                            release_handler,
                            unpack_release,
                            [ReleasePackageLink],
                            ?TIMEOUT
                        )
                    of
                        {ok, Vsn} ->
                            {ok, Vsn};
                        {error, {existing_release, Vsn}} ->
                            %% sometimes the user may have removed the release/<vsn> dir
                            %% for an `unpacked` release, then we need to re-unpack it from
                            %% the .tar ball
                            untar_for_unpacked_release(str(RelName), Vsn),
                            {ok, Vsn};
                        {error, _} = Error ->
                            Error
                    end
            end;
        Other ->
            Other
    end.

untar_for_unpacked_release(RelName, Vsn) ->
    {ok, Root} = file:get_cwd(),
    RelDir = filename:join([Root, "releases"]),
    %% untar the .tar file, so release/<vsn> will be created
    Tar = filename:join([RelDir, Vsn, RelName ++ ".tar.gz"]),
    extract_tar(Root, Tar),

    %% create RELEASE file
    RelFile = filename:join([RelDir, Vsn, RelName ++ ".rel"]),
    release_handler:create_RELEASES(Root, RelFile),

    %% Clean release
    _ = file:delete(Tar),
    _ = file:delete(RelFile).

extract_tar(Cwd, Tar) ->
    case erl_tar:extract(Tar, [keep_old_files, {cwd, Cwd}, compressed]) of
        ok -> ok;
        % New erl_tar (R3A).
        {error, {Name, Reason}} -> throw({error, {cannot_extract_file, Name, Reason}})
    end.

%% 1. look for a release package tarball with the provided version:
%%      releases/<relname>-*<version>*.tar.gz
%% 2. create a symlink from a fixed location (ie. releases/<version>/<relname>.tar.gz)
%%    to the release package tarball found in 1.
%% 3. return a tuple with the paths to the release package and
%%    to the symlink that is to be provided to release handler
find_and_link_release_package(Version, RelName, IsEnterprise) ->
    RelNameStr = atom_to_list(RelName),
    %% regardless of the location of the release package, we'll
    %% always give release handler the same path which is the symlink
    %% the path to the package link is relative to "releases/" because
    %% that's what release handler is expecting
    ReleaseHandlerPackageLink = filename:join(Version, RelNameStr),
    %% this is the symlink name we'll create once
    %% we've found where the actual release package is located
    ReleaseLink = filename:join([
        "releases",
        Version,
        RelNameStr ++ ".tar.gz"
    ]),
    ReleaseNamePattern =
        case IsEnterprise of
            false -> RelNameStr;
            true -> RelNameStr ++ "-enterprise"
        end,
    FilePattern = lists:flatten([ReleaseNamePattern, "-", Version, "*.tar.gz"]),
    TarBalls = filename:join(["releases", FilePattern]),
    case filelib:wildcard(TarBalls) of
        [] ->
            {undefined, undefined};
        [Filename] when is_list(Filename) ->
            %% the release handler expects a fixed nomenclature (<relname>.tar.gz)
            %% so give it just that by creating a symlink to the tarball
            %% we found.
            %% make sure that the dir where we're creating the link in exists
            ok = filelib:ensure_dir(filename:join([filename:dirname(ReleaseLink), "dummy"])),
            %% create the symlink pointing to the full path name of the
            %% release package we found
            make_symlink_or_copy(filename:absname(Filename), ReleaseLink),
            {Filename, ReleaseHandlerPackageLink};
        Files ->
            ?ERROR(
                "Found more than one package for version: '~s', "
                "files: ~p",
                [Version, Files]
            ),
            erlang:halt(47)
    end.

make_symlink_or_copy(Filename, ReleaseLink) ->
    case file:make_symlink(Filename, ReleaseLink) of
        ok ->
            ok;
        {error, eexist} ->
            ?INFO("Symlink ~p already exists, recreate it", [ReleaseLink]),
            ok = file:delete(ReleaseLink),
            make_symlink_or_copy(Filename, ReleaseLink);
        {error, Reason} when Reason =:= eperm; Reason =:= enotsup ->
            {ok, _} = file:copy(Filename, ReleaseLink);
        {error, Reason} ->
            ?ERROR("Create symlink ~p failed, error: ~p", [ReleaseLink, Reason]),
            erlang:halt(47)
    end.

parse_version(V) when is_list(V) ->
    hd(string:tokens(V, "/")).

check_and_install(TargetNode, Vsn) ->
    %% Backup the sys.config, this will be used when we check and install release
    %% NOTE: We cannot backup the old sys.config directly, because the
    %% configs for plugins are only in app-envs, not in the old sys.config
    Configs0 =
        [
            {AppName, rpc:call(TargetNode, application, get_all_env, [AppName], ?TIMEOUT)}
         || {AppName, _, _} <- rpc:call(TargetNode, application, which_applications, [], ?TIMEOUT)
        ],
    Configs1 = [{AppName, Conf} || {AppName, Conf} <- Configs0, Conf =/= []],
    ok = file:write_file(
        filename:join(["releases", Vsn, "sys.config"]), io_lib:format("~p.", [Configs1])
    ),

    %% check and install release
    case
        rpc:call(
            TargetNode,
            release_handler,
            check_install_release,
            [Vsn],
            ?TIMEOUT
        )
    of
        {ok, _OtherVsn, _Desc} ->
            ok;
        {error, Reason} ->
            ?ERROR("Call release_handler:check_install_release failed: ~p.", [Reason]),
            erlang:halt(3)
    end,
    case
        rpc:call(
            TargetNode,
            release_handler,
            install_release,
            [Vsn, [{update_paths, true}]],
            ?TIMEOUT
        )
    of
        {ok, _, _} ->
            ?INFO("Installed Release: ~s.", [Vsn]),
            ok;
        {error, {no_such_release, Vsn}} ->
            VerList =
                iolist_to_binary(
                    [io_lib:format("* ~s\t~s~n", [V, S]) || {V, S} <- which_releases(TargetNode)]
                ),
            ?INFO("Installed versions:~n~s", [VerList]),
            ?ERROR("Unable to revert to '~s' - not installed.", [Vsn]),
            erlang:halt(2);
        %% as described in http://erlang.org/doc/man/appup.html, when performing a relup
        %% with soft purge:
        %%      If the value is soft_purge, release_handler:install_release/1
        %%      returns {error,{old_processes,Mod}}
        {error, {old_processes, Mod}} ->
            ?ERROR(
                "Unable to install '~s' - old processes still running code from module ~p",
                [Vsn, Mod]
            ),
            erlang:halt(3);
        {error, Reason1} ->
            ?ERROR("Call release_handler:install_release failed: ~p", [Reason1]),
            erlang:halt(4)
    end.

maybe_permafy(TargetNode, RelName, Vsn, Opts) ->
    case proplists:get_value(permanent, Opts, true) of
        true ->
            permafy(TargetNode, RelName, Vsn);
        false ->
            ok
    end.

permafy(TargetNode, RelName, Vsn) ->
    RelNameStr = atom_to_list(RelName),
    ok = rpc:call(
        TargetNode,
        release_handler,
        make_permanent,
        [Vsn],
        ?TIMEOUT
    ),
    ?INFO("Made release permanent: ~p", [Vsn]),
    %% upgrade/downgrade the scripts by replacing them
    Scripts = [RelNameStr, RelNameStr ++ "_ctl", "nodetool", "install_upgrade.escript"],
    [
        {ok, _} = file:copy(
            filename:join(["bin", File ++ "-" ++ Vsn]),
            filename:join(["bin", File])
        )
     || File <- Scripts
    ],
    %% update the vars
    UpdatedVars = io_lib:format("REL_VSN=\"~s\"~nERTS_VSN=\"~s\"~n", [Vsn, erts_vsn()]),
    file:write_file(filename:absname(filename:join(["releases", "emqx_vars"])), UpdatedVars, [
        append
    ]).

remove_release(TargetNode, Vsn) ->
    case rpc:call(TargetNode, release_handler, remove_release, [Vsn], ?TIMEOUT) of
        ok ->
            ?INFO("Uninstalled Release: ~s", [Vsn]),
            ok;
        {error, Reason} ->
            ?ERROR("Call release_handler:remove_release failed: ~p", [Reason]),
            erlang:halt(3)
    end.

which_releases(TargetNode) ->
    R = rpc:call(TargetNode, release_handler, which_releases, [], ?TIMEOUT),
    [{V, S} || {_, V, _, S} <- R].

%% the running release version is either the only one marked `current´
%% or, if none exists, the one marked `permanent`
current_release_version(TargetNode) ->
    R = rpc:call(
        TargetNode,
        release_handler,
        which_releases,
        [],
        ?TIMEOUT
    ),
    Versions = [{S, V} || {_, V, _, S} <- R],
    %% current version takes priority over the permanent
    proplists:get_value(
        current,
        Versions,
        proplists:get_value(permanent, Versions)
    ).

print_existing_versions(TargetNode) ->
    VerList = iolist_to_binary([
        io_lib:format("* ~s\t~s~n", [V, S])
     || {V, S} <- which_releases(TargetNode)
    ]),
    ?INFO("Installed versions:~n~s", [VerList]).

start_distribution(TargetNode, NameTypeArg, Cookie) ->
    MyNode = make_script_node(TargetNode),
    {ok, _Pid} = net_kernel:start([MyNode, get_name_type(NameTypeArg)]),
    erlang:set_cookie(node(), Cookie),
    case {net_kernel:hidden_connect_node(TargetNode), net_adm:ping(TargetNode)} of
        {true, pong} ->
            ok;
        {_, pang} ->
            ?INFO("Node ~p not responding to pings.", [TargetNode]),
            erlang:halt(1)
    end,
    {ok, Cwd} = file:get_cwd(),
    ok = rpc:call(TargetNode, file, set_cwd, [Cwd], ?TIMEOUT),
    TargetNode.

make_script_node(Node) ->
    [Name, Host] = string:tokens(atom_to_list(Node), "@"),
    list_to_atom(lists:concat(["remsh_", Name, "_upgrader_", os:getpid(), "@", Host])).

%% get name type from arg
get_name_type(NameTypeArg) ->
    case NameTypeArg of
        "-sname" ->
            shortnames;
        _ ->
            longnames
    end.

erts_vsn() ->
    {ok, Str} = file:read_file(filename:join(["releases", "start_erl.data"])),
    [ErtsVsn, _] = string:tokens(binary_to_list(Str), " "),
    ErtsVsn.

validate_target_version(TargetVersion, TargetNode) ->
    CurrentVersion = current_release_version(TargetNode),
    case {get_major_minor_vsn(CurrentVersion), get_major_minor_vsn(TargetVersion)} of
        {{Major, Minor}, {Major, Minor}} ->
            ok;
        _ ->
            ?ERROR(
                "Cannot upgrade/downgrade from '~s' to '~s'~n"
                "Hot upgrade is only supported between patch releases.",
                [CurrentVersion, TargetVersion]
            ),
            erlang:halt(48)
    end.

get_major_minor_vsn(Version) ->
    Parts = parse_semver(Version),
    [Major | Rem0] = Parts,
    [Minor | _Rem1] = Rem0,
    {Major, Minor}.

parse_semver(Version) ->
    case re:run(Version, ?SEMVER_RE, [{capture, all_but_first, binary}]) of
        {match, Parts} ->
            Parts;
        nomatch ->
            ?ERROR("Invalid semantic version: '~s'~n", [Version]),
            erlang:halt(22)
    end.

str(A) when is_atom(A) ->
    atom_to_list(A);
str(A) when is_binary(A) ->
    binary_to_list(A);
str(A) when is_list(A) ->
    (A).
