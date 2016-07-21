#!/usr/bin/env escript
%%! -noshell -noinput
%% -*- mode: erlang;erlang-indent-level: 4;indent-tabs-mode: nil -*-
%% ex: ft=erlang ts=4 sw=4 et

-define(TIMEOUT, 300000).
-define(INFO(Fmt,Args), io:format(Fmt,Args)).

%% Unpack or upgrade to a new tar.gz release
main(["unpack", RelName, NameTypeArg, NodeName, Cookie, VersionArg]) ->
    TargetNode = start_distribution(NodeName, NameTypeArg, Cookie),
    WhichReleases = which_releases(TargetNode),
    Version = parse_version(VersionArg),
    case proplists:get_value(Version, WhichReleases) of
        undefined ->
            %% not installed, so unpack tarball:
            ?INFO("Release ~s not found, attempting to unpack releases/~s/~s.tar.gz~n",[Version,Version,RelName]),
            ReleasePackage = Version ++ "/" ++ RelName,
            case rpc:call(TargetNode, release_handler, unpack_release,
                          [ReleasePackage], ?TIMEOUT) of
                {ok, Vsn} ->
                    ?INFO("Unpacked successfully: ~p~n", [Vsn]);
                {error, UnpackReason} ->
                    print_existing_versions(TargetNode),
                    ?INFO("Unpack failed: ~p~n",[UnpackReason]),
                    erlang:halt(2)
            end;
        old ->
            %% no need to unpack, has been installed previously
            ?INFO("Release ~s is marked old, switching to it.~n",[Version]);
        unpacked ->
            ?INFO("Release ~s is already unpacked, now installing.~n",[Version]);
        current ->
            ?INFO("Release ~s is already installed and current. Making permanent.~n",[Version]);
        permanent ->
            ?INFO("Release ~s is already installed, and set permanent.~n",[Version])
    end;
main(["install", RelName, NameTypeArg, NodeName, Cookie, VersionArg]) ->
    TargetNode = start_distribution(NodeName, NameTypeArg, Cookie),
    WhichReleases = which_releases(TargetNode),
    Version = parse_version(VersionArg),
    case proplists:get_value(Version, WhichReleases) of
        undefined ->
            %% not installed, so unpack tarball:
            ?INFO("Release ~s not found, attempting to unpack releases/~s/~s.tar.gz~n",[Version,Version,RelName]),
            ReleasePackage = Version ++ "/" ++ RelName,
            case rpc:call(TargetNode, release_handler, unpack_release,
                          [ReleasePackage], ?TIMEOUT) of
                {ok, Vsn} ->
                    ?INFO("Unpacked successfully: ~p~n", [Vsn]),
                    install_and_permafy(TargetNode, RelName, Vsn);
                {error, UnpackReason} ->
                    print_existing_versions(TargetNode),
                    ?INFO("Unpack failed: ~p~n",[UnpackReason]),
                    erlang:halt(2)
            end;
        old ->
            %% no need to unpack, has been installed previously
            ?INFO("Release ~s is marked old, switching to it.~n",[Version]),
            install_and_permafy(TargetNode, RelName, Version);
        unpacked ->
            ?INFO("Release ~s is already unpacked, now installing.~n",[Version]),
            install_and_permafy(TargetNode, RelName, Version);
        current -> %% installed and in-use, just needs to be permanent
            ?INFO("Release ~s is already installed and current. Making permanent.~n",[Version]),
            permafy(TargetNode, RelName, Version);
        permanent ->
            ?INFO("Release ~s is already installed, and set permanent.~n",[Version])
    end;
main(_) ->
    erlang:halt(1).

parse_version(V) when is_list(V) ->
    hd(string:tokens(V,"/")).

install_and_permafy(TargetNode, RelName, Vsn) ->
    case rpc:call(TargetNode, release_handler, check_install_release, [Vsn], ?TIMEOUT) of
        {ok, _OtherVsn, _Desc} ->
            ok;
        {error, Reason} ->
            ?INFO("ERROR: release_handler:check_install_release failed: ~p~n",[Reason]),
            erlang:halt(3)
    end,
    case rpc:call(TargetNode, release_handler, install_release, [Vsn], ?TIMEOUT) of
        {ok, _, _} ->
            ?INFO("Installed Release: ~s~n", [Vsn]),
            permafy(TargetNode, RelName, Vsn),
            ok;
        {error, {no_such_release, Vsn}} ->
            VerList =
                iolist_to_binary(
                    [io_lib:format("* ~s\t~s~n",[V,S]) ||  {V,S} <- which_releases(TargetNode)]),
            ?INFO("Installed versions:~n~s", [VerList]),
            ?INFO("ERROR: Unable to revert to '~s' - not installed.~n", [Vsn]),
            erlang:halt(2)
    end.

permafy(TargetNode, RelName, Vsn) ->
    ok = rpc:call(TargetNode, release_handler, make_permanent, [Vsn], ?TIMEOUT),
    file:copy(filename:join(["bin", RelName++"-"++Vsn]),
              filename:join(["bin", RelName])),
    ?INFO("Made release permanent: ~p~n", [Vsn]),
    ok.

which_releases(TargetNode) ->
    R = rpc:call(TargetNode, release_handler, which_releases, [], ?TIMEOUT),
    [ {V, S} ||  {_,V,_, S} <- R ].

print_existing_versions(TargetNode) ->
    VerList = iolist_to_binary([
            io_lib:format("* ~s\t~s~n",[V,S])
            ||  {V,S} <- which_releases(TargetNode) ]),
    ?INFO("Installed versions:~n~s", [VerList]).

start_distribution(NodeName, NameTypeArg, Cookie) ->
    MyNode = make_script_node(NodeName),
    {ok, _Pid} = net_kernel:start([MyNode, get_name_type(NameTypeArg)]),
    erlang:set_cookie(node(), list_to_atom(Cookie)),
    TargetNode = list_to_atom(NodeName),
    case {net_kernel:connect_node(TargetNode),
          net_adm:ping(TargetNode)} of
        {true, pong} ->
            ok;
        {_, pang} ->
            io:format("Node ~p not responding to pings.\n", [TargetNode]),
            erlang:halt(1)
    end,
    {ok, Cwd} = file:get_cwd(),
    ok = rpc:call(TargetNode, file, set_cwd, [Cwd], ?TIMEOUT),
    TargetNode.

make_script_node(Node) ->
    [Name, Host] = string:tokens(Node, "@"),
    list_to_atom(lists:concat([Name, "_upgrader_", os:getpid(), "@", Host])).

%% get name type from arg
get_name_type(NameTypeArg) ->
	case NameTypeArg of
		"-sname" ->
			shortnames;
		_ ->
			longnames
	end.
