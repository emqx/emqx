%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mqtt_quic_test_runner).

-export([ensure/0]).

-define(RELEASE_TAG, "v0.3.0").
-define(RELEASE_BASE_URL, "https://github.com/qzhuyan/mqtt_quic_tests/releases/download/").

ensure() ->
    case os:getenv("MQTT_QUIC_TEST_BIN") of
        false ->
            ensure_release_runner();
        Exe ->
            ensure_override(Exe)
    end.

ensure_override(Exe) ->
    case filelib:is_regular(Exe) of
        true ->
            case os:find_executable(filename:absname(Exe)) of
                false -> ct:fail("MQTT_QUIC_TEST_BIN is not executable: ~s", [Exe]);
                _ -> Exe
            end;
        false ->
            ct:fail("MQTT_QUIC_TEST_BIN is not a regular file: ~s", [Exe])
    end.

ensure_release_runner() ->
    {Asset, Digest} = release_asset(os:type(), erlang:system_info(system_architecture)),
    CacheDir = filename:join([cache_root(), ?RELEASE_TAG]),
    Exe = filename:join([CacheDir, Asset, "mqtt_quic_test"]),
    Marker = filename:join(CacheDir, Asset ++ ".sha256"),
    case cache_valid(Exe, Marker, Digest) of
        true ->
            ensure_executable(Exe);
        false ->
            download_release_runner(CacheDir, Asset, Digest, Exe, Marker)
    end.

release_asset({unix, linux}, SystemArchitecture) ->
    case cpu_arch(SystemArchitecture) of
        x86_64 ->
            {
                "mqtt_quic_test-linux-x86_64-musl",
                "1937945ccb935d2c054fb904a0031ceb70ce76e545d17da57f2989bd2e92d445"
            };
        aarch64 ->
            {
                "mqtt_quic_test-linux-aarch64-musl",
                "9764b3d4f33c41bdd78478d5e38aa8471d263c906ef7a893dce52399712738ee"
            }
    end;
release_asset({unix, darwin}, SystemArchitecture) ->
    case cpu_arch(SystemArchitecture) of
        x86_64 ->
            {
                "mqtt_quic_test-macos-x86_64",
                "6e9a9f1a3dfa7c89d3ab4705e384fbb08a77e6427a6262cd9884cf8b05806a0f"
            };
        aarch64 ->
            {
                "mqtt_quic_test-macos-aarch64",
                "70c8e8c14824074a2931daa3e3fdcaef7e4ce8a6f87b018c3c26992342791a06"
            }
    end;
release_asset(OsType, SystemArchitecture) ->
    ct:fail("mqtt_quic_test ~s has no asset for ~p (~s)", [
        ?RELEASE_TAG,
        OsType,
        SystemArchitecture
    ]).

cpu_arch(SystemArchitecture0) ->
    SystemArchitecture = string:lowercase(unicode:characters_to_list(SystemArchitecture0)),
    case
        {
            lists:prefix("x86_64", SystemArchitecture) orelse
                lists:prefix("amd64", SystemArchitecture),
            lists:prefix("aarch64", SystemArchitecture) orelse
                lists:prefix("arm64", SystemArchitecture)
        }
    of
        {true, false} ->
            x86_64;
        {false, true} ->
            aarch64;
        _ ->
            ct:fail("mqtt_quic_test ~s does not support architecture ~s", [
                ?RELEASE_TAG,
                SystemArchitecture
            ])
    end.

cache_root() ->
    BeamDir = filename:dirname(filename:absname(code:which(?MODULE))),
    ProjectRoot = find_project_root(BeamDir),
    filename:join([ProjectRoot, "_build", "test", "mqtt_quic_test"]).

find_project_root(Dir) ->
    try emqx_common_test_helpers:proj_root() of
        ProjectRoot -> ProjectRoot
    catch
        error:undef -> find_project_root_from(Dir)
    end.

find_project_root_from(Dir) ->
    case filelib:is_regular(filename:join(Dir, "rebar.config")) of
        true ->
            Dir;
        false ->
            Parent = filename:dirname(Dir),
            case Parent =:= Dir of
                true -> ct:fail("cannot locate the EMQX project root from ~s", [Dir]);
                false -> find_project_root_from(Parent)
            end
    end.

cache_valid(Exe, Marker, Digest) ->
    filelib:is_regular(Exe) andalso
        case file:read_file(Marker) of
            {ok, MarkerContents} ->
                string:trim(binary_to_list(MarkerContents)) =:= Digest;
            {error, _} ->
                false
        end.

download_release_runner(CacheDir, Asset, Digest, Exe, Marker) ->
    ok = filelib:ensure_dir(Exe),
    Archive = Asset ++ ".tar.gz",
    TempArchive = filename:join(
        CacheDir,
        "." ++ Archive ++ "." ++ integer_to_list(erlang:unique_integer([positive]))
    ),
    Url = ?RELEASE_BASE_URL ++ ?RELEASE_TAG ++ "/" ++ Archive,
    try
        download(Url, TempArchive),
        verify_archive(TempArchive, Digest),
        case erl_tar:extract(TempArchive, [compressed, {cwd, CacheDir}]) of
            ok -> ok;
            {error, Reason} -> ct:fail("failed to extract ~s: ~p", [Archive, Reason])
        end,
        true = filelib:is_regular(Exe),
        ok = file:write_file(Marker, [Digest, "\n"]),
        ensure_executable(Exe)
    after
        file:delete(TempArchive)
    end.

download(Url, Destination) ->
    case os:find_executable("curl") of
        false ->
            ct:fail("curl executable not found; set MQTT_QUIC_TEST_BIN to a local runner");
        Curl ->
            Args = [
                "-L",
                "--fail",
                "--silent",
                "--show-error",
                "--retry",
                "3",
                "--output",
                Destination,
                Url
            ],
            case run_executable(Curl, Args) of
                {ok, _Output} ->
                    ok;
                {error, ExitStatus, Output} ->
                    ct:fail("failed to download ~s (curl status ~p):~n~s", [
                        Url,
                        ExitStatus,
                        Output
                    ])
            end
    end.

verify_archive(Archive, ExpectedDigest) ->
    {ok, Contents} = file:read_file(Archive),
    ActualDigest = string:lowercase(
        binary_to_list(binary:encode_hex(crypto:hash(sha256, Contents)))
    ),
    case ActualDigest of
        ExpectedDigest ->
            ok;
        _ ->
            ct:fail("checksum mismatch for ~s: expected ~s, got ~s", [
                Archive,
                ExpectedDigest,
                ActualDigest
            ])
    end.

ensure_executable(Exe) ->
    case filelib:is_regular(Exe) of
        true ->
            ok = file:change_mode(Exe, 8#755),
            Exe;
        false ->
            ct:fail("mqtt_quic_test executable not found: ~s", [Exe])
    end.

run_executable(Exe, Args) ->
    Port = open_port({spawn_executable, Exe}, [
        binary,
        exit_status,
        stderr_to_stdout,
        use_stdio,
        {args, Args}
    ]),
    collect_port(Port, []).

collect_port(Port, Acc) ->
    receive
        {Port, {data, Data}} ->
            collect_port(Port, [Data | Acc]);
        {Port, {exit_status, 0}} ->
            {ok, port_output(Acc)};
        {Port, {exit_status, ExitStatus}} ->
            {error, ExitStatus, port_output(Acc)}
    after 120000 ->
        port_close(Port),
        {error, timeout, port_output(Acc)}
    end.

port_output(Acc) ->
    unicode:characters_to_list(iolist_to_binary(lists:reverse(Acc))).
