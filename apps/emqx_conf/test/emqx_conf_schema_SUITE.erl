%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_conf_schema_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(TCConfig) ->
    File = emqx_schema:naive_env_interpolation(<<"${EMQX_ETC_DIR}/acl.conf">>),
    ok = filelib:ensure_dir(File),
    filelib:is_regular(File) orelse file:write_file(File, <<>>),
    TCConfig.

end_per_suite(_TCConfig) ->
    ok.

init_per_testcase(TestCase, TCConfig) ->
    Dir = filename:join([?config(priv_dir, TCConfig), TestCase]),
    CAFile = filename:join(Dir, "ca-bundle.pem"),
    ok = filelib:ensure_dir(CAFile),
    ok = file:write_file(CAFile, <<>>),
    [{ca_file, CAFile} | TCConfig].

end_per_testcase(_TestCase, _TCConfig) ->
    meck:unload(),
    ok.

mock_probe_paths(Paths) ->
    ok = meck:new(emqx_conf_schema, [passthrough, no_history]),
    ok = meck:expect(emqx_conf_schema, system_ca_bundle_paths, 0, Paths).

generate(TrustedCerts) ->
    Conf = iolist_to_binary(
        io_lib:format(
            """
            node {
              name = "emqx1@127.0.0.1"
              cookie = "emqxsecretcookie"
              data_dir = "data"
              trusted_certs = "~ts"
            }
            """,
            [TrustedCerts]
        )
    ),
    {ok, Hocon} = hocon:binary(Conf, #{format => richmap}),
    hocon_tconf:generate(emqx_conf_schema, Hocon).

%% CI runs the tests as root, which can read the file after `chmod 000'.
make_unreadable(Path) ->
    ok = file:change_mode(Path, 8#000),
    case file:open(Path, [read, raw]) of
        {error, eacces} ->
            ok;
        {ok, Fd} ->
            ok = file:close(Fd),
            ok = meck:new(file, [passthrough, no_history, unstick]),
            ok = meck:expect(file, open, fun
                (P, _Modes) when P =:= Path -> {error, eacces};
                (P, Modes) -> meck:passthrough([P, Modes])
            end)
    end.

app_env(App, Key, ConfList) ->
    proplists:get_value(Key, proplists:get_value(App, ConfList, [])).

assert_rejected(TrustedCerts, Cause) ->
    try generate(TrustedCerts) of
        _ -> ct:fail({accepted, TrustedCerts})
    catch
        throw:Reason ->
            Text = lists:flatten(io_lib:format("~0p", [Reason])),
            ?assertNotEqual(nomatch, string:find(Text, Cause), Text)
    end.

-doc """
`system_default` resolves to the first probed file that exists, and the hackney and emqx
application environments get the same path.
""".
t_system_default_resolves(TCConfig) ->
    CAFile = ?config(ca_file, TCConfig),
    Missing = filename:join(filename:dirname(CAFile), "missing.pem"),
    mock_probe_paths([Missing, CAFile]),
    ConfList = generate("system_default"),
    ?assertEqual(CAFile, app_env(emqx, trusted_certs_path, ConfList)),
    ?assertEqual(CAFile, app_env(hackney, cacertfile_path, ConfList)),
    ok.

-doc """
When no probed file exists, `system_default` is accepted and neither application
environment gets a path.
""".
t_system_default_probe_miss(TCConfig) ->
    CAFile = ?config(ca_file, TCConfig),
    mock_probe_paths([filename:join(filename:dirname(CAFile), "missing.pem")]),
    ConfList = generate("system_default"),
    ?assertEqual(undefined, app_env(emqx, trusted_certs_path, ConfList)),
    ?assertEqual(undefined, app_env(hackney, cacertfile_path, ConfList)),
    ok.

-doc """
`file:///abs/path` to an existing file is accepted, and both application environments get
the path.
""".
t_file_url_absolute(TCConfig) ->
    CAFile = ?config(ca_file, TCConfig),
    ConfList = generate("file://" ++ CAFile),
    ?assertEqual(CAFile, app_env(emqx, trusted_certs_path, ConfList)),
    ?assertEqual(CAFile, app_env(hackney, cacertfile_path, ConfList)),
    ok.

-doc """
`file://` with a relative path is rejected.
""".
t_file_url_relative_rejected(_TCConfig) ->
    assert_rejected("file://etc/certs/ca.pem", "trusted_certs_path_not_absolute").

-doc """
A path without the `file://` prefix is rejected.
""".
t_bare_path_rejected(TCConfig) ->
    assert_rejected(?config(ca_file, TCConfig), "bad_trusted_certs").

-doc """
`file://` with an absolute path to a file that does not exist is rejected.
""".
t_file_url_missing_rejected(TCConfig) ->
    CAFile = ?config(ca_file, TCConfig),
    Missing = filename:join(filename:dirname(CAFile), "missing.pem"),
    assert_rejected("file://" ++ Missing, "trusted_certs_file_not_found").

-doc """
`file://` with a file that the user that runs EMQX cannot read is rejected.
""".
t_file_url_unreadable_rejected(TCConfig) ->
    CAFile = ?config(ca_file, TCConfig),
    make_unreadable(CAFile),
    assert_rejected("file://" ++ CAFile, "trusted_certs_file_not_readable").

-doc """
`system_default` skips a probed file that the user that runs EMQX cannot read.
""".
t_system_default_skips_unreadable(TCConfig) ->
    CAFile = ?config(ca_file, TCConfig),
    Unreadable = filename:join(filename:dirname(CAFile), "unreadable.pem"),
    ok = file:write_file(Unreadable, <<>>),
    make_unreadable(Unreadable),
    mock_probe_paths([Unreadable, CAFile]),
    ConfList = generate("system_default"),
    ?assertEqual(CAFile, app_env(emqx, trusted_certs_path, ConfList)),
    ok.
