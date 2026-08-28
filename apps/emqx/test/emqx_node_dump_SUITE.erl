%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_node_dump_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(DASHBOARD_PASSWORD, <<"nodedump_test_passwd_1">>).
-define(REDACTED, <<"******">>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_testcase(TestCase, Config0) ->
    emqx_license_test_lib:mock_parser(),
    {LicenseKey, Config} = license_key(TestCase, Config0),
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            {emqx_license, iolist_to_binary(["license.key = \"", LicenseKey, "\""])},
            {emqx_dashboard,
                iolist_to_binary([
                    "dashboard { listeners.http.bind = 0, default_password = \"",
                    ?DASHBOARD_PASSWORD,
                    "\" }"
                ])}
        ],
        #{work_dir => emqx_cth_suite:work_dir(TestCase, Config)}
    ),
    [{apps, Apps} | Config].

end_per_testcase(_TestCase, Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)),
    emqx_license_test_lib:unmock_parser().

license_key(t_conf_dump_license_file_key, Config) ->
    LicenseContent = emqx_license_test_lib:make_license(#{}),
    Path = filename:join(?config(priv_dir, Config), "node_dump_test.lic"),
    ok = file:write_file(Path, LicenseContent),
    Key = iolist_to_binary(["file://", Path]),
    {Key, [{license_secret, LicenseContent} | Config]};
license_key(_TestCase, Config) ->
    Key = emqx_license_test_lib:make_license(#{}),
    {Key, [{license_secret, Key} | Config]}.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

-doc """
Checks that the config dump keeps sensitive values out of the output:
`dashboard.default_password` and an inline `license.key` must not occur
in the rendered HOCON, while both keys must remain present with a
redacted value.
""".
t_conf_dump_redacts_secrets(Config) ->
    LicenseSecret = ?config(license_secret, Config),
    Dump = conf_dump(),
    ?assertEqual(nomatch, binary:match(Dump, ?DASHBOARD_PASSWORD)),
    ?assertEqual(nomatch, binary:match(Dump, LicenseSecret)),
    {ok, Conf} = hocon:binary(Dump),
    ?assertEqual(
        ?REDACTED,
        emqx_utils_maps:deep_get([<<"dashboard">>, <<"default_password">>], Conf)
    ),
    ?assertEqual(
        ?REDACTED,
        emqx_utils_maps:deep_get([<<"license">>, <<"key">>], Conf)
    ),
    %% non-sensitive parts of the config are still dumped
    ?assert(maps:is_key(<<"mqtt">>, Conf)).

-doc """
Checks that a `license.key` configured as a `file://` URI does not get
the referenced file's content into the config dump, and that the key
remains present with a redacted value.
""".
t_conf_dump_license_file_key(Config) ->
    LicenseSecret = ?config(license_secret, Config),
    Dump = conf_dump(),
    ?assertEqual(nomatch, binary:match(Dump, LicenseSecret)),
    ?assertEqual(nomatch, binary:match(Dump, ?DASHBOARD_PASSWORD)),
    {ok, Conf} = hocon:binary(Dump),
    ?assertEqual(
        ?REDACTED,
        emqx_utils_maps:deep_get([<<"license">>, <<"key">>], Conf)
    ).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

conf_dump() ->
    iolist_to_binary(emqx_node_dump:conf_dump()).
