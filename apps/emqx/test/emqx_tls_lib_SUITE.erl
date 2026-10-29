%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_tls_lib_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(TRUSTED, "/trusted/ca-bundle.pem").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(TCConfig) ->
    Apps = emqx_cth_suite:start([ssl], #{work_dir => emqx_cth_suite:work_dir(TCConfig)}),
    [{apps, Apps} | TCConfig].

end_per_suite(TCConfig) ->
    emqx_cth_suite:stop(?config(apps, TCConfig)).

init_per_testcase(_TestCase, TCConfig) ->
    application:unset_env(emqx, trusted_certs_path),
    TCConfig.

end_per_testcase(_TestCase, _TCConfig) ->
    application:unset_env(emqx, trusted_certs_path),
    ok.

client_opts(Overrides) ->
    emqx_tls_lib:to_client_opts(tls, maps:merge(#{enable => true}, Overrides)).

certs_dir() ->
    emqx_common_test_helpers:deps_path(emqx, "etc/certs").

-doc """
`verify_peer` with no CA gets the `node.trusted_certs` file as `cacertfile`.
""".
t_verify_peer_no_ca_injects_trusted_certs(_TCConfig) ->
    ok = application:set_env(emqx, trusted_certs_path, ?TRUSTED),
    Opts = client_opts(#{verify => verify_peer}),
    ?assertEqual([?TRUSTED], proplists:get_all_values(cacertfile, Opts)),
    Opts2 = client_opts(#{verify => verify_peer, cacertfile => <<>>}),
    ?assertEqual([?TRUSTED], proplists:get_all_values(cacertfile, Opts2)),
    ok.

-doc """
`verify_none` gets no `cacertfile`, because `ssl` opens the file even when it does not verify.
""".
t_verify_none_untouched(_TCConfig) ->
    ok = application:set_env(emqx, trusted_certs_path, ?TRUSTED),
    ?assertEqual(undefined, proplists:get_value(cacertfile, client_opts(#{verify => verify_none}))),
    ?assertEqual(undefined, proplists:get_value(cacertfile, client_opts(#{}))),
    ok.

-doc """
An explicit `cacertfile` is kept, and `node.trusted_certs` is not added.
""".
t_explicit_cacertfile_untouched(_TCConfig) ->
    ok = application:set_env(emqx, trusted_certs_path, ?TRUSTED),
    CA = filename:join(certs_dir(), "cacert.pem"),
    Opts = client_opts(#{verify => verify_peer, cacertfile => list_to_binary(CA)}),
    ?assertEqual([CA], proplists:get_all_values(cacertfile, Opts)),
    ok.

-doc """
When `node.trusted_certs` resolves to no file, `verify_peer` with no CA gets no `cacertfile`.
""".
t_no_trusted_certs_path_untouched(_TCConfig) ->
    Opts = client_opts(#{verify => verify_peer}),
    ?assertEqual(undefined, proplists:get_value(cacertfile, Opts)),
    ?assertEqual(undefined, proplists:get_value(cacerts, Opts)),
    ok.

-doc """
With no CA source, `ssl` rejects `verify_peer` options instead of connecting without
verification. This is what a client sees when `node.trusted_certs` resolves to no file.
""".
t_verify_peer_no_ca_ssl_rejects_options(_TCConfig) ->
    {ok, LSock} = gen_tcp:listen(0, [{ip, {127, 0, 0, 1}}]),
    {ok, Port} = inet:port(LSock),
    try
        Opts = client_opts(#{verify => verify_peer}),
        ?assertMatch(
            {error, {options, incompatible, _}},
            ssl:connect("localhost", Port, Opts, 1000)
        )
    after
        gen_tcp:close(LSock)
    end,
    ok.

-doc """
`cacertfile = system_default` resolves to the `node.trusted_certs` file, also with
`verify_none`.
""".
t_system_default_cacertfile_trusted_certs(_TCConfig) ->
    ok = application:set_env(emqx, trusted_certs_path, ?TRUSTED),
    lists:foreach(
        fun(Verify) ->
            Opts = client_opts(#{verify => Verify, cacertfile => <<"system_default">>}),
            ?assertEqual([?TRUSTED], proplists:get_all_values(cacertfile, Opts), Verify),
            ?assertEqual(undefined, proplists:get_value(cacerts, Opts), Verify)
        end,
        [verify_peer, verify_none]
    ),
    ok.

-doc """
When `node.trusted_certs` resolves to no file, `cacertfile = system_default` uses the CA
certificates that OTP loads from the OS.
""".
t_system_default_cacertfile_os_cacerts(_TCConfig) ->
    CACerts = public_key:cacerts_get(),
    Opts = client_opts(#{verify => verify_peer, cacertfile => <<"system_default">>}),
    ?assertEqual(undefined, proplists:get_value(cacertfile, Opts)),
    ?assertEqual(CACerts, proplists:get_value(cacerts, Opts)),
    ok.

-doc """
The certificate file checks keep `cacertfile = system_default`, and still reject it for
other file options.
""".
t_system_default_cacertfile_file_checks(TCConfig) ->
    Dir = ?config(priv_dir, TCConfig),
    lists:foreach(
        fun(SSL) ->
            ?assertEqual({ok, SSL}, emqx_tls_lib:ensure_ssl_files(Dir, SSL)),
            ?assertEqual(SSL, emqx_tls_lib:drop_invalid_certs(SSL))
        end,
        [
            #{<<"enable">> => true, <<"cacertfile">> => <<"system_default">>},
            #{enable => true, cacertfile => <<"system_default">>}
        ]
    ),
    ?assertMatch(
        {error, #{which_option := <<"certfile">>}},
        emqx_tls_lib:ensure_ssl_files(Dir, #{<<"certfile">> => <<"system_default">>})
    ),
    ok.
