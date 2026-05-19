-module(emqx_acme_issuer_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    [
        t_pre_state_bundle_empty,
        t_pre_state_bundle_in_use,
        t_pre_state_treats_partial_bundle_as_empty,
        t_maybe_migrate_listeners_noop_in_scenario_a,
        t_maybe_migrate_listeners_calls_emqx_conf_in_scenario_b,
        t_migrate_one_listener_skips_missing,
        t_migrate_one_listener_updates_existing,
        t_build_acc_key_opt_uses_config_path_when_set,
        t_build_acc_key_opt_uses_bundle_when_unset,
        t_build_acc_key_opt_reuses_bundle_acc_key,
        t_build_acc_key_opt_passes_password_path_through,
        t_ensure_acc_key_file_creates_pem_when_missing,
        t_ensure_acc_key_file_leaves_existing_file_alone,
        t_store_result_never_writes_acc_key_to_bundle
    ].

init_per_testcase(_, Config) ->
    ok = meck:new(emqx_managed_certs, [non_strict, no_link]),
    ok = meck:new(emqx_config, [non_strict, no_link]),
    ok = meck:new(emqx_conf, [non_strict, no_link]),
    Config.

end_per_testcase(_, _Config) ->
    catch meck:unload(emqx_managed_certs),
    catch meck:unload(emqx_config),
    catch meck:unload(emqx_conf),
    ok.

-doc "pre_state/2 returns bundle_empty when list_managed_files returns "
"{error, enoent}, i.e. the bundle directory does not exist yet.".
t_pre_state_bundle_empty(_Config) ->
    meck:expect(
        emqx_managed_certs,
        list_managed_files,
        fun(_Ns, _Bundle) -> {error, enoent} end
    ),
    ?assertEqual(bundle_empty, emqx_acme_issuer:pre_state(global, <<"acme">>)).

-doc "pre_state/2 returns bundle_in_use when both chain and key are present.".
t_pre_state_bundle_in_use(_Config) ->
    meck:expect(
        emqx_managed_certs,
        list_managed_files,
        fun(_Ns, _Bundle) ->
            {ok, #{
                chain => #{path => <<"/tmp/chain.pem">>},
                key => #{path => <<"/tmp/key.pem">>}
            }}
        end
    ),
    ?assertEqual(bundle_in_use, emqx_acme_issuer:pre_state(global, <<"acme">>)).

-doc "pre_state/2 returns bundle_empty when only acc_key is present (no "
"chain or key); a half-populated bundle should not be treated as "
"in-use because the listener cannot serve TLS from it.".
t_pre_state_treats_partial_bundle_as_empty(_Config) ->
    meck:expect(
        emqx_managed_certs,
        list_managed_files,
        fun(_Ns, _Bundle) ->
            {ok, #{acc_key => #{path => <<"/tmp/acc_key.pem">>}}}
        end
    ),
    ?assertEqual(bundle_empty, emqx_acme_issuer:pre_state(global, <<"acme">>)).

-doc "Scenario A: bundle was already in use; no listener config rewrite, "
"so emqx_conf:update/3 must never be called.".
t_maybe_migrate_listeners_noop_in_scenario_a(_Config) ->
    meck:expect(emqx_conf, update, fun(_, _, _) -> {ok, undefined} end),
    ok = emqx_acme_issuer:maybe_migrate_listeners(
        bundle_in_use,
        <<"acme">>,
        [{ssl, default}]
    ),
    ?assertEqual(0, meck:num_calls(emqx_conf, update, '_')).

-doc "Scenario B: bundle was empty before issuance; emqx_conf:update/3 must "
"be called once per existing listener.".
t_maybe_migrate_listeners_calls_emqx_conf_in_scenario_b(_Config) ->
    meck:expect(
        emqx_config,
        find_listener_conf,
        fun(_Type, _Name, _Path) -> {ok, #{enable => true}} end
    ),
    meck:expect(emqx_conf, update, fun(_, _, _) -> {ok, undefined} end),
    ok = emqx_acme_issuer:maybe_migrate_listeners(
        bundle_empty,
        <<"acme">>,
        [{ssl, default}, {wss, external}]
    ),
    ?assertEqual(2, meck:num_calls(emqx_conf, update, '_')),
    %% Confirm the path and override_to are correct.
    History = meck:history(emqx_conf),
    Calls = [Args || {_, {_, update, Args}, _} <- History],
    ?assertMatch(
        [
            [[listeners, ssl, default], _, #{override_to := cluster}],
            [[listeners, wss, external], _, #{override_to := cluster}]
        ],
        Calls
    ).

-doc "migrate_one_listener/3 skips when find_listener_conf returns "
"{not_found, _, _}; emqx_conf:update/3 must not be called.".
t_migrate_one_listener_skips_missing(_Config) ->
    meck:expect(
        emqx_config,
        find_listener_conf,
        fun(_T, _N, _P) -> {not_found, undefined, undefined} end
    ),
    meck:expect(emqx_conf, update, fun(_, _, _) -> {ok, undefined} end),
    ?assertEqual(
        ok,
        emqx_acme_issuer:migrate_one_listener(<<"acme">>, ssl, ghost)
    ),
    ?assertEqual(0, meck:num_calls(emqx_conf, update, '_')).

-doc "migrate_one_listener/3 calls emqx_conf:update/3 with the right path "
"and ssl_options.managed_certs.bundle_name when the listener exists.".
t_migrate_one_listener_updates_existing(_Config) ->
    meck:expect(
        emqx_config,
        find_listener_conf,
        fun(_T, _N, _P) -> {ok, #{enable => true, ssl_options => #{}}} end
    ),
    meck:expect(emqx_conf, update, fun(_, _, _) -> {ok, undefined} end),
    ?assertEqual(
        ok,
        emqx_acme_issuer:migrate_one_listener(<<"acme">>, ssl, default)
    ),
    [{_, {_, update, [Path, Request, Opts]}, _}] = meck:history(emqx_conf),
    ?assertEqual([listeners, ssl, default], Path),
    ?assertEqual(cluster, maps:get(override_to, Opts)),
    %% emqx_listeners:pre_config_update/3 expects {update, NewConf}.
    {update, NewConf} = Request,
    ?assertEqual(true, maps:get(<<"enable">>, NewConf)),
    SSLOpts = maps:get(<<"ssl_options">>, NewConf),
    ?assertEqual(
        #{<<"bundle_name">> => <<"acme">>},
        maps:get(<<"managed_certs">>, SSLOpts)
    ).

-doc "Operator-override path: when acc_key_config is set, build_acc_key_opt "
"returns that file:// path verbatim and never consults the bundle. "
"emqx_managed_certs is mocked to crash so any accidental fallback fails "
"loudly.".
t_build_acc_key_opt_uses_config_path_when_set(_Config) ->
    meck:expect(
        emqx_managed_certs,
        list_managed_files,
        fun(_Ns, _Bundle) -> erlang:error(should_not_be_called) end
    ),
    Tmp = unique_tmp_path(<<"acc_key_opt">>),
    Path = "file://" ++ Tmp,
    Params = #{
        acc_key_config => Path,
        acc_key_password_config => undefined,
        cert_type => ec
    },
    ?assertEqual(
        #{acc_key => Path},
        emqx_acme_issuer:build_acc_key_opt(Params, <<"acme">>)
    ),
    ?assertEqual(0, meck:num_calls(emqx_managed_certs, list_managed_files, '_')),
    file:delete(Tmp).

-doc "Default path: when acc_key_config is undefined, the plugin generates "
"the account key in-memory and stores it via add_managed_files (which "
"replicates cluster-wide). The returned acc_key URI points at the bundle "
"slot's path, so acme-erlang-client reads back the same file the cluster "
"shares.".
t_build_acc_key_opt_uses_bundle_when_unset(_Config) ->
    Counter = {?MODULE, list_calls},
    Captured = {?MODULE, added_files_capture},
    persistent_term:put(Counter, 0),
    BundlePath = "/tmp/emqx_acme_test_bundle/acc-key.pem",
    meck:expect(
        emqx_managed_certs,
        list_managed_files,
        fun(_Ns, _Bundle) ->
            N = persistent_term:get(Counter),
            persistent_term:put(Counter, N + 1),
            case N of
                0 -> {ok, #{}};
                _ -> {ok, #{acc_key => #{path => BundlePath}}}
            end
        end
    ),
    meck:expect(
        emqx_managed_certs,
        add_managed_files,
        fun(_Ns, _Bundle, Files) ->
            persistent_term:put(Captured, Files),
            ok
        end
    ),
    Params = #{
        acc_key_config => undefined,
        acc_key_password_config => undefined,
        cert_type => ec
    },
    ?assertEqual(
        #{acc_key => "file://" ++ BundlePath},
        emqx_acme_issuer:build_acc_key_opt(Params, <<"acme">>)
    ),
    Files = persistent_term:get(Captured),
    ?assert(maps:is_key(acc_key, Files)),
    %% Sanity: the captured value must be a real PEM, not a placeholder.
    Pem = maps:get(acc_key, Files),
    [Entry | _] = public_key:pem_decode(iolist_to_binary(Pem)),
    ?assertEqual('ECPrivateKey', element(1, public_key:pem_entry_decode(Entry))),
    persistent_term:erase(Counter),
    persistent_term:erase(Captured).

-doc "Renewal path: when the bundle already has an acc_key, "
"build_acc_key_opt returns the existing path and does NOT call "
"add_managed_files (so we don't churn the cluster-shared identity).".
t_build_acc_key_opt_reuses_bundle_acc_key(_Config) ->
    BundlePath = "/tmp/emqx_acme_test_bundle/acc-key.pem",
    meck:expect(
        emqx_managed_certs,
        list_managed_files,
        fun(_Ns, _Bundle) ->
            {ok, #{acc_key => #{path => BundlePath}}}
        end
    ),
    meck:expect(
        emqx_managed_certs,
        add_managed_files,
        fun(_Ns, _Bundle, _Files) -> erlang:error(should_not_be_called) end
    ),
    Params = #{
        acc_key_config => undefined,
        acc_key_password_config => undefined,
        cert_type => ec
    },
    ?assertEqual(
        #{acc_key => "file://" ++ BundlePath},
        emqx_acme_issuer:build_acc_key_opt(Params, <<"acme">>)
    ),
    ?assertEqual(0, meck:num_calls(emqx_managed_certs, add_managed_files, '_')).

-doc "acc_key_password_config flows through as acc_key_pass for "
"acme_client_issuance:run/2 (encrypted PEM support).".
t_build_acc_key_opt_passes_password_path_through(_Config) ->
    Tmp = unique_tmp_path(<<"acc_key_pass">>),
    Path = "file://" ++ Tmp,
    PassPath = "file:///does/not/need/to/exist.txt",
    Params = #{
        acc_key_config => Path,
        acc_key_password_config => PassPath,
        cert_type => ec
    },
    ?assertEqual(
        #{acc_key => Path, acc_key_pass => PassPath},
        emqx_acme_issuer:build_acc_key_opt(Params, <<"acme">>)
    ),
    file:delete(Tmp).

-doc "ensure_acc_key_file/2 creates a PEM-encoded EC private key at the "
"configured path on first issuance (zero-config single-node UX).".
t_ensure_acc_key_file_creates_pem_when_missing(_Config) ->
    Path = unique_tmp_path(<<"ensure_create">>),
    Uri = "file://" ++ Path,
    ?assertNot(filelib:is_regular(Path)),
    ok = emqx_acme_issuer:ensure_acc_key_file(Uri, ec),
    ?assert(filelib:is_regular(Path)),
    {ok, Pem} = file:read_file(Path),
    [Entry | _] = public_key:pem_decode(Pem),
    Decoded = public_key:pem_entry_decode(Entry),
    ?assertEqual('ECPrivateKey', element(1, Decoded)),
    file:delete(Path).

-doc "ensure_acc_key_file/2 leaves an existing file untouched (so the "
"plugin doesn't trash a key the operator placed there).".
t_ensure_acc_key_file_leaves_existing_file_alone(_Config) ->
    Path = unique_tmp_path(<<"ensure_keep">>),
    Uri = "file://" ++ Path,
    Marker = <<"-----BEGIN MARKER-----\nplaceholder\n-----END MARKER-----\n">>,
    ok = filelib:ensure_dir(Path),
    ok = file:write_file(Path, Marker),
    ok = emqx_acme_issuer:ensure_acc_key_file(Uri, ec),
    {ok, After} = file:read_file(Path),
    ?assertEqual(Marker, After),
    file:delete(Path).

-doc "store_result/2 writes only chain and key to the bundle — the "
"bundle's acc_key slot is never touched, regardless of how the ACME "
"account key was sourced. (Operator owns the account key file; the "
"bundle is purely cert output.)".
t_store_result_never_writes_acc_key_to_bundle(_Config) ->
    Captured = {?MODULE, captured_files},
    meck:expect(
        emqx_managed_certs,
        add_managed_files,
        fun(_Ns, _Bundle, Files) ->
            persistent_term:put(Captured, Files),
            ok
        end
    ),
    EcKey = public_key:generate_key({namedCurve, secp256r1}),
    %% Empty cert_chain dodges the cost of building a valid OTPCertificate
    %% from scratch. The encoder maps over an empty list and produces an
    %% empty PEM, which is fine: this test asserts the *key set* of the
    %% Files map, not its content.
    Result = #{
        acc_key => EcKey,
        cert_key => EcKey,
        cert_chain => []
    },
    Params = #{
        bundle_name => <<"acme">>,
        pre_state => bundle_in_use,
        listener_ids => [],
        acc_key_config => "file:///etc/emqx/acme/acc_key.pem",
        acc_key_password_config => undefined
    },
    ok = emqx_acme_issuer:store_result(Result, Params),
    Files = persistent_term:get(Captured),
    ?assert(maps:is_key(chain, Files)),
    ?assert(maps:is_key(key, Files)),
    ?assertNot(maps:is_key(acc_key, Files)),
    persistent_term:erase(Captured).

%% Helper: produce a unique-per-test, never-pre-existing tmp path.
unique_tmp_path(Tag) ->
    {A, B, C} = erlang:timestamp(),
    Name = io_lib:format(
        "emqx_acme_issuer_test_~s_~p_~p_~p.pem",
        [Tag, A, B, C]
    ),
    filename:join("/tmp", lists:flatten(Name)).
