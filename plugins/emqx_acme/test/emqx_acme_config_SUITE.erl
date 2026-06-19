-module(emqx_acme_config_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    [
        t_parse_defaults,
        t_parse_listener_ids_happy_path,
        t_parse_listener_ids_empty_string_is_no_op,
        t_parse_listener_ids_accepts_legacy_list,
        t_parse_listener_ids_rejects_tcp,
        t_parse_listener_ids_rejects_quic,
        t_parse_listener_ids_accepts_unknown_name,
        t_parse_acc_key_file_uri,
        t_parse_acc_key_password_file_uri,
        t_parse_acc_key_rejects_non_file_scheme,
        t_parse_acc_key_treats_empty_as_unset,
        t_parse_acc_key_password_treats_null_as_unset,
        t_parse_enable_dashboard_https_default_true,
        t_parse_enable_dashboard_https_disabled,
        t_parse_dashboard_https_port_default,
        t_parse_dashboard_https_port_override,
        t_parse_listener_ids_rejects_dashboard,
        t_parse_acc_key_stores_env_var_unresolved,
        t_resolve_file_uri_expands_env_var,
        t_parse_domains_csv_splits_and_trims,
        t_parse_domains_drops_empty_entries,
        t_parse_domains_accepts_legacy_list,
        t_parse_contact_csv_splits_and_trims,
        t_parse_domains_rejects_email_like_entry,
        t_parse_domains_rejects_whitespace_inside_label,
        t_parse_domains_rejects_url_like_entry,
        t_avsc_acc_key_fields_are_plain_string
    ].

-define(TEST_ENV_VAR, "EMQX_ACME_TEST_DIR").
-define(TEST_ENV_VALUE, "/tmp/emqx_acme_test").

init_per_suite(Config) ->
    %% Use a plugin-private env var to exercise naive_env_interpolation
    %% without colliding with EMQX_ETC_DIR / EMQX_LOG_DIR (which have
    %% fallbacks in emqx_utils_schema and would obscure the test signal).
    true = os:putenv(?TEST_ENV_VAR, ?TEST_ENV_VALUE),
    Config.

end_per_suite(_Config) ->
    true = os:unsetenv(?TEST_ENV_VAR),
    ok.

-doc "Defaults: cert_bundle_name=acme, listener_ids=[], cert_type=ec, "
"domains=[], acc_key=undefined (plugin manages it inside the cert bundle "
"by default; acc_key only takes a value as an operator override), "
"acc_key_password=undefined.".
t_parse_defaults(_Config) ->
    {ok, S} = emqx_acme_config:parse(#{}),
    ?assertEqual(<<"acme">>, maps:get(cert_bundle_name, S)),
    ?assertEqual([], maps:get(listener_ids, S)),
    ?assertEqual(ec, maps:get(cert_type, S)),
    ?assertEqual([], maps:get(domains, S)),
    ?assertEqual(undefined, maps:get(acc_key, S)),
    ?assertEqual(undefined, maps:get(acc_key_password, S)).

-doc "listener_ids is a comma-separated string in the dashboard form; "
"each entry parses to {Type, NameAtom}. Surrounding whitespace and "
"empty entries (e.g. trailing commas) are tolerated.".
t_parse_listener_ids_happy_path(_Config) ->
    {ok, S} = emqx_acme_config:parse(
        #{<<"listener_ids">> => <<"ssl:default, wss:external ,">>}
    ),
    ?assertEqual(
        [{ssl, default}, {wss, external}],
        maps:get(listener_ids, S)
    ).

-doc "Empty string (or all-whitespace / only commas) parses to []; this "
"is the supported way to issue a cert for the dashboard HTTPS listener "
"only, without rewriting any MQTT SSL/WSS listener.".
t_parse_listener_ids_empty_string_is_no_op(_Config) ->
    {ok, S1} = emqx_acme_config:parse(#{<<"listener_ids">> => <<"">>}),
    ?assertEqual([], maps:get(listener_ids, S1)),
    {ok, S2} = emqx_acme_config:parse(#{<<"listener_ids">> => <<" , ,">>}),
    ?assertEqual([], maps:get(listener_ids, S2)),
    {ok, S3} = emqx_acme_config:parse(#{<<"listener_ids">> => null}),
    ?assertEqual([], maps:get(listener_ids, S3)).

-doc "Back-compat: a list-of-binaries listener_ids (from configs persisted "
"before the CSV migration, or set via emqx_acme_config:update/1 with the "
"older shape) is accepted and normalized to the same {Type, NameAtom} "
"list.".
t_parse_listener_ids_accepts_legacy_list(_Config) ->
    {ok, S} = emqx_acme_config:parse(
        #{<<"listener_ids">> => [<<"ssl:default">>, <<"wss:external">>]}
    ),
    ?assertEqual(
        [{ssl, default}, {wss, external}],
        maps:get(listener_ids, S)
    ).

-doc "tcp:<name> is rejected with {invalid_listener_type, <<\"tcp\">>}.".
t_parse_listener_ids_rejects_tcp(_Config) ->
    ?assertMatch(
        {error, {invalid_listener_type, <<"tcp">>}},
        emqx_acme_config:parse(#{<<"listener_ids">> => <<"tcp:default">>})
    ).

-doc "quic:<name> is rejected; QUIC managed certs are not in v1 scope.".
t_parse_listener_ids_rejects_quic(_Config) ->
    ?assertMatch(
        {error, {invalid_listener_type, <<"quic">>}},
        emqx_acme_config:parse(#{<<"listener_ids">> => <<"quic:default">>})
    ).

-doc "A listener name that does not yet correspond to a real listener is "
"accepted at parse time; runtime issuance silently skips it.".
t_parse_listener_ids_accepts_unknown_name(_Config) ->
    {ok, S} = emqx_acme_config:parse(
        #{<<"listener_ids">> => <<"ssl:not_yet_defined">>}
    ),
    ?assertEqual([{ssl, not_yet_defined}], maps:get(listener_ids, S)).

-doc "acc_key accepts a file:// URI and stores it as a string.".
t_parse_acc_key_file_uri(_Config) ->
    {ok, S} = emqx_acme_config:parse(
        #{<<"acc_key">> => <<"file:///etc/emqx/acme/acc_key.pem">>}
    ),
    ?assertEqual("file:///etc/emqx/acme/acc_key.pem", maps:get(acc_key, S)).

-doc "acc_key_password accepts a file:// URI (typical use: encrypted PEM).".
t_parse_acc_key_password_file_uri(_Config) ->
    {ok, S} = emqx_acme_config:parse(
        #{
            <<"acc_key">> => <<"file:///etc/emqx/acme/acc_key.pem">>,
            <<"acc_key_password">> => <<"file:///etc/emqx/acme/acc_key_password.txt">>
        }
    ),
    ?assertEqual(
        "file:///etc/emqx/acme/acc_key_password.txt",
        maps:get(acc_key_password, S)
    ).

-doc "acc_key rejects values that don't start with file://.".
t_parse_acc_key_rejects_non_file_scheme(_Config) ->
    ?assertMatch(
        {error, {invalid_file_uri, <<"acc_key">>, <<"https://example.com/key.pem">>}},
        emqx_acme_config:parse(
            #{<<"acc_key">> => <<"https://example.com/key.pem">>}
        )
    ).

-doc "Empty strings for acc_key / acc_key_password are treated as unset, "
"so a cleared dashboard field falls back to the plugin's bundle-managed "
"default (acc_key => undefined) rather than rejecting the empty value.".
t_parse_acc_key_treats_empty_as_unset(_Config) ->
    {ok, S} = emqx_acme_config:parse(
        #{<<"acc_key">> => <<"">>, <<"acc_key_password">> => <<"">>}
    ),
    ?assertEqual(undefined, maps:get(acc_key, S)),
    ?assertEqual(undefined, maps:get(acc_key_password, S)).

-doc "Parser accepts a literal `null` for acc_key_password and treats "
"it as 'unset', so a config that carries that value loads cleanly.".
t_parse_acc_key_password_treats_null_as_unset(_Config) ->
    {ok, S} = emqx_acme_config:parse(
        #{<<"acc_key_password">> => null}
    ),
    ?assertEqual(undefined, maps:get(acc_key_password, S)).

-doc "enable_dashboard_https is true by default so single-node community "
"deployments get a TLS-enabled dashboard automatically once a cert is issued.".
t_parse_enable_dashboard_https_default_true(_Config) ->
    {ok, S} = emqx_acme_config:parse(#{}),
    ?assertEqual(true, maps:get(enable_dashboard_https, S)).

-doc "Operators can opt out of automatic dashboard HTTPS by setting the flag "
"to false; existing dashboard listeners are then left entirely untouched.".
t_parse_enable_dashboard_https_disabled(_Config) ->
    {ok, S} = emqx_acme_config:parse(#{<<"enable_dashboard_https">> => false}),
    ?assertEqual(false, maps:get(enable_dashboard_https, S)).

-doc "dashboard_https_port defaults to 18084 (the EMQX dashboard's "
"conventional HTTPS port).".
t_parse_dashboard_https_port_default(_Config) ->
    {ok, S} = emqx_acme_config:parse(#{}),
    ?assertEqual(18084, maps:get(dashboard_https_port, S)).

-doc "dashboard_https_port can be overridden to avoid clashes with "
"another service on the host (e.g. 8443).".
t_parse_dashboard_https_port_override(_Config) ->
    {ok, S} = emqx_acme_config:parse(#{<<"dashboard_https_port">> => 8443}),
    ?assertEqual(8443, maps:get(dashboard_https_port, S)).

-doc "The dashboard's HTTPS listener is owned by enable_dashboard_https; "
"listener_ids only accepts MQTT ssl/wss entries and rejects dashboard:* "
"so the two paths can't get out of sync.".
t_parse_listener_ids_rejects_dashboard(_Config) ->
    Result = emqx_acme_config:parse(
        #{<<"listener_ids">> => <<"dashboard:https">>}
    ),
    ?assertMatch({error, {invalid_listener_type, <<"dashboard">>}}, Result).

-doc "Parser stores file:// URIs verbatim — env-var interpolation is "
"deferred to use sites (ensure_acc_key_file / build_acc_key_opt) so the "
"stored config matches what the operator typed in the dashboard.".
t_parse_acc_key_stores_env_var_unresolved(_Config) ->
    Raw = <<"file://${", ?TEST_ENV_VAR, "}/acc_key.pem">>,
    {ok, S} = emqx_acme_config:parse(#{<<"acc_key">> => Raw}),
    ?assertEqual(binary_to_list(Raw), maps:get(acc_key, S)).

-doc "resolve_file_uri/1 expands ${VAR} via naive_env_interpolation, which "
"is what use sites call before opening the file or handing the path to "
"acme-erlang-client.".
t_resolve_file_uri_expands_env_var(_Config) ->
    Input = "file://${" ++ ?TEST_ENV_VAR ++ "}/acc_key.pem",
    Expected = "file://" ++ ?TEST_ENV_VALUE ++ "/acc_key.pem",
    ?assertEqual(Expected, emqx_acme_config:resolve_file_uri(Input)),
    ?assertEqual(undefined, emqx_acme_config:resolve_file_uri(undefined)).

-doc "domains is a comma-separated string in the dashboard form (the "
"input-array UI was hard to use). The parser splits on ',' and trims "
"surrounding whitespace so 'a, b' parses the same as 'a,b'.".
t_parse_domains_csv_splits_and_trims(_Config) ->
    {ok, S} = emqx_acme_config:parse(
        #{<<"domains">> => <<"mqtt.example.com, mqtt2.example.com ,mqtt3.example.com">>}
    ),
    ?assertEqual(
        [<<"mqtt.example.com">>, <<"mqtt2.example.com">>, <<"mqtt3.example.com">>],
        maps:get(domains, S)
    ).

-doc "Trailing/leading/duplicate commas produce empty entries that the "
"parser drops, so a stray comma doesn't sneak an empty domain into the "
"ACME request.".
t_parse_domains_drops_empty_entries(_Config) ->
    {ok, S} = emqx_acme_config:parse(
        #{<<"domains">> => <<",mqtt.example.com,,mqtt2.example.com,">>}
    ),
    ?assertEqual(
        [<<"mqtt.example.com">>, <<"mqtt2.example.com">>],
        maps:get(domains, S)
    ).

-doc "Back-compat: an existing config that still has a list-of-binaries "
"form (e.g. saved before the CSV migration, or set via "
"emqx_acme_config:update/1 with the older shape) is normalized to the "
"same list shape we produce from CSV parsing.".
t_parse_domains_accepts_legacy_list(_Config) ->
    {ok, S} = emqx_acme_config:parse(
        #{<<"domains">> => [<<"a.example.com">>, <<"b.example.com">>]}
    ),
    ?assertEqual(
        [<<"a.example.com">>, <<"b.example.com">>],
        maps:get(domains, S)
    ).

-doc "contact uses the same CSV machinery; typical input is a list of "
"mailto: URIs.".
t_parse_contact_csv_splits_and_trims(_Config) ->
    {ok, S} = emqx_acme_config:parse(
        #{<<"contact">> => <<"mailto:admin@example.com, mailto:ops@example.com">>}
    ),
    ?assertEqual(
        [<<"mailto:admin@example.com">>, <<"mailto:ops@example.com">>],
        maps:get(contact, S)
    ).

-doc "A domain entry containing `@` is rejected at parse time with "
"`{invalid_domain, <<\"admin@example\">>}`.".
t_parse_domains_rejects_email_like_entry(_Config) ->
    ?assertMatch(
        {error, {invalid_domain, <<"admin@example">>}},
        emqx_acme_config:parse(
            #{<<"domains">> => <<"mqtt.example.com,admin@example">>}
        )
    ).

-doc "A domain entry with whitespace inside the label is rejected. "
"Surrounding whitespace around the comma separators is trimmed by "
"split_csv_bin/1 and remains accepted.".
t_parse_domains_rejects_whitespace_inside_label(_Config) ->
    ?assertMatch(
        {error, {invalid_domain, <<"foo bar.example.com">>}},
        emqx_acme_config:parse(
            #{<<"domains">> => <<"foo bar.example.com">>}
        )
    ).

-doc "A domain entry containing `/` (typically a pasted URL) is "
"rejected at parse time.".
t_parse_domains_rejects_url_like_entry(_Config) ->
    ?assertMatch(
        {error, {invalid_domain, <<"https://mqtt.example.com">>}},
        emqx_acme_config:parse(
            #{<<"domains">> => <<"https://mqtt.example.com">>}
        )
    ).

-doc "acc_key and acc_key_password are declared as plain \"string\" in "
"the avsc schema. The dashboard's untagged JSON (`\"acc_key\": "
"\"file://...\"`) decodes directly against a string type; a union with "
"`null` would require the tagged form `{\"string\": \"...\"}`.".
t_avsc_acc_key_fields_are_plain_string(_Config) ->
    Avsc = read_avsc(),
    Fields = maps:get(<<"fields">>, Avsc),
    [AccKey] = [F || F <- Fields, maps:get(<<"name">>, F) =:= <<"acc_key">>],
    [AccKeyPw] = [F || F <- Fields, maps:get(<<"name">>, F) =:= <<"acc_key_password">>],
    ?assertEqual(<<"string">>, maps:get(<<"type">>, AccKey)),
    ?assertEqual(<<"string">>, maps:get(<<"type">>, AccKeyPw)).

read_avsc() ->
    Path = filename:join([code:lib_dir(emqx_acme), "priv", "config_schema.avsc"]),
    {ok, Bin} = file:read_file(Path),
    emqx_utils_json:decode(Bin).
