%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_sso_saml_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("esaml/include/esaml.hrl").

%% Keycloak integration test settings
-define(KEYCLOAK_HOST, "172.100.239.120").
-define(KEYCLOAK_HTTP_PORT, 8080).
-define(KEYCLOAK_REALM, "emqx").
-define(KEYCLOAK_TEST_USER, "testuser").
-define(KEYCLOAK_TEST_PASSWORD, "testpassword").

%% SP certificate paths (for SP signing tests)
-define(SP_CERT_DIR, ".ci/docker-compose-file/keycloak/sp_certs").

-define(BASE_URL, "http://127.0.0.1:18083").

%%------------------------------------------------------------------------------
%% CT Callbacks
%%------------------------------------------------------------------------------

all() ->
    [
        {group, unit},
        {group, api},
        {group, keycloak_integration},
        {group, signature_combinations}
    ].

groups() ->
    [
        {unit, [sequence], [
            t_validate_signature_config,
            t_maybe_load_cert_or_key
        ]},
        {api, [sequence], [
            t_sso_running_disabled,
            t_saml_metadata_not_initialized
        ]},
        {keycloak_integration, [sequence], [
            t_saml_init_with_keycloak,
            t_saml_metadata_generation,
            t_saml_login_redirect,
            t_saml_callback_invalid_response,
            t_saml_full_login_flow
        ]},
        {signature_combinations, [sequence], [
            t_sig_combo_no_sp_both_idp,
            t_sig_combo_no_sp_envelope_only,
            t_sig_combo_no_sp_assertion_only,
            t_sig_combo_no_signatures,
            t_sig_combo_sp_sign_both_idp
        ]}
    ].

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_management,
            {emqx_dashboard, "dashboard.listeners.http { enable = true, bind = 18083 }"},
            emqx_dashboard_sso
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    _ = emqx_common_test_http:create_default_app(),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_group(keycloak_integration, Config) ->
    init_keycloak_group(Config);
init_per_group(signature_combinations, Config) ->
    init_keycloak_group(Config);
init_per_group(_, Config) ->
    Config.

init_keycloak_group(Config) ->
    case is_keycloak_available() of
        true ->
            ct:pal("Keycloak is available, running integration tests"),
            Config;
        false ->
            ct:pal("Keycloak is not available, skipping integration tests"),
            {skip,
                "Keycloak not available. Start with: "
                "docker-compose -f .ci/docker-compose-file/docker-compose-keycloak.yaml up -d"}
    end.

end_per_group(keycloak_integration, Config) ->
    cleanup_saml_module(),
    Config;
end_per_group(signature_combinations, Config) ->
    cleanup_saml_module(),
    Config;
end_per_group(_, Config) ->
    Config.

init_per_testcase(Case, Config) ->
    ct:pal("Starting test case: ~p", [Case]),
    Config.

end_per_testcase(Case, _Config) ->
    ct:pal("Finished test case: ~p", [Case]),
    %% Cleanup any SAML backend created during test
    _ = emqx_dashboard_sso_manager:delete(saml),
    ok.

%%------------------------------------------------------------------------------
%% Unit Tests
%%------------------------------------------------------------------------------

t_validate_signature_config(_Config) ->
    %% Test signature configuration validation
    %% When both signatures are disabled, should log warning but succeed
    FakeFingerprints = [crypto:hash(sha, <<"fake_cert">>)],

    %% Valid: both signatures enabled with fingerprints
    ?assertEqual(
        ok, emqx_dashboard_sso_saml:validate_signature_config(true, true, FakeFingerprints)
    ),

    %% Valid: envelope only
    ?assertEqual(
        ok, emqx_dashboard_sso_saml:validate_signature_config(true, false, FakeFingerprints)
    ),

    %% Valid: assertion only
    ?assertEqual(
        ok, emqx_dashboard_sso_saml:validate_signature_config(false, true, FakeFingerprints)
    ),

    %% Valid but warns: no signatures (insecure)
    ?assertEqual(
        ok, emqx_dashboard_sso_saml:validate_signature_config(false, false, FakeFingerprints)
    ),

    ok.

t_maybe_load_cert_or_key(_Config) ->
    %% Test cert/key loading helper
    LoadFun = fun(Path) -> {loaded, Path} end,

    %% undefined returns undefined
    ?assertEqual(undefined, emqx_dashboard_sso_saml:maybe_load_cert_or_key(undefined, LoadFun)),

    %% Empty binary returns undefined
    ?assertEqual(undefined, emqx_dashboard_sso_saml:maybe_load_cert_or_key(<<>>, LoadFun)),

    %% Empty string returns undefined
    ?assertEqual(undefined, emqx_dashboard_sso_saml:maybe_load_cert_or_key("", LoadFun)),

    %% Binary path calls function with list
    ?assertEqual(
        {loaded, "/path/to/cert"},
        emqx_dashboard_sso_saml:maybe_load_cert_or_key(<<"/path/to/cert">>, LoadFun)
    ),

    %% List path calls function directly
    ?assertEqual(
        {loaded, "/path/to/cert"},
        emqx_dashboard_sso_saml:maybe_load_cert_or_key("/path/to/cert", LoadFun)
    ),

    ok.

%%------------------------------------------------------------------------------
%% API Tests
%%------------------------------------------------------------------------------

t_sso_running_disabled(_Config) ->
    %% SSO running endpoint should show empty when not configured
    {ok, Result} = request(get, uri(["sso", "running"]), []),
    ?assertEqual([], emqx_utils_json:decode(Result, [return_maps])),
    ok.

t_saml_metadata_not_initialized(_Config) ->
    %% Metadata endpoint should return error when SAML is not initialized
    Result = request(get, uri(["sso", "saml", "metadata"]), []),
    case Result of
        {ok, _} ->
            ct:fail("Expected error, got success");
        {error, {_, 404, _}} ->
            %% Not found - SAML not configured
            ok;
        {error, {_, 400, _}} ->
            %% Bad request - also acceptable
            ok;
        {error, {_, Code, _}} ->
            ct:fail("Expected 400 or 404, got ~p", [Code])
    end.

%%------------------------------------------------------------------------------
%% Keycloak Integration Tests
%%------------------------------------------------------------------------------

t_saml_init_with_keycloak(_Config) ->
    %% Initialize SAML module with Keycloak (default settings)
    Config = make_saml_config(#{
        sp_entity_id => <<"emqx-sp-default">>,
        sp_sign_request => false,
        sp_public_key => <<>>,
        sp_private_key => <<>>,
        idp_signs_envelopes => true,
        idp_signs_assertions => true
    }),

    %% Initialize SAML via create
    Result = emqx_dashboard_sso_saml:create(Config),
    ?assertMatch({ok, _}, Result),

    {ok, State} = Result,
    ?assert(maps:is_key(sp, State)),
    ?assert(maps:is_key(idp_meta, State)),

    %% Store state for other tests
    persistent_term:put(saml_test_state, State).

t_saml_metadata_generation(_Config) ->
    %% Test SP metadata generation
    State = persistent_term:get(saml_test_state),

    SP = maps:get(sp, State),
    MetadataXml = esaml_sp:generate_metadata(SP),
    Metadata = xmerl:export_simple([MetadataXml], xmerl_xml),
    MetadataBin = iolist_to_binary(Metadata),
    ?assert(is_binary(MetadataBin)),

    %% Metadata should be valid XML
    ?assertMatch({match, _}, re:run(MetadataBin, "EntityDescriptor")),
    ?assertMatch({match, _}, re:run(MetadataBin, "entityID=")),
    ?assertMatch({match, _}, re:run(MetadataBin, "AssertionConsumerService")).

t_saml_login_redirect(_Config) ->
    %% Test SAML login redirect generation
    State = persistent_term:get(saml_test_state),

    Req = #{headers => #{}},
    {redirect, Response} = emqx_dashboard_sso_saml:login(Req, State),

    ?assertMatch({302, _, _}, Response),
    {302, Headers, _Body} = Response,

    %% Should have location header pointing to Keycloak
    Location = maps:get(<<"location">>, Headers),
    ?assert(is_binary(Location)),
    ?assertMatch({match, _}, re:run(Location, "SAMLRequest=")).

t_saml_callback_invalid_response(_Config) ->
    %% Test SAML callback with invalid response
    State = persistent_term:get(saml_test_state),

    %% Invalid SAMLResponse
    InvalidBase64 = base64:encode(<<"not valid xml">>),
    Req1 = #{body => <<"SAMLResponse=", InvalidBase64/binary>>},
    Result1 = emqx_dashboard_sso_saml:callback(Req1, State),
    ?assertMatch({error, _}, Result1),

    %% Empty SAMLResponse should fail gracefully
    EmptyBase64 = base64:encode(<<>>),
    Req2 = #{body => <<"SAMLResponse=", EmptyBase64/binary>>},
    Result2 = emqx_dashboard_sso_saml:callback(Req2, State),
    ?assertMatch({error, _}, Result2).

t_saml_full_login_flow(_Config) ->
    %% Full SAML E2E login flow test with Keycloak
    State = persistent_term:get(saml_test_state),

    ct:pal("=== Starting Full E2E SAML Login Flow ==="),

    %% Step 1: Get SAML login redirect URL from EMQX
    ct:pal("Step 1: Getting SAML AuthnRequest redirect URL"),
    Req = #{headers => #{}},
    {redirect, {302, Headers, _}} = emqx_dashboard_sso_saml:login(Req, State),
    LoginUrl = binary_to_list(maps:get(<<"location">>, Headers)),
    ct:pal("SAML Login URL: ~s", [LoginUrl]),

    %% Step 2: Follow redirect to Keycloak login page
    ct:pal("Step 2: Following redirect to Keycloak"),
    {ok, {{_, 200, _}, LoginHeaders, LoginPageBody}} =
        httpc:request(get, {LoginUrl, []}, [{autoredirect, true}], [{body_format, binary}]),

    Cookies = extract_cookies(LoginHeaders),
    ct:pal("Got Keycloak session cookies: ~p", [length(Cookies)]),

    %% Step 3: Parse login form to get action URL
    ct:pal("Step 3: Parsing Keycloak login form"),
    {ok, FormAction} = extract_form_action(LoginPageBody),
    ct:pal("Form action URL: ~s", [FormAction]),

    %% Step 4: Submit credentials to Keycloak
    ct:pal("Step 4: Submitting credentials to Keycloak"),
    LoginData =
        "username=" ++ ?KEYCLOAK_TEST_USER ++ "&password=" ++ ?KEYCLOAK_TEST_PASSWORD ++
            "&credentialId=",
    CookieHeader = {"Cookie", format_cookies(Cookies)},

    {ok, {{_, LoginRespCode, _}, LoginRespHeaders, LoginRespBody}} =
        httpc:request(
            post,
            {FormAction, [CookieHeader], "application/x-www-form-urlencoded", LoginData},
            [{autoredirect, false}],
            [{body_format, binary}]
        ),

    ct:pal("Login response code: ~p", [LoginRespCode]),

    %% Step 5: Handle the SAML response
    SAMLResponse =
        case LoginRespCode of
            200 ->
                ct:pal("Step 5: Extracting SAMLResponse from form (POST binding)"),
                {ok, SAMLResp} = extract_saml_response(LoginRespBody),
                SAMLResp;
            302 ->
                ct:pal("Step 5: Following redirect to get SAMLResponse"),
                RedirectLocation = proplists:get_value("location", LoginRespHeaders),
                ct:pal("Redirect to: ~s", [RedirectLocation]),
                NewCookies = extract_cookies(LoginRespHeaders) ++ Cookies,
                {ok, {{_, 200, _}, _, RedirectBody}} =
                    httpc:request(
                        get,
                        {RedirectLocation, [{"Cookie", format_cookies(NewCookies)}]},
                        [{autoredirect, false}],
                        [{body_format, binary}]
                    ),
                {ok, SAMLResp} = extract_saml_response(RedirectBody),
                SAMLResp
        end,

    ct:pal("Got SAMLResponse (length: ~p bytes)", [byte_size(SAMLResponse)]),

    %% Step 6: Submit SAMLResponse to EMQX callback
    ct:pal("Step 6: Submitting SAMLResponse to EMQX callback"),
    CallbackBody = <<"SAMLResponse=", (uri_encode(SAMLResponse))/binary>>,
    CallbackReq = #{body => CallbackBody},

    CallbackResult = emqx_dashboard_sso_saml:callback(CallbackReq, State),
    ct:pal("Callback result: ~p", [CallbackResult]),

    %% Step 7: Verify successful login
    ct:pal("Step 7: Verifying login result"),
    case CallbackResult of
        {redirect, Username, {302, RedirectHeaders, _}} ->
            ct:pal("SUCCESS! User logged in: ~s", [Username]),
            RedirectUrl = maps:get(<<"location">>, RedirectHeaders),
            ct:pal("Redirect URL: ~s", [RedirectUrl]),
            ?assertMatch({match, _}, re:run(RedirectUrl, "login_meta=")),
            ?assertEqual(<<"testuser">>, Username);
        {error, Reason} ->
            ct:pal("Login failed with reason: ~p", [Reason]),
            ct:fail("SAML callback failed: ~p", [Reason])
    end,

    ct:pal("=== Full E2E SAML Login Flow Completed Successfully ==="),
    ok.

%%------------------------------------------------------------------------------
%% Signature Combination E2E Tests
%%------------------------------------------------------------------------------

t_sig_combo_no_sp_both_idp(_Config) ->
    ct:pal("=== E2E Signature Combo: SP=false, Envelope=true, Assertion=true (Default) ==="),

    Config = make_saml_config(#{
        sp_entity_id => <<"emqx-sp-default">>,
        sp_sign_request => false,
        sp_public_key => <<>>,
        sp_private_key => <<>>,
        idp_signs_envelopes => true,
        idp_signs_assertions => true
    }),

    run_signature_combo_e2e_test(Config, #{
        expect_sp_signature => false,
        test_name => "SP=false, Envelope=true, Assertion=true"
    }).

t_sig_combo_no_sp_envelope_only(_Config) ->
    ct:pal("=== E2E Signature Combo: SP=false, Envelope=true, Assertion=false ==="),

    Config = make_saml_config(#{
        sp_entity_id => <<"emqx-sp-envelope-only">>,
        sp_sign_request => false,
        sp_public_key => <<>>,
        sp_private_key => <<>>,
        idp_signs_envelopes => true,
        idp_signs_assertions => false
    }),

    run_signature_combo_e2e_test(Config, #{
        expect_sp_signature => false,
        test_name => "SP=false, Envelope=true, Assertion=false"
    }).

t_sig_combo_no_sp_assertion_only(_Config) ->
    ct:pal("=== E2E Signature Combo: SP=false, Envelope=false, Assertion=true ==="),

    Config = make_saml_config(#{
        sp_entity_id => <<"emqx-sp-assertion-only">>,
        sp_sign_request => false,
        sp_public_key => <<>>,
        sp_private_key => <<>>,
        idp_signs_envelopes => false,
        idp_signs_assertions => true
    }),

    run_signature_combo_e2e_test(Config, #{
        expect_sp_signature => false,
        test_name => "SP=false, Envelope=false, Assertion=true"
    }).

t_sig_combo_no_signatures(_Config) ->
    ct:pal("=== E2E Signature Combo: SP=false, Envelope=false, Assertion=false (INSECURE) ==="),

    Config = make_saml_config(#{
        sp_entity_id => <<"emqx-sp-no-signatures">>,
        sp_sign_request => false,
        sp_public_key => <<>>,
        sp_private_key => <<>>,
        idp_signs_envelopes => false,
        idp_signs_assertions => false
    }),

    run_signature_combo_e2e_test(Config, #{
        expect_sp_signature => false,
        test_name => "SP=false, Envelope=false, Assertion=false (INSECURE)"
    }).

t_sig_combo_sp_sign_both_idp(_Config) ->
    ct:pal("=== E2E Signature Combo: SP=true, Envelope=true, Assertion=true (Full Security) ==="),

    {SpPublicKey, SpPrivateKey} = get_sp_cert_paths(),

    Config = make_saml_config(#{
        sp_entity_id => <<"emqx-sp-full-security">>,
        sp_sign_request => true,
        sp_public_key => SpPublicKey,
        sp_private_key => SpPrivateKey,
        idp_signs_envelopes => true,
        idp_signs_assertions => true
    }),

    run_signature_combo_e2e_test(Config, #{
        expect_sp_signature => true,
        test_name => "SP=true, Envelope=true, Assertion=true (Full Security)"
    }).

%%------------------------------------------------------------------------------
%% Helper Functions
%%------------------------------------------------------------------------------

uri(Parts) ->
    ?BASE_URL ++ "/" ++ filename:join(["api", "v5" | Parts]).

request(Method, Url, Body) ->
    emqx_mgmt_api_test_util:request_api(Method, Url, [], [], Body).

%% @doc Check if Keycloak is available for integration tests
is_keycloak_available() ->
    Url = keycloak_metadata_url(),
    case httpc:request(get, {Url, []}, [{timeout, 5000}], []) of
        {ok, {{_, 200, _}, _, _}} ->
            true;
        _ ->
            false
    end.

keycloak_metadata_url() ->
    "http://" ++ ?KEYCLOAK_HOST ++ ":" ++ integer_to_list(?KEYCLOAK_HTTP_PORT) ++
        "/realms/" ++ ?KEYCLOAK_REALM ++ "/protocol/saml/descriptor".

%% @doc Make SAML configuration with defaults
make_saml_config(Overrides) ->
    Defaults = #{
        enable => true,
        dashboard_addr => <<"http://172.100.239.1:18083">>,
        idp_metadata_url => list_to_binary(keycloak_metadata_url()),
        sp_sign_request => false,
        sp_public_key => <<>>,
        sp_private_key => <<>>,
        idp_signs_envelopes => true,
        idp_signs_assertions => true
    },
    maps:merge(Defaults, Overrides).

%% @doc Get SP certificate paths
get_sp_cert_paths() ->
    BaseDir = get_project_root(),
    SpPublicKey = filename:join([BaseDir, ?SP_CERT_DIR, "sp_public.pem"]),
    SpPrivateKey = filename:join([BaseDir, ?SP_CERT_DIR, "sp_private.pem"]),
    {list_to_binary(SpPublicKey), list_to_binary(SpPrivateKey)}.

%% @doc Get project root directory
get_project_root() ->
    {ok, Cwd} = file:get_cwd(),
    find_project_root(Cwd).

find_project_root("/") ->
    {ok, Cwd} = file:get_cwd(),
    Cwd;
find_project_root(Dir) ->
    %% Look for the .ci directory which only exists at the umbrella project root
    CiDir = filename:join(Dir, ".ci"),
    case filelib:is_dir(CiDir) of
        true -> Dir;
        false -> find_project_root(filename:dirname(Dir))
    end.

%% @doc Cleanup SAML module state
cleanup_saml_module() ->
    catch persistent_term:erase(saml_test_state),
    _ = emqx_dashboard_sso_manager:delete(saml),
    ok.

%%------------------------------------------------------------------------------
%% E2E Test Helper Functions
%%------------------------------------------------------------------------------

%% @doc Extract cookies from HTTP response headers
extract_cookies(Headers) ->
    [Cookie || {"set-cookie", Cookie} <- Headers].

%% @doc Format cookies for Cookie header
format_cookies(Cookies) ->
    CookieParts = [extract_cookie_value(C) || C <- Cookies],
    string:join(CookieParts, "; ").

extract_cookie_value(Cookie) ->
    [NameValue | _] = string:split(Cookie, ";"),
    string:trim(NameValue).

%% @doc Extract form action URL from Keycloak login page HTML
extract_form_action(Html) ->
    case re:run(Html, <<"action=\"([^\"]+)\"">>, [{capture, [1], binary}]) of
        {match, [ActionUrl]} ->
            Unescaped = binary:replace(ActionUrl, <<"&amp;">>, <<"&">>, [global]),
            {ok, binary_to_list(Unescaped)};
        nomatch ->
            {error, form_action_not_found}
    end.

%% @doc Extract SAMLResponse from Keycloak's auto-submit form
extract_saml_response(Html) ->
    case
        re:run(Html, <<"name=\"SAMLResponse\"[^>]*value=\"([^\"]+)\"">>, [{capture, [1], binary}])
    of
        {match, [SAMLResponse]} ->
            {ok, SAMLResponse};
        nomatch ->
            case
                re:run(Html, <<"value=\"([^\"]+)\"[^>]*name=\"SAMLResponse\"">>, [
                    {capture, [1], binary}
                ])
            of
                {match, [SAMLResponse]} ->
                    {ok, SAMLResponse};
                nomatch ->
                    {error, saml_response_not_found}
            end
    end.

%% @doc URL encode binary for form submission
uri_encode(Bin) when is_binary(Bin) ->
    list_to_binary(uri_encode_string(binary_to_list(Bin))).

uri_encode_string([]) ->
    [];
uri_encode_string([H | T]) ->
    if
        H >= $a, H =< $z -> [H | uri_encode_string(T)];
        H >= $A, H =< $Z -> [H | uri_encode_string(T)];
        H >= $0, H =< $9 -> [H | uri_encode_string(T)];
        H =:= $-; H =:= $_; H =:= $.; H =:= $~ -> [H | uri_encode_string(T)];
        true -> [$%, hex(H div 16), hex(H rem 16) | uri_encode_string(T)]
    end.

hex(N) when N < 10 -> $0 + N;
hex(N) -> $A + N - 10.

%% @doc Run E2E signature combination test
run_signature_combo_e2e_test(Config, TestOpts) ->
    #{expect_sp_signature := ExpectSpSig, test_name := TestName} = TestOpts,

    %% Step 1: Initialize SAML with the configuration
    ct:pal("Step 1: Initializing SAML with config for ~s", [TestName]),
    {ok, State} = emqx_dashboard_sso_saml:create(Config),
    ?assert(maps:is_key(sp, State)),
    ?assert(maps:is_key(idp_meta, State)),

    %% Verify SP signature settings
    SP = maps:get(sp, State),
    ?assertEqual(ExpectSpSig, SP#esaml_sp.sp_sign_requests),

    %% Step 2: Get login redirect URL
    ct:pal("Step 2: Getting SAML AuthnRequest redirect URL"),
    {redirect, {302, Headers, _}} = emqx_dashboard_sso_saml:login(#{headers => #{}}, State),
    LoginUrl = binary_to_list(maps:get(<<"location">>, Headers)),

    %% Verify SP signature presence in URL
    case ExpectSpSig of
        true ->
            ?assertMatch({match, _}, re:run(LoginUrl, "Signature=")),
            ?assertMatch({match, _}, re:run(LoginUrl, "SigAlg=")),
            ct:pal("Verified: AuthnRequest URL contains SP signature");
        false ->
            ?assertMatch(nomatch, re:run(LoginUrl, "Signature=")),
            ct:pal("Verified: AuthnRequest URL does NOT contain SP signature")
    end,

    %% Step 3: Follow redirect to Keycloak login page
    ct:pal("Step 3: Following redirect to Keycloak"),
    {ok, {{_, 200, _}, LoginHeaders, LoginPageBody}} =
        httpc:request(get, {LoginUrl, []}, [{autoredirect, true}], [{body_format, binary}]),
    Cookies = extract_cookies(LoginHeaders),
    ct:pal("Got Keycloak session cookies: ~p", [length(Cookies)]),

    %% Step 4: Parse login form to get action URL
    ct:pal("Step 4: Parsing Keycloak login form"),
    {ok, FormAction} = extract_form_action(LoginPageBody),

    %% Step 5: Submit credentials to Keycloak
    ct:pal("Step 5: Submitting credentials to Keycloak"),
    LoginData =
        "username=" ++ ?KEYCLOAK_TEST_USER ++ "&password=" ++ ?KEYCLOAK_TEST_PASSWORD ++
            "&credentialId=",
    CookieHeader = {"Cookie", format_cookies(Cookies)},

    {ok, {{_, LoginRespCode, _}, LoginRespHeaders, LoginRespBody}} =
        httpc:request(
            post,
            {FormAction, [CookieHeader], "application/x-www-form-urlencoded", LoginData},
            [{autoredirect, false}],
            [{body_format, binary}]
        ),

    ct:pal("Login response code: ~p", [LoginRespCode]),

    %% Step 6: Handle the SAML response
    SAMLResponse =
        case LoginRespCode of
            200 ->
                ct:pal("Step 6: Extracting SAMLResponse from form (POST binding)"),
                {ok, SAMLResp} = extract_saml_response(LoginRespBody),
                SAMLResp;
            302 ->
                ct:pal("Step 6: Following redirect to get SAMLResponse"),
                RedirectLocation = proplists:get_value("location", LoginRespHeaders),
                NewCookies = extract_cookies(LoginRespHeaders) ++ Cookies,
                {ok, {{_, 200, _}, _, RedirectBody}} =
                    httpc:request(
                        get,
                        {RedirectLocation, [{"Cookie", format_cookies(NewCookies)}]},
                        [{autoredirect, false}],
                        [{body_format, binary}]
                    ),
                {ok, SAMLResp} = extract_saml_response(RedirectBody),
                SAMLResp
        end,

    ct:pal("Got SAMLResponse (length: ~p bytes)", [byte_size(SAMLResponse)]),

    %% Step 7: Submit SAMLResponse to EMQX callback
    ct:pal("Step 7: Submitting SAMLResponse to EMQX callback"),
    CallbackBody = <<"SAMLResponse=", (uri_encode(SAMLResponse))/binary>>,
    CallbackReq = #{body => CallbackBody},

    CallbackResult = emqx_dashboard_sso_saml:callback(CallbackReq, State),
    ct:pal("Callback result: ~p", [CallbackResult]),

    %% Step 8: Verify successful login
    ct:pal("Step 8: Verifying login result"),
    case CallbackResult of
        {redirect, Username, {302, RedirectHeaders, _}} ->
            ct:pal("SUCCESS! User logged in: ~s", [Username]),
            RedirectUrl = maps:get(<<"location">>, RedirectHeaders),
            ?assertMatch({match, _}, re:run(RedirectUrl, "login_meta=")),
            ?assertEqual(<<"testuser">>, Username);
        {error, Reason} ->
            ct:pal("Login failed with reason: ~p", [Reason]),
            ct:fail("SAML callback failed for ~s: ~p", [TestName, Reason])
    end,

    %% Cleanup
    _ = emqx_dashboard_sso_manager:delete(saml),
    ct:pal("=== E2E Signature Combo Test PASSED: ~s ===", [TestName]),
    ok.
