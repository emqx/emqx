%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_license_parser_legacy_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("public_key/include/public_key.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    _ = application:load(emqx_conf),
    emqx_common_test_helpers:start_apps([emqx_license], fun set_special_configs/1),
    Config.

end_per_suite(_) ->
    emqx_common_test_helpers:stop_apps([emqx_license]),
    ok.

init_per_testcase(_Case, Config) ->
    {ok, _} = emqx_cluster_rpc:start_link(node(), emqx_cluster_rpc, 1000),
    Config.

end_per_testcase(_Case, _Config) ->
    ok.

set_special_configs(emqx_license) ->
    Config = #{file => emqx_license_test_lib:default_license()},
    emqx_config:put([license], Config);
set_special_configs(_) ->
    ok.

%%------------------------------------------------------------------------------
%% Tests - emqx_license_parser API
%%------------------------------------------------------------------------------

t_parse(_Config) ->
    ?assertMatch({ok, _}, emqx_license_parser:parse(sample_license(), public_key_pem())),

    Res1 = emqx_license_parser:parse(tampered_license(), public_key_pem()),
    ?assertMatch({error, _}, Res1),
    {error, Errors} = Res1,
    ?assertEqual(
        invalid_signature,
        proplists:get_value(emqx_license_parser_legacy, Errors)
    ),

    ok.

t_dump(_Config) ->
    {ok, License} = emqx_license_parser:parse(sample_license(), public_key_pem()),
    ?assertEqual(
        [
            {customer, <<"EMQ X Evaluation">>},
            {email, "contact@emqx.io"},
            {deployment, "default"},
            {max_connections, 10},
            {start_at, <<"2020-06-20">>},
            {expiry_at, <<"2049-01-01">>},
            {type, <<"official">>},
            {customer_type, 10},
            {expiry, false}
        ],
        emqx_license_parser:dump(License)
    ).

t_customer_type(_Config) ->
    {ok, License} = emqx_license_parser:parse(sample_license(), public_key_pem()),
    ?assertEqual(10, emqx_license_parser:customer_type(License)).

t_license_type(_Config) ->
    {ok, License} = emqx_license_parser:parse(sample_license(), public_key_pem()),
    ?assertEqual(1, emqx_license_parser:license_type(License)).

t_max_connections(_Config) ->
    {ok, License} = emqx_license_parser:parse(sample_license(), public_key_pem()),
    ?assertEqual(10, emqx_license_parser:max_connections(License)).

t_expiry_date(_Config) ->
    {ok, License} = emqx_license_parser:parse(sample_license(), public_key_pem()),
    ?assertEqual({2049, 1, 1}, emqx_license_parser:expiry_date(License)).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

%% not used for this parser, but required for the behaviour.
public_key_pem() ->
    emqx_license_test_lib:public_key_pem().

sample_license() ->
    emqx_license_test_lib:legacy_license().

tampered_license() ->
    LicenseBin = emqx_license_test_lib:legacy_license(),
    [{'Certificate', DerCert, _}] = public_key:pem_decode(LicenseBin),
    Cert = public_key:pkix_decode_cert(DerCert, otp),
    TbsCert = Cert#'OTPCertificate'.tbsCertificate,
    Validity0 = TbsCert#'OTPTBSCertificate'.validity,
    Validity = Validity0#'Validity'{notBefore = {utcTime, "19800620030252Z"}},

    TamperedCert = Cert#'OTPCertificate'{
        tbsCertificate =
            TbsCert#'OTPTBSCertificate'{
                validity = Validity
            }
    },
    TamperedCertDer = public_key:pkix_encode('OTPCertificate', TamperedCert, otp),
    public_key:pem_encode([{'Certificate', TamperedCertDer, not_encrypted}]).
