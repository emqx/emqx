%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_default_cert_gen_SUITE).

-compile([nowarn_export_all, export_all]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("public_key/include/public_key.hrl").

%%------------------------------------------------------------------------------
%% CT Boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

sans() ->
    [
        {dns, "localhost"},
        {ip, {127, 0, 0, 1}},
        {ip, {0, 0, 0, 0, 0, 0, 0, 1}}
    ].

decode_cert(Pem) ->
    [{'Certificate', Der, not_encrypted}] = public_key:pem_decode(Pem),
    {Der, public_key:pkix_decode_cert(Der, otp)}.

tbs(#'OTPCertificate'{tbsCertificate = TBS}) ->
    TBS.

extension(ExtnID, #'OTPTBSCertificate'{extensions = Exts}) ->
    case lists:keyfind(ExtnID, #'Extension'.extnID, Exts) of
        false -> undefined;
        #'Extension'{extnValue = Value} -> Value
    end.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

-doc "A leaf certificate validates against the CA that signed it.".
t_leaf_validates_against_ca(_TCConfig) ->
    #{chain := ChainPem, ca := CaPem} = emqx_default_cert:self_signed_bundle(#{
        cn => "localhost", sans => sans()
    }),
    {LeafDer, _} = decode_cert(ChainPem),
    {CaDer, _} = decode_cert(CaPem),
    ?assertMatch({ok, _}, public_key:pkix_path_validation(CaDer, [LeafDer], [])).

-doc "SANs on the leaf carry the DNS name and both the IPv4 and IPv6 addresses.".
t_sans_dns_and_ip(_TCConfig) ->
    #{chain := ChainPem} = emqx_default_cert:self_signed_bundle(#{
        cn => "localhost", sans => sans()
    }),
    {_, LeafOtp} = decode_cert(ChainPem),
    SANs = extension(?'id-ce-subjectAltName', tbs(LeafOtp)),
    ?assert(lists:member({dNSName, "localhost"}, SANs)),
    ?assert(lists:member({iPAddress, <<127, 0, 0, 1>>}, SANs)),
    IPv6Bytes = <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1>>,
    ?assert(lists:member({iPAddress, IPv6Bytes}, SANs)).

-doc "The CA certificate has basicConstraints CA:TRUE; the leaf has CA:FALSE.".
t_basic_constraints(_TCConfig) ->
    #{chain := ChainPem, ca := CaPem} = emqx_default_cert:self_signed_bundle(#{
        cn => "localhost", sans => sans()
    }),
    {_, LeafOtp} = decode_cert(ChainPem),
    {_, CaOtp} = decode_cert(CaPem),
    ?assertMatch(
        #'BasicConstraints'{cA = true},
        extension(?'id-ce-basicConstraints', tbs(CaOtp))
    ),
    ?assertMatch(
        #'BasicConstraints'{cA = false},
        extension(?'id-ce-basicConstraints', tbs(LeafOtp))
    ).

-doc "The validity window starts in the past and spans the configured number of days.".
t_validity_window(_TCConfig) ->
    #{chain := ChainPem} = emqx_default_cert:self_signed_bundle(#{
        cn => "localhost", sans => sans()
    }),
    {_, LeafOtp} = decode_cert(ChainPem),
    #'OTPTBSCertificate'{
        validity = #'Validity'{
            notBefore = {generalTime, NotBefore},
            notAfter = {generalTime, NotAfter}
        }
    } = tbs(LeafOtp),
    Today = calendar:date_to_gregorian_days(erlang:date()),
    NotBeforeDay = parse_generaltime_day(NotBefore),
    NotAfterDay = parse_generaltime_day(NotAfter),
    ?assertEqual(Today - 1, NotBeforeDay),
    ?assertEqual(Today + 3650, NotAfterDay).

parse_generaltime_day([Y1, Y2, Y3, Y4, Mo1, Mo2, D1, D2 | _]) ->
    Year = list_to_integer([Y1, Y2, Y3, Y4]),
    Month = list_to_integer([Mo1, Mo2]),
    Day = list_to_integer([D1, D2]),
    calendar:date_to_gregorian_days({Year, Month, Day}).

-doc "Two calls to self_signed_bundle/1 produce different keys and different serial numbers.".
t_distinct_keys_and_serials(_TCConfig) ->
    #{key := Key1, chain := Chain1} = emqx_default_cert:self_signed_bundle(#{cn => "localhost"}),
    #{key := Key2, chain := Chain2} = emqx_default_cert:self_signed_bundle(#{cn => "localhost"}),
    ?assertNotEqual(Key1, Key2),
    {_, Otp1} = decode_cert(Chain1),
    {_, Otp2} = decode_cert(Chain2),
    #'OTPTBSCertificate'{serialNumber = Serial1} = tbs(Otp1),
    #'OTPTBSCertificate'{serialNumber = Serial2} = tbs(Otp2),
    ?assertNotEqual(Serial1, Serial2).

-doc "self_signed_bundle/1 returns no CA private key, in any form, in any value.".
t_no_ca_key_in_bundle(_TCConfig) ->
    Bundle = emqx_default_cert:self_signed_bundle(#{cn => "localhost", sans => sans()}),
    ?assertEqual([ca, chain, key], lists:sort(maps:keys(Bundle))),
    lists:foreach(
        fun(Pem) ->
            Entries = public_key:pem_decode(Pem),
            ?assertEqual(
                [],
                [
                    E
                 || {Kind, _, _} = E <- Entries,
                    Kind =:= 'RSAPrivateKey' orelse Kind =:= 'ECPrivateKey'
                ]
            )
        end,
        [maps:get(chain, Bundle), maps:get(ca, Bundle)]
    ).

-doc "Both rsa and ec key types generate a bundle whose leaf validates against its CA.".
t_rsa_and_ec_key_types(_TCConfig) ->
    lists:foreach(
        fun(KeyType) ->
            #{chain := ChainPem, ca := CaPem} = emqx_default_cert:self_signed_bundle(#{
                cn => "localhost", sans => sans(), key_type => KeyType
            }),
            {LeafDer, _} = decode_cert(ChainPem),
            {CaDer, _} = decode_cert(CaPem),
            ?assertMatch({ok, _}, public_key:pkix_path_validation(CaDer, [LeafDer], []))
        end,
        [rsa, ec]
    ).
