%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_default_cert).

-moduledoc """
Self-signed X.509 certificate generation using the OTP `public_key`
application, with no external tools required.

`self_signed_bundle/1` generates a one-off root CA and a leaf
certificate signed by it, and returns only the leaf key, the leaf
certificate and the CA certificate: the CA private key is discarded
after signing. No further certificate can be issued under that CA, so
a caller that trusts only this CA is safe even though the CA's key was
never kept anywhere.

Key parameters:

* The default key type is `ec`, using `secp256r1`: smaller keys and
  faster handshakes than RSA, and universally supported by TLS
  clients. `rsa` (2048 bits) is also available for callers that need
  it. Both are the current minimum widely accepted for a server
  certificate.
* Certificates are valid for 3650 days (about ten years) starting one
  day in the past, so a small clock skew between the generating node
  and a client does not make the certificate look not-yet-valid.

This module has no callers. It is a pure cryptography library.
""".

%% ASN.1 record fields and values (e.g. `namedCurve`, `generalTime`)
%% are camel-cased by the public_key application.
-elvis([{elvis_style, atom_naming_convention, disable}]).

-include_lib("public_key/include/public_key.hrl").
-include("emqx_managed_certs.hrl").

-export([
    generate_ca/1,
    generate_cert/2,
    self_signed_bundle/1
]).

-export_type([pem_pair/0, cert_bundle/0, san/0, key_type/0]).

-define(VALIDITY_DAYS, 3650).
-define(CURVE, secp256r1).
-define(RSA_KEY_SIZE, 2048).

-type pem() :: binary().
-type pem_pair() :: #{cert_pem := pem(), key_pem := pem()}.
-type san() :: {dns, string()} | {ip, inet:ip_address()}.
-type key_type() :: rsa | ec.
-type cert_bundle() :: #{
    ?FILE_KIND_KEY := pem(),
    ?FILE_KIND_CHAIN := pem(),
    ?FILE_KIND_CA := pem()
}.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-doc """
Generates a root CA certificate and its private key.

Options: `cn` (required) becomes the subject common name; `org` is the
subject organization name (defaults to `"EMQX"`); `key_type` is `ec`
(default) or `rsa`.
""".
-spec generate_ca(#{cn := string(), org => string(), key_type => key_type()}) -> pem_pair().
generate_ca(#{cn := _} = Opts) ->
    Key = gen_key(maps:get(key_type, Opts, ec)),
    Subject = subject(Opts),
    TBS = tbs_certificate(Subject, Subject, Key, Key, ca_extensions()),
    Der = public_key:pkix_sign(TBS, Key),
    #{cert_pem => cert_to_pem(Der), key_pem => key_to_pem(Key)}.

-doc """
Generates a certificate signed by the given CA.

Options: `cn` (required) becomes the subject common name; `org` is the
subject organization name (defaults to `"EMQX"`); `sans` is the list of
subject alternative names (defaults to `[]`); `key_type` is `ec`
(default) or `rsa`.
""".
-spec generate_cert(pem_pair(), #{
    cn := string(),
    org => string(),
    sans => [san()],
    key_type => key_type()
}) -> pem_pair().
generate_cert(#{cert_pem := CaCertPem, key_pem := CaKeyPem}, Opts) ->
    SANs = maps:get(sans, Opts, []),
    CaKey = pem_to_key(CaKeyPem),
    Issuer = cert_subject(CaCertPem),
    Key = gen_key(maps:get(key_type, Opts, ec)),
    TBS = tbs_certificate(subject(Opts), Issuer, Key, CaKey, leaf_extensions(Key, SANs)),
    Der = public_key:pkix_sign(TBS, CaKey),
    #{cert_pem => cert_to_pem(Der), key_pem => key_to_pem(Key)}.

-doc """
Generates a complete self-signed certificate bundle: a one-off CA and a
leaf certificate signed by it. The returned map is keyed by
managed-certs file kinds. The CA private key is generated, used to
sign the leaf, and then discarded: it is not part of the returned
bundle and is not retained anywhere, so nothing can issue another
certificate under that CA afterwards.
""".
-spec self_signed_bundle(#{cn := string(), org => string(), sans => [san()], key_type => key_type()}) ->
    cert_bundle().
self_signed_bundle(Opts) ->
    #{cert_pem := CaCertPem} = CA = generate_ca(#{cn => "EMQX Self-Signed CA"}),
    #{cert_pem := CertPem, key_pem := KeyPem} = generate_cert(CA, Opts),
    #{
        ?FILE_KIND_KEY => KeyPem,
        ?FILE_KIND_CHAIN => CertPem,
        ?FILE_KIND_CA => CaCertPem
    }.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

gen_key(rsa) ->
    public_key:generate_key({rsa, ?RSA_KEY_SIZE, 65537});
gen_key(ec) ->
    public_key:generate_key({namedCurve, ?CURVE}).

tbs_certificate(Subject, Issuer, SubjectKey, SignerKey, Extensions) ->
    #'OTPTBSCertificate'{
        version = v3,
        serialNumber = serial_number(),
        signature = signature_algorithm(SignerKey),
        issuer = Issuer,
        validity = validity(),
        subject = Subject,
        subjectPublicKeyInfo = public_key_info(SubjectKey),
        extensions = Extensions
    }.

%% 8 random bytes plus 1, so the serial number is always positive
%% (no sign-bit ambiguity in the DER INTEGER encoding) and practically
%% never repeats between two certificates generated on the same node.
serial_number() ->
    1 + binary:decode_unsigned(crypto:strong_rand_bytes(8)).

signature_algorithm(#'RSAPrivateKey'{}) ->
    #'SignatureAlgorithm'{
        algorithm = ?'sha256WithRSAEncryption',
        %% DER-encoded NULL, the shape `public_key:pkix_decode_cert(_, otp)`
        %% itself uses for this field
        parameters = {asn1_OPENTYPE, <<5, 0>>}
    };
signature_algorithm(#'ECPrivateKey'{parameters = Params}) ->
    #'SignatureAlgorithm'{
        algorithm = ?'ecdsa-with-SHA256',
        parameters = Params
    }.

validity() ->
    From = shift_date(erlang:date(), -1),
    To = shift_date(erlang:date(), ?VALIDITY_DAYS),
    #'Validity'{
        notBefore = {generalTime, format_date(From)},
        notAfter = {generalTime, format_date(To)}
    }.

shift_date(Date, OffsetDays) ->
    calendar:gregorian_days_to_date(calendar:date_to_gregorian_days(Date) + OffsetDays).

format_date({Y, M, D}) ->
    lists:flatten(io_lib:format("~w~2..0w~2..0w000000Z", [Y, M, D])).

public_key_info(#'RSAPrivateKey'{modulus = N, publicExponent = E}) ->
    #'OTPSubjectPublicKeyInfo'{
        algorithm = #'PublicKeyAlgorithm'{
            algorithm = ?'rsaEncryption',
            parameters = 'NULL'
        },
        subjectPublicKey = #'RSAPublicKey'{modulus = N, publicExponent = E}
    };
public_key_info(#'ECPrivateKey'{parameters = Params, publicKey = PubKey}) ->
    #'OTPSubjectPublicKeyInfo'{
        algorithm = #'PublicKeyAlgorithm'{
            algorithm = ?'id-ecPublicKey',
            parameters = Params
        },
        subjectPublicKey = #'ECPoint'{point = PubKey}
    }.

subject(Opts) ->
    #{cn := CN} = Opts,
    Org = maps:get(org, Opts, "EMQX"),
    {rdnSequence, [
        [attribute(?'id-at-commonName', {printableString, CN})],
        [attribute(?'id-at-organizationName', {printableString, Org})]
    ]}.

attribute(Type, Value) ->
    #'AttributeTypeAndValue'{type = Type, value = Value}.

cert_subject(CertPem) ->
    [{'Certificate', Der, not_encrypted}] = public_key:pem_decode(CertPem),
    #'OTPCertificate'{tbsCertificate = #'OTPTBSCertificate'{subject = Subject}} =
        public_key:pkix_decode_cert(Der, otp),
    Subject.

ca_extensions() ->
    [
        #'Extension'{
            extnID = ?'id-ce-basicConstraints',
            extnValue = #'BasicConstraints'{cA = true},
            critical = true
        },
        #'Extension'{
            extnID = ?'id-ce-keyUsage',
            extnValue = [keyCertSign, cRLSign],
            critical = true
        }
    ].

leaf_extensions(Key, SANs) ->
    [
        #'Extension'{
            extnID = ?'id-ce-basicConstraints',
            extnValue = #'BasicConstraints'{cA = false},
            critical = true
        },
        #'Extension'{
            extnID = ?'id-ce-keyUsage',
            extnValue = key_usage(Key),
            critical = false
        },
        #'Extension'{
            extnID = ?'id-ce-extKeyUsage',
            extnValue = [?'id-kp-serverAuth', ?'id-kp-clientAuth'],
            critical = false
        }
        | san_extension(SANs)
    ].

key_usage(#'RSAPrivateKey'{}) ->
    [digitalSignature, keyEncipherment];
key_usage(#'ECPrivateKey'{}) ->
    [digitalSignature, keyAgreement].

san_extension([]) ->
    [];
san_extension(SANs) ->
    [
        #'Extension'{
            extnID = ?'id-ce-subjectAltName',
            extnValue = lists:map(fun san_value/1, SANs),
            critical = false
        }
    ].

san_value({dns, Name}) ->
    {dNSName, Name};
san_value({ip, IPAddr}) ->
    {iPAddress, ip_address_bytes(IPAddr)}.

ip_address_bytes({A, B, C, D}) ->
    [A, B, C, D];
ip_address_bytes({_, _, _, _, _, _, _, _} = IPv6) ->
    lists:append([[I bsr 8, I band 16#ff] || I <- tuple_to_list(IPv6)]).

cert_to_pem(Der) ->
    public_key:pem_encode([{'Certificate', Der, not_encrypted}]).

key_to_pem(#'RSAPrivateKey'{} = Key) ->
    Der = public_key:der_encode('RSAPrivateKey', Key),
    public_key:pem_encode([{'RSAPrivateKey', Der, not_encrypted}]);
key_to_pem(#'ECPrivateKey'{} = Key) ->
    Der = public_key:der_encode('ECPrivateKey', Key),
    public_key:pem_encode([{'ECPrivateKey', Der, not_encrypted}]).

pem_to_key(Pem) ->
    [Entry] = public_key:pem_decode(Pem),
    public_key:pem_entry_decode(Entry).
