%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_cth_tls).

-include_lib("public_key/include/public_key.hrl").

-export([gen_cert/1]).
-export([write_cert/2]).
-export([write_cert/3]).
-export([write_pem/2]).

%% -------------------------------------------------------------------
%% Certificate Issuing
%% Heavily inspired by: ${ERL_SRC}/lib/public_key/test/erl_make_certs.erl
%% -------------------------------------------------------------------

-type pem_entry() :: public_key:pem_entry().
-type certificate() :: pem_entry().
-type private_key() :: pem_entry().

-type cert_subject() :: #{
    name => string(),
    email => string(),
    city => string(),
    state => string(),
    org => string(),
    org_unit => string(),
    country => string(),
    serial => string(),
    title => string(),
    dnQualifer => string()
}.

-type cert_validity() ::
    {_From :: calendar:date(), _To :: calendar:date()}.

-type cert_extensions() :: #{
    basic_constraints => false | ca | _PathLenContraint :: pos_integer(),
    key_usage => false | certsign
}.

%% @doc Generate a certificate and a private key.
%% If you need root (CA) certificate, use `root` as `issuer` option. By default, the
%% generated certificate will have according extensions (constraints, key usage, etc).
%% Once root certificate + private key pair is generated, you can use the result
%% as `issuer` option to generate other certificates signed by this root.
-spec gen_cert(Opts) -> {certificate(), private_key()} when
    Opts :: #{
        key := ec | rsa | PrivKeyIn,
        issuer := root | {CertificateIn, PrivKeyIn},
        subject => cert_subject(),
        validity => cert_validity(),
        extensions => cert_extensions() | false
    },
    CertificateIn :: certificate() | public_key:der_encoded() | #'OTPCertificate'{},
    PrivKeyIn :: private_key() | _PEM :: binary().
gen_cert(Opts) ->
    SubjectPrivateKey = get_privkey(Opts),
    {TBSCert, IssuerKey} = make_tbs(SubjectPrivateKey, Opts),
    Cert = public_key:pkix_sign(TBSCert, IssuerKey),
    true = verify_signature(Cert, IssuerKey),
    {encode_cert(Cert), encode_privkey(SubjectPrivateKey)}.

get_privkey(#{key := Algo}) when is_atom(Algo) ->
    gen_privkey(Algo);
get_privkey(#{key := Key}) ->
    decode_privkey(Key).

make_tbs(SubjectKey, Opts) ->
    {Issuer, IssuerKey} = issuer(Opts, SubjectKey),
    Subject =
        case Opts of
            #{issuer := root} ->
                Issuer;
            #{} ->
                subject(Opts)
        end,
    {
        #'OTPTBSCertificate'{
            version = v3,
            serialNumber = rand:uniform(1000000000000),
            signature = sign_algorithm(IssuerKey, Opts),
            issuer = Issuer,
            validity = validity(Opts),
            subject = Subject,
            subjectPublicKeyInfo = publickey(SubjectKey),
            extensions = extensions(Opts)
        },
        IssuerKey
    }.

issuer(Opts = #{issuer := root}, SubjectKey) ->
    %% Self signed
    {subject(Opts), SubjectKey};
issuer(#{issuer := {Issuer, IssuerKey}}, _SubjectKey) ->
    {issuer_subject(Issuer), decode_privkey(IssuerKey)}.

issuer_subject({'Certificate', IssuerDer, _}) when is_binary(IssuerDer) ->
    issuer_subject(IssuerDer);
issuer_subject(IssuerDer) when is_binary(IssuerDer) ->
    issuer_subject(public_key:pkix_decode_cert(IssuerDer, otp));
issuer_subject(#'OTPCertificate'{tbsCertificate = #'OTPTBSCertificate'{subject = Subject}}) ->
    Subject.

subject(Opts = #{}) ->
    Subject = maps:get(subject, Opts, #{}),
    Entries = maps:map(
        fun(N, V) -> [subject_entry(N, V)] end,
        maps:merge(default_subject(Opts), Subject)
    ),
    {rdnSequence, maps:values(Entries)}.

subject_entry(name, Name) ->
    typed_attr(?'id-at-commonName', {printableString, Name});
subject_entry(email, Email) ->
    typed_attr(?'id-emailAddress', Email);
subject_entry(city, City) ->
    typed_attr(?'id-at-localityName', {printableString, City});
subject_entry(state, State) ->
    typed_attr(?'id-at-stateOrProvinceName', {printableString, State});
subject_entry(org, Org) ->
    typed_attr(?'id-at-organizationName', {printableString, Org});
subject_entry(org_unit, OrgUnit) ->
    typed_attr(?'id-at-organizationalUnitName', {printableString, OrgUnit});
subject_entry(country, Country) ->
    typed_attr(?'id-at-countryName', Country);
subject_entry(serial, Serial) ->
    typed_attr(?'id-at-serialNumber', Serial);
subject_entry(title, Title) ->
    typed_attr(?'id-at-title', {printableString, Title});
subject_entry(dnQualifer, DnQ) ->
    typed_attr(?'id-at-dnQualifier', DnQ).

subject_info(Info, Subject, Default) ->
    case subject_info(Info, Subject) of
        undefined -> Default;
        Value -> Value
    end.

subject_info(Info, {rdnSequence, Entries}) ->
    subject_info(Info, Entries);
subject_info(name, Entries) when is_list(Entries) ->
    get_string(find_subject_entry(?'id-at-commonName', Entries));
subject_info(org, Entries) when is_list(Entries) ->
    get_string(find_subject_entry(?'id-at-organizationName', Entries));
subject_info(org_unit, Entries) when is_list(Entries) ->
    get_string(find_subject_entry(?'id-at-organizationalUnitName', Entries));
subject_info(country, Entries) when is_list(Entries) ->
    find_subject_entry(?'id-at-countryName', Entries).

find_subject_entry(Oid, Entries) ->
    emqx_maybe:from_list([
        Value
     || Attrs <- Entries,
        #'AttributeTypeAndValue'{type = T, value = Value} <- Attrs,
        T =:= Oid
    ]).

get_string({printableString, String}) ->
    String;
get_string(undefined) ->
    undefined.

typed_attr(Type, Value) ->
    #'AttributeTypeAndValue'{type = Type, value = Value}.

sign_algorithm(#'ECPrivateKey'{parameters = Parms}, _Opts) ->
    #'SignatureAlgorithm'{
        algorithm = ?'ecdsa-with-SHA256',
        parameters = Parms
    }.

validity(Opts) ->
    {From, To} = maps:get(validity, Opts, default_validity()),
    #'Validity'{
        notBefore = {generalTime, format_date(From)},
        notAfter = {generalTime, format_date(To)}
    }.

publickey(#'ECPrivateKey'{parameters = Params, publicKey = PubKey}) ->
    #'OTPSubjectPublicKeyInfo'{
        algorithm = #'PublicKeyAlgorithm'{
            algorithm = ?'id-ecPublicKey',
            parameters = Params
        },
        subjectPublicKey = #'ECPoint'{point = PubKey}
    }.

extensions(#{extensions := false}) ->
    asn1_NOVALUE;
extensions(Opts) ->
    Exts = maps:get(extensions, Opts, #{}),
    Default = default_extensions(Opts),
    maps:fold(
        fun(Name, Data, Acc) -> Acc ++ extension(Name, Data) end,
        [],
        maps:merge(Default, Exts)
    ).

extension(basic_constraints, false) ->
    [];
extension(basic_constraints, ca) ->
    [
        #'Extension'{
            extnID = ?'id-ce-basicConstraints',
            extnValue = #'BasicConstraints'{cA = true},
            critical = true
        }
    ];
extension(basic_constraints, Len) when is_integer(Len) ->
    [
        #'Extension'{
            extnID = ?'id-ce-basicConstraints',
            extnValue = #'BasicConstraints'{cA = true, pathLenConstraint = Len},
            critical = true
        }
    ];
extension(key_usage, false) ->
    [];
extension(key_usage, certsign) ->
    [
        #'Extension'{
            extnID = ?'id-ce-keyUsage',
            extnValue = [keyCertSign],
            critical = true
        }
    ].

default_validity() ->
    {shift_date(date(), -1), shift_date(date(), +7)}.

default_subject(#{issuer := root}) ->
    #{
        name => "RootCA",
        org => "EMQ",
        org_unit => "EMQX",
        country => "CN"
    };
default_subject(#{}) ->
    #{
        name => "Server",
        org => "EMQ",
        org_unit => "EMQX",
        country => "CN"
    }.

default_extensions(#{issuer := root}) ->
    #{
        basic_constraints => ca,
        key_usage => certsign
    };
default_extensions(#{}) ->
    #{}.

%% -------------------------------------------------------------------

verify_signature(CertDer, #'ECPrivateKey'{parameters = Params, publicKey = PubKey}) ->
    public_key:pkix_verify(CertDer, {#'ECPoint'{point = PubKey}, Params});
verify_signature(CertDer, KeyPem) ->
    verify_signature(CertDer, decode_privkey(KeyPem)).

%% -------------------------------------------------------------------

gen_privkey(ec) ->
    public_key:generate_key({namedCurve, secp256k1});
gen_privkey(rsa) ->
    public_key:generate_key({rsa, 2048, 17}).

decode_privkey(#'ECPrivateKey'{} = Key) ->
    Key;
decode_privkey(#'RSAPrivateKey'{} = Key) ->
    Key;
decode_privkey(PemEntry = {_, _, _}) ->
    public_key:pem_entry_decode(PemEntry);
decode_privkey(PemBinary) when is_binary(PemBinary) ->
    [KeyInfo] = public_key:pem_decode(PemBinary),
    decode_privkey(KeyInfo).

-spec encode_privkey(#'ECPrivateKey'{} | #'RSAPrivateKey'{}) -> private_key().
encode_privkey(Key = #'ECPrivateKey'{}) ->
    {ok, Der} = 'OTP-PUB-KEY':encode('ECPrivateKey', Key),
    {'ECPrivateKey', Der, not_encrypted};
encode_privkey(Key = #'RSAPrivateKey'{}) ->
    {ok, Der} = 'OTP-PUB-KEY':encode('RSAPrivateKey', Key),
    {'RSAPrivateKey', Der, not_encrypted}.

-spec encode_cert(public_key:der_encoded()) -> certificate().
encode_cert(Der) ->
    {'Certificate', Der, not_encrypted}.

%% -------------------------------------------------------------------

shift_date(Date, Offset) ->
    calendar:gregorian_days_to_date(calendar:date_to_gregorian_days(Date) + Offset).

format_date({Y, M, D}) ->
    lists:flatten(io_lib:format("~w~2..0w~2..0w000000Z", [Y, M, D])).

%% -------------------------------------------------------------------

%% @doc Write certificate + private key pair to respective files.
%% Files are created in the given directory. The filenames are derived
%% from the subject information in the certificate.
-spec write_cert(_Dir :: file:name(), {certificate(), private_key()}) ->
    {file:name(), file:name()}.
write_cert(Dir, {Cert, Key}) ->
    Subject = issuer_subject(Cert),
    Filename = subject_info(org, Subject, "ORG") ++ "." ++ subject_info(name, Subject, "XXX"),
    write_cert(Dir, Filename, {Cert, Key}).

-spec write_cert(_Dir :: file:name(), _Prefix :: string(), {certificate(), private_key()}) ->
    {file:name(), file:name()}.
write_cert(Dir, Filename, {Cert, Key}) ->
    Certfile = filename:join(Dir, Filename ++ ".crt"),
    Keyfile = filename:join(Dir, Filename ++ ".key"),
    ok = write_pem(Certfile, Cert),
    ok = write_pem(Keyfile, Key),
    {Certfile, Keyfile}.

-spec write_pem(file:name(), pem_entry() | [pem_entry()]) ->
    ok | {error, file:posix()}.
write_pem(Name, Entries = [_ | _]) ->
    file:write_file(Name, public_key:pem_encode(Entries));
write_pem(Name, Entry) ->
    write_pem(Name, [Entry]).
