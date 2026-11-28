%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_niftls).

-moduledoc """
TLS server state machine on Rustls, with no socket I/O.

The caller owns the socket. It gives the bytes that it reads to `feed/2`, sends the `Out` bytes
that it gets back, and parses the decrypted `App` bytes. `encrypt/2` returns the TLS records for
outgoing data. `close/1` returns the close_notify alert to send before the socket closes.

The handshake runs on a dirty CPU scheduler. After the handshake, the record layer runs on a
normal scheduler, and one NIF call handles at most `16384` bytes of input. This module splits
larger input.

Only one process may use a `tls()` at a time. Use the `tls()` that the last call returned.
""".

-export([config/1, new/1, feed/2, encrypt/2, close/1]).

-export_type([conf/0, tls/0, handshake_info/0]).

-include_lib("public_key/include/public_key.hrl").

%% The largest input of one NIF call on a normal scheduler.
-define(CHUNK, 16384).

%% Options that change the Rustls configuration.
-define(HONORED_OPTS, [
    cert,
    certfile,
    key,
    keyfile,
    password,
    cacerts,
    cacertfile,
    verify,
    fail_if_no_peer_cert,
    versions,
    ciphers,
    supported_groups,
    alpn_preferred_protocols,
    honor_cipher_order
]).

%% Options that have no effect here. The caller applies the timeouts. Rustls never renegotiates,
%% and this backend has no session resumption. Rustls has a fixed limit on the chain depth.
-define(IGNORED_OPTS, [
    handshake_timeout,
    hibernate_after,
    log_level,
    gc_after_handshake,
    secure_renegotiate,
    client_renegotiation,
    reuse_sessions,
    depth
]).

%% The cipher suites of the Rustls ring provider.
-define(RUSTLS_CIPHERS, [
    "TLS13_AES_256_GCM_SHA384",
    "TLS13_AES_128_GCM_SHA256",
    "TLS13_CHACHA20_POLY1305_SHA256",
    "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
    "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
    "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256",
    "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
    "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
    "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256"
]).

-define(KEY_TYPES, ['RSAPrivateKey', 'ECPrivateKey', 'PrivateKeyInfo']).

-opaque conf() :: reference().
-opaque tls() :: {handshake | data, reference()}.

-type handshake_info() :: #{
    protocol := 'tlsv1.2' | 'tlsv1.3',
    selected_cipher_suite := ssl:erl_cipher_suite(),
    sni_hostname := string() | undefined,
    alpn := binary() | undefined,
    peercert := public_key:der_encoded() | undefined,
    peer_certs := [public_key:der_encoded()]
}.

-doc """
Make a Rustls server configuration from OTP ssl server options, for example the result of
`maps:from_list(emqx_tls_lib:to_server_opts(tls, SslOptions))`. All connections of a listener
share it.

An option that this backend cannot honor returns an error that names the option. Examples are
PSK (`user_lookup_fun`), TLS 1.0 and 1.1, a cipher suite or a group that Rustls does not support,
`verify_fun`, and CRL options.
""".
-spec config(map()) -> {ok, conf()} | {error, map()}.
config(Opts0) when is_map(Opts0) ->
    Opts = maps:filter(fun(_K, V) -> V =/= undefined end, Opts0),
    maybe
        ok ?= check_keys(Opts),
        {ok, Certs} ?= certs(Opts),
        {ok, Key} ?= key(Opts),
        ok ?= check_key_match(Certs, Key),
        {ok, CACerts} ?= cacerts(Opts),
        {ok, {Tls12, Tls13}} ?= versions(maps:get(versions, Opts, ['tlsv1.3', 'tlsv1.2'])),
        {ok, Ciphers} ?= ciphers(maps:get(ciphers, Opts, [])),
        {ok, Groups} ?= groups(maps:get(supported_groups, Opts, [])),
        NifOpts = #{
            certs => Certs,
            key => Key,
            cacerts => CACerts,
            verify_peer => maps:get(verify, Opts, verify_none) =:= verify_peer,
            fail_if_no_peer_cert => maps:get(fail_if_no_peer_cert, Opts, false),
            tls12 => Tls12,
            tls13 => Tls13,
            ciphers => Ciphers,
            groups => Groups,
            alpn => maps:get(alpn_preferred_protocols, Opts, []),
            honor_cipher_order => maps:get(honor_cipher_order, Opts, false)
        },
        {ok, Conf} ?= emqx_nif:tls_config(NifOpts),
        {ok, Conf}
    else
        {error, Detail} when is_binary(Detail) ->
            {error, #{reason => rustls_config_error, detail => Detail}};
        {error, _} = Error ->
            Error
    end.

-doc "Make the TLS state of one server connection.".
-spec new(conf()) -> tls().
new(Conf) ->
    case emqx_nif:tls_new(Conf) of
        {ok, Ref} -> {handshake, Ref};
        {error, Reason} -> error({niftls_new, Reason})
    end.

-doc """
Give the bytes read from the socket.

- `ok`: send `Out`, then parse `App`. Both can be empty.
- `handshake_done`: as `ok`, once, when the handshake completes.
- `close`: send `Out` (for example an alert), then close the socket.
- `error`: close the socket.

A close_notify from the peer is not an error. `feed/2` returns the data before it with `ok`.
""".
-spec feed(tls(), binary()) ->
    {ok, iodata(), binary(), tls()}
    | {handshake_done, iodata(), binary(), handshake_info(), tls()}
    | {close, iodata(), binary()}
    | {error, term()}.
feed({handshake, Ref}, Bin) ->
    case emqx_nif:tls_feed_handshake(Ref, Bin) of
        {ok, Out, App} ->
            {ok, Out, App, {handshake, Ref}};
        {handshake_done, Out, App, Info} ->
            {handshake_done, Out, App, handshake_info(Info), {data, Ref}};
        Other ->
            Other
    end;
feed({data, Ref}, Bin) ->
    feed_data(Ref, Bin, [], []).

-doc "Encrypt outgoing data. Send `Out` to the socket.".
-spec encrypt(tls(), iodata()) -> {ok, iodata()} | {error, term()}.
encrypt({_, Ref}, Data) ->
    case iolist_size(Data) =< ?CHUNK of
        true -> emqx_nif:tls_encrypt(Ref, Data);
        false -> encrypt_chunks(Ref, iolist_to_binary(Data), [])
    end.

-doc "Return the close_notify alert. Send it before the socket closes.".
-spec close(tls()) -> binary().
close({_, Ref}) ->
    emqx_nif:tls_close(Ref).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

feed_data(Ref, Bin, OutAcc, AppAcc) ->
    {Chunk, Rest} = split(Bin),
    case emqx_nif:tls_feed(Ref, Chunk) of
        {ok, Out, App} when Rest =:= <<>>, AppAcc =:= [] ->
            {ok, join(OutAcc, Out), App, {data, Ref}};
        {ok, Out, App} when Rest =:= <<>> ->
            {ok, join(OutAcc, Out), iolist_to_binary(join(AppAcc, App)), {data, Ref}};
        {ok, Out, App} ->
            feed_data(Ref, Rest, [Out | OutAcc], [App | AppAcc]);
        {close, Out, Reason} ->
            {close, join(OutAcc, Out), Reason};
        {error, _} = Error ->
            Error
    end.

encrypt_chunks(Ref, Bin, OutAcc) ->
    {Chunk, Rest} = split(Bin),
    case emqx_nif:tls_encrypt(Ref, Chunk) of
        {ok, Out} when Rest =:= <<>> ->
            {ok, join(OutAcc, Out)};
        {ok, Out} ->
            encrypt_chunks(Ref, Rest, [Out | OutAcc]);
        {error, _} = Error ->
            Error
    end.

split(<<Chunk:?CHUNK/binary, Rest/binary>>) -> {Chunk, Rest};
split(Bin) -> {Bin, <<>>}.

join([], Last) -> Last;
join(Acc, Last) -> lists:reverse(Acc, [Last]).

handshake_info(#{
    protocol := Protocol,
    cipher_suite := Cipher,
    sni_hostname := SNI,
    alpn := ALPN,
    peer_certs := PeerCerts
}) ->
    #{
        protocol => Protocol,
        selected_cipher_suite => ssl:str_to_suite(otp_cipher_name(binary_to_list(Cipher))),
        sni_hostname => to_list(SNI),
        alpn => ALPN,
        peercert =>
            case PeerCerts of
                [Cert | _] -> Cert;
                [] -> undefined
            end,
        peer_certs => PeerCerts
    }.

to_list(undefined) -> undefined;
to_list(Bin) -> binary_to_list(Bin).

%% Rustls names a TLS 1.3 suite `TLS13_...`. The RFC and OTP name it `TLS_...`.
otp_cipher_name("TLS13_" ++ Rest) -> "TLS_" ++ Rest;
otp_cipher_name(Name) -> Name.

check_keys(Opts) ->
    Known = ?HONORED_OPTS ++ ?IGNORED_OPTS,
    case [K || K <- lists:sort(maps:keys(Opts)), not lists:member(K, Known)] of
        [] -> ok;
        [K | _] -> {error, #{reason => unsupported_option, option => K}}
    end.

certs(#{cert := Der}) when is_binary(Der) ->
    {ok, [Der]};
certs(#{cert := Ders}) when is_list(Ders) ->
    {ok, Ders};
certs(#{certfile := File}) ->
    maybe
        {ok, Entries} ?= read_pem(certfile, File),
        case [Der || {'Certificate', Der, not_encrypted} <- Entries] of
            [] -> {error, #{reason => no_certificate, file => File}};
            Ders -> {ok, Ders}
        end
    end;
certs(_) ->
    {error, #{reason => no_certificate}}.

key(#{key := {Type, Der}}) when is_binary(Der) ->
    case lists:member(Type, ?KEY_TYPES) of
        true -> {ok, {Type, Der}};
        false -> {error, #{reason => unsupported_key_type, type => Type}}
    end;
key(#{key := _}) ->
    {error, #{reason => unsupported_option, option => key}};
key(#{keyfile := File} = Opts) ->
    maybe
        {ok, Entries} ?= read_pem(keyfile, File),
        case [E || {Type, _, _} = E <- Entries, lists:member(Type, ?KEY_TYPES)] of
            [] -> {error, #{reason => no_private_key, file => File}};
            [Entry | _] -> decode_key(Entry, maps:get(password, Opts, ""), File)
        end
    end;
key(_) ->
    {error, #{reason => no_private_key}}.

decode_key({Type, Der, not_encrypted}, _Password, _File) ->
    {ok, {Type, Der}};
decode_key(Entry, Password, File) ->
    try public_key:pem_entry_decode(Entry, password(Password)) of
        Key -> encode_key(Key, File)
    catch
        _:_ -> {error, #{reason => bad_private_key_password, file => File}}
    end.

encode_key(#'RSAPrivateKey'{} = Key, _File) ->
    {ok, {'RSAPrivateKey', public_key:der_encode('RSAPrivateKey', Key)}};
encode_key(#'ECPrivateKey'{parameters = {namedCurve, Curve}} = Key, File) ->
    case lists:member(Curve, [?'secp256r1', ?'secp384r1']) of
        true -> {ok, {'ECPrivateKey', public_key:der_encode('ECPrivateKey', Key)}};
        false -> {error, #{reason => unsupported_key_type, file => File}}
    end;
encode_key(_Key, File) ->
    {error, #{reason => unsupported_key_type, file => File}}.

%% Compare the public key of the private key with the public key of the leaf certificate.
check_key_match([Leaf | _], {Type, Der}) ->
    try
        Cert = public_key:pkix_decode_cert(Leaf, otp),
        #'OTPCertificate'{tbsCertificate = TBS} = Cert,
        #'OTPTBSCertificate'{subjectPublicKeyInfo = SPKI} = TBS,
        #'OTPSubjectPublicKeyInfo'{subjectPublicKey = CertPub} = SPKI,
        key_matches(public_key:der_decode(Type, Der), CertPub)
    of
        true -> ok;
        false -> {error, #{reason => key_does_not_match_certificate}}
    catch
        _:_ -> {error, #{reason => bad_certificate_or_key}}
    end.

key_matches(#'RSAPrivateKey'{modulus = N, publicExponent = E}, CertPub) ->
    CertPub =:= #'RSAPublicKey'{modulus = N, publicExponent = E};
key_matches(#'ECPrivateKey'{publicKey = Point}, #'ECPoint'{point = CertPoint}) when
    is_binary(Point)
->
    Point =:= CertPoint;
key_matches(#'ECPrivateKey'{}, #'ECPoint'{}) ->
    %% The key file has no public key. Rustls checks the signature in the handshake.
    true;
key_matches(#'ECPrivateKey'{}, _CertPub) ->
    false;
key_matches(_Key, _CertPub) ->
    %% Rustls checks the other key types when it loads the key.
    true.

password(Fun) when is_function(Fun, 0) -> Fun();
password(Password) when is_binary(Password) -> binary_to_list(Password);
password(Password) -> Password.

cacerts(#{cacerts := Ders}) when is_list(Ders) ->
    {ok, Ders};
cacerts(#{cacertfile := File}) ->
    maybe
        {ok, Entries} ?= read_pem(cacertfile, File),
        {ok, [Der || {'Certificate', Der, not_encrypted} <- Entries]}
    end;
cacerts(_) ->
    {ok, []}.

read_pem(Option, File) ->
    case file:read_file(File) of
        {ok, Pem} -> {ok, public_key:pem_decode(Pem)};
        {error, Reason} -> {error, #{reason => Reason, option => Option, file => File}}
    end.

versions(Versions) ->
    case Versions -- ['tlsv1.2', 'tlsv1.3'] of
        [] when Versions =/= [] ->
            {ok, {lists:member('tlsv1.2', Versions), lists:member('tlsv1.3', Versions)}};
        [] ->
            {error, #{reason => no_tls_version}};
        [V | _] ->
            {error, #{reason => unsupported_tls_version, version => V}}
    end.

ciphers(Ciphers) ->
    Supported = [{ssl:str_to_suite(otp_cipher_name(N)), N} || N <- ?RUSTLS_CIPHERS],
    map_all(
        fun(Cipher) ->
            case lists:keyfind(cipher_suite(Cipher), 1, Supported) of
                {_, Name} -> {ok, list_to_binary(Name)};
                false -> {error, #{reason => unsupported_cipher, cipher => Cipher}}
            end
        end,
        Ciphers
    ).

cipher_suite(Suite) when is_map(Suite) -> Suite;
cipher_suite(Name) when is_binary(Name) -> cipher_suite(binary_to_list(Name));
cipher_suite(Name) when is_list(Name) -> ssl:str_to_suite(Name);
cipher_suite(Other) -> Other.

groups(Groups) ->
    Supported = [
        {x25519, <<"X25519">>}, {secp256r1, <<"secp256r1">>}, {secp384r1, <<"secp384r1">>}
    ],
    map_all(
        fun(Group) ->
            case lists:keyfind(Group, 1, Supported) of
                {_, Name} -> {ok, Name};
                false -> {error, #{reason => unsupported_group, group => Group}}
            end
        end,
        Groups
    ).

map_all(F, List) ->
    lists:foldr(
        fun
            (X, {ok, Acc}) ->
                case F(X) of
                    {ok, Y} -> {ok, [Y | Acc]};
                    Error -> Error
                end;
            (_, Error) ->
                Error
        end,
        {ok, []},
        List
    ).
