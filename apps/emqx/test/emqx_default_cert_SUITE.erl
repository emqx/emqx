%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_default_cert_SUITE).

-compile([nowarn_export_all, export_all]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("public_key/include/public_key.hrl").
-include("emqx_managed_certs.hrl").
-include("emqx_config.hrl").

suite() -> [{timetrap, {minutes, 2}}].

%%------------------------------------------------------------------------------
%% CT Boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

%% Nothing here needs a listener running, and not binding ports keeps the suite
%% from colliding with anything else on the host.
emqx_app_spec() ->
    {emqx, #{override_env => [{boot_modules, [broker]}]}}.

init_per_testcase(t_bundles_are_per_node = TCName, TCConfig) ->
    Nodes = emqx_cth_cluster:start(
        [
            {emqx_default_cert_SUITE1, #{role => core, apps => [emqx_app_spec()]}},
            {emqx_default_cert_SUITE2, #{role => core, apps => [emqx_app_spec()]}}
        ],
        #{work_dir => emqx_cth_suite:work_dir(TCName, TCConfig)}
    ),
    [{nodes, Nodes} | TCConfig];
init_per_testcase(TCName, TCConfig) ->
    Apps = emqx_cth_suite:start([emqx_app_spec()], #{
        work_dir => emqx_cth_suite:work_dir(TCName, TCConfig)
    }),
    [{apps, Apps} | TCConfig].

end_per_testcase(t_bundles_are_per_node, TCConfig) ->
    ok = emqx_cth_cluster:stop(?config(nodes, TCConfig)),
    ok;
end_per_testcase(_TCName, TCConfig) ->
    ok = emqx_cth_suite:stop(?config(apps, TCConfig)),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

ensure() ->
    {ok, Files} = emqx_default_cert:ensure_localhost_bundle(),
    Files.

%% `list_managed_files/2' reports `enoent' rather than an empty map when the
%% bundle directory does not exist at all.
bundle_files() ->
    case emqx_managed_certs:list_managed_files(?global_ns, ?NODE_DEFAULT_CERT_BUNDLE_NAME) of
        {ok, Files} -> Files;
        {error, enoent} -> #{}
    end.

%% The bundle's file contents keyed by file kind, so two bundles can be compared
%% without depending on the paths they happen to live at.
contents(Files) ->
    maps:map(
        fun(_Kind, #{path := Path}) ->
            {ok, Contents} = file:read_file(Path),
            Contents
        end,
        Files
    ).

bundle_contents() ->
    contents(bundle_files()).

bundle_contents(Node) ->
    {ok, Files} = erpc:call(Node, emqx_default_cert, ensure_localhost_bundle, []),
    maps:map(
        fun(_Kind, #{path := Path}) ->
            {ok, Contents} = erpc:call(Node, file, read_file, [Path]),
            Contents
        end,
        Files
    ).

decode_cert(Pem) ->
    [{'Certificate', Der, not_encrypted} | _] = public_key:pem_decode(Pem),
    public_key:pkix_decode_cert(Der, otp).

extension(ExtnID, #'OTPCertificate'{tbsCertificate = #'OTPTBSCertificate'{extensions = Exts}}) ->
    case lists:keyfind(ExtnID, #'Extension'.extnID, Exts) of
        false -> undefined;
        #'Extension'{extnValue = Value} -> Value
    end.

common_name(#'OTPCertificate'{tbsCertificate = TBS}) ->
    #'OTPTBSCertificate'{subject = {rdnSequence, RDNs}} = TBS,
    [Value] = [
        V
     || RDN <- RDNs,
        #'AttributeTypeAndValue'{type = ?'id-at-commonName', value = V} <- RDN
    ],
    case Value of
        {_StringType, CN} -> iolist_to_binary(CN);
        CN -> iolist_to_binary(CN)
    end.

%% The chain holds the leaf first, then the CA that signed it.
chain_entries(ChainPem) ->
    [{'Certificate', LeafDer, not_encrypted}, {'Certificate', CaDer, not_encrypted}] =
        public_key:pem_decode(ChainPem),
    {LeafDer, CaDer}.

path_validates(ChainPem) ->
    {LeafDer, CaDer} = chain_entries(ChainPem),
    public_key:pkix_path_validation(CaDer, [LeafDer], []).

%% The leaf's public key as stored in the certificate, and the one derived from
%% the stored private key. They match only if both files come from the same
%% generation.
cert_public_key(CertPem) ->
    #'OTPCertificate'{tbsCertificate = TBS} = decode_cert(CertPem),
    #'OTPTBSCertificate'{subjectPublicKeyInfo = SPKI} = TBS,
    #'OTPSubjectPublicKeyInfo'{subjectPublicKey = PubKey} = SPKI,
    PubKey.

key_public_key(KeyPem) ->
    [Entry] = public_key:pem_decode(KeyPem),
    case public_key:pem_entry_decode(Entry) of
        #'ECPrivateKey'{publicKey = Point} ->
            #'ECPoint'{point = Point};
        #'RSAPrivateKey'{modulus = N, publicExponent = E} ->
            #'RSAPublicKey'{
                modulus = N,
                publicExponent = E
            }
    end.

assert_self_consistent(Contents) ->
    #{?FILE_KIND_KEY := KeyPem, ?FILE_KIND_CHAIN := ChainPem} = Contents,
    ?assertMatch({ok, _}, path_validates(ChainPem)),
    ?assertEqual(cert_public_key(ChainPem), key_public_key(KeyPem)).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

-doc "The bundle is generated on demand when the node does not have one.".
t_generates_when_absent(_TCConfig) ->
    %% Nothing generates at boot, so there is no bundle until it is asked for.
    ?assertEqual(#{}, bundle_files()),
    ?assertMatch(#{?FILE_KIND_KEY := _, ?FILE_KIND_CHAIN := _}, ensure()).

-doc "The generated leaf certificate is for CN=localhost and carries the localhost SANs.".
t_generated_cert_identifies_localhost(_TCConfig) ->
    #{?FILE_KIND_CHAIN := ChainPem} = contents(ensure()),
    Cert = decode_cert(ChainPem),
    ?assertEqual(<<"localhost">>, common_name(Cert)),
    SANs = extension(?'id-ce-subjectAltName', Cert),
    ?assert(lists:member({dNSName, "localhost"}, SANs), #{sans => SANs}),
    ?assert(lists:member({iPAddress, <<127, 0, 0, 1>>}, SANs), #{sans => SANs}),
    IPv6 = <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1>>,
    ?assert(lists:member({iPAddress, IPv6}, SANs), #{sans => SANs}).

-doc "The generated key, leaf certificate and CA belong to each other.".
t_generated_bundle_is_self_consistent(_TCConfig) ->
    assert_self_consistent(contents(ensure())).

-doc """
The bundle is a key and a chain, with no `ca' file: that slot is the trust
anchor for verifying peers, which this node has no business filling in for
itself. The CA travels in the chain instead, so a client can trust this node.
""".
t_bundle_has_chain_and_no_ca_file(_TCConfig) ->
    Contents = contents(ensure()),
    ?assertEqual([chain, key], lists:sort(maps:keys(Contents))),
    #{?FILE_KIND_CHAIN := ChainPem} = Contents,
    {_LeafDer, CaDer} = chain_entries(ChainPem),
    CaCert = public_key:pkix_decode_cert(CaDer, otp),
    ?assertMatch(
        #'BasicConstraints'{cA = true},
        extension(?'id-ce-basicConstraints', CaCert)
    ).

-doc "The bundle holds no CA private key: the signing key is discarded at generation time.".
t_no_ca_key_in_bundle(_TCConfig) ->
    #{?FILE_KIND_CHAIN := ChainPem} = contents(ensure()),
    Entries = public_key:pem_decode(ChainPem),
    ?assertEqual([], [E || {Type, _, _} = E <- Entries, Type =/= 'Certificate']).

-doc "Asking again returns the existing bundle instead of generating another.".
t_ensure_is_idempotent(_TCConfig) ->
    Before = contents(ensure()),
    ?assertEqual(Before, contents(ensure())).

-doc """
A complete bundle already stored under this name is used as it is. This is what
lets an operator supply their own default certificate, and what lets a node that
inherited the cluster's bundle through `emqx_conf' data sync keep it.
""".
t_existing_bundle_is_kept(_TCConfig) ->
    #{?FILE_KIND_KEY := #{path := KeyPath}, ?FILE_KIND_CHAIN := #{path := CertPath}} = ensure(),
    %% Stands in for an operator-supplied or peer-synced bundle: content that
    %% generation would certainly not reproduce.
    ok = file:write_file(KeyPath, <<"supplied-key">>),
    ok = file:write_file(CertPath, <<"supplied-cert">>),
    ?assertMatch(
        #{?FILE_KIND_KEY := <<"supplied-key">>, ?FILE_KIND_CHAIN := <<"supplied-cert">>},
        contents(ensure())
    ).

-doc "An incomplete bundle is replaced, so a half-written one does not persist.".
t_incomplete_bundle_is_regenerated(_TCConfig) ->
    #{?FILE_KIND_KEY := #{path := KeyPath}} = ensure(),
    ok = file:delete(KeyPath),
    Contents = contents(ensure()),
    ?assertMatch(#{?FILE_KIND_KEY := _}, Contents),
    assert_self_consistent(Contents).

-doc """
Deleting the bundle keeps it deleted until something asks for a default
certificate again: generation is driven by demand, not by boot.
""".
t_deleted_bundle_stays_deleted(_TCConfig) ->
    _ = ensure(),
    ok = emqx_managed_certs:delete_bundle(?global_ns, ?NODE_DEFAULT_CERT_BUNDLE_NAME),
    ?assertEqual(#{}, bundle_files()),
    %% Only an explicit request brings it back.
    ?assertMatch(#{?FILE_KIND_KEY := _}, ensure()),
    ?assertMatch(#{?FILE_KIND_KEY := _}, bundle_files()).

-doc """
Concurrent callers never interleave into a mixed bundle: whichever generation
lands, the stored key, certificate and CA belong to each other.
""".
t_concurrent_ensure_is_consistent(_TCConfig) ->
    Parent = self(),
    Workers = [
        spawn_link(fun() ->
            Parent ! {self(), emqx_default_cert:ensure_localhost_bundle()}
        end)
     || _ <- lists:seq(1, 8)
    ],
    lists:foreach(
        fun(Worker) ->
            receive
                {Worker, Result} -> ?assertMatch({ok, #{?FILE_KIND_KEY := _}}, Result)
            after 30_000 -> ct:fail("worker ~p timed out", [Worker])
            end
        end,
        Workers
    ),
    assert_self_consistent(bundle_contents()).

-doc """
Each node generates and keeps its own bundle: the default certificate is a
per-node identity and is not pushed to the other nodes in the cluster.
""".
t_bundles_are_per_node(TCConfig) ->
    [N1, N2] = ?config(nodes, TCConfig),
    Bundle1 = bundle_contents(N1),
    Bundle2 = bundle_contents(N2),
    assert_self_consistent(Bundle1),
    assert_self_consistent(Bundle2),
    ?assertNotEqual(maps:get(?FILE_KIND_KEY, Bundle1), maps:get(?FILE_KIND_KEY, Bundle2)),
    ?assertNotEqual(maps:get(?FILE_KIND_CHAIN, Bundle1), maps:get(?FILE_KIND_CHAIN, Bundle2)).
