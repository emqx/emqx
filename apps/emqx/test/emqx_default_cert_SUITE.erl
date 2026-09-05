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

%% The bundle is generated before the listeners start, so these tests do not need
%% any listener running — and not binding ports keeps the suite from colliding
%% with anything else on the host.
emqx_app_spec() ->
    {emqx, #{override_env => [{boot_modules, [broker]}]}}.

init_per_testcase(t_fresh_cluster_nodes_keep_own_bundles = TCName, TCConfig) ->
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

end_per_testcase(t_fresh_cluster_nodes_keep_own_bundles, TCConfig) ->
    ok = emqx_cth_cluster:stop(?config(nodes, TCConfig)),
    ok;
end_per_testcase(_TCName, TCConfig) ->
    ok = emqx_cth_suite:stop(?config(apps, TCConfig)),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

bundle_files() ->
    {ok, Files} = emqx_managed_certs:list_managed_files(?global_ns, ?DEFAULT_CERT_BUNDLE_NAME),
    Files.

%% The bundle's file contents keyed by file kind, so two bundles can be compared
%% without depending on the paths they happen to live at.
bundle_contents() ->
    maps:map(
        fun(_Kind, #{path := Path}) ->
            {ok, Contents} = file:read_file(Path),
            Contents
        end,
        bundle_files()
    ).

bundle_contents(Node) ->
    {ok, Files} = erpc:call(
        Node, emqx_managed_certs, list_managed_files, [?global_ns, ?DEFAULT_CERT_BUNDLE_NAME]
    ),
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

path_validates(CertPem, CaPem) ->
    [{'Certificate', LeafDer, not_encrypted}] = public_key:pem_decode(CertPem),
    [{'Certificate', CaDer, not_encrypted}] = public_key:pem_decode(CaPem),
    public_key:pkix_path_validation(CaDer, [LeafDer], []).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

-doc "Booting the node generates the default `localhost' certificate bundle.".
t_generated_at_boot(_TCConfig) ->
    ?assertMatch(
        #{?FILE_KIND_KEY := _, ?FILE_KIND_CHAIN := _, ?FILE_KIND_CA := _},
        bundle_files()
    ).

-doc "The generated leaf certificate is for CN=localhost and carries the localhost SANs.".
t_generated_cert_identifies_localhost(_TCConfig) ->
    #{?FILE_KIND_CHAIN := CertPem} = bundle_contents(),
    Cert = decode_cert(CertPem),
    ?assertEqual(<<"localhost">>, common_name(Cert)),
    SANs = extension(?'id-ce-subjectAltName', Cert),
    ?assert(lists:member({dNSName, "localhost"}, SANs), #{sans => SANs}),
    ?assert(lists:member({iPAddress, <<127, 0, 0, 1>>}, SANs), #{sans => SANs}),
    IPv6 = <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1>>,
    ?assert(lists:member({iPAddress, IPv6}, SANs), #{sans => SANs}).

-doc "The generated leaf certificate validates against the CA stored alongside it.".
t_generated_leaf_validates_against_ca(_TCConfig) ->
    #{?FILE_KIND_CHAIN := CertPem, ?FILE_KIND_CA := CaPem} = bundle_contents(),
    ?assertMatch({ok, _}, path_validates(CertPem, CaPem)).

-doc "The bundle holds no CA private key: the signing key is discarded at generation time.".
t_no_ca_key_in_bundle(_TCConfig) ->
    Contents = bundle_contents(),
    ?assertEqual([ca, chain, key], lists:sort(maps:keys(Contents))),
    lists:foreach(
        fun(Kind) ->
            Entries = public_key:pem_decode(maps:get(Kind, Contents)),
            ?assertEqual(
                [],
                [E || {Type, _, _} = E <- Entries, Type =/= 'Certificate'],
                #{kind => Kind}
            )
        end,
        [?FILE_KIND_CHAIN, ?FILE_KIND_CA]
    ).

-doc "Running the boot hook again keeps the existing bundle instead of regenerating it.".
t_ensure_is_idempotent(_TCConfig) ->
    Before = bundle_contents(),
    ok = emqx_default_cert:ensure_localhost_bundle(),
    ?assertEqual(Before, bundle_contents()).

-doc """
A bundle already on disk is kept, not overwritten. This is what lets a node that
inherited the cluster's bundle through `emqx_conf' data sync keep it: the boot hook
runs after that sync and finds a complete bundle.
""".
t_existing_bundle_is_kept(_TCConfig) ->
    #{?FILE_KIND_KEY := #{path := KeyPath}, ?FILE_KIND_CHAIN := #{path := CertPath}} =
        bundle_files(),
    %% Stands in for a bundle synced from a cluster peer: content that
    %% generation would certainly not reproduce.
    ok = file:write_file(KeyPath, <<"inherited-key">>),
    ok = file:write_file(CertPath, <<"inherited-cert">>),
    ok = emqx_default_cert:ensure_localhost_bundle(),
    ?assertMatch(
        #{?FILE_KIND_KEY := <<"inherited-key">>, ?FILE_KIND_CHAIN := <<"inherited-cert">>},
        bundle_contents()
    ).

-doc "An incomplete bundle is regenerated, so a half-written one does not persist.".
t_incomplete_bundle_is_regenerated(_TCConfig) ->
    #{?FILE_KIND_KEY := #{path := KeyPath}} = bundle_files(),
    ok = file:delete(KeyPath),
    ok = emqx_default_cert:ensure_localhost_bundle(),
    #{?FILE_KIND_CHAIN := CertPem, ?FILE_KIND_CA := CaPem} = Contents = bundle_contents(),
    ?assertMatch(#{?FILE_KIND_KEY := _}, Contents),
    ?assertMatch({ok, _}, path_validates(CertPem, CaPem)).

-doc """
Two nodes forming a fresh cluster each generate and keep their own bundle. Each
node's default certificate is its own identity; they are not expected to converge.
""".
t_fresh_cluster_nodes_keep_own_bundles(TCConfig) ->
    [N1, N2] = ?config(nodes, TCConfig),
    Bundle1 = bundle_contents(N1),
    Bundle2 = bundle_contents(N2),
    ?assertMatch(#{?FILE_KIND_KEY := _, ?FILE_KIND_CHAIN := _, ?FILE_KIND_CA := _}, Bundle1),
    ?assertMatch(#{?FILE_KIND_KEY := _, ?FILE_KIND_CHAIN := _, ?FILE_KIND_CA := _}, Bundle2),
    ?assertNotEqual(maps:get(?FILE_KIND_KEY, Bundle1), maps:get(?FILE_KIND_KEY, Bundle2)),
    ?assertNotEqual(maps:get(?FILE_KIND_CHAIN, Bundle1), maps:get(?FILE_KIND_CHAIN, Bundle2)),
    %% Stable: neither node adopts the other's bundle while clustered.
    ?assertEqual(Bundle1, bundle_contents(N1)),
    ?assertEqual(Bundle2, bundle_contents(N2)).
