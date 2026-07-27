%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_default_cert_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("public_key/include/public_key.hrl").
-include_lib("emqx/include/emqx_managed_certs.hrl").
-include_lib("emqx/include/emqx_config.hrl").

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

-doc """
Generated self-signed bundles are internally consistent: the server
certificate is verifiable against the bundled CA, carries the requested
CN and SANs, and the key matches the certificate.
""".
t_generate_self_signed(_Config) ->
    Bundle = emqx_default_cert:self_signed_bundle(#{
        cn => "localhost",
        sans => [
            {dns, "localhost"},
            {ip, {127, 0, 0, 1}},
            {ip, {0, 0, 0, 0, 0, 0, 0, 1}}
        ]
    }),
    #{
        ?FILE_KIND_KEY := KeyPem,
        ?FILE_KIND_CHAIN := CertPem,
        ?FILE_KIND_CA := CaPem
    } = Bundle,
    [{'Certificate', CertDer, not_encrypted}] = public_key:pem_decode(CertPem),
    [{'Certificate', CaDer, not_encrypted}] = public_key:pem_decode(CaPem),
    %% chain verifies against the CA
    ?assertMatch(
        {ok, _},
        public_key:pkix_path_validation(CaDer, [CertDer], [])
    ),
    %% subject and SANs
    ?assertEqual("localhost", cert_cn(CertDer)),
    OTPCert = public_key:pkix_decode_cert(CertDer, otp),
    TBS = OTPCert#'OTPCertificate'.tbsCertificate,
    Exts = TBS#'OTPTBSCertificate'.extensions,
    #'Extension'{extnValue = SANs} = lists:keyfind(
        ?'id-ce-subjectAltName', #'Extension'.extnID, Exts
    ),
    ?assert(lists:member({dNSName, "localhost"}, SANs), #{sans => SANs}),
    %% key matches the certificate
    [KeyEntry] = public_key:pem_decode(KeyPem),
    #'RSAPrivateKey'{modulus = N} = public_key:pem_entry_decode(KeyEntry),
    #'OTPCertificate'{
        tbsCertificate = #'OTPTBSCertificate'{
            subjectPublicKeyInfo = #'OTPSubjectPublicKeyInfo'{
                subjectPublicKey = #'RSAPublicKey'{modulus = CertN}
            }
        }
    } = OTPCert,
    ?assertEqual(N, CertN),
    ok.

-doc """
A fresh node boots with no shipped certificate files, generates the
`localhost` bundle at first boot, and the default `ssl` listener serves
the generated certificate.
""".
t_fresh_boot_generates_bundle(Config) ->
    [NodeSpec] = mk_cluster(?FUNCTION_NAME, [fresh1], Config),
    [Node] = emqx_cth_cluster:start([NodeSpec]),
    try
        Bundle = get_bundle_files(Node),
        ?assertMatch(#{?FILE_KIND_KEY := _, ?FILE_KIND_CHAIN := _, ?FILE_KIND_CA := _}, Bundle),
        ServedDer = get_served_cert(Node),
        ?assertEqual("localhost", cert_cn(ServedDer)),
        %% served certificate is exactly the generated one
        #{?FILE_KIND_CHAIN := ChainPem} = Bundle,
        [{'Certificate', ChainDer, not_encrypted}] = public_key:pem_decode(ChainPem),
        ?assertEqual(ChainDer, ServedDer),
        ok
    after
        emqx_cth_cluster:stop([Node])
    end.

-doc """
Restarting a node does not regenerate the bundle: the private key stays
the same across restarts.
""".
t_restart_keeps_bundle(Config) ->
    [NodeSpec] = mk_cluster(?FUNCTION_NAME, [restart1], Config),
    [Node] = emqx_cth_cluster:start([NodeSpec]),
    try
        #{?FILE_KIND_KEY := KeyPem0} = get_bundle_files(Node),
        [Node] = emqx_cth_cluster:restart(NodeSpec),
        #{?FILE_KIND_KEY := KeyPem1} = get_bundle_files(Node),
        ?assertEqual(KeyPem0, KeyPem1),
        ok
    after
        emqx_cth_cluster:stop([Node])
    end.

-doc """
A listener with operator-configured certificate files serves the
operator's certificate; the generated bundle does not override it.
""".
t_operator_certs_honored(Config) ->
    WorkDir = emqx_cth_suite:work_dir(?FUNCTION_NAME, Config),
    ok = filelib:ensure_path(WorkDir),
    CA = #{cert_pem := CaPem} = emqx_default_cert:generate_ca(#{cn => "OperatorCA"}),
    #{cert_pem := CertPem, key_pem := KeyPem} =
        emqx_default_cert:generate_cert(CA, #{cn => "operator.example.com"}),
    CertFile = filename:join(WorkDir, "operator-cert.pem"),
    KeyFile = filename:join(WorkDir, "operator-key.pem"),
    CaFile = filename:join(WorkDir, "operator-cacert.pem"),
    ok = file:write_file(CertFile, CertPem),
    ok = file:write_file(KeyFile, KeyPem),
    ok = file:write_file(CaFile, CaPem),
    ListenerConf = io_lib:format(
        "listeners.ssl.default.ssl_options { certfile = ~p, keyfile = ~p, cacertfile = ~p }",
        [CertFile, KeyFile, CaFile]
    ),
    [NodeSpec] = mk_cluster(?FUNCTION_NAME, [operator1], lists:flatten(ListenerConf), Config),
    [Node] = emqx_cth_cluster:start([NodeSpec]),
    try
        ServedDer = get_served_cert(Node),
        ?assertEqual("operator.example.com", cert_cn(ServedDer)),
        ok
    after
        emqx_cth_cluster:stop([Node])
    end.

-doc """
A node joining a cluster inherits the peer's `localhost` bundle via the
data sync which runs before its listeners start; the joining node does
not generate a conflicting bundle of its own.
""".
t_join_inherits_bundle(Config) ->
    [Spec1, Spec2] = mk_cluster(?FUNCTION_NAME, [join1, join2], Config),
    [N1] = emqx_cth_cluster:start([Spec1]),
    Bundle1 = get_bundle_files(N1),
    #{?FILE_KIND_KEY := Key1, ?FILE_KIND_CHAIN := Chain1} = Bundle1,
    %% second node boots and joins the first: data sync runs in
    %% emqx_conf before emqx starts listeners
    [N2] = emqx_cth_cluster:start([Spec2]),
    try
        #{?FILE_KIND_KEY := Key2, ?FILE_KIND_CHAIN := Chain2} = get_bundle_files(N2),
        ?assertEqual(Key1, Key2),
        ?assertEqual(Chain1, Chain2),
        %% both listeners serve the same certificate
        [{'Certificate', ChainDer, not_encrypted}] = public_key:pem_decode(Chain1),
        ?assertEqual(ChainDer, get_served_cert(N1)),
        ?assertEqual(ChainDer, get_served_cert(N2)),
        ok
    after
        emqx_cth_cluster:stop([N1, N2])
    end.

-doc """
A clustered node that somehow diverged (its local bundle differs from
the cluster's) converges back to the cluster's bundle when it restarts,
because the data sync overwrites the local bundle before listeners
start.
""".
t_diverged_bundle_converges_on_restart(Config) ->
    [Spec1, Spec2] = mk_cluster(?FUNCTION_NAME, [conv1, conv2], Config),
    [N1] = emqx_cth_cluster:start([Spec1]),
    [N2] = emqx_cth_cluster:start([Spec2]),
    try
        #{?FILE_KIND_KEY := Key1} = get_bundle_files(N1),
        %% overwrite N2's local bundle with a freshly generated one
        %% to simulate divergence
        ?ON(N2, begin
            Bundle = emqx_default_cert:self_signed_bundle(#{cn => "localhost"}),
            ok = emqx_managed_certs:add_managed_files_v1(?global_ns, <<"localhost">>, Bundle)
        end),
        #{?FILE_KIND_KEY := DivergedKey} = get_bundle_files(N2),
        ?assertNotEqual(Key1, DivergedKey),
        %% restart: sync from the running peer wins over the local bundle
        [N2] = emqx_cth_cluster:restart(Spec2),
        #{?FILE_KIND_KEY := Key2} = get_bundle_files(N2),
        ?assertEqual(Key1, Key2),
        ok
    after
        emqx_cth_cluster:stop([N1, N2])
    end.

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

mk_cluster(TC, Names, Config) ->
    mk_cluster(TC, Names, "", Config).

mk_cluster(TC, Names, ExtraEmqxConf, Config) ->
    emqx_cth_cluster:mk_nodespecs(
        [{Name, #{apps => [{emqx_conf, #{}}, {emqx, ExtraEmqxConf}]}} || Name <- Names],
        #{work_dir => emqx_cth_suite:work_dir(TC, Config)}
    ).

get_bundle_files(Node) ->
    ?ON(Node, begin
        {ok, Files} = emqx_managed_certs:list_managed_files(?global_ns, <<"localhost">>),
        maps:map(
            fun(_Kind, #{path := Path}) ->
                {ok, Contents} = file:read_file(Path),
                Contents
            end,
            Files
        )
    end).

get_served_cert(Node) ->
    {_Host, Port} = ?ON(Node, emqx_config:get([listeners, ssl, default, bind])),
    {ok, Sock} = ssl:connect("127.0.0.1", Port, [{verify, verify_none}, {active, false}], 5_000),
    {ok, Der} = ssl:peercert(Sock),
    ok = ssl:close(Sock),
    Der.

cert_cn(Der) ->
    #'OTPCertificate'{
        tbsCertificate = #'OTPTBSCertificate'{subject = {rdnSequence, RDNs}}
    } = public_key:pkix_decode_cert(Der, otp),
    [CN] = [
        Value
     || Attrs <- RDNs,
        #'AttributeTypeAndValue'{type = ?'id-at-commonName', value = {printableString, Value}} <-
            Attrs
    ],
    CN.
