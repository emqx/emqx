%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mgmt_api_certs_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/emqx_config.hrl").
-include_lib("emqx/include/emqx_managed_certs.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-define(local, local).
-define(cluster, cluster).

-define(AUTH_HEADER_PD_KEY, {?MODULE, auth_header}).

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).
-define(ON_ALL(NODES, BODY), erpc:multicall(NODES, fun() -> BODY end)).

-define(FILE_KIND_CA_BIN, atom_to_binary(?FILE_KIND_CA)).
-define(FILE_KIND_CHAIN_BIN, atom_to_binary(?FILE_KIND_CHAIN)).
-define(FILE_KIND_KEY_BIN, atom_to_binary(?FILE_KIND_KEY)).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all_with_matrix(?MODULE).

groups() ->
    emqx_common_test_helpers:groups_with_matrix(?MODULE).

init_per_suite(TCConfig) ->
    TCConfig.

end_per_suite(_TCConfig) ->
    ok.

init_per_group(?local, TCConfig) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(?local, TCConfig)}
    ),
    [
        {apps, Apps},
        {nodes, [node()]}
        | TCConfig
    ];
init_per_group(?cluster, TCConfig) ->
    AppSpecs = [
        emqx_conf,
        emqx_management
    ],
    Nodes = emqx_cth_cluster:start(
        [
            {mgmt_api_tls1, #{apps => AppSpecs ++ [emqx_mgmt_api_test_util:emqx_dashboard()]}},
            {mgmt_api_tls2, #{apps => AppSpecs}}
        ],
        #{work_dir => emqx_cth_suite:work_dir(?cluster, TCConfig)}
    ),
    [
        {cluster, Nodes},
        {nodes, Nodes}
        | TCConfig
    ];
init_per_group(_Group, TCConfig) ->
    TCConfig.

end_per_group(?local, TCConfig) ->
    Apps = get_config(apps, TCConfig),
    ok = emqx_cth_suite:stop(Apps),
    ok;
end_per_group(?cluster, TCConfig) ->
    Nodes = get_config(cluster, TCConfig),
    ok = emqx_cth_cluster:stop(Nodes),
    ok;
end_per_group(_Group, _TCConfig) ->
    ok.

init_per_testcase(_TestCase, TCConfig) ->
    maybe
        [N1 | _] ?= get_config(cluster, TCConfig, undefined),
        AuthHeader = ?ON(N1, emqx_mgmt_api_test_util:auth_header_()),
        put_auth_header(AuthHeader)
    end,
    TCConfig.

end_per_testcase(_TestCase, TCConfig) ->
    Nodes = get_config(nodes, TCConfig),
    ?ON_ALL(Nodes, ok = emqx_managed_certs:clean_certs_dir()),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

get_config(Key, TCConfig) ->
    case proplists:get_value(Key, TCConfig, undefined) of
        undefined ->
            error({missing_required_config, Key, TCConfig});
        Value ->
            Value
    end.

get_config(Key, TCConfig, Default) ->
    proplists:get_value(Key, TCConfig, Default).

simple_request(Params) ->
    AuthHeader = get_auth_header(),
    emqx_mgmt_api_test_util:simple_request(Params#{auth_header => AuthHeader}).

get_auth_header() ->
    case get(?AUTH_HEADER_PD_KEY) of
        undefined -> emqx_mgmt_api_test_util:auth_header_();
        Header -> Header
    end.

put_auth_header(Header) ->
    put(?AUTH_HEADER_PD_KEY, Header).

create_managed_ns(Ns) ->
    URL = emqx_mgmt_api_test_util:api_path(["mt", "ns", Ns]),
    simple_request(#{
        method => post,
        url => URL
    }).

list_bundles_global() ->
    URL = emqx_mgmt_api_test_util:api_path(["certs", "global", "list"]),
    simple_request(#{
        method => get,
        url => URL
    }).

list_bundles_ns(Ns) ->
    URL = emqx_mgmt_api_test_util:api_path(["certs", "ns", Ns, "list"]),
    simple_request(#{
        method => get,
        url => URL
    }).

list_files_global(BundleName) ->
    URL = emqx_mgmt_api_test_util:api_path(["certs", "global", "name", BundleName]),
    simple_request(#{
        method => get,
        url => URL
    }).

list_files_ns(Ns, BundleName) ->
    URL = emqx_mgmt_api_test_util:api_path(["certs", "ns", Ns, "name", BundleName]),
    simple_request(#{
        method => get,
        url => URL
    }).

delete_bundle_global(BundleName) ->
    URL = emqx_mgmt_api_test_util:api_path(["certs", "global", "name", BundleName]),
    simple_request(#{
        method => delete,
        url => URL
    }).

delete_bundle_ns(Ns, BundleName) ->
    URL = emqx_mgmt_api_test_util:api_path(["certs", "ns", Ns, "name", BundleName]),
    simple_request(#{
        method => delete,
        url => URL
    }).

upload_file_global(BundleName, Kind, Contents) ->
    URL = emqx_mgmt_api_test_util:api_path(["certs", "global", "name", BundleName]),
    Body = #{Kind => Contents},
    simple_request(#{
        method => post,
        url => URL,
        body => Body
    }).

upload_file_ns(Ns, BundleName, Kind, Contents) ->
    URL = emqx_mgmt_api_test_util:api_path(["certs", "ns", Ns, "name", BundleName]),
    Body = #{Kind => Contents},
    simple_request(#{
        method => post,
        url => URL,
        body => Body
    }).

upload_files_multipart_global(BundleName, Files) ->
    URL = emqx_mgmt_api_test_util:api_path(["certs", "global", "name", BundleName]),
    Res = emqx_mgmt_api_test_util:upload_request(#{
        url => URL,
        files => Files,
        mime_type => <<"application/octet-stream">>,
        other_params => [],
        auth_token => get_auth_header(),
        method => post
    }),
    emqx_mgmt_api_test_util:simplify_decode_result(Res).

upload_files_multipart_ns(Ns, BundleName, Files) ->
    URL = emqx_mgmt_api_test_util:api_path(["certs", "ns", Ns, "name", BundleName]),
    Res = emqx_mgmt_api_test_util:upload_request(#{
        url => URL,
        files => Files,
        mime_type => <<"application/octet-stream">>,
        other_params => [],
        auth_token => get_auth_header(),
        method => post
    }),
    emqx_mgmt_api_test_util:simplify_decode_result(Res).

gen_cert(Opts) ->
    #{
        cert := Cert,
        key := Key,
        cert_pem := CertPEM,
        key_pem := KeyPEM
    } = emqx_cth_tls:gen_cert_pem(Opts),
    #{
        cert_key => {Cert, Key},
        cert_pem => CertPEM,
        key_pem => KeyPEM
    }.

pem_encode(X, undefined = _Password) ->
    public_key:pem_encode([X]);
pem_encode(X, Password) when is_binary(Password) ->
    Y = public_key:pem_entry_decode(X),
    Type = element(1, X),
    Salt = crypto:strong_rand_bytes(8),
    Entry = public_key:pem_entry_encode(Type, Y, {{"DES-EDE3-CBC", Salt}, str(Password)}),
    public_key:pem_encode([Entry]).

str(X) -> emqx_utils_conv:str(X).

assert_same_bundles(Namespace, TCConfig) ->
    Nodes = get_config(nodes, TCConfig),
    Bundles0 = [FirstBundle0 | _] = ?ON_ALL(Nodes, emqx_managed_certs:list_bundles(Namespace)),
    ?assertEqual([FirstBundle0], lists:usort(Bundles0)),
    {ok, Bundles1} = FirstBundle0,
    Bundles2 =
        [FirstBundle2 | _] = ?ON_ALL(Nodes, begin
            lists:foldl(
                fun(BundleName, Acc) ->
                    Hashes = get_file_hashes(Namespace, BundleName),
                    Acc#{BundleName => Hashes}
                end,
                #{},
                Bundles1
            )
        end),
    ?assertEqual([FirstBundle2], lists:usort(Bundles2)),
    ok.

get_file_hashes(Namespace, BundleName) ->
    maybe
        {ok, Files0} ?= emqx_managed_certs:list_managed_files(Namespace, BundleName),
        Files =
            maps:fold(
                fun(Kind, #{path := Path}, Acc) ->
                    {ok, Contents} = file:read_file(Path),
                    MD5 = md5(Contents),
                    Acc#{Kind => MD5}
                end,
                #{},
                Files0
            ),
        {ok, Files}
    end.

read_md5(Path) ->
    {ok, Contents} = file:read_file(Path),
    md5(Contents).

md5(Data) ->
    erlang:md5(Data).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

%% Some tests for CRUD operations related to global and namespaced certificate files.
t_crud() ->
    [{matrix, true}].
t_crud(matrix) ->
    [[?local], [?cluster]];
t_crud(TCConfig) when is_list(TCConfig) ->
    Ns1 = <<"some_namespace1">>,
    Ns2 = <<"some_namespace2">>,

    ?assertMatch({200, []}, list_bundles_global()),
    %% List as empty even if namespace is not explictly created.
    ?assertMatch({200, []}, list_bundles_ns(Ns1)),
    assert_same_bundles(?global_ns, TCConfig),
    assert_same_bundles(Ns1, TCConfig),

    Bundle1 = <<"bundle1">>,

    %% Should we instead return an empty list?
    ?assertMatch({404, _}, list_files_global(Bundle1)),
    ?assertMatch({404, _}, list_files_ns(Ns1, Bundle1)),

    ?assertMatch({204, _}, delete_bundle_global(Bundle1)),
    ?assertMatch({204, _}, delete_bundle_ns(Ns1, Bundle1)),

    %% Still list as empty after namespace is explictly created.
    %% {204, _} = create_managed_ns(Ns),
    %% ?assertMatch({200, []}, list_bundles_ns(Ns)),
    %% ?assertMatch({404, _}, list_files_ns(Ns, Bundle1)),
    %% ?assertMatch({204, _}, delete_bundle_ns(Ns, Bundle1)),

    %% Upload some files
    #{
        cert_key := CertKeyRoot,
        cert_pem := CA1
    } = gen_cert(#{key => ec, issuer => root}),
    #{
        cert_pem := Cert1,
        key_pem := Key1
    } = gen_cert(#{key => ec, issuer => CertKeyRoot}),

    ?assertMatch({204, _}, upload_file_global(Bundle1, ?FILE_KIND_CA, CA1)),

    ?assertMatch({200, [#{<<"name">> := Bundle1}]}, list_bundles_global()),
    ?assertMatch({200, []}, list_bundles_ns(Ns1)),
    ?assertMatch(
        {200, #{<<"ca">> := #{<<"path">> := _}}},
        list_files_global(Bundle1)
    ),
    ?assertMatch({404, _}, list_files_ns(Ns1, Bundle1)),
    ?assertMatch({404, _}, list_files_ns(Ns2, Bundle1)),

    %% Expected contents found on all nodes.
    {200, #{<<"ca">> := #{<<"path">> := PathCAGlobal1}}} = list_files_global(Bundle1),
    ?assertEqual(md5(CA1), read_md5(PathCAGlobal1)),
    assert_same_bundles(?global_ns, TCConfig),
    assert_same_bundles(Ns1, TCConfig),

    %% Upload new contents; should replace at the same path
    #{cert_pem := CA2} = gen_cert(#{key => ec, issuer => root}),
    ?assertMatch({204, _}, upload_file_global(Bundle1, ?FILE_KIND_CA, CA2)),
    ?assertMatch(
        {200, #{<<"ca">> := #{<<"path">> := PathCAGlobal1}}},
        list_files_global(Bundle1)
    ),
    ?assertEqual(md5(CA2), read_md5(PathCAGlobal1)),
    assert_same_bundles(?global_ns, TCConfig),
    assert_same_bundles(Ns1, TCConfig),

    %% Upload other files (and original CA, for consistency)
    ?assertMatch({204, _}, upload_file_global(Bundle1, ?FILE_KIND_CA, CA1)),
    ?assertMatch({204, _}, upload_file_global(Bundle1, ?FILE_KIND_CHAIN, Cert1)),
    ?assertMatch({204, _}, upload_file_global(Bundle1, ?FILE_KIND_KEY, Key1)),

    ?assertMatch(
        {200, #{
            <<"ca">> := #{<<"path">> := _},
            <<"chain">> := #{<<"path">> := _},
            <<"key">> := #{<<"path">> := _}
        }},
        list_files_global(Bundle1)
    ),
    {200, #{
        <<"chain">> := #{<<"path">> := PathCertGlobal1},
        <<"key">> := #{<<"path">> := PathKeyGlobal1}
    }} = list_files_global(Bundle1),
    ?assertEqual(md5(Cert1), read_md5(PathCertGlobal1)),
    ?assertEqual(md5(Key1), read_md5(PathKeyGlobal1)),
    assert_same_bundles(?global_ns, TCConfig),

    %% Upload stuff to namespaced bundle
    #{
        cert_key := CertKeyRoot3,
        cert_pem := CA3
    } = gen_cert(#{key => ec, issuer => root}),
    #{
        cert_pem := Cert2,
        key_pem := Key2
    } = gen_cert(#{key => ec, issuer => CertKeyRoot3}),

    ?assertMatch({204, _}, upload_file_ns(Ns1, Bundle1, ?FILE_KIND_CA, CA3)),
    ?assertMatch({204, _}, upload_file_ns(Ns1, Bundle1, ?FILE_KIND_CHAIN, Cert2)),
    ?assertMatch({204, _}, upload_file_ns(Ns1, Bundle1, ?FILE_KIND_KEY, Key2)),
    ?assertMatch(
        {200, #{
            <<"ca">> := #{<<"path">> := _},
            <<"chain">> := #{<<"path">> := _},
            <<"key">> := #{<<"path">> := _}
        }},
        list_files_ns(Ns1, Bundle1)
    ),
    {200, #{
        <<"ca">> := #{<<"path">> := PathCANs1},
        <<"chain">> := #{<<"path">> := PathCertNs1},
        <<"key">> := #{<<"path">> := PathKeyNs1}
    }} = list_files_ns(Ns1, Bundle1),
    ?assertEqual(md5(CA3), read_md5(PathCANs1)),
    ?assertEqual(md5(Cert2), read_md5(PathCertNs1)),
    ?assertEqual(md5(Key2), read_md5(PathKeyNs1)),

    %% Other existing files are untouched
    ?assertEqual(md5(CA1), read_md5(PathCAGlobal1)),
    ?assertEqual(md5(Cert1), read_md5(PathCertGlobal1)),
    ?assertEqual(md5(Key1), read_md5(PathKeyGlobal1)),

    ?assertMatch({404, _}, list_files_ns(Ns2, Bundle1)),

    %% Different namespaces and bundles are independent
    #{
        cert_key := CertKeyRoot4,
        cert_pem := CA4
    } = gen_cert(#{key => ec, issuer => root}),
    #{
        cert_pem := Cert3,
        key_pem := Key3
    } = gen_cert(#{key => ec, issuer => CertKeyRoot4}),

    Bundle2 = <<"bundle2">>,

    ?assertMatch({204, _}, upload_file_ns(Ns2, Bundle1, ?FILE_KIND_CA, CA4)),

    ?assertMatch({204, _}, upload_file_ns(Ns2, Bundle2, ?FILE_KIND_CHAIN, Cert3)),

    ?assertNotMatch(
        {200, #{
            <<"chain">> := #{<<"path">> := _},
            <<"key">> := #{<<"path">> := _}
        }},
        list_files_ns(Ns2, Bundle1)
    ),
    ?assertNotMatch(
        {200, #{<<"ca">> := #{<<"path">> := _}}},
        list_files_ns(Ns2, Bundle2)
    ),

    ?assertMatch({200, [#{<<"name">> := Bundle1}]}, list_bundles_global()),
    ?assertMatch({200, [#{<<"name">> := Bundle1}]}, list_bundles_ns(Ns1)),
    ?assertMatch({200, [#{<<"name">> := Bundle1}, #{<<"name">> := Bundle2}]}, list_bundles_ns(Ns2)),

    %% Delete
    ?assertMatch({204, _}, delete_bundle_ns(Ns2, Bundle1)),
    ?assertMatch({404, _}, list_files_ns(Ns2, Bundle1)),
    ?assertMatch({200, _}, list_files_ns(Ns2, Bundle2)),
    ?assertMatch({200, _}, list_files_ns(Ns1, Bundle1)),
    ?assertMatch({200, _}, list_files_global(Bundle1)),

    ?assertMatch({204, _}, delete_bundle_ns(Ns2, Bundle2)),
    ?assertMatch({404, _}, list_files_ns(Ns2, Bundle1)),
    ?assertMatch({404, _}, list_files_ns(Ns2, Bundle2)),
    ?assertMatch({200, _}, list_files_ns(Ns1, Bundle1)),
    ?assertMatch({200, _}, list_files_global(Bundle1)),

    ?assertMatch({200, [#{<<"name">> := Bundle1}]}, list_bundles_global()),
    ?assertMatch({200, [#{<<"name">> := Bundle1}]}, list_bundles_ns(Ns1)),
    ?assertMatch({200, []}, list_bundles_ns(Ns2)),

    %% Other existing files are untouched
    ?assertEqual(md5(CA3), read_md5(PathCANs1)),
    ?assertEqual(md5(Cert2), read_md5(PathCertNs1)),
    ?assertEqual(md5(Key2), read_md5(PathKeyNs1)),
    assert_same_bundles(?global_ns, TCConfig),
    assert_same_bundles(Ns1, TCConfig),
    assert_same_bundles(Ns2, TCConfig),

    %% Cleanup other bundles
    ?assertMatch({204, _}, delete_bundle_ns(Ns1, Bundle1)),
    ?assertMatch({204, _}, delete_bundle_global(Bundle1)),
    %% Idempotent responses
    ?assertMatch({204, _}, delete_bundle_ns(Ns1, Bundle1)),
    ?assertMatch({204, _}, delete_bundle_global(Bundle1)),

    ?assertMatch({200, []}, list_bundles_global()),
    ?assertMatch({200, []}, list_bundles_ns(Ns1)),
    ?assertMatch({200, []}, list_bundles_ns(Ns2)),

    assert_same_bundles(?global_ns, TCConfig),
    assert_same_bundles(Ns1, TCConfig),
    assert_same_bundles(Ns2, TCConfig),

    %% Upload password protected key
    #{cert_key := CertKeyRoot5} = gen_cert(#{key => ec, issuer => root}),
    Password4 = <<"secretP@s$">>,
    #{key_pem := Key4} = gen_cert(#{
        key => ec,
        issuer => CertKeyRoot5,
        password => Password4
    }),
    ?assertMatch({204, _}, upload_file_global(Bundle1, ?FILE_KIND_KEY, Key4)),
    ?assertMatch({204, _}, upload_file_global(Bundle1, ?FILE_KIND_KEY_PASSWORD, Password4)),
    ?assertMatch(
        {200, #{
            <<"key">> := #{<<"path">> := _},
            <<"key_password">> := #{<<"path">> := _}
        }},
        list_files_global(Bundle1)
    ),
    assert_same_bundles(?global_ns, TCConfig),

    ?assertMatch({204, _}, delete_bundle_global(Bundle1)),

    %% Upload account key before others.  The other files should be rejected, since they
    %% will be controlled by the ACME client.  We still allow updating CA and the account
    %% key itself, though.
    ?assertMatch({204, _}, upload_file_global(Bundle1, ?FILE_KIND_ACC_KEY, Key3)),
    ?assertMatch({400, _}, upload_file_global(Bundle1, ?FILE_KIND_KEY, Key4)),
    ?assertMatch({400, _}, upload_file_global(Bundle1, ?FILE_KIND_CHAIN, Cert3)),
    %% Update allowed
    ?assertMatch({204, _}, upload_file_global(Bundle1, ?FILE_KIND_ACC_KEY, Key3)),
    ?assertMatch({204, _}, upload_file_global(Bundle1, ?FILE_KIND_CA, CA4)),

    %% Uploading an unknown kind
    ?assertMatch({400, _}, upload_file_global(Bundle1, some_unknown_type, Key3)),
    ?assertMatch({400, _}, upload_file_ns(Ns1, Bundle1, some_unknown_type, Key3)),

    ok.

-doc """
Checks that we restrict the bundle name to a valid format.
""".
t_bundle_name_validation() ->
    [{matrix, true}].
t_bundle_name_validation(matrix) ->
    [[?local]];
t_bundle_name_validation(TCConfig) when is_list(TCConfig) ->
    Ns = <<"some_ns">>,
    BadBundleNames = [
        binary:copy(<<"a">>, 255),
        <<"-">>,
        <<":">>,
        <<"*">>,
        <<"ç"/utf8>>
    ],
    #{cert_pem := CA} = gen_cert(#{key => ec, issuer => root}),
    lists:foreach(
        fun(BadBundleName0) ->
            ct:pal("bad bundle name: ~ts", [BadBundleName0]),
            BadBundleName = uri_string:quote(BadBundleName0),
            ?assertMatch(
                {400, #{
                    <<"message">> := #{
                        <<"kind">> := <<"validation_error">>,
                        <<"path">> := <<"name">>,
                        <<"value">> := BadBundleName0
                    }
                }},
                upload_file_global(BadBundleName, ?FILE_KIND_CA, CA),
                #{bundle_name => BadBundleName}
            ),
            ?assertMatch(
                {400, #{
                    <<"message">> := #{
                        <<"kind">> := <<"validation_error">>,
                        <<"path">> := <<"name">>,
                        <<"value">> := BadBundleName0
                    }
                }},
                upload_file_ns(Ns, BadBundleName, ?FILE_KIND_CA, CA),
                #{bundle_name => BadBundleName}
            ),
            ok
        end,
        BadBundleNames
    ),

    ok.

-doc """
Checks that we can list the contents of namespaced bundle dirs when escaping/quoting is
necessary.

Namespaces currently have no restriction on them, so they need to be escaped.
""".
t_escape_namespace_in_dirs() ->
    [{matrix, true}].
t_escape_namespace_in_dirs(matrix) ->
    [[?local]];
t_escape_namespace_in_dirs(TCConfig) when is_list(TCConfig) ->
    NsThatNeedsQuoting = <<":bad*nś-!">>,
    %% We need to quote it here, otherwise it's not a valid HTTP request path.
    NsQuoted = uri_string:quote(NsThatNeedsQuoting),
    BundleName = <<"a">>,
    #{cert_pem := CA} = gen_cert(#{key => ec, issuer => root}),
    ?assertMatch({204, _}, upload_file_ns(NsQuoted, BundleName, ?FILE_KIND_CA, CA)),
    ?assertMatch({200, [#{<<"name">> := BundleName}]}, list_bundles_ns(NsQuoted)),
    ?assertMatch(
        {200, #{<<"ca">> := #{<<"path">> := _}}},
        list_files_ns(NsQuoted, BundleName)
    ),
    {200, #{<<"ca">> := #{<<"path">> := Path}}} = list_files_ns(NsQuoted, BundleName),
    %% Path segments are quoted to avoid bad characters.
    ?assertEqual(match, re:run(Path, NsQuoted, [{capture, none}]), #{path => Path}),
    ok.

-doc """
Smoke tests for multipart, multi-file uploads.
""".
t_smoke_multipart() ->
    [{matrix, true}].
t_smoke_multipart(matrix) ->
    [[?local], [?cluster]];
t_smoke_multipart(TCConfig) when is_list(TCConfig) ->
    #{
        cert_key := CertKeyRoot1,
        cert_pem := CA1
    } = gen_cert(#{key => ec, issuer => root}),
    #{
        cert_pem := Cert1,
        key_pem := Key1
    } = gen_cert(#{key => ec, issuer => CertKeyRoot1}),

    Bundle1 = <<"bundle1">>,
    Files1 = [
        {?FILE_KIND_CA_BIN, <<"unused_ca_filename.pem">>, CA1},
        {?FILE_KIND_CHAIN_BIN, <<"unused_chain_filename.pem">>, Cert1},
        {?FILE_KIND_KEY_BIN, <<"unused_key_filename.pem">>, Key1}
    ],
    ?assertMatch({204, _}, upload_files_multipart_global(Bundle1, Files1)),
    assert_same_bundles(?global_ns, TCConfig),

    {200, #{
        <<"ca">> := #{<<"path">> := PathCAGlobal1},
        <<"chain">> := #{<<"path">> := PathCertGlobal1},
        <<"key">> := #{<<"path">> := PathKeyGlobal1}
    }} = list_files_global(Bundle1),
    ?assertEqual(md5(CA1), read_md5(PathCAGlobal1)),
    ?assertEqual(md5(Cert1), read_md5(PathCertGlobal1)),
    ?assertEqual(md5(Key1), read_md5(PathKeyGlobal1)),

    Ns2 = <<"ns2">>,
    Bundle2 = <<"bundle2">>,
    #{
        cert_key := CertKeyRoot2,
        cert_pem := CA2
    } = gen_cert(#{key => ec, issuer => root}),
    #{
        cert_pem := Cert2,
        key_pem := Key2
    } = gen_cert(#{key => ec, issuer => CertKeyRoot2}),
    Files2 = [
        {?FILE_KIND_CA_BIN, <<"unused_ca_filename.pem">>, CA2},
        {?FILE_KIND_CHAIN_BIN, <<"unused_chain_filename.pem">>, Cert2},
        {?FILE_KIND_KEY_BIN, <<"unused_key_filename.pem">>, Key2}
    ],
    ?assertMatch({204, _}, upload_files_multipart_ns(Ns2, Bundle2, Files2)),
    assert_same_bundles(Ns2, TCConfig),

    {200, #{
        <<"ca">> := #{<<"path">> := PathCANs2},
        <<"chain">> := #{<<"path">> := PathCertNs2},
        <<"key">> := #{<<"path">> := PathKeyNs2}
    }} = list_files_ns(Ns2, Bundle2),
    ?assertEqual(md5(CA2), read_md5(PathCANs2)),
    ?assertEqual(md5(Cert2), read_md5(PathCertNs2)),
    ?assertEqual(md5(Key2), read_md5(PathKeyNs2)),

    %% Uploading nothing
    ?assertMatch({400, _}, upload_files_multipart_global(Bundle2, [])),
    ?assertMatch({400, _}, upload_files_multipart_ns(Ns2, Bundle2, [])),

    ok.
