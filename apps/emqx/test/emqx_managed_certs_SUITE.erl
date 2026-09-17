%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_managed_certs_SUITE).

-compile([nowarn_export_all, export_all]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/asserts.hrl").
-include("emqx_config.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% CT Boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(TCConfig) ->
    Apps = emqx_cth_suite:start([emqx_conf], #{work_dir => emqx_cth_suite:work_dir(TCConfig)}),
    [{apps, Apps} | TCConfig].

end_per_suite(TCConfig) ->
    Apps = ?config(apps, TCConfig),
    ok = emqx_cth_suite:stop(Apps),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

list_dir(Namespace, Bundle) ->
    {ok, Files} = file:list_dir(emqx_managed_certs:dir(Namespace, Bundle)),
    lists:sort(Files).

read_file(Namespace, Bundle, Filename) ->
    Path = filename:join(emqx_managed_certs:dir(Namespace, Bundle), Filename),
    {ok, Contents} = file:read_file(Path),
    Contents.

gen_ca_pem() ->
    #{cert_pem := PEM} = emqx_cth_tls:gen_cert_pem(#{key => ec, issuer => root}),
    PEM.

mk_managed_certs_struct(?global_ns, Bundle) ->
    #{<<"bundle_name">> => Bundle};
mk_managed_certs_struct(Ns, Bundle) when is_binary(Ns) ->
    #{<<"namespace">> => Ns, <<"bundle_name">> => Bundle}.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_find_references(_TCConfig) ->
    Ns1 = <<"ns1">>,
    Ns2 = <<"ns2">>,
    Mk = fun(Ns, Bundle) -> mk_managed_certs_struct(Ns, Bundle) end,
    NsConfigs = #{
        ?global_ns => #{
            <<"a0">> => #{
                <<"a1">> => [
                    #{<<"managed_certs">> => Mk(?global_ns, <<"bundle1">>)},
                    [#{<<"managed_certs">> => Mk(?global_ns, <<"bundle2">>)}],
                    #{<<"ssl">> => #{<<"managed_certs">> => Mk(?global_ns, <<"bundle3">>)}}
                ],
                <<"a2">> => #{
                    <<"a3">> => #{<<"managed_certs">> => Mk(?global_ns, <<"bundle4">>)},
                    <<"a4">> => [#{<<"managed_certs">> => Mk(?global_ns, <<"bundle5">>)}],
                    <<"a5">> => #{<<"managed_certs">> => Mk(Ns1, <<"bundle4">>)}
                }
            },
            <<"b0">> => #{<<"managed_certs">> => Mk(?global_ns, <<"bundle6">>)},
            <<"c0">> => #{<<"managed_certs">> => Mk(Ns1, <<"bundle1">>)},
            <<"d0">> => #{
                <<"ssl_options">> => #{<<"managed_certs">> => Mk(?global_ns, <<"bundle7">>)}
            }
        },
        Ns1 => #{
            <<"a0">> => #{
                <<"a1">> => [
                    #{<<"managed_certs">> => Mk(Ns1, <<"bundle1">>)},
                    [#{<<"managed_certs">> => Mk(Ns1, <<"bundle2">>)}],
                    #{<<"ssl">> => #{<<"managed_certs">> => Mk(Ns1, <<"bundle3">>)}}
                ],
                <<"a2">> => #{
                    <<"a3">> => #{<<"managed_certs">> => Mk(Ns2, <<"bundle4">>)},
                    <<"a4">> => [#{<<"managed_certs">> => Mk(?global_ns, <<"bundle5">>)}],
                    <<"a5">> => #{<<"managed_certs">> => Mk(Ns1, <<"bundle4">>)},
                    <<"a6">> => #{<<"managed_certs">> => Mk(Ns1, <<"bundle1">>)}
                }
            }
        }
    },
    Refs = fun(Ns, Bundle) ->
        emqx_managed_certs:do_find_references(NsConfigs, Ns, Bundle)
    end,

    ?assertSameSet([], Refs(Ns2, <<"bundle1">>)),

    %% Same bundle name exists on multiple namespaces.
    ?assertSameSet(
        [{?global_ns, [<<"a0">>, <<"a2">>, <<"a3">>]}],
        Refs(?global_ns, <<"bundle4">>)
    ),
    ?assertSameSet(
        [{Ns1, [<<"a0">>, <<"a2">>, <<"a3">>]}],
        Refs(Ns2, <<"bundle4">>)
    ),

    %% Same reference on different namespaces
    ?assertSameSet(
        [
            {?global_ns, [<<"c0">>]},
            {Ns1, [<<"a0">>, <<"a1">>, 1]},
            {Ns1, [<<"a0">>, <<"a2">>, <<"a6">>]}
        ],
        Refs(Ns1, <<"bundle1">>)
    ),
    ?assertSameSet(
        [
            {?global_ns, [<<"a0">>, <<"a2">>, <<"a4">>, 1]},
            {Ns1, [<<"a0">>, <<"a2">>, <<"a4">>, 1]}
        ],
        Refs(?global_ns, <<"bundle5">>)
    ),

    %% Pretty path
    ?assertSameSet(
        [{?global_ns, [<<"a0">>, <<"a1">>, 3]}],
        Refs(?global_ns, <<"bundle3">>)
    ),
    ?assertSameSet(
        [{Ns1, [<<"a0">>, <<"a1">>, 3]}],
        Refs(Ns1, <<"bundle3">>)
    ),
    ?assertSameSet(
        [{?global_ns, [<<"d0">>]}],
        Refs(?global_ns, <<"bundle7">>)
    ),

    ok.

-doc """
Verifies that a failed write leaves every file of the bundle unchanged and
leaves no temporary file behind.
""".
t_write_failure_changes_nothing(_TCConfig) ->
    Ns = <<"ns1">>,
    Bundle = <<"write_failure">>,
    Old = #{ca => <<"old ca">>, key => <<"old key">>},
    ?assertEqual(ok, emqx_managed_certs:install_files(Ns, Bundle, Old)),
    ?assertEqual(ok, emqx_managed_certs:add_managed_files_v1(Ns, Bundle, Old)),
    %% An atom is not valid file content: writing it fails with `badarg'.
    New = #{ca => <<"new ca">>, key => not_iodata},
    ?assertMatch(
        {error, #{key := {error, badarg}}},
        emqx_managed_certs:install_files(Ns, Bundle, New)
    ),
    ?assertMatch(
        {error, #{key := {error, badarg}}},
        emqx_managed_certs:add_managed_files_v1(Ns, Bundle, New)
    ),
    ?assertEqual(["ca.pem", "key.pem"], list_dir(Ns, Bundle)),
    ?assertEqual(<<"old ca">>, read_file(Ns, Bundle, "ca.pem")),
    ?assertEqual(<<"old key">>, read_file(Ns, Bundle, "key.pem")),
    ok.

-doc """
Verifies that a failed rename is reported for its kind, the other kinds are
still written, and no temporary file is left behind.
""".
t_rename_failure_reported(_TCConfig) ->
    Ns = <<"ns1">>,
    Bundle = <<"rename_failure">>,
    Dir = emqx_managed_certs:dir(Ns, Bundle),
    %% A directory in place of `chain.pem' makes the rename over it fail.
    ok = filelib:ensure_path(filename:join(Dir, "chain.pem")),
    ?assertMatch(
        {error, #{chain := {error, _}} = Errors} when map_size(Errors) =:= 1,
        emqx_managed_certs:add_managed_files_v1(Ns, Bundle, #{
            ca => <<"new ca">>, chain => <<"new chain">>
        })
    ),
    ?assertEqual(["ca.pem", "chain.pem"], list_dir(Ns, Bundle)),
    ?assertEqual(<<"new ca">>, read_file(Ns, Bundle, "ca.pem")),
    ok.

-doc """
Verifies CA merging on a single node: new certificates are appended to the
existing file content, known ones are skipped, and bad input is rejected.
""".
t_merge_ca_certs(_TCConfig) ->
    %% The BPAPI table is not populated in this suite.
    ok = meck:new(emqx_bpapi, [passthrough, no_link]),
    ok = meck:expect(emqx_bpapi, nodes_supporting_bpapi_version, fun(_, _) -> [node()] end),
    try
        do_t_merge_ca_certs()
    after
        meck:unload(emqx_bpapi)
    end.

do_t_merge_ca_certs() ->
    Bundle = <<"merge_ca">>,
    CA1 = gen_ca_pem(),
    CA2 = gen_ca_pem(),
    ?assertEqual(
        {error, bundle_not_found},
        emqx_managed_certs:merge_ca_certs(?global_ns, Bundle, CA1)
    ),
    %% The existing file has no trailing newline: one is added before the new certificates.
    Existing = string:trim(CA1, trailing),
    ok = emqx_managed_certs:install_files(?global_ns, Bundle, #{ca => Existing}),
    ?assertEqual(
        {ok, #{added => 1, total => 2}},
        emqx_managed_certs:merge_ca_certs(?global_ns, Bundle, <<CA2/binary, CA1/binary>>)
    ),
    ?assertEqual(<<Existing/binary, "\n", CA2/binary>>, read_file(?global_ns, Bundle, "ca.pem")),
    ?assertEqual(
        {ok, #{added => 0, total => 2}},
        emqx_managed_certs:merge_ca_certs(?global_ns, Bundle, CA2)
    ),
    #{key_pem := Key} = emqx_cth_tls:gen_cert_pem(#{key => ec, issuer => root}),
    ?assertMatch(
        {error, {bad_ca_certs, _}},
        emqx_managed_certs:merge_ca_certs(?global_ns, Bundle, Key)
    ),
    ?assertMatch(
        {error, {bad_ca_certs, _}},
        emqx_managed_certs:merge_ca_certs(?global_ns, Bundle, <<"garbage">>)
    ),
    ?assertEqual(
        {error, bad_namespace},
        emqx_managed_certs:merge_ca_certs(<<"..">>, Bundle, CA1)
    ),
    ?assertEqual(
        {error, bad_namespace},
        emqx_managed_certs:delete_ca_cert(<<"..">>, Bundle, <<0:256>>)
    ),
    ?assertEqual(
        {error, bundle_not_found},
        emqx_managed_certs:delete_ca_cert(?global_ns, <<"no_such_bundle">>, <<0:256>>)
    ),
    ?assertEqual(
        {error, cert_not_found},
        emqx_managed_certs:delete_ca_cert(?global_ns, Bundle, <<0:256>>)
    ),
    ok.
