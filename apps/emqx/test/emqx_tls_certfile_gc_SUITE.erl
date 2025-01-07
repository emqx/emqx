%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_tls_certfile_gc_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("typerefl/include/types.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-include_lib("snabbkaffe/include/test_macros.hrl").

-define(of_pid(PID, EVENTS), [E || #{?snk_meta := #{pid := __Pid}} = E <- EVENTS, __Pid == PID]).

-import(hoconsc, [mk/1, mk/2, ref/2]).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TC, Config) ->
    TCAbsDir = filename:join(?config(priv_dir, Config), TC),
    ok = application:set_env(emqx, data_dir, TCAbsDir),
    ok = snabbkaffe:start_trace(),
    [{tc_name, atom_to_list(TC)}, {tc_absdir, TCAbsDir} | Config].

end_per_testcase(_TC, _Config) ->
    ok = snabbkaffe:stop(),
    _ = emqx_schema_hooks:erase_injections(),
    _ = emqx_config:erase_all(),
    ok.

t_no_orphans(Config) ->
    SSL0 = #{
        <<"keyfile">> => key(),
        <<"certfile">> => cert(),
        <<"cacertfile">> => cert()
    },
    {ok, SSL} = emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir("ssl", SSL0),
    {ok, SSLUnused} = emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir("unused", SSL0),
    SSLKeyfile = maps:get(<<"keyfile">>, SSL),
    ok = load_config(#{
        <<"clients">> => [
            #{<<"transport">> => #{<<"ssl">> => SSL}}
        ],
        <<"servers">> => #{
            <<"noname">> => #{<<"ssl">> => SSL}
        }
    }),
    % Should not be considered an orphan: it's a symlink.
    ok = file:make_symlink(
        SSLKeyfile,
        filename:join(?config(tc_absdir, Config), filename:basename(SSLKeyfile))
    ),
    % Should not be considered orphans: files are now read-only.
    ok = file:change_mode(maps:get(<<"keyfile">>, SSLUnused), 8#400),
    ok = file:change_mode(maps:get(<<"certfile">>, SSLUnused), 8#400),
    ok = file:change_mode(maps:get(<<"cacertfile">>, SSLUnused), 8#400),
    % Verify there are no orphans
    ?assertEqual(
        #{},
        orphans()
    ),
    % Verify there are no orphans, since SSL config is still in use
    ok = put_config([<<"servers">>, <<"noname">>, <<"ssl">>], #{<<"enable">> => false}),
    ?assertEqual(
        #{},
        orphans()
    ).

t_collect_orphans(_Config) ->
    % 0. Set up a client and two servers (each with the same set of certfiles).
    SSL0 = #{
        <<"keyfile">> => key(),
        <<"certfile">> => cert(),
        <<"cacertfile">> => cert()
    },
    SSL1 = SSL0#{
        <<"ocsp">> => #{<<"issuer_pem">> => cert()}
    },
    {ok, SSL2} = emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir("client", SSL0),
    {ok, SSL3} = emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir("server", SSL1),
    ok = load_config(#{
        <<"clients">> => [
            #{<<"transport">> => #{<<"ssl">> => SSL2}}
        ],
        <<"servers">> => #{
            <<"name">> => #{<<"ssl">> => SSL3},
            <<"noname">> => #{<<"ssl">> => SSL3}
        }
    }),
    Orphans1 = orphans(),
    ?assertEqual(
        #{},
        Orphans1
    ),
    % 1. Remove clients from the config
    ok = put_config([<<"clients">>], []),
    Orphans2 = orphans(),
    ?assertMatch(
        M = #{} when map_size(M) == 3,
        Orphans2
    ),
    % All orphans are newly observed, nothing to collect
    ?assertEqual(
        [],
        collect(convicts(Orphans2, Orphans1))
    ),
    % Same orphans are "observed", should be collected
    ?assertMatch(
        [
            {collect, _DirClient, ok},
            {collect, _CACert, ok},
            {collect, _Cert, ok},
            {collect, _Key, ok}
        ],
        collect(convicts(Orphans2, Orphans2))
    ),
    % 2. Remove server from the config
    ok = put_config([<<"servers">>, <<"name">>, <<"ssl">>], #{}),
    Orphans3 = orphans(),
    % Files are still referenced by the "noname" server
    ?assertEqual(
        #{},
        Orphans3
    ),
    % 3. Remove another server from the config
    ok = put_config([<<"servers">>, <<"noname">>, <<"ssl">>], #{}),
    Orphans4 = orphans(),
    ?assertMatch(
        M = #{} when map_size(M) == 4,
        Orphans4
    ),
    ?assertMatch(
        [
            {collect, _DirServer, ok},
            {collect, _IssuerPEM, ok},
            {collect, _CACert, ok},
            {collect, _Cert, ok},
            {collect, _Key, ok}
        ],
        collect(convicts(Orphans4, Orphans4))
    ),
    % No more orphans left
    ?assertEqual(
        #{},
        orphans()
    ).

t_gc_runs_periodically(_Config) ->
    {ok, Pid} = emqx_tls_certfile_gc:start_link(500),

    % Set up two servers in the config, each with its own set of certfiles
    SSL = #{
        <<"keyfile">> => key(),
        <<"certfile">> => cert()
    },
    {ok, SSL1} = emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir("s1", SSL),
    SSL1Keyfile = emqx_utils_fs:canonicalize(maps:get(<<"keyfile">>, SSL1)),
    SSL1Certfile = emqx_utils_fs:canonicalize(maps:get(<<"certfile">>, SSL1)),
    {ok, SSL2} = emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir("s2", SSL#{
        <<"ocsp">> => #{<<"issuer_pem">> => cert()}
    }),
    SSL2Keyfile = emqx_utils_fs:canonicalize(maps:get(<<"keyfile">>, SSL2)),
    SSL2Certfile = emqx_utils_fs:canonicalize(maps:get(<<"certfile">>, SSL2)),
    SSL2IssPEM = emqx_utils_fs:canonicalize(
        emqx_utils_maps:deep_get([<<"ocsp">>, <<"issuer_pem">>], SSL2)
    ),
    ok = load_config(#{
        <<"servers">> => #{
            <<"name">> => #{<<"ssl">> => SSL1},
            <<"noname">> => #{<<"ssl">> => SSL2}
        }
    }),

    % Wait for a periodic collection event
    ?check_trace(
        ?block_until(#{?snk_kind := tls_certfile_gc_periodic, ?snk_span := {complete, _}}),
        fun(Events) ->
            % Nothing should have been collected yet
            ?assertMatch(
                [
                    #{?snk_kind := tls_certfile_gc_periodic, ?snk_span := start},
                    #{?snk_kind := tls_certfile_gc_periodic, ?snk_span := {complete, _}}
                ],
                ?of_pid(Pid, Events)
            )
        end
    ),

    % Delete the server from the config
    ok = put_config([<<"servers">>, <<"noname">>, <<"ssl">>], #{}),

    % Wait for a periodic collection event
    ?check_trace(
        ?block_until(#{?snk_kind := tls_certfile_gc_periodic, ?snk_span := {complete, _}}),
        fun(Events) ->
            % Nothing should have been collected yet, certfiles considered orphans for the first time
            ?assertMatch(
                [
                    #{?snk_kind := tls_certfile_gc_periodic, ?snk_span := start},
                    #{?snk_kind := tls_certfile_gc_periodic, ?snk_span := {complete, _}}
                ],
                ?of_pid(Pid, Events)
            )
        end
    ),

    % Delete another server from the config
    ok = put_config([<<"servers">>, <<"name">>, <<"ssl">>], #{}),

    % Wait for next periodic collection event
    ?check_trace(
        ?block_until(#{?snk_kind := tls_certfile_gc_periodic, ?snk_span := {complete, _}}),
        fun(Events) ->
            % SSL2 certfiles should have been collected now, but not SSL1
            ?assertMatch(
                [
                    #{?snk_kind := tls_certfile_gc_periodic, ?snk_span := start},
                    #{?snk_kind := "tls_certfile_gc_collected", filename := SSL2IssPEM},
                    #{?snk_kind := "tls_certfile_gc_collected", filename := SSL2Keyfile},
                    #{?snk_kind := "tls_certfile_gc_collected", filename := SSL2Certfile},
                    #{?snk_kind := "tls_certfile_gc_collected", filename := _SSL2Dir},
                    #{?snk_kind := tls_certfile_gc_periodic, ?snk_span := {complete, _}}
                ],
                ?of_pid(Pid, Events)
            )
        end
    ),

    % Wait for next periodic collection event
    ?check_trace(
        ?block_until(#{?snk_kind := tls_certfile_gc_periodic, ?snk_span := {complete, _}}),
        fun(Events) ->
            % SSL1 certfiles should have been collected finally, they were considered orphans before
            ?assertMatch(
                [
                    #{?snk_kind := tls_certfile_gc_periodic, ?snk_span := start},
                    #{?snk_kind := "tls_certfile_gc_collected", filename := SSL1Keyfile},
                    #{?snk_kind := "tls_certfile_gc_collected", filename := SSL1Certfile},
                    #{?snk_kind := "tls_certfile_gc_collected", filename := _SSL1Dir},
                    #{?snk_kind := tls_certfile_gc_periodic, ?snk_span := {complete, _}}
                ],
                ?of_pid(Pid, Events)
            )
        end
    ),

    ok = proc_lib:stop(Pid).

t_gc_spares_recreated_certfiles(_Config) ->
    {ok, Pid} = emqx_tls_certfile_gc:start_link(),

    % Create two sets of certfiles, with no references to them
    SSL = #{
        <<"keyfile">> => key(),
        <<"certfile">> => cert()
    },
    {ok, SSL1} = emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir("s1", SSL),
    SSL1Keyfile = emqx_utils_fs:canonicalize(maps:get(<<"keyfile">>, SSL1)),
    SSL1Certfile = emqx_utils_fs:canonicalize(maps:get(<<"certfile">>, SSL1)),
    {ok, SSL2} = emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir("s2", SSL),
    SSL2Keyfile = emqx_utils_fs:canonicalize(maps:get(<<"keyfile">>, SSL2)),
    SSL2Certfile = emqx_utils_fs:canonicalize(maps:get(<<"certfile">>, SSL2)),
    ok = load_config(#{}),

    % Nothing should have been collected yet
    ?assertMatch(
        {ok, []},
        emqx_tls_certfile_gc:run()
    ),

    % At least one second should pass, since mtime has second resolution
    ok = timer:sleep(1000),
    ok = file:change_time(SSL2Keyfile, erlang:localtime()),
    ok = file:change_time(SSL2Certfile, erlang:localtime()),
    % Only SSL1 certfiles should have been collected
    ?assertMatch(
        {ok, [
            {collect, _SSL1Dir, ok},
            {collect, SSL1Certfile, ok},
            {collect, SSL1Keyfile, ok}
        ]},
        emqx_tls_certfile_gc:run()
    ),

    % Recreate the SSL2 certfiles
    ok = file:delete(SSL2Keyfile),
    ok = file:delete(SSL2Certfile),
    {ok, _} = emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir("s2", SSL),
    % Nothing should have been collected
    ?assertMatch(
        {ok, []},
        emqx_tls_certfile_gc:run()
    ),

    ok = proc_lib:stop(Pid).

t_gc_spares_symlinked_datadir(Config) ->
    {ok, Pid} = emqx_tls_certfile_gc:start_link(),

    % Create a certfiles set and a server that references it
    SSL = #{
        <<"keyfile">> => key(),
        <<"certfile">> => cert(),
        <<"ocsp">> => #{<<"issuer_pem">> => cert()}
    },
    {ok, SSL1} = emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir("srv", SSL),
    SSL1Keyfile = emqx_utils_fs:canonicalize(maps:get(<<"keyfile">>, SSL1)),

    ok = load_config(#{
        <<"servers">> => #{<<"srv">> => #{<<"ssl">> => SSL1}}
    }),

    % Change the emqx data_dir to a symlink
    TCAbsDir = ?config(tc_absdir, Config),
    TCAbsLink = filename:join(?config(priv_dir, Config), ?config(tc_name, Config) ++ ".symlink"),
    ok = file:make_symlink(TCAbsDir, TCAbsLink),
    ok = application:set_env(emqx, data_dir, TCAbsLink),

    % Make a hardlink to the SSL1 keyfile, that looks like a managed SSL file
    SSL1KeyfileHardlink = filename:join([
        filename:dirname(SSL1Keyfile),
        "hardlink",
        filename:basename(SSL1Keyfile)
    ]),
    ok = filelib:ensure_dir(SSL1KeyfileHardlink),
    ok = file:make_link(SSL1Keyfile, SSL1KeyfileHardlink),

    % Nothing should have been collected
    ?assertMatch(
        {ok, []},
        emqx_tls_certfile_gc:force()
    ),

    ok = put_config([<<"servers">>, <<"srv">>, <<"ssl">>], #{}),

    % Everything should have been collected, including the hardlink
    ?assertMatch(
        {ok, [
            {collect, _SSL1Dir, ok},
            {collect, _SSL1Certfile, ok},
            {collect, _SSL1KeyfileHardlinkDir, ok},
            {collect, _SSL1KeyfileHardlink, ok},
            {collect, _SSL1Keyfile, ok},
            {collect, _SSL1IssuerPEM, ok}
        ]},
        emqx_tls_certfile_gc:force()
    ),

    ok = proc_lib:stop(Pid).

t_gc_active(Config) ->
    Apps = emqx_cth_suite:start(
        [emqx],
        #{work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, Config)}
    ),
    try
        ?assertEqual(
            {ok, []},
            emqx_tls_certfile_gc:run()
        )
    after
        emqx_cth_suite:stop(Apps)
    end.

orphans() ->
    emqx_tls_certfile_gc:orphans(emqx:mutable_certs_dir()).

convicts(Orphans, OrphansLast) ->
    emqx_tls_certfile_gc:convicts(Orphans, OrphansLast).

collect(Convicts) ->
    emqx_tls_certfile_gc:collect_files(Convicts, emqx:mutable_certs_dir()).

load_config(Config) ->
    emqx_config:init_load(
        ?MODULE,
        emqx_utils_json:encode(#{<<?MODULE_STRING>> => Config})
    ).

put_config(Path, SubConfig) ->
    emqx_config:put_raw([<<?MODULE_STRING>> | Path], SubConfig).

cert() ->
    <<
        "-----BEGIN CERTIFICATE-----\n"
        "MIIFljCCA36gAwIBAgICEAEwDQYJKoZIhvcNAQELBQAwazELMAkGA1UEBhMCU0Ux\n"
        "EjAQBgNVBAgMCVN0b2NraG9sbTESMBAGA1UECgwJTXlPcmdOYW1lMRkwFwYDVQQL\n"
        "DBBNeUludGVybWVkaWF0ZUNBMRkwFwYDVQQDDBBNeUludGVybWVkaWF0ZUNBMB4X\n"
        "DTIzMDExMjEzMDgxNloXDTMzMDQxOTEzMDgxNlowdzELMAkGA1UEBhMCU0UxEjAQ\n"
        "BgNVBAgMCVN0b2NraG9sbTESMBAGA1UEBwwJU3RvY2tob2xtMRIwEAYDVQQKDAlN\n"
        "eU9yZ05hbWUxGTAXBgNVBAsMEE15SW50ZXJtZWRpYXRlQ0ExETAPBgNVBAMMCE15\n"
        "Q2xpZW50MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAvGuAShewEo8V\n"
        "/+aWVO/MuUt92m8K0Ut4nC2gOvpjMjf8mhSSf6KfnxPklsFwP4fdyPOjOiXwCsf3\n"
        "1QO5fjVr8to3iGTHhEyZpzRcRqmw1eYJC7iDh3BqtYLAT30R+Kq6Mk+f4tXB5Lp/\n"
        "2jXgdi0wshWagCPgJO3CtiwGyE8XSa+Q6EBYwzgh3NFbgYdJma4x+S86Y/5WfmXP\n"
        "zF//UipsFp4gFUqwGuj6kJrN9NnA1xCiuOxCyN4JuFNMfM/tkeh26jAp0OHhJGsT\n"
        "s3YiUm9Dpt7Rs7o0so9ov9K+hgDFuQw9HZW3WIJI99M5a9QZ4ZEQqKpABtYBl/Nb\n"
        "VPXcr+T3fQIDAQABo4IBNjCCATIwCQYDVR0TBAIwADARBglghkgBhvhCAQEEBAMC\n"
        "BaAwMwYJYIZIAYb4QgENBCYWJE9wZW5TU0wgR2VuZXJhdGVkIENsaWVudCBDZXJ0\n"
        "aWZpY2F0ZTAdBgNVHQ4EFgQUOIChBA5aZB0dPWEtALfMIfSopIIwHwYDVR0jBBgw\n"
        "FoAUTHCGOxVSibq1D5KqrrExQRO+c/MwDgYDVR0PAQH/BAQDAgXgMB0GA1UdJQQW\n"
        "MBQGCCsGAQUFBwMCBggrBgEFBQcDBDA7BgNVHR8ENDAyMDCgLqAshipodHRwOi8v\n"
        "bG9jYWxob3N0Ojk4NzgvaW50ZXJtZWRpYXRlLmNybC5wZW0wMQYIKwYBBQUHAQEE\n"
        "JTAjMCEGCCsGAQUFBzABhhVodHRwOi8vbG9jYWxob3N0Ojk4NzcwDQYJKoZIhvcN\n"
        "AQELBQADggIBAE0qTL5WIWcxRPU9oTrzJ+oxMTp1JZ7oQdS+ZekLkQ8mP7T6C/Ew\n"
        "6YftjvkopnHUvn842+PTRXSoEtlFiTccmA60eMAai2tn5asxWBsLIRC9FH3LzOgV\n"
        "/jgyY7HXuh8XyDBCDD+Sj9QityO+accTHijYAbHPAVBwmZU8nO5D/HsxLjRrCfQf\n"
        "qf4OQpX3l1ryOi19lqoRXRGwcoZ95dqq3YgTMlLiEqmerQZSR6iSPELw3bcwnAV1\n"
        "hoYYzeKps3xhwszCTz2+WaSsUO2sQlcFEsZ9oHex/02UiM4a8W6hGFJl5eojErxH\n"
        "7MqaSyhwwyX6yt8c75RlNcUThv+4+TLkUTbTnWgC9sFjYfd5KSfAdIMp3jYzw3zw\n"
        "XEMTX5FaLaOCAfUDttPzn+oNezWZ2UyFTQXQE2CazpRdJoDd04qVg9WLpQxLYRP7\n"
        "xSFEHulOPccdAYF2C45yNtJAZyWKfGaAZIxrgEXbMkcdDMlYphpRwpjS8SIBNZ31\n"
        "KFE8BczKrg2qO0ywIjanPaRgrFVmeSvBKeU/YLQVx6fZMgOk6vtidLGZLyDXy0Ff\n"
        "yaZSoj+on++RDz1IXb96Y8scuNlfcYI8QeoNjwiLtf80BV8SRJiG4e/jTvMf/z9L\n"
        "kWrnDWvx4xkUmxFg4TK42dkNp7sEYBTlVVq9fjKE92ha7FGZRqsxOLNQ\n"
        "-----END CERTIFICATE-----\n"
    >>.

key() ->
    <<
        "-----BEGIN EC PRIVATE KEY-----\n"
        "MHQCAQEEICKTbbathzvD8zvgjL7qRHhW4alS0+j0Loo7WeYX9AxaoAcGBSuBBAAK\n"
        "oUQDQgAEJBdF7MIdam5T4YF3JkEyaPKdG64TVWCHwr/plC0QzNVJ67efXwxlVGTo\n"
        "ju0VBj6tOX1y6C0U+85VOM0UU5xqvw==\n"
        "-----END EC PRIVATE KEY-----\n"
    >>.

%%--------------------------------------------------------------------
%% Schema
%% -------------------------------------------------------------------

roots() ->
    [?MODULE].

namespace() ->
    "ct".

fields(?MODULE) ->
    [
        {servers, mk(hoconsc:map(string(), ref(?MODULE, server)))},
        {clients, mk(hoconsc:array(ref(?MODULE, client)))}
    ];
fields(server) ->
    [
        {ssl, mk(ref(emqx_schema, "listener_ssl_opts"))}
    ];
fields(client) ->
    [
        {transport,
            mk(
                ref(?MODULE, transport),
                #{required => false}
            )}
    ];
fields(transport) ->
    [
        {ssl,
            mk(
                ref(emqx_schema, "ssl_client_opts"),
                #{default => #{<<"enable">> => false}}
            )}
    ].
