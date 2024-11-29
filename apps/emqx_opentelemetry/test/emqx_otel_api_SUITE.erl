%%--------------------------------------------------------------------
%% Copyright (c) 2020-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_otel_api_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(OTEL_API_PATH, emqx_mgmt_api_test_util:api_path(["opentelemetry"])).
-define(CONF_PATH, [opentelemetry]).

-define(CACERT, <<
    "-----BEGIN CERTIFICATE-----\n"
    "MIIDUTCCAjmgAwIBAgIJAPPYCjTmxdt/MA0GCSqGSIb3DQEBCwUAMD8xCzAJBgNV\n"
    "BAYTAkNOMREwDwYDVQQIDAhoYW5nemhvdTEMMAoGA1UECgwDRU1RMQ8wDQYDVQQD\n"
    "DAZSb290Q0EwHhcNMjAwNTA4MDgwNjUyWhcNMzAwNTA2MDgwNjUyWjA/MQswCQYD\n"
    "VQQGEwJDTjERMA8GA1UECAwIaGFuZ3pob3UxDDAKBgNVBAoMA0VNUTEPMA0GA1UE\n"
    "AwwGUm9vdENBMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAzcgVLex1\n"
    "EZ9ON64EX8v+wcSjzOZpiEOsAOuSXOEN3wb8FKUxCdsGrsJYB7a5VM/Jot25Mod2\n"
    "juS3OBMg6r85k2TWjdxUoUs+HiUB/pP/ARaaW6VntpAEokpij/przWMPgJnBF3Ur\n"
    "MjtbLayH9hGmpQrI5c2vmHQ2reRZnSFbY+2b8SXZ+3lZZgz9+BaQYWdQWfaUWEHZ\n"
    "uDaNiViVO0OT8DRjCuiDp3yYDj3iLWbTA/gDL6Tf5XuHuEwcOQUrd+h0hyIphO8D\n"
    "tsrsHZ14j4AWYLk1CPA6pq1HIUvEl2rANx2lVUNv+nt64K/Mr3RnVQd9s8bK+TXQ\n"
    "KGHd2Lv/PALYuwIDAQABo1AwTjAdBgNVHQ4EFgQUGBmW+iDzxctWAWxmhgdlE8Pj\n"
    "EbQwHwYDVR0jBBgwFoAUGBmW+iDzxctWAWxmhgdlE8PjEbQwDAYDVR0TBAUwAwEB\n"
    "/zANBgkqhkiG9w0BAQsFAAOCAQEAGbhRUjpIred4cFAFJ7bbYD9hKu/yzWPWkMRa\n"
    "ErlCKHmuYsYk+5d16JQhJaFy6MGXfLgo3KV2itl0d+OWNH0U9ULXcglTxy6+njo5\n"
    "CFqdUBPwN1jxhzo9yteDMKF4+AHIxbvCAJa17qcwUKR5MKNvv09C6pvQDJLzid7y\n"
    "E2dkgSuggik3oa0427KvctFf8uhOV94RvEDyqvT5+pgNYZ2Yfga9pD/jjpoHEUlo\n"
    "88IGU8/wJCx3Ds2yc8+oBg/ynxG8f/HmCC1ET6EHHoe2jlo8FpU/SgGtghS1YL30\n"
    "IWxNsPrUP+XsZpBJy/mvOhE5QXo6Y35zDqqj8tI7AGmAWu22jg==\n"
    "-----END CERTIFICATE-----"
>>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    %% This is called by emqx_machine in EMQX release
    emqx_otel_app:configure_otel_deps(),
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx_management,
            {emqx_dashboard, "dashboard.listeners.http { enable = true, bind = 18083 }"},
            emqx_opentelemetry
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    Auth = auth_header(),
    [{suite_apps, Apps}, {auth, Auth} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)),
    emqx_config:delete_override_conf_files(),
    ok.

init_per_testcase(_TC, Config) ->
    emqx_conf:update(
        ?CONF_PATH,
        #{
            <<"traces">> => #{<<"enable">> => false},
            <<"metrics">> => #{<<"enable">> => false},
            <<"logs">> => #{<<"enable">> => false}
        },
        #{}
    ),
    Config.

end_per_testcase(_TC, _Config) ->
    ok.

auth_header() ->
    {ok, API} = emqx_common_test_http:create_default_app(),
    emqx_common_test_http:auth_header(API).

t_get(Config) ->
    Auth = ?config(auth, Config),
    Path = ?OTEL_API_PATH,
    {ok, Resp} = emqx_mgmt_api_test_util:request_api(get, Path, Auth),
    ?assertMatch(
        #{
            <<"traces">> := #{<<"enable">> := false},
            <<"metrics">> := #{<<"enable">> := false},
            <<"logs">> := #{<<"enable">> := false}
        },
        emqx_utils_json:decode(Resp)
    ).

t_put_enable_disable(Config) ->
    Auth = ?config(auth, Config),
    Path = ?OTEL_API_PATH,
    EnableAllReq = #{
        <<"traces">> => #{<<"enable">> => true},
        <<"metrics">> => #{<<"enable">> => true},
        <<"logs">> => #{<<"enable">> => true}
    },
    ?assertMatch({ok, _}, emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, EnableAllReq)),
    ?assertMatch(
        #{
            traces := #{enable := true},
            metrics := #{enable := true},
            logs := #{enable := true}
        },
        emqx:get_config(?CONF_PATH)
    ),

    DisableAllReq = #{
        <<"traces">> => #{<<"enable">> => false},
        <<"metrics">> => #{<<"enable">> => false},
        <<"logs">> => #{<<"enable">> => false}
    },
    ?assertMatch({ok, _}, emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, DisableAllReq)),
    ?assertMatch(
        #{
            traces := #{enable := false},
            metrics := #{enable := false},
            logs := #{enable := false}
        },
        emqx:get_config(?CONF_PATH)
    ).

t_put_invalid(Config) ->
    Auth = ?config(auth, Config),
    Path = ?OTEL_API_PATH,

    ?assertMatch(
        {error, {_, 400, _}},
        emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, #{
            <<"exporter">> => #{<<"endpoint">> => <<>>}
        })
    ),
    ?assertMatch(
        {error, {_, 400, _}},
        emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, #{
            <<"exporter">> => #{<<"endpoint">> => <<"unknown://somehost.org">>}
        })
    ),
    ?assertMatch(
        {error, {_, 400, _}},
        emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, #{
            <<"exporter">> => #{<<"endpoint">> => <<"https://somehost.org:99999">>}
        })
    ),
    ?assertMatch(
        {error, {_, 400, _}},
        emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, #{
            <<"exporter">> => #{<<"endpoint">> => <<"https://somehost.org:99999">>}
        })
    ),
    ?assertMatch(
        {error, {_, 400, _}},
        emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, #{
            <<"exporter">> => #{<<"unknown_field">> => <<"foo">>}
        })
    ),
    ?assertMatch(
        {error, {_, 400, _}},
        emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, #{
            <<"exporter">> => #{<<"protocol">> => <<"unknown">>}
        })
    ),
    ?assertMatch(
        {error, {_, 400, _}},
        emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, #{
            <<"traces">> => #{<<"filter">> => #{<<"unknown_filter">> => <<"foo">>}}
        })
    ),
    ?assertMatch(
        {error, {_, 400, _}},
        emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, #{
            <<"logs">> => #{<<"level">> => <<"foo">>}
        })
    ),
    ?assertMatch(
        {error, {_, 400, _}},
        emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, #{
            <<"metrics">> => #{<<"interval">> => <<"foo">>}
        })
    ),
    ?assertMatch(
        {error, {_, 400, _}},
        emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, #{
            <<"logs">> => #{<<"unknown_field">> => <<"foo">>}
        })
    ),
    ?assertMatch(
        {error, {_, 400, _}},
        emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, #{<<"unknown_field">> => <<"foo">>})
    ).

t_put_valid(Config) ->
    Auth = ?config(auth, Config),
    Path = ?OTEL_API_PATH,

    ?assertMatch(
        {ok, _},
        emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, #{
            <<"exporter">> => #{<<"endpoint">> => <<"nohost.com">>}
        })
    ),
    ?assertEqual(<<"http://nohost.com/">>, emqx:get_config(?CONF_PATH ++ [exporter, endpoint])),

    ?assertMatch(
        {ok, _}, emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, #{<<"exporter">> => #{}})
    ),
    ?assertMatch({ok, _}, emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, #{})),
    ?assertMatch(
        {ok, _}, emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, #{<<"traces">> => #{}})
    ),
    ?assertMatch(
        {ok, _}, emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, #{<<"logs">> => #{}})
    ),
    ?assertMatch(
        {ok, _}, emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, #{<<"metrics">> => #{}})
    ),
    ?assertMatch(
        {ok, _},
        emqx_mgmt_api_test_util:request_api(
            put,
            Path,
            "",
            Auth,
            #{<<"exporter">> => #{}, <<"traces">> => #{}, <<"logs">> => #{}, <<"metrics">> => #{}}
        )
    ),
    ?assertMatch(
        {ok, _},
        emqx_mgmt_api_test_util:request_api(
            put,
            Path,
            "",
            Auth,
            #{
                <<"exporter">> => #{
                    <<"endpoint">> => <<"https://localhost:4317">>, <<"protocol">> => <<"grpc">>
                },
                <<"traces">> => #{
                    <<"enable">> => true,
                    <<"max_queue_size">> => 10,
                    <<"exporting_timeout">> => <<"10s">>,
                    <<"scheduled_delay">> => <<"20s">>,
                    <<"filter">> => #{<<"trace_all">> => true}
                },
                <<"logs">> => #{
                    <<"level">> => <<"warning">>,
                    <<"max_queue_size">> => 100,
                    <<"exporting_timeout">> => <<"10s">>,
                    <<"scheduled_delay">> => <<"1s">>
                },
                <<"metrics">> => #{
                    %% alias for "interval"
                    <<"scheduled_delay">> => <<"15321ms">>
                }
            }
        ),
        %% alias check
        ?assertEqual(15_321, emqx:get_config(?CONF_PATH ++ [metrics, interval]))
    ).

t_put_cert(Config) ->
    Auth = ?config(auth, Config),
    Path = ?OTEL_API_PATH,
    SSL = #{<<"enable">> => true, <<"cacertfile">> => ?CACERT},
    SSLDisabled = #{<<"enable">> => false, <<"cacertfile">> => ?CACERT},
    Conf = #{<<"exporter">> => #{<<"ssl_options">> => SSL}},
    Conf1 = #{<<"exporter">> => #{<<"ssl_options">> => SSLDisabled}},
    {ok, Body} = emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, Conf),
    #{<<"exporter">> := #{<<"ssl_options">> := #{<<"cacertfile">> := CaFile}}} = emqx_utils_json:decode(
        Body
    ),
    ct:pal("CA certfile: ~p", [CaFile]),
    ?assert(filelib:is_file(CaFile)),
    {ok, Body1} = emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, Conf1),
    #{<<"exporter">> := #{<<"ssl_options">> := #{<<"cacertfile">> := CaFile1}}} = emqx_utils_json:decode(
        Body1
    ),
    ct:pal("CA certfile1: ~p", [CaFile1]),
    ?assertNot(filelib:is_file(CaFile1)).
