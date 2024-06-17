%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster_link_api_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(API_PATH, emqx_mgmt_api_test_util:api_path(["cluster", "links"])).
-define(CONF_PATH, [cluster, links]).

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
            emqx_cluster_link
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    Auth = auth_header(),
    [{suite_apps, Apps}, {auth, Auth} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)),
    emqx_config:delete_override_conf_files(),
    ok.

auth_header() ->
    {ok, API} = emqx_common_test_http:create_default_app(),
    emqx_common_test_http:auth_header(API).

init_per_testcase(_TC, Config) ->
    {ok, _} = emqx_cluster_link_config:update([]),
    Config.

end_per_testcase(_TC, _Config) ->
    ok.

t_put_get_valid(Config) ->
    Auth = ?config(auth, Config),
    Path = ?API_PATH,
    {ok, Resp} = emqx_mgmt_api_test_util:request_api(get, Path, Auth),
    ?assertMatch([], emqx_utils_json:decode(Resp)),

    Link1 = #{
        <<"pool_size">> => 1,
        <<"server">> => <<"emqxcl_2.nohost:31883">>,
        <<"topics">> => [<<"t/test-topic">>, <<"t/test/#">>],
        <<"name">> => <<"emqcl_1">>
    },
    Link2 = #{
        <<"pool_size">> => 1,
        <<"server">> => <<"emqxcl_2.nohost:41883">>,
        <<"topics">> => [<<"t/test-topic">>, <<"t/test/#">>],
        <<"name">> => <<"emqcl_2">>
    },
    ?assertMatch({ok, _}, emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, [Link1, Link2])),

    {ok, Resp1} = emqx_mgmt_api_test_util:request_api(get, Path, Auth),
    ?assertMatch([Link1, Link2], emqx_utils_json:decode(Resp1)),

    DisabledLink1 = Link1#{<<"enable">> => false},
    ?assertMatch(
        {ok, _}, emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, [DisabledLink1, Link2])
    ),

    {ok, Resp2} = emqx_mgmt_api_test_util:request_api(get, Path, Auth),
    ?assertMatch([DisabledLink1, Link2], emqx_utils_json:decode(Resp2)),

    SSL = #{<<"enable">> => true, <<"cacertfile">> => ?CACERT},
    SSLLink1 = Link1#{<<"ssl">> => SSL},
    ?assertMatch(
        {ok, _}, emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, [Link2, SSLLink1])
    ),
    {ok, Resp3} = emqx_mgmt_api_test_util:request_api(get, Path, Auth),

    ?assertMatch(
        [Link2, #{<<"ssl">> := #{<<"enable">> := true, <<"cacertfile">> := _Path}}],
        emqx_utils_json:decode(Resp3)
    ).

t_put_invalid(Config) ->
    Auth = ?config(auth, Config),
    Path = ?API_PATH,
    Link = #{
        <<"pool_size">> => 1,
        <<"server">> => <<"emqxcl_2.nohost:31883">>,
        <<"topics">> => [<<"t/test-topic">>, <<"t/test/#">>],
        <<"name">> => <<"emqcl_1">>
    },
    ?assertMatch(
        {error, {_, 400, _}}, emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, [Link, Link])
    ),
    ?assertMatch(
        {error, {_, 400, _}},
        emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, [maps:remove(<<"name">>, Link)])
    ).
