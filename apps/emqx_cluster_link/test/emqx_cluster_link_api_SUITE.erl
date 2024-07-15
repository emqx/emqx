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

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    %% This is called by emqx_machine in EMQX release
    emqx_otel_app:configure_otel_deps(),
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard(),
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
    emqx_mgmt_api_test_util:auth_header_().

init_per_testcase(_TC, Config) ->
    {ok, _} = emqx_cluster_link_config:update([]),
    Config.

end_per_testcase(_TC, _Config) ->
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

api_root() ->
    <<"cluster/links">>.

list() ->
    Path = emqx_mgmt_api_test_util:api_path([api_root()]),
    emqx_mgmt_api_test_util:simple_request(get, Path, _Params = "").

get_link(Name) ->
    Path = emqx_mgmt_api_test_util:api_path([api_root(), Name]),
    emqx_mgmt_api_test_util:simple_request(get, Path, _Params = "").

delete_link(Name) ->
    Path = emqx_mgmt_api_test_util:api_path([api_root(), Name]),
    emqx_mgmt_api_test_util:simple_request(delete, Path, _Params = "").

update_link(Name, Params) ->
    Path = emqx_mgmt_api_test_util:api_path([api_root(), Name]),
    emqx_mgmt_api_test_util:simple_request(put, Path, Params).

create_link(Name, Params0) ->
    Params = Params0#{<<"name">> => Name},
    Path = emqx_mgmt_api_test_util:api_path([api_root()]),
    emqx_mgmt_api_test_util:simple_request(post, Path, Params).

link_params() ->
    link_params(_Overrides = #{}).

link_params(Overrides) ->
    Default = #{
        <<"clientid">> => <<"linkclientid">>,
        <<"username">> => <<"myusername">>,
        <<"pool_size">> => 1,
        <<"server">> => <<"emqxcl_2.nohost:31883">>,
        <<"topics">> => [<<"t/test-topic">>, <<"t/test/#">>]
    },
    emqx_utils_maps:deep_merge(Default, Overrides).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_put_get_valid(_Config) ->
    ?assertMatch({200, []}, list()),

    Name1 = <<"emqcl_1">>,
    Link1 = link_params(#{
        <<"server">> => <<"emqxcl_2.nohost:31883">>,
        <<"name">> => Name1
    }),
    Name2 = <<"emqcl_2">>,
    Link2 = link_params(#{
        <<"server">> => <<"emqxcl_2.nohost:41883">>,
        <<"name">> => Name2
    }),
    ?assertMatch({201, _}, create_link(Name1, Link1)),
    ?assertMatch({201, _}, create_link(Name2, Link2)),
    ?assertMatch({200, [_, _]}, list()),

    DisabledLink1 = Link1#{<<"enable">> => false},
    ?assertMatch({200, _}, update_link(Name1, maps:remove(<<"name">>, DisabledLink1))),
    ?assertMatch({200, #{<<"enable">> := false}}, get_link(Name1)),
    ?assertMatch({200, #{<<"enable">> := true}}, get_link(Name2)),

    SSL = #{<<"enable">> => true, <<"cacertfile">> => ?CACERT},
    SSLLink1 = Link1#{<<"ssl">> => SSL},
    ?assertMatch({200, _}, update_link(Name1, maps:remove(<<"name">>, SSLLink1))),
    ?assertMatch(
        {200, #{<<"ssl">> := #{<<"enable">> := true, <<"cacertfile">> := _Path}}},
        get_link(Name1)
    ),
    ok.

t_put_invalid(_Config) ->
    Name = <<"l1">>,
    {201, _} = create_link(Name, link_params()),
    ?assertMatch(
        {400, _},
        update_link(Name, maps:remove(<<"server">>, link_params()))
    ).

t_crud(_Config) ->
    %% No links initially.
    ?assertMatch({200, []}, list()),
    NameA = <<"a">>,
    ?assertMatch({404, _}, get_link(NameA)),
    ?assertMatch({404, _}, delete_link(NameA)),
    ?assertMatch({404, _}, update_link(NameA, link_params())),

    Params1 = link_params(),
    ?assertMatch({201, #{<<"name">> := NameA}}, create_link(NameA, Params1)),
    ?assertMatch({400, #{<<"code">> := <<"ALREADY_EXISTS">>}}, create_link(NameA, Params1)),
    ?assertMatch({200, [#{<<"name">> := NameA}]}, list()),
    ?assertMatch({200, #{<<"name">> := NameA}}, get_link(NameA)),

    Params2 = Params1#{<<"pool_size">> := 2},
    ?assertMatch({200, #{<<"name">> := NameA}}, update_link(NameA, Params2)),

    ?assertMatch({204, _}, delete_link(NameA)),
    ?assertMatch({404, _}, delete_link(NameA)),
    ?assertMatch({404, _}, get_link(NameA)),
    ?assertMatch({404, _}, update_link(NameA, Params1)),
    ?assertMatch({200, []}, list()),

    ok.
