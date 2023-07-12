%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_license_http_api_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    _ = application:load(emqx_conf),
    emqx_config:save_schema_mod_and_names(emqx_license_schema),
    emqx_common_test_helpers:start_apps([emqx_license, emqx_dashboard], fun set_special_configs/1),
    Config.

end_per_suite(_) ->
    emqx_common_test_helpers:stop_apps([emqx_license, emqx_dashboard]),
    LicenseKey = emqx_license_test_lib:make_license(#{max_connections => "100"}),
    Config = #{key => LicenseKey},
    emqx_config:put([license], Config),
    RawConfig = #{<<"key">> => LicenseKey},
    emqx_config:put_raw([<<"license">>], RawConfig),
    persistent_term:erase(emqx_license_test_pubkey),
    ok.

set_special_configs(emqx_dashboard) ->
    emqx_dashboard_api_test_helpers:set_default_config(<<"license_admin">>);
set_special_configs(emqx_license) ->
    LicenseKey = emqx_license_test_lib:make_license(#{max_connections => "100"}),
    Config = #{
        key => LicenseKey, connection_low_watermark => 0.75, connection_high_watermark => 0.8
    },
    emqx_config:put([license], Config),
    RawConfig = #{
        <<"key">> => LicenseKey,
        <<"connection_low_watermark">> => <<"75%">>,
        <<"connection_high_watermark">> => <<"80%">>
    },
    emqx_config:put_raw([<<"license">>], RawConfig),
    ok = persistent_term:put(
        emqx_license_test_pubkey,
        emqx_license_test_lib:public_key_pem()
    ),
    ok;
set_special_configs(_) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    {ok, _} = emqx_cluster_rpc:start_link(node(), emqx_cluster_rpc, 1000),
    Config.

end_per_testcase(_TestCase, _Config) ->
    {ok, _} = reset_license(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

request(Method, Uri, Body) ->
    emqx_dashboard_api_test_helpers:request(<<"license_admin">>, Method, Uri, Body).

uri(Segments) ->
    emqx_dashboard_api_test_helpers:uri(Segments).

get_license() ->
    maps:from_list(emqx_license_checker:dump()).

default_license() ->
    emqx_license_test_lib:make_license(#{max_connections => "100"}).

reset_license() ->
    emqx_license:update_key(default_license()).

assert_untouched_license() ->
    ?assertMatch(
        #{max_connections := 100},
        get_license()
    ).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_license_info(_Config) ->
    Res = request(get, uri(["license"]), []),
    ?assertMatch({ok, 200, _}, Res),
    {ok, 200, Payload} = Res,
    ?assertEqual(
        #{
            <<"customer">> => <<"Foo">>,
            <<"customer_type">> => 10,
            <<"deployment">> => <<"bar-deployment">>,
            <<"email">> => <<"contact@foo.com">>,
            <<"expiry">> => false,
            <<"expiry_at">> => <<"2295-10-27">>,
            <<"max_connections">> => 100,
            <<"start_at">> => <<"2022-01-11">>,
            <<"type">> => <<"trial">>
        },
        emqx_utils_json:decode(Payload, [return_maps])
    ),
    ok.

t_license_upload_key_success(_Config) ->
    NewKey = emqx_license_test_lib:make_license(#{max_connections => "999"}),
    Res = request(
        post,
        uri(["license"]),
        #{key => NewKey}
    ),
    ?assertMatch({ok, 200, _}, Res),
    {ok, 200, Payload} = Res,
    ?assertEqual(
        #{
            <<"customer">> => <<"Foo">>,
            <<"customer_type">> => 10,
            <<"deployment">> => <<"bar-deployment">>,
            <<"email">> => <<"contact@foo.com">>,
            <<"expiry">> => false,
            <<"expiry_at">> => <<"2295-10-27">>,
            <<"max_connections">> => 999,
            <<"start_at">> => <<"2022-01-11">>,
            <<"type">> => <<"trial">>
        },
        emqx_utils_json:decode(Payload, [return_maps])
    ),
    ?assertMatch(
        #{max_connections := 999},
        get_license()
    ),
    ok.

t_license_upload_key_bad_key(_Config) ->
    BadKey = <<"bad key">>,
    Res = request(
        post,
        uri(["license"]),
        #{key => BadKey}
    ),
    ?assertMatch({ok, 400, _}, Res),
    {ok, 400, Payload} = Res,
    ?assertEqual(
        #{
            <<"code">> => <<"BAD_REQUEST">>,
            <<"message">> => <<"Bad license key">>
        },
        emqx_utils_json:decode(Payload, [return_maps])
    ),
    assert_untouched_license(),
    ok.

t_license_upload_key_not_json(_Config) ->
    Res = request(
        post,
        uri(["license"]),
        <<"">>
    ),
    ?assertMatch({ok, 400, _}, Res),
    {ok, 400, Payload} = Res,
    ?assertEqual(
        #{
            <<"code">> => <<"BAD_REQUEST">>,
            <<"message">> => <<"Invalid request params">>
        },
        emqx_utils_json:decode(Payload, [return_maps])
    ),
    assert_untouched_license(),
    ok.

t_license_setting(_Config) ->
    %% get
    GetRes = request(get, uri(["license", "setting"]), []),
    validate_setting(GetRes, <<"75%">>, <<"80%">>),
    %% update
    Low = <<"50%">>,
    High = <<"55%">>,
    UpdateRes = request(put, uri(["license", "setting"]), #{
        <<"connection_low_watermark">> => Low,
        <<"connection_high_watermark">> => High
    }),
    validate_setting(UpdateRes, Low, High),
    ?assertEqual(0.5, emqx_config:get([license, connection_low_watermark])),
    ?assertEqual(0.55, emqx_config:get([license, connection_high_watermark])),

    %% update bad setting low >= high
    ?assertMatch(
        {ok, 400, _},
        request(put, uri(["license", "setting"]), #{
            <<"connection_low_watermark">> => <<"50%">>,
            <<"connection_high_watermark">> => <<"50%">>
        })
    ),
    ?assertMatch(
        {ok, 400, _},
        request(put, uri(["license", "setting"]), #{
            <<"connection_low_watermark">> => <<"51%">>,
            <<"connection_high_watermark">> => <<"50%">>
        })
    ),
    ok.

validate_setting(Res, ExpectLow, ExpectHigh) ->
    ?assertMatch({ok, 200, _}, Res),
    {ok, 200, Payload} = Res,
    ?assertEqual(
        #{
            <<"connection_low_watermark">> => ExpectLow,
            <<"connection_high_watermark">> => ExpectHigh
        },
        emqx_utils_json:decode(Payload, [return_maps])
    ).
