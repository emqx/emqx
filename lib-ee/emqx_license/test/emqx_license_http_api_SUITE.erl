%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    ok = meck:new(emqx_license_parser, [non_strict, passthrough, no_history, no_link]),
    ok = meck:expect(
        emqx_license_parser,
        parse,
        fun(X) ->
            emqx_license_parser:parse(
                X,
                emqx_license_test_lib:public_key_pem()
            )
        end
    ),
    emqx_common_test_helpers:start_apps([emqx_license, emqx_dashboard], fun set_special_configs/1),
    Config.

end_per_suite(_) ->
    emqx_common_test_helpers:stop_apps([emqx_license, emqx_dashboard]),
    ok = meck:unload([emqx_license_parser]),
    Config = #{type => file, file => emqx_license_test_lib:default_license()},
    emqx_config:put([license], Config),
    RawConfig = #{<<"type">> => file, <<"file">> => emqx_license_test_lib:default_license()},
    emqx_config:put_raw([<<"license">>], RawConfig),
    ok.

set_special_configs(emqx_dashboard) ->
    emqx_dashboard_api_test_helpers:set_default_config(<<"license_admin">>);
set_special_configs(emqx_license) ->
    LicenseKey = emqx_license_test_lib:make_license(#{max_connections => "100"}),
    Config = #{type => key, key => LicenseKey},
    emqx_config:put([license], Config),
    RawConfig = #{<<"type">> => key, <<"key">> => LicenseKey},
    emqx_config:put_raw([<<"license">>], RawConfig);
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
        emqx_json:decode(Payload, [return_maps])
    ),
    ok.

t_license_upload_file_success(_Config) ->
    NewKey = emqx_license_test_lib:make_license(#{max_connections => "999"}),
    Path = "/tmp/new.lic",
    ok = file:write_file(Path, NewKey),
    try
        ?assertEqual(
            {ok, 200, <<"ok">>},
            request(
                post,
                uri(["license", "upload"]),
                #{file => Path}
            )
        ),
        ?assertMatch(
            #{max_connections := 999},
            get_license()
        ),
        ok
    after
        ok = file:delete(Path),
        ok
    end.

t_license_upload_file_not_found(_Config) ->
    ?assertEqual(
        {ok, 404, <<"file not found">>},
        request(
            post,
            uri(["license", "upload"]),
            #{file => "/tmp/inexistent.lic"}
        )
    ),
    assert_untouched_license(),
    ok.

t_license_upload_file_reading_error(_Config) ->
    %% eisdir
    Path = "/tmp/",
    ?assertEqual(
        {ok, 400, <<"bad request">>},
        request(
            post,
            uri(["license", "upload"]),
            #{file => Path}
        )
    ),
    assert_untouched_license(),
    ok.

t_license_upload_file_bad_license(_Config) ->
    Path = "/tmp/bad.lic",
    ok = file:write_file(Path, <<"bad key">>),
    try
        ?assertEqual(
            {ok, 400, <<"bad request">>},
            request(
                post,
                uri(["license", "upload"]),
                #{file => Path}
            )
        ),
        assert_untouched_license(),
        ok
    after
        ok = file:delete(Path),
        ok
    end.

t_license_upload_key_success(_Config) ->
    NewKey = emqx_license_test_lib:make_license(#{max_connections => "999"}),
    ?assertEqual(
        {ok, 200, <<"ok">>},
        request(
            post,
            uri(["license", "upload"]),
            #{key => NewKey}
        )
    ),
    ?assertMatch(
        #{max_connections := 999},
        get_license()
    ),
    ok.

t_license_upload_key_bad_key(_Config) ->
    BadKey = <<"bad key">>,
    ?assertEqual(
        {ok, 400, <<"bad request">>},
        request(
            post,
            uri(["license", "upload"]),
            #{key => BadKey}
        )
    ),
    assert_untouched_license(),
    ok.
