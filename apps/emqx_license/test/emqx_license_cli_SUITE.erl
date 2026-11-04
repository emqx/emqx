%%--------------------------------------------------------------------
%% Copyright (c) 2022-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_license_cli_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            {emqx_license, #{
                config => #{license => #{key => emqx_license_test_lib:default_license()}}
            }}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(_Case, Config) ->
    emqx_license_test_lib:mock_parser(),
    Config.

end_per_testcase(_Case, _Config) ->
    emqx_license_test_lib:unmock_parser(),
    ok.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_help(_Config) ->
    _ = emqx_license_cli:license([]).

t_info(_Config) ->
    _ = emqx_license_cli:license(["info"]).

%% Load signed licenses and verify that CLI output preserves UTF-8 customer names.
t_info_unicode(Config) ->
    try
        lists:foreach(
            fun(Customer) ->
                LicenseKey = emqx_license_test_lib:make_license(#{
                    name => binary_to_list(Customer)
                }),
                ?assertMatch({ok, _}, emqx_license:update_key(LicenseKey)),
                Output = capture_license_info(Config),
                Lines = binary:split(Output, <<"\n">>, [global]),
                ?assert(lists:member(<<"customer        : ", Customer/binary>>, Lines)),
                ?assert(lists:member(<<"email           : contact@foo.com">>, Lines)),
                ?assert(lists:member(<<"deployment      : bar-deployment">>, Lines))
            end,
            [<<"Foo">>, <<"示例科技有限公司"/utf8>>, <<"客户🚀"/utf8>>]
        )
    after
        {ok, _} = emqx_license:update_key(<<"default">>)
    end.

t_update(_Config) ->
    LicenseValue = emqx_license_test_lib:default_license(),
    _ = emqx_license_cli:license(["update", LicenseValue]),
    _ = emqx_license_cli:license(["reload"]),
    _ = emqx_license_cli:license(["update", "Invalid License Value"]).

t_conf_update(_Config) ->
    LicenseKey = emqx_license_test_lib:make_license(#{max_connections => "123"}),
    Conf = #{
        <<"connection_high_watermark">> => <<"50%">>,
        <<"connection_low_watermark">> => <<"45%">>,
        <<"key">> => LicenseKey
    },
    ?assertMatch({ok, _}, emqx:update_config([license], Conf)),
    ?assertEqual(
        #{
            connection_high_watermark => 0.5,
            connection_low_watermark => 0.45,
            dynamic_max_connections => 25,
            key => LicenseKey
        },
        emqx:get_config([license])
    ),
    Dump = maps:from_list(emqx_license_checker:dump()),
    ?assertMatch(
        #{
            max_connections := 123,
            max_sessions := 123
        },
        Dump
    ),
    ok.

capture_license_info(Config) ->
    Path = filename:join(?config(priv_dir, Config), "license-info.txt"),
    {ok, Device} = file:open(Path, [write, {encoding, utf8}]),
    GroupLeader = group_leader(),
    try
        true = group_leader(Device, self()),
        ok = emqx_license_cli:license(["info"])
    after
        true = group_leader(GroupLeader, self()),
        ok = file:close(Device)
    end,
    {ok, Output} = file:read_file(Path),
    Output.
