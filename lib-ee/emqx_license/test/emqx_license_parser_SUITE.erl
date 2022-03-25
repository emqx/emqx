%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_license_parser_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    _ = application:load(emqx_conf),
    emqx_common_test_helpers:start_apps([emqx_license], fun set_special_configs/1),
    Config.

end_per_suite(_) ->
    emqx_common_test_helpers:stop_apps([emqx_license]),
    ok.

init_per_testcase(_Case, Config) ->
    {ok, _} = emqx_cluster_rpc:start_link(node(), emqx_cluster_rpc, 1000),
    Config.

end_per_testcase(_Case, _Config) ->
    ok.

set_special_configs(emqx_license) ->
    Config = #{file => emqx_license_test_lib:default_license()},
    emqx_config:put([license], Config);
set_special_configs(_) ->
    ok.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_parse(_Config) ->
    ?assertMatch({ok, _}, emqx_license_parser:parse(sample_license(), public_key_pem())),

    %% invalid version
    ?assertMatch(
        {error, [{emqx_license_parser_v20220101, invalid_version}]},
        emqx_license_parser:parse(
            emqx_license_test_lib:make_license(
                [
                    "220101",
                    "0",
                    "10",
                    "Foo",
                    "contact@foo.com",
                    "20220111",
                    "100000",
                    "10"
                ]
            ),
            public_key_pem()
        )
    ),

    %% invalid field number
    ?assertMatch(
        {error, [{emqx_license_parser_v20220101, invalid_field_number}]},
        emqx_license_parser:parse(
            emqx_license_test_lib:make_license(
                [
                    "220111",
                    "0",
                    "10",
                    "Foo",
                    "Bar",
                    "contact@foo.com",
                    "20220111",
                    "100000",
                    "10"
                ]
            ),
            public_key_pem()
        )
    ),

    ?assertMatch(
        {error, [
            {emqx_license_parser_v20220101, [
                {type, invalid_license_type},
                {customer_type, invalid_customer_type},
                {date_start, invalid_date},
                {days, invalid_int_value}
            ]}
        ]},
        emqx_license_parser:parse(
            emqx_license_test_lib:make_license(
                [
                    "220111",
                    "zero",
                    "ten",
                    "Foo",
                    "contact@foo.com",
                    "20220231",
                    "-10",
                    "10"
                ]
            ),
            public_key_pem()
        )
    ),

    ?assertMatch(
        {error, [
            {emqx_license_parser_v20220101, [
                {type, invalid_license_type},
                {customer_type, invalid_customer_type},
                {date_start, invalid_date},
                {days, invalid_int_value}
            ]}
        ]},
        emqx_license_parser:parse(
            emqx_license_test_lib:make_license(
                [
                    "220111",
                    "zero",
                    "ten",
                    "Foo",
                    "contact@foo.com",
                    "2022-02-1st",
                    "-10",
                    "10"
                ]
            ),
            public_key_pem()
        )
    ),

    %% invalid signature
    [LicensePart, _] = binary:split(
        emqx_license_test_lib:make_license(
            [
                "220111",
                "0",
                "10",
                "Foo",
                "contact@foo.com",
                "20220111",
                "100000",
                "10"
            ]
        ),
        <<".">>
    ),
    [_, SignaturePart] = binary:split(
        emqx_license_test_lib:make_license(
            [
                "220111",
                "1",
                "10",
                "Foo",
                "contact@foo.com",
                "20220111",
                "100000",
                "10"
            ]
        ),
        <<".">>
    ),

    ?assertMatch(
        {error, [{emqx_license_parser_v20220101, invalid_signature}]},
        emqx_license_parser:parse(
            iolist_to_binary([LicensePart, <<".">>, SignaturePart]),
            public_key_pem()
        )
    ),

    %% totally invalid strings as license
    ?assertMatch(
        {error, [_ | _]},
        emqx_license_parser:parse(
            <<"badlicense">>,
            public_key_pem()
        )
    ),

    ?assertMatch(
        {error, [_ | _]},
        emqx_license_parser:parse(
            <<"bad.license">>,
            public_key_pem()
        )
    ).

t_dump(_Config) ->
    {ok, License} = emqx_license_parser:parse(sample_license(), public_key_pem()),

    ?assertEqual(
        [
            {customer, <<"Foo">>},
            {email, <<"contact@foo.com">>},
            {max_connections, 10},
            {start_at, <<"2022-01-11">>},
            {expiry_at, <<"2295-10-27">>},
            {type, <<"trial">>},
            {customer_type, 10},
            {expiry, false}
        ],
        emqx_license_parser:dump(License)
    ).

t_customer_type(_Config) ->
    {ok, License} = emqx_license_parser:parse(sample_license(), public_key_pem()),

    ?assertEqual(10, emqx_license_parser:customer_type(License)).

t_license_type(_Config) ->
    {ok, License} = emqx_license_parser:parse(sample_license(), public_key_pem()),

    ?assertEqual(0, emqx_license_parser:license_type(License)).

t_max_connections(_Config) ->
    {ok, License} = emqx_license_parser:parse(sample_license(), public_key_pem()),

    ?assertEqual(10, emqx_license_parser:max_connections(License)).

t_expiry_date(_Config) ->
    {ok, License} = emqx_license_parser:parse(sample_license(), public_key_pem()),

    ?assertEqual({2295, 10, 27}, emqx_license_parser:expiry_date(License)).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

public_key_pem() ->
    emqx_license_test_lib:public_key_pem().

sample_license() ->
    emqx_license_test_lib:make_license(
        [
            "220111",
            "0",
            "10",
            "Foo",
            "contact@foo.com",
            "20220111",
            "100,000",
            "10"
        ]
    ).
