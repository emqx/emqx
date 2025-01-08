%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_license_parser_SUITE).

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

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_parse(_Config) ->
    Parser = emqx_license_parser_v20220101,
    ?assertMatch({ok, _}, emqx_license_parser:parse(sample_license(), public_key_pem())),

    %% invalid version
    Res1 = emqx_license_parser:parse(
        emqx_license_test_lib:make_license(
            [
                "220101",
                "0",
                "10",
                "Foo",
                "contact@foo.com",
                "bar-deployment",
                "20220111",
                "100000",
                "10"
            ]
        ),
        public_key_pem()
    ),
    ?assertMatch({error, _}, Res1),
    {error, Err1} = Res1,
    ?assertMatch(#{error := invalid_version}, find_error(Parser, Err1)),

    %% invalid field number
    Res2 = emqx_license_parser:parse(
        emqx_license_test_lib:make_license(
            [
                "220111",
                "0",
                "10",
                "Foo",
                % one extra field
                "Bar",
                "contact@foo.com",
                "default-deployment",
                "20220111",
                "100000",
                "10"
            ]
        ),
        public_key_pem()
    ),
    ?assertMatch({error, _}, Res2),
    {error, Err2} = Res2,
    ?assertMatch(
        #{error := unexpected_number_of_fields},
        find_error(Parser, Err2)
    ),

    Res3 = emqx_license_parser:parse(
        emqx_license_test_lib:make_license(
            [
                "220111",
                "zero",
                "ten",
                "Foo",
                "contact@foo.com",
                "default-deployment",
                "20220231",
                "-10",
                "10"
            ]
        ),
        public_key_pem()
    ),
    ?assertMatch({error, _}, Res3),
    {error, Err3} = Res3,
    ?assertMatch(
        #{
            error :=
                #{
                    type := invalid_license_type,
                    customer_type := invalid_customer_type,
                    date_start := invalid_date,
                    days := invalid_int_value
                }
        },
        find_error(Parser, Err3)
    ),

    Res4 = emqx_license_parser:parse(
        emqx_license_test_lib:make_license(
            [
                "220111",
                "zero",
                "ten",
                "Foo",
                "contact@foo.com",
                "default-deployment",
                "2022-02-1st",
                "-10",
                "10"
            ]
        ),
        public_key_pem()
    ),
    ?assertMatch({error, _}, Res4),
    {error, Err4} = Res4,

    ?assertMatch(
        #{
            error :=
                #{
                    type := invalid_license_type,
                    customer_type := invalid_customer_type,
                    date_start := invalid_date,
                    days := invalid_int_value
                }
        },
        find_error(Parser, Err4)
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
                "default-deployment",
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
                "default-deployment",
                "20220111",
                "100000",
                "10"
            ]
        ),
        <<".">>
    ),

    Res5 = emqx_license_parser:parse(
        iolist_to_binary([LicensePart, <<".">>, SignaturePart]),
        public_key_pem()
    ),
    ?assertMatch({error, _}, Res5),
    {error, Err5} = Res5,
    ?assertMatch(
        #{error := invalid_signature},
        find_error(Parser, Err5)
    ),

    %% totally invalid strings as license
    ?assertMatch(
        {error, #{parse_results := [#{error := bad_license_format}]}},
        emqx_license_parser:parse(
            <<"badlicense">>,
            public_key_pem()
        )
    ),

    ?assertMatch(
        {error, #{parse_results := [#{error := bad_license_format}]}},
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
            {deployment, <<"default-deployment">>},
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

t_empty_string(_Config) ->
    ?assertMatch(
        {error, #{
            parse_results := [
                #{
                    error := empty_string,
                    module := emqx_license_parser_v20220101
                }
                | _
            ]
        }},
        emqx_license_parser:parse(<<>>)
    ).

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
            "default-deployment",
            "20220111",
            "100,000",
            "10"
        ]
    ).

find_error(Module, #{parse_results := Results}) ->
    find_error(Module, Results);
find_error(Module, [#{module := Module} = Result | _Results]) ->
    Result;
find_error(Module, [_Result | Results]) ->
    find_error(Module, Results).
