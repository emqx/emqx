%%--------------------------------------------------------------------
%% Copyright (c) 2022-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    ?assertMatch(#{error := invalid_license_format}, find_error(Parser, Err1)),

    %% invalid field number
    Res2 = emqx_license_parser:parse(
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
                "10",
                "inf",
                "unknown"
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

    %% invalid max_tps
    Res6 = emqx_license_parser:parse(
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
                "100",
                "invalid_tps"
            ]
        ),
        public_key_pem()
    ),
    ?assertMatch({error, _}, Res6),
    {error, Err6} = Res6,
    ?assertMatch(
        #{error := #{max_tps := invalid_max_tps}},
        find_error(Parser, Err6)
    ),

    ?assertMatch(
        {error, #{parse_results := [#{error := bad_license_format}]}},
        emqx_license_parser:parse(
            <<"bad.license">>,
            public_key_pem()
        )
    ).

t_parse_file_read_error(_Config) ->
    ?assertMatch(
        {error, #{license_file := _, read_error := _}},
        emqx_license_parser:parse(<<"file:///no/such/license.file">>, public_key_pem())
    ).

t_parse_exception_caught(_Config) ->
    meck:new(emqx_license_parser_v20220101, [passthrough, no_history]),
    meck:expect(emqx_license_parser_v20220101, parse, fun(_Payload, _Key) ->
        erlang:error(bad_parse)
    end),
    Res = emqx_license_parser:parse(sample_license(), public_key_pem()),
    ?assertMatch(
        {error, #{
            parse_results := [
                #{module := emqx_license_parser_v20220101, error := bad_parse, stacktrace := _}
            ]
        }},
        Res
    ),
    meck:unload(emqx_license_parser_v20220101).

t_parse_invalid_max_sessions(_Config) ->
    Parser = emqx_license_parser_v20220101,
    Res = emqx_license_parser:parse(
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
                "invalid"
            ]
        ),
        public_key_pem()
    ),
    ?assertMatch({error, _}, Res),
    {error, Err} = Res,
    ?assertMatch(
        #{error := #{max_sessions := invalid_connection_limit}},
        find_error(Parser, Err)
    ).

t_parse_zero_sessions_invalid_combination(_Config) ->
    Res = emqx_license_parser:parse(
        emqx_license_test_lib:make_license(
            [
                "220111",
                "1",
                "0",
                "Foo",
                "contact@foo.com",
                "default-deployment",
                "20220111",
                "100000",
                "0"
            ]
        ),
        public_key_pem()
    ),
    ?assertMatch({ok, #{data := _}}, Res),
    {ok, #{data := Data}} = Res,
    ?assertEqual({error, invalid_connection_limit}, maps:get(max_sessions, Data)).

t_parse_zero_sessions_bad_type_ctype(_Config) ->
    Parser = emqx_license_parser_v20220101,
    Res = emqx_license_parser:parse(
        emqx_license_test_lib:make_license(
            [
                "220111",
                "bad",
                "bad",
                "Foo",
                "contact@foo.com",
                "default-deployment",
                "20220111",
                "100000",
                "0"
            ]
        ),
        public_key_pem()
    ),
    ?assertMatch({error, _}, Res),
    {error, Err} = Res,
    #{error := ErrorMap} = find_error(Parser, Err),
    ?assertMatch(
        #{type := invalid_license_type, customer_type := invalid_customer_type},
        ErrorMap
    ),
    ?assertEqual(false, maps:is_key(max_sessions, ErrorMap)).

t_dump(_Config) ->
    {ok, License} = emqx_license_parser:parse(sample_license(), public_key_pem()),

    ?assertEqual(
        [
            {customer, <<"Foo">>},
            {email, <<"contact@foo.com">>},
            {deployment, <<"default-deployment">>},
            {max_sessions, 10},
            {start_at, <<"2022-01-11">>},
            {expiry_at, <<"2295-10-27">>},
            {type, <<"trial">>},
            {customer_type, 10},
            {expiry, false},
            {max_tps, infinity}
        ],
        emqx_license_parser:dump(License)
    ).

t_customer_type(_Config) ->
    {ok, License} = emqx_license_parser:parse(sample_license(), public_key_pem()),

    ?assertEqual(10, emqx_license_parser:customer_type(License)).

t_license_type(_Config) ->
    {ok, License} = emqx_license_parser:parse(sample_license(), public_key_pem()),

    ?assertEqual(0, emqx_license_parser:license_type(License)).

t_max_sessions(_Config) ->
    {ok, License} = emqx_license_parser:parse(sample_license(), public_key_pem()),

    ?assertEqual(10, emqx_license_parser:max_sessions(License)).

t_max_uptime_seconds(_Config) ->
    {ok, LicenseEvaluation} = emqx_license_parser:parse(
        emqx_license_test_lib:make_license(#{customer_type => "10"}), public_key_pem()
    ),
    {ok, LicenseMedium} = emqx_license_parser:parse(
        emqx_license_test_lib:make_license(#{customer_type => "1"}), public_key_pem()
    ),

    ?assert(is_integer(emqx_license_parser:max_uptime_seconds(LicenseEvaluation))),
    ?assertEqual(infinity, emqx_license_parser:max_uptime_seconds(LicenseMedium)).

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

t_default_is_not_community_in_ct(_Config) ->
    Default = emqx_license_parser:default(),
    Community = emqx_license_parser:community(),
    Evaluation = emqx_license_parser:evaluation(),
    ?assertEqual(Evaluation, Default),
    ?assertNotEqual(Community, Default).

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
