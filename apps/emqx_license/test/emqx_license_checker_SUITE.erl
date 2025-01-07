%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_license_checker_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_license_test_lib:mock_parser(),
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            {emqx_license, #{
                config => #{license => #{key => emqx_license_test_lib:default_test_license()}}
            }}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_license_test_lib:unmock_parser(),
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(t_default_limits, Config) ->
    ok = application:stop(emqx_license),
    Config;
init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(t_default_limits, _Config) ->
    {ok, _} = application:ensure_all_started(emqx_license);
end_per_testcase(_Case, _Config) ->
    ok.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_default_limits(_Config) ->
    ?assertMatch({error, no_license}, emqx_license_checker:limits()).

t_dump(_Config) ->
    License = mk_license(
        [
            "220111",
            "0",
            "10",
            "Foo",
            "contact@foo.com",
            "bar",
            "20220111",
            "100000",
            "10"
        ]
    ),

    #{} = emqx_license_checker:update(License),

    ?assertEqual(
        [
            {customer, <<"Foo">>},
            {email, <<"contact@foo.com">>},
            {deployment, <<"bar">>},
            {max_connections, 10},
            {start_at, <<"2022-01-11">>},
            {expiry_at, <<"2295-10-27">>},
            {type, <<"trial">>},
            {customer_type, 10},
            {expiry, false}
        ],
        emqx_license_checker:dump()
    ).

t_update(_Config) ->
    License = mk_license(
        [
            "220111",
            "0",
            "10",
            "Foo",
            "contact@foo.com",
            "bar",
            "20220111",
            "100000",
            "123"
        ]
    ),
    #{} = emqx_license_checker:update(License),

    ?assertMatch(
        {ok, #{max_connections := 123}},
        emqx_license_checker:limits()
    ).

t_check_by_timer(_Config) ->
    ?check_trace(
        begin
            ?wait_async_action(
                begin
                    erlang:send(
                        emqx_license_checker,
                        check_license
                    )
                end,
                #{?snk_kind := emqx_license_checked},
                1000
            )
        end,
        fun(Trace) ->
            ?assertMatch([_ | _], ?of_kind(emqx_license_checked, Trace))
        end
    ).

t_expired_trial(_Config) ->
    {NowDate, _} = calendar:universal_time(),
    Date10DaysAgo = calendar:gregorian_days_to_date(
        calendar:date_to_gregorian_days(NowDate) - 10
    ),

    License = mk_license(
        [
            "220111",
            "0",
            "10",
            "Foo",
            "contact@foo.com",
            "bar",
            format_date(Date10DaysAgo),
            "1",
            "123"
        ]
    ),
    #{} = emqx_license_checker:update(License),

    ?assertMatch(
        {ok, #{max_connections := expired}},
        emqx_license_checker:limits()
    ).

t_overexpired_small_client(_Config) ->
    {NowDate, _} = calendar:universal_time(),
    Date100DaysAgo = calendar:gregorian_days_to_date(
        calendar:date_to_gregorian_days(NowDate) - 100
    ),

    License = mk_license(
        [
            "220111",
            "1",
            "0",
            "Foo",
            "contact@foo.com",
            "bar",
            format_date(Date100DaysAgo),
            "1",
            "123"
        ]
    ),
    #{} = emqx_license_checker:update(License),

    ?assertMatch(
        {ok, #{max_connections := expired}},
        emqx_license_checker:limits()
    ).

t_overexpired_medium_client(_Config) ->
    {NowDate, _} = calendar:universal_time(),
    Date100DaysAgo = calendar:gregorian_days_to_date(
        calendar:date_to_gregorian_days(NowDate) - 100
    ),

    License = mk_license(
        [
            "220111",
            "1",
            "1",
            "Foo",
            "contact@foo.com",
            "bar",
            format_date(Date100DaysAgo),
            "1",
            "123"
        ]
    ),
    #{} = emqx_license_checker:update(License),

    ?assertMatch(
        {ok, #{max_connections := 123}},
        emqx_license_checker:limits()
    ).

t_recently_expired_small_client(_Config) ->
    {NowDate, _} = calendar:universal_time(),
    Date10DaysAgo = calendar:gregorian_days_to_date(
        calendar:date_to_gregorian_days(NowDate) - 10
    ),

    License = mk_license(
        [
            "220111",
            "1",
            "0",
            "Foo",
            "contact@foo.com",
            "bar",
            format_date(Date10DaysAgo),
            "1",
            "123"
        ]
    ),
    #{} = emqx_license_checker:update(License),

    ?assertMatch(
        {ok, #{max_connections := 123}},
        emqx_license_checker:limits()
    ).

t_unknown_calls(_Config) ->
    ok = gen_server:cast(emqx_license_checker, some_cast),
    some_msg = erlang:send(emqx_license_checker, some_msg),
    ?assertEqual(unknown, gen_server:call(emqx_license_checker, some_request)).

t_refresh_no_change(Config) when is_list(Config) ->
    {ok, License} = write_test_license(Config, ?FUNCTION_NAME, 1, 111),
    #{} = emqx_license_checker:update(License),
    ?check_trace(
        begin
            ?wait_async_action(
                begin
                    erlang:send(
                        emqx_license_checker,
                        refresh
                    )
                end,
                #{?snk_kind := emqx_license_refresh_no_change},
                1000
            )
        end,
        fun(Trace) ->
            ?assertMatch([_ | _], ?of_kind(emqx_license_refresh_no_change, Trace))
        end
    ).

t_refresh_change(Config) when is_list(Config) ->
    {ok, License} = write_test_license(Config, ?FUNCTION_NAME, 1, 111),
    #{} = emqx_license_checker:update(License),
    {ok, License2} = write_test_license(Config, ?FUNCTION_NAME, 2, 222),
    ?check_trace(
        begin
            ?wait_async_action(
                begin
                    erlang:send(
                        emqx_license_checker,
                        refresh
                    )
                end,
                #{?snk_kind := emqx_license_refresh_changed},
                1000
            )
        end,
        fun(Trace) ->
            ?assertMatch(
                [#{new_license := License2} | _], ?of_kind(emqx_license_refresh_changed, Trace)
            )
        end
    ).

t_refresh_failure(Config) when is_list(Config) ->
    Filename = test_license_file_name(Config, ?FUNCTION_NAME),
    {ok, License} = write_test_license(Config, ?FUNCTION_NAME, 1, 111),
    Summary = emqx_license_parser:summary(License),
    #{} = emqx_license_checker:update(License),
    ok = file:write_file(Filename, <<"invalid license">>),
    ?check_trace(
        begin
            ?wait_async_action(
                begin
                    erlang:send(
                        emqx_license_checker,
                        refresh
                    )
                end,
                #{?snk_kind := emqx_license_refresh_failed},
                1000
            )
        end,
        fun(Trace) ->
            ?assertMatch(
                [#{continue_with_license := Summary} | _],
                ?of_kind(emqx_license_refresh_failed, Trace)
            )
        end
    ).

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

write_test_license(Config, Name, ExpireInDays, Connections) ->
    {NowDate, _} = calendar:universal_time(),
    DateTomorrow = calendar:gregorian_days_to_date(
        calendar:date_to_gregorian_days(NowDate) + ExpireInDays
    ),
    Fields = [
        "220111",
        "1",
        "0",
        "Foo",
        "contact@foo.com",
        "bar",
        format_date(DateTomorrow),
        "1",
        integer_to_list(Connections)
    ],
    FileName = test_license_file_name(Config, Name),
    ok = write_license_file(FileName, Fields),
    emqx_license_parser:parse(<<"file://", FileName/binary>>).

test_license_file_name(Config, Name) ->
    Dir = ?config(data_dir, Config),
    iolist_to_binary(filename:join(Dir, atom_to_list(Name) ++ ".lic")).

write_license_file(FileName, Fields) ->
    EncodedLicense = emqx_license_test_lib:make_license(Fields),
    ok = filelib:ensure_dir(FileName),
    ok = file:write_file(FileName, EncodedLicense).

mk_license(Fields) ->
    EncodedLicense = emqx_license_test_lib:make_license(Fields),
    {ok, License} = emqx_license_parser:parse(
        EncodedLicense,
        emqx_license_test_lib:public_key_pem()
    ),
    License.

format_date({Year, Month, Day}) ->
    lists:flatten(
        io_lib:format(
            "~4..0w~2..0w~2..0w",
            [Year, Month, Day]
        )
    ).
