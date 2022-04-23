%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    _ = application:load(emqx_conf),
    ok = emqx_common_test_helpers:start_apps([emqx_license], fun set_special_configs/1),
    Config.

end_per_suite(_) ->
    ok = emqx_common_test_helpers:stop_apps([emqx_license]).

init_per_testcase(t_default_limits, Config) ->
    ok = emqx_common_test_helpers:stop_apps([emqx_license]),
    Config;
init_per_testcase(_Case, Config) ->
    {ok, _} = emqx_cluster_rpc:start_link(node(), emqx_cluster_rpc, 1000),
    Config.

end_per_testcase(t_default_limits, _Config) ->
    ok = emqx_common_test_helpers:start_apps([emqx_license], fun set_special_configs/1);
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

t_update_by_timer(_Config) ->
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

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

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
