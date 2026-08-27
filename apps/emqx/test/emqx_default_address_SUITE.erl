%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_default_address_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(GROUP_CASES, [t_default_binds, t_explicit_bind_not_rewritten]).

all() ->
    All = emqx_common_test_helpers:all(?MODULE),
    [
        {group, legacy},
        {group, hardened}
    ] ++ (All -- ?GROUP_CASES).

groups() ->
    [
        {legacy, [], ?GROUP_CASES},
        {hardened, [], ?GROUP_CASES}
    ].

init_per_suite(Config) ->
    emqx_common_test_helpers:clear_security_profile(),
    emqx_common_test_helpers:clear_default_address(),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:clear_security_profile(),
    emqx_common_test_helpers:clear_default_address().

init_per_group(Profile, Config) when Profile =:= legacy; Profile =:= hardened ->
    emqx_common_test_helpers:set_security_profile(Profile),
    Apps = emqx_cth_suite:start(
        [emqx],
        #{work_dir => emqx_cth_suite:work_dir(Profile, Config)}
    ),
    [{apps, Apps}, {security_profile, Profile} | Config].

end_per_group(_Profile, Config) ->
    emqx_cth_suite:stop(?config(apps, Config)),
    emqx_common_test_helpers:clear_security_profile().

-doc """
Asserts the resolved default MQTT listener binds for every address value,
on both the schema-default path and the bare-port converter path, under
the group's security profile.
""".
t_default_binds(Config) ->
    Profile = ?config(security_profile, Config),
    lists:foreach(
        fun({Address, Expected}) ->
            ct:pal("address ~p, expected ~p", [Address, Expected]),
            with_address(Address, fun() -> assert_all_paths(Expected) end)
        end,
        address_cases(Profile)
    ).

-doc """
Asserts that an explicit "IP:port" bind is never rewritten by the default
address, while sibling bare-port binds in the same update are.
""".
t_explicit_bind_not_rewritten(_Config) ->
    with_address("loopback", fun() ->
        {ok, _} = emqx:update_config([listeners], #{
            <<"tcp">> => #{<<"default">> => #{<<"bind">> => <<"127.0.0.3:1883">>}},
            <<"ssl">> => #{<<"default">> => #{<<"bind">> => 8883}},
            <<"ws">> => #{<<"default">> => #{<<"bind">> => 8083}},
            <<"wss">> => #{<<"default">> => #{<<"bind">> => 8084}}
        }),
        ?assertEqual(
            {{127, 0, 0, 3}, 1883}, emqx:get_config([listeners, tcp, default, bind])
        ),
        ?assertEqual(
            {{127, 0, 0, 1}, 8883}, emqx:get_config([listeners, ssl, default, bind])
        ),
        ?assertEqual(
            {{127, 0, 0, 1}, 8083}, emqx:get_config([listeners, ws, default, bind])
        ),
        ?assertEqual(
            {{127, 0, 0, 1}, 8084}, emqx:get_config([listeners, wss, default, bind])
        )
    end).

-doc """
Asserts that each keyword and literal value resolves to the expected
address, for both the mqtt and the dashboard scope.
""".
t_resolver_values(_Config) ->
    lists:foreach(
        fun({Address, Expected}) ->
            with_address(Address, fun() ->
                ?assertEqual(Expected, emqx_default_address:resolve(mqtt)),
                ?assertEqual(Expected, emqx_default_address:resolve(dashboard))
            end)
        end,
        [
            {"loopback", loopback},
            {"all", {0, 0, 0, 0}},
            {"192.0.2.7", {192, 0, 2, 7}},
            {"::1", {0, 0, 0, 0, 0, 0, 0, 1}},
            {"::", {0, 0, 0, 0, 0, 0, 0, 0}},
            {"hostname_i", hostname_address()}
        ]
    ).

-doc """
Asserts that with the variable unset the resolver falls back to the
security profile policy.
""".
t_resolver_profile_fallback(_Config) ->
    emqx_common_test_helpers:clear_default_address(),
    ?assertEqual(any, emqx_default_address:resolve(mqtt)),
    ?assertEqual(any, emqx_default_address:resolve(dashboard)),
    emqx_common_test_helpers:with_security_profile(hardened, fun() ->
        emqx_default_address:clear(),
        ?assertEqual(loopback, emqx_default_address:resolve(mqtt)),
        ?assertEqual(loopback, emqx_default_address:resolve(dashboard))
    end),
    emqx_default_address:clear().

-doc """
Asserts that a value that is neither a keyword nor a literal address makes
the resolver exit.
""".
t_resolver_invalid(_Config) ->
    lists:foreach(
        fun(Address) ->
            with_address(Address, fun() ->
                ?assertExit(
                    {invalid_default_address, _}, emqx_default_address:resolve(mqtt)
                )
            end)
        end,
        ["bogus", "999.1.1.1", "LOOPBACK", "0.0.0.0:1883"]
    ).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

with_address(unset, Fun) ->
    emqx_common_test_helpers:clear_default_address(),
    Fun();
with_address(Address, Fun) ->
    emqx_common_test_helpers:with_default_address(Address, Fun).

address_cases(Profile) ->
    [
        {unset, profile_address(Profile)},
        {"loopback", {127, 0, 0, 1}},
        {"all", {0, 0, 0, 0}},
        {"127.0.0.2", {127, 0, 0, 2}},
        {"::", {0, 0, 0, 0, 0, 0, 0, 0}},
        {"hostname_i", hostname_address()}
    ].

profile_address(legacy) -> any;
profile_address(hardened) -> {127, 0, 0, 1}.

hostname_address() ->
    {ok, Hostname} = inet:gethostname(),
    case inet:getaddrs(Hostname, inet) of
        {ok, [IP | _]} ->
            IP;
        {error, _} ->
            {ok, [IP | _]} = inet:getaddrs(Hostname, inet6),
            IP
    end.

assert_all_paths(Expected) ->
    %% Schema-eval path: absent listeners get the full default config.
    {ok, _} = emqx:update_config([listeners], #{}),
    assert_default_binds(Expected, full),
    %% Schema-eval path: present but empty listeners get the field default.
    {ok, _} = emqx:update_config([listeners], #{
        <<"tcp">> => #{<<"default">> => #{}},
        <<"ssl">> => #{<<"default">> => #{}},
        <<"ws">> => #{<<"default">> => #{}},
        <<"wss">> => #{<<"default">> => #{}}
    }),
    assert_default_binds(Expected, schema),
    %% Bare-port converter path.
    {ok, _} = emqx:update_config([listeners], #{
        <<"tcp">> => #{<<"default">> => #{<<"bind">> => 1883}},
        <<"ssl">> => #{<<"default">> => #{<<"bind">> => 8883}},
        <<"ws">> => #{<<"default">> => #{<<"bind">> => 8083}},
        <<"wss">> => #{<<"default">> => #{<<"bind">> => 8084}}
    }),
    assert_default_binds(Expected, schema).

assert_default_binds(Expected, Source) ->
    ?assertEqual(
        expected_bind(Expected, Source, 1883), emqx:get_config([listeners, tcp, default, bind])
    ),
    ?assertEqual(
        expected_bind(Expected, Source, 8883), emqx:get_config([listeners, ssl, default, bind])
    ),
    ?assertEqual(
        expected_bind(Expected, Source, 8083), emqx:get_config([listeners, ws, default, bind])
    ),
    ?assertEqual(
        expected_bind(Expected, Source, 8084), emqx:get_config([listeners, wss, default, bind])
    ).

expected_bind(any, full, Port) -> {{0, 0, 0, 0}, Port};
expected_bind(any, schema, Port) -> Port;
expected_bind(IP, _Source, Port) -> {IP, Port}.
