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
Asserts that the checked config keeps the static schema-default binds and
that the started listeners bind the resolved default address, for every
address value, under the group's security profile.
""".
t_default_binds(Config) ->
    Profile = ?config(security_profile, Config),
    lists:foreach(
        fun({Address, Expected}) ->
            ct:pal("address ~p, expected ~p", [Address, Expected]),
            restart_with_address(Address),
            assert_static_config_binds(),
            assert_effective_binds(Expected)
        end,
        address_cases(Profile)
    ),
    restart_with_address(unset).

-doc """
Asserts that an explicit "IP:port" bind is bound as configured while
sibling bare-port binds get the default address.
""".
t_explicit_bind_not_rewritten(_Config) ->
    restart_with_address("loopback"),
    {ok, _} = emqx:update_config([listeners], #{
        <<"tcp">> => #{<<"default">> => #{<<"bind">> => <<"127.0.0.3:1883">>}},
        <<"ssl">> => #{<<"default">> => #{<<"bind">> => 8883}},
        <<"ws">> => #{<<"default">> => #{<<"bind">> => 8083}},
        <<"wss">> => #{<<"default">> => #{<<"bind">> => 8084}}
    }),
    ?assertEqual(
        {{127, 0, 0, 3}, 1883}, emqx:get_config([listeners, tcp, default, bind])
    ),
    ?assertEqual(8883, emqx:get_config([listeners, ssl, default, bind])),
    ?assertEqual({{127, 0, 0, 3}, 1883}, esockd_listen_on('tcp:default')),
    ?assertEqual({{127, 0, 0, 1}, 8883}, esockd_listen_on('ssl:default')),
    ?assertEqual({{127, 0, 0, 1}, 8083}, ranch:get_addr('ws:default')),
    ?assertEqual({{127, 0, 0, 1}, 8084}, ranch:get_addr('wss:default')),
    {ok, _} = emqx:update_config([listeners], #{
        <<"tcp">> => #{<<"default">> => #{<<"bind">> => 1883}},
        <<"ssl">> => #{<<"default">> => #{<<"bind">> => 8883}},
        <<"ws">> => #{<<"default">> => #{<<"bind">> => 8083}},
        <<"wss">> => #{<<"default">> => #{<<"bind">> => 8084}}
    }),
    restart_with_address(unset).

-doc """
Asserts that each keyword, literal and hostname value resolves to the
expected address, for both the mqtt and the dashboard scope.
""".
t_resolver_values(_Config) ->
    {ok, LocalHost} = inet:gethostname(),
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
            {"nodename", nodename_address()},
            {LocalHost, resolve_host(LocalHost)}
        ]
    ).

-doc """
Asserts that with the config unset the resolver falls back to the
security profile policy, and that the profile does not cover the gateway
scope.
""".
t_resolver_profile_fallback(_Config) ->
    emqx_common_test_helpers:clear_default_address(),
    ?assertEqual(any, emqx_default_address:resolve(mqtt)),
    ?assertEqual(any, emqx_default_address:resolve(dashboard)),
    ?assertEqual(any, emqx_default_address:resolve(gateway)),
    emqx_common_test_helpers:with_security_profile(hardened, fun() ->
        emqx_default_address:clear(),
        ?assertEqual(loopback, emqx_default_address:resolve(mqtt)),
        ?assertEqual(loopback, emqx_default_address:resolve(dashboard)),
        ?assertEqual(any, emqx_default_address:resolve(gateway))
    end),
    emqx_default_address:clear().

-doc """
Asserts that listen_on/2 applies the address to bare-port binds only and
returns explicit binds unchanged.
""".
t_listen_on(_Config) ->
    emqx_common_test_helpers:clear_default_address(),
    ?assertEqual(1883, emqx_default_address:listen_on(mqtt, 1883)),
    ?assertEqual(1883, emqx_default_address:listen_on(gateway, 1883)),
    with_address("loopback", fun() ->
        ?assertEqual({{127, 0, 0, 1}, 1883}, emqx_default_address:listen_on(mqtt, 1883)),
        ?assertEqual({{127, 0, 0, 1}, 1883}, emqx_default_address:listen_on(gateway, 1883)),
        ?assertEqual(
            {{1, 2, 3, 4}, 1883}, emqx_default_address:listen_on(mqtt, {{1, 2, 3, 4}, 1883})
        )
    end),
    with_address("192.0.2.7", fun() ->
        ?assertEqual({{192, 0, 2, 7}, 1883}, emqx_default_address:listen_on(mqtt, 1883))
    end),
    emqx_common_test_helpers:with_security_profile(hardened, fun() ->
        emqx_default_address:clear(),
        ?assertEqual({{127, 0, 0, 1}, 1883}, emqx_default_address:listen_on(mqtt, 1883)),
        %% The security profile does not cover gateway binds.
        ?assertEqual(1883, emqx_default_address:listen_on(gateway, 1883))
    end),
    emqx_default_address:clear().

-doc """
Asserts that a value that is neither a keyword, a literal address, nor a
syntactically valid hostname makes the resolver exit.
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
        ["0.0.0.0:1883", "-bad", "bad-", "under_score"]
    ).

-doc """
Asserts that a syntactically valid hostname that does not resolve makes
the resolver exit. The .invalid TLD is reserved and never resolves.
""".
t_resolver_unresolvable_hostname(_Config) ->
    with_address("host.invalid", fun() ->
        ?assertExit(
            {invalid_default_address, _}, emqx_default_address:resolve(mqtt)
        )
    end).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

with_address(Address, Fun) ->
    emqx_common_test_helpers:with_default_address(Address, Fun).

address_cases(Profile) ->
    [
        {unset, profile_address(Profile)},
        {"loopback", {127, 0, 0, 1}},
        {"all", {0, 0, 0, 0}},
        {"127.0.0.2", {127, 0, 0, 2}},
        {"::", {0, 0, 0, 0, 0, 0, 0, 0}},
        {"nodename", nodename_address()}
    ].

profile_address(legacy) -> any;
profile_address(hardened) -> {127, 0, 0, 1}.

nodename_address() ->
    [_Name, Host] = string:split(atom_to_list(node()), "@"),
    case inet:parse_address(Host) of
        {ok, IP} ->
            IP;
        {error, _} ->
            resolve_host(Host)
    end.

resolve_host(Host) ->
    case inet:getaddrs(Host, inet) of
        {ok, [IP | _]} ->
            IP;
        {error, _} ->
            {ok, [IP | _]} = inet:getaddrs(Host, inet6),
            IP
    end.

listener_ids() ->
    ['tcp:default', 'ssl:default', 'ws:default', 'wss:default'].

restart_with_address(Address) ->
    %% The resolved address is fixed for a node's lifetime; simulate a
    %% reboot by stopping the listeners under the current resolution first.
    lists:foreach(
        fun(Id) -> ok = emqx_listeners:stop_listener(Id) end, listener_ids()
    ),
    case Address of
        unset -> emqx_common_test_helpers:clear_default_address();
        _ -> emqx_common_test_helpers:set_default_address(Address)
    end,
    lists:foreach(
        fun(Id) -> ok = emqx_listeners:start_listener(Id) end, listener_ids()
    ).

assert_static_config_binds() ->
    ?assertEqual(1883, emqx:get_config([listeners, tcp, default, bind])),
    ?assertEqual(8883, emqx:get_config([listeners, ssl, default, bind])),
    ?assertEqual(8083, emqx:get_config([listeners, ws, default, bind])),
    ?assertEqual(8084, emqx:get_config([listeners, wss, default, bind])).

assert_effective_binds(Expected) ->
    ?assertEqual(expected_listen_on(Expected, 1883), esockd_listen_on('tcp:default')),
    ?assertEqual(expected_listen_on(Expected, 8883), esockd_listen_on('ssl:default')),
    ?assertEqual(expected_ranch_addr(Expected, 8083), ranch:get_addr('ws:default')),
    ?assertEqual(expected_ranch_addr(Expected, 8084), ranch:get_addr('wss:default')).

expected_listen_on(any, Port) -> Port;
expected_listen_on(IP, Port) -> {IP, Port}.

expected_ranch_addr(any, Port) -> {{0, 0, 0, 0}, Port};
expected_ranch_addr(IP, Port) -> {IP, Port}.

esockd_listen_on(Id) ->
    [ListenOn] = [L || {{I, L}, _Pid} <- esockd:listeners(), I =:= Id],
    ListenOn.
