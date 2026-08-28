%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_dashboard_listener_config_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    [t_change_i18n_lang, {group, legacy}, {group, hardened}].

groups() ->
    [
        {legacy, [], [t_http_default_bind_security_profile, t_http_default_bind_default_address]},
        {hardened, [], [t_http_default_bind_security_profile, t_http_default_bind_default_address]}
    ].

init_per_suite(Config) ->
    emqx_common_test_helpers:clear_security_profile(),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:clear_security_profile().

init_per_group(Profile, Config) when Profile =:= legacy; Profile =:= hardened ->
    emqx_common_test_helpers:set_security_profile(Profile),
    [{security_profile, Profile} | Config].

end_per_group(_Profile, _Config) ->
    emqx_common_test_helpers:clear_security_profile().

init_per_testcase(TestCase, Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(TestCase, Config)}
    ),
    [{apps, Apps} | Config].

end_per_testcase(_TestCase, Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

t_change_i18n_lang(_Config) ->
    ?check_trace(
        {_, {ok, _}} = ?wait_async_action(
            change_i18n_lang(zh),
            #{?snk_kind := regenerate_dispatch, i18n_lang := zh},
            10_000
        ),
        []
    ).

t_http_default_bind_security_profile(Config) ->
    Profile = ?config(security_profile, Config),
    ok = assert_http_default_bind(false, expected_http_bind(Profile, false), inet),
    ok = assert_http_default_bind(true, expected_http_bind(Profile, true), inet6).

-doc """
Asserts that node.default_listener_address overrides the security profile for the
bare-port dashboard bind, that the loopback keyword keeps the inet6-aware
loopback resolution, and that an IPv6 address implies inet6 in the socket
options.
""".
t_http_default_bind_default_address(_Config) ->
    with_address("loopback", fun() ->
        ok = assert_http_default_bind(false, {{127, 0, 0, 1}, 18083}, inet),
        ok = assert_http_default_bind(true, {{0, 0, 0, 0, 0, 0, 0, 1}, 18083}, inet6)
    end),
    %% `all' keeps the bare port, which binds every interface.
    with_address("all", fun() ->
        ok = assert_http_default_bind(false, 18083, inet)
    end),
    with_address("127.0.0.2", fun() ->
        ok = assert_http_default_bind(false, {{127, 0, 0, 2}, 18083}, inet)
    end),
    with_address("::1", fun() ->
        ok = assert_http_default_bind(false, {{0, 0, 0, 0, 0, 0, 0, 1}, 18083}, inet6)
    end).

with_address(Address, Fun) ->
    emqx_common_test_helpers:with_default_address(Address, Fun).

change_i18n_lang(Lang) ->
    {ok, _} = emqx_conf:update([dashboard], {change_i18n_lang, Lang}, #{}),
    ok.

assert_http_default_bind(Inet6, ExpectedBind, ExpectedInetOpt) ->
    {ok, _} = emqx:update_config([dashboard, listeners], #{
        <<"http">> => #{
            <<"enable">> => true,
            <<"bind">> => 18083,
            <<"inet6">> => Inet6,
            <<"ipv6_v6only">> => false
        }
    }),
    [Listener] = emqx_dashboard:list_listeners(),
    ?assertMatch(
        {'http:dashboard', http, ExpectedBind, _RanchOpts, _ProtoOpts},
        Listener
    ),
    {'http:dashboard', http, ExpectedBind, RanchOpts, _ProtoOpts} = Listener,
    SocketOpts = maps:get(socket_opts, RanchOpts),
    ?assert(lists:member(ExpectedInetOpt, SocketOpts)),
    ?assertEqual(false, lists:keymember(bind, 1, SocketOpts)),
    ok.

expected_http_bind(legacy, _Inet6) -> 18083;
expected_http_bind(hardened, false) -> {{127, 0, 0, 1}, 18083};
expected_http_bind(hardened, true) -> {{0, 0, 0, 0, 0, 0, 0, 1}, 18083}.
