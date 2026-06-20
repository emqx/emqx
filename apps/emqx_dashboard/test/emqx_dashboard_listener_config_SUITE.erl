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
    emqx_common_test_helpers:all(?MODULE).

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
    emqx_common_test_helpers:clear_security_profile(),
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

t_http_default_bind_security_profile(_Config) ->
    ok = assert_http_default_bind("legacy", false, 18083, inet),
    ok = assert_http_default_bind("legacy", true, 18083, inet6),
    ok = assert_http_default_bind("hardened", false, {{127, 0, 0, 1}, 18083}, inet),
    ok = assert_http_default_bind("hardened", true, {{0, 0, 0, 0, 0, 0, 0, 1}, 18083}, inet6).

change_i18n_lang(Lang) ->
    {ok, _} = emqx_conf:update([dashboard], {change_i18n_lang, Lang}, #{}),
    ok.

assert_http_default_bind(Profile, Inet6, ExpectedBind, ExpectedInetOpt) ->
    emqx_common_test_helpers:set_security_profile(Profile),
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
