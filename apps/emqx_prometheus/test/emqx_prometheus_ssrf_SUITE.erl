%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_prometheus_ssrf_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(CACHE_KEY, {emqx_utils_ssrf, cache}).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_rule_engine,
            emqx_management,
            {emqx_prometheus, #{start => false}},
            {emqx_license, "license.key = default"},
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    persistent_term:erase(?CACHE_KEY),
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(_Case, Config) ->
    _ = emqx_cth_suite:start_app(
        emqx_prometheus,
        #{config => emqx_prometheus_SUITE:config(default)}
    ),
    set_ssrf_deny([<<"169.254.0.0/16">>]),
    Config.

end_per_testcase(_Case, _Config) ->
    persistent_term:erase(?CACHE_KEY),
    _ = emqx_cth_suite:stop_apps([emqx_prometheus]),
    ok.

t_push_gateway_denied_url_rejected(_Config) ->
    Conf = push_gateway_conf(<<"http://169.254.169.254:9091/metrics">>),
    ?assertMatch({error, {"HTTP/1.1", 400, _}}, put_prometheus_conf(Conf)).

t_push_gateway_disabled_default_url_accepted(_Config) ->
    %% SSRF policy is enabled and denies loopback, but the push gateway stays
    %% disabled with its untouched default URL: no outbound request can be
    %% made, so the config must be accepted.
    set_ssrf_deny([<<"127.0.0.0/8">>]),
    {ok, Response} = get_prometheus_conf(),
    #{<<"push_gateway">> := PushGateway} = Conf = emqx_utils_json:decode(Response),
    DisableConf = Conf#{<<"push_gateway">> => PushGateway#{<<"enable">> => false}},
    {ok, _} = put_prometheus_conf(DisableConf).

t_push_gateway_enabled_default_url_rejected(_Config) ->
    %% Once the push gateway is enabled its URL becomes a live outbound
    %% target; the default loopback URL must not bypass the SSRF policy.
    set_ssrf_deny([<<"127.0.0.0/8">>]),
    {ok, Response} = get_prometheus_conf(),
    #{<<"push_gateway">> := PushGateway} = Conf = emqx_utils_json:decode(Response),
    EnableConf = Conf#{<<"push_gateway">> => PushGateway#{<<"enable">> => true}},
    ?assertMatch({error, {"HTTP/1.1", 400, _}}, put_prometheus_conf(EnableConf)).

t_legacy_push_gateway_server_denied_rejected(_Config) ->
    Conf = legacy_conf(true, <<"http://169.254.169.254:9091">>),
    ?assertMatch({error, {"HTTP/1.1", 400, _}}, put_prometheus_conf(Conf)).

t_legacy_enabled_default_url_rejected(_Config) ->
    set_ssrf_deny([<<"127.0.0.0/8">>]),
    Conf = legacy_conf(true, <<"http://127.0.0.1:9091">>),
    ?assertMatch({error, {"HTTP/1.1", 400, _}}, put_prometheus_conf(Conf)).

t_legacy_disabled_default_url_accepted(_Config) ->
    set_ssrf_deny([<<"127.0.0.0/8">>]),
    Conf = legacy_conf(false, <<"http://127.0.0.1:9091">>),
    {ok, _} = put_prometheus_conf(Conf).

t_allowed_url_accepted(_Config) ->
    Conf = push_gateway_conf(<<"http://8.8.8.8:9091/metrics">>),
    {ok, _} = put_prometheus_conf(Conf).

t_push_httpc_autoredirect_disabled(_Config) ->
    Self = self(),
    meck:new(httpc, [passthrough, no_link]),
    meck:expect(
        httpc,
        request,
        fun(Method, Req = {_Url, _Headers, _ContentType, _Data}, HttpOpts, Opts) ->
            Self ! {http_opts, HttpOpts},
            meck:passthrough([Method, Req, HttpOpts, Opts])
        end
    ),
    try
        Conf = emqx_prometheus_config:conf(),
        ?assertMatch(ok, emqx_prometheus_sup:start_child(emqx_prometheus, Conf)),
        receive
            {http_opts, HttpOpts} ->
                ?assertEqual(false, proplists:get_value(autoredirect, HttpOpts))
        after 5000 ->
            ct:fail(no_push_request_made)
        end
    after
        meck:unload(httpc)
    end.

%%--------------------------------------------------------------------
%% Helpers

set_ssrf_deny(DenyCidrs) ->
    persistent_term:put(
        ?CACHE_KEY,
        #{
            enable => true,
            allow_cidrs => [],
            deny_cidrs => emqx_utils_ssrf:compile_cidrs(DenyCidrs),
            deny_hosts => []
        }
    ).

push_gateway_conf(Url) ->
    {ok, Response} = get_prometheus_conf(),
    #{<<"push_gateway">> := PushGateway} = Conf = emqx_utils_json:decode(Response),
    Conf#{<<"push_gateway">> => PushGateway#{<<"enable">> => true, <<"url">> => Url}}.

%% A minimal legacy-shaped config: at least one legacy-only key so the union
%% schema picks the legacy struct; all other legacy fields fall back to their
%% defaults.
legacy_conf(Enable, Url) ->
    #{
        <<"enable">> => Enable,
        <<"push_gateway_server">> => Url
    }.

get_prometheus_conf() ->
    Auth = emqx_mgmt_api_test_util:auth_header_(),
    emqx_mgmt_api_test_util:request_api(get, api_path(), "", Auth).

put_prometheus_conf(Conf) ->
    Auth = emqx_mgmt_api_test_util:auth_header_(),
    emqx_mgmt_api_test_util:request_api(put, api_path(), "", Auth, Conf).

api_path() ->
    emqx_mgmt_api_test_util:api_path(["prometheus"]).
