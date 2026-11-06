%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_jt808_ssrf_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_jt808.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(CACHE_KEY, {emqx_utils_ssrf, cache}).
-define(REGISTRY_URL, <<"http://127.0.0.1:8991/jt808/registry">>).
-define(AUTH_URL, <<"http://127.0.0.1:8991/jt808/auth">>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_gateway
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    persistent_term:erase(?CACHE_KEY),
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(_Case, Config) ->
    set_ssrf_deny([<<"169.254.0.0/16">>]),
    Config.

end_per_testcase(_Case, _Config) ->
    persistent_term:erase(?CACHE_KEY),
    _ = emqx_gateway_conf:unload_gateway(jt808),
    ok.

t_registry_url_denied_rejected(_Config) ->
    Conf = jt808_config(
        #{<<"registry">> => <<"http://169.254.169.254:8991/jt808/registry">>}
    ),
    ?assertMatch(
        {error, #{kind := validation_error}},
        emqx_gateway_conf:load_gateway(jt808, Conf)
    ).

t_authentication_url_denied_rejected(_Config) ->
    Conf = jt808_config(
        #{<<"authentication">> => <<"http://169.254.169.254:8991/jt808/auth">>}
    ),
    ?assertMatch(
        {error, #{kind := validation_error}},
        emqx_gateway_conf:load_gateway(jt808, Conf)
    ).

t_allowed_url_accepted(_Config) ->
    Conf = jt808_config(
        #{
            <<"registry">> => <<"http://8.8.8.8:8991/jt808/registry">>,
            <<"authentication">> => <<"http://8.8.8.8:8991/jt808/auth">>
        }
    ),
    {ok, _} = emqx_gateway_conf:load_gateway(jt808, Conf).

t_bad_scheme_rejected(_Config) ->
    Conf = jt808_config(#{<<"registry">> => <<"abc://abc">>}),
    ?assertMatch(
        {error, #{kind := validation_error}},
        emqx_gateway_conf:load_gateway(jt808, Conf)
    ).

t_bad_url_format_rejected(_Config) ->
    Conf = jt808_config(#{<<"authentication">> => <<"not a url">>}),
    ?assertMatch(
        {error, #{kind := validation_error}},
        emqx_gateway_conf:load_gateway(jt808, Conf)
    ).

t_anonymous_without_urls_accepted(_Config) ->
    Conf = jt808_config(
        #{
            <<"allow_anonymous">> => true,
            <<"registry">> => undefined,
            <<"authentication">> => undefined
        }
    ),
    {ok, _} = emqx_gateway_conf:load_gateway(jt808, Conf).

t_autoredirect_disabled(_Config) ->
    Self = self(),
    meck:new(httpc, [passthrough, no_link]),
    meck:expect(
        httpc,
        request,
        fun(_Method, _Req, HttpOpts, _Opts) ->
            Self ! {http_opts, HttpOpts},
            {ok, {{"HTTP/1.1", 200, "OK"}, [], <<"{\"code\":0,\"authcode\":\"123\"}">>}}
        end
    ),
    try
        Auth = #auth{
            allow_anonymous = false,
            registry = ?REGISTRY_URL,
            authentication = ?AUTH_URL
        },
        RegFrame = #{
            <<"header">> => #{<<"phone">> => <<"000123456789">>},
            <<"body">> => #{}
        },
        ?assertEqual({ok, <<"123">>}, emqx_jt808_auth:register(RegFrame, Auth)),
        receive
            {http_opts, HttpOpts} ->
                ?assertEqual(false, proplists:get_value(autoredirect, HttpOpts))
        after 2000 ->
            ct:fail(no_http_request_made)
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

jt808_config(AuthOverrides) ->
    Auth =
        maps:merge(
            #{
                <<"allow_anonymous">> => false,
                <<"registry">> => ?REGISTRY_URL,
                <<"authentication">> => ?AUTH_URL
            },
            maps:filter(fun(_K, V) -> V =/= undefined end, AuthOverrides)
        ),
    base_config(#{<<"proto">> => #{<<"auth">> => Auth}}).

base_config(Overrides) ->
    maps:merge(
        #{
            <<"enable">> => true,
            <<"enable_stats">> => true,
            <<"frame">> => #{<<"max_length">> => 8192},
            <<"idle_timeout">> => <<"30s">>,
            <<"max_retry_times">> => 3,
            <<"message_queue_len">> => 10,
            <<"mountpoint">> => <<"jt808/${clientid}/">>,
            <<"proto">> => #{
                <<"dn_topic">> => <<"${phone}/dn">>,
                <<"up_topic">> => <<"${phone}/up">>
            },
            <<"retry_interval">> => <<"8s">>
        },
        Overrides
    ).
