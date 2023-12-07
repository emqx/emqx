%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------
-module(emqx_otel_api_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(OTEL_API_PATH, emqx_mgmt_api_test_util:api_path(["opentelemetry"])).
-define(CONF_PATH, [opentelemetry]).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    %% This is called by emqx_machine in EMQX release
    emqx_otel_app:configure_otel_deps(),
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx_management,
            {emqx_dashboard, "dashboard.listeners.http { enable = true, bind = 18083 }"},
            emqx_opentelemetry
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    Auth = auth_header(),
    [{suite_apps, Apps}, {auth, Auth} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)),
    emqx_config:delete_override_conf_files(),
    ok.

init_per_testcase(_TC, Config) ->
    emqx_conf:update(
        ?CONF_PATH,
        #{
            <<"traces">> => #{<<"enable">> => false},
            <<"metrics">> => #{<<"enable">> => false},
            <<"logs">> => #{<<"enable">> => false}
        },
        #{}
    ),
    Config.

end_per_testcase(_TC, _Config) ->
    ok.

auth_header() ->
    {ok, API} = emqx_common_test_http:create_default_app(),
    emqx_common_test_http:auth_header(API).

t_get(Config) ->
    Auth = ?config(auth, Config),
    Path = ?OTEL_API_PATH,
    {ok, Resp} = emqx_mgmt_api_test_util:request_api(get, Path, Auth),
    ?assertMatch(
        #{
            <<"traces">> := #{<<"enable">> := false},
            <<"metrics">> := #{<<"enable">> := false},
            <<"logs">> := #{<<"enable">> := false}
        },
        emqx_utils_json:decode(Resp)
    ).

t_put_enable_disable(Config) ->
    Auth = ?config(auth, Config),
    Path = ?OTEL_API_PATH,
    EnableAllReq = #{
        <<"traces">> => #{<<"enable">> => true},
        <<"metrics">> => #{<<"enable">> => true},
        <<"logs">> => #{<<"enable">> => true}
    },
    ?assertMatch({ok, _}, emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, EnableAllReq)),
    ?assertMatch(
        #{
            traces := #{enable := true},
            metrics := #{enable := true},
            logs := #{enable := true}
        },
        emqx:get_config(?CONF_PATH)
    ),

    DisableAllReq = #{
        <<"traces">> => #{<<"enable">> => false},
        <<"metrics">> => #{<<"enable">> => false},
        <<"logs">> => #{<<"enable">> => false}
    },
    ?assertMatch({ok, _}, emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, DisableAllReq)),
    ?assertMatch(
        #{
            traces := #{enable := false},
            metrics := #{enable := false},
            logs := #{enable := false}
        },
        emqx:get_config(?CONF_PATH)
    ).

t_put_invalid(Config) ->
    Auth = ?config(auth, Config),
    Path = ?OTEL_API_PATH,

    ?assertMatch(
        {error, {_, 400, _}},
        emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, #{
            <<"exporter">> => #{<<"endpoint">> => <<>>}
        })
    ),
    ?assertMatch(
        {error, {_, 400, _}},
        emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, #{
            <<"exporter">> => #{<<"endpoint">> => <<"unknown://somehost.org">>}
        })
    ),
    ?assertMatch(
        {error, {_, 400, _}},
        emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, #{
            <<"exporter">> => #{<<"endpoint">> => <<"https://somehost.org:99999">>}
        })
    ),
    ?assertMatch(
        {error, {_, 400, _}},
        emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, #{
            <<"exporter">> => #{<<"endpoint">> => <<"https://somehost.org:99999">>}
        })
    ),
    ?assertMatch(
        {error, {_, 400, _}},
        emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, #{
            <<"exporter">> => #{<<"unknown_field">> => <<"foo">>}
        })
    ),
    ?assertMatch(
        {error, {_, 400, _}},
        emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, #{
            <<"exporter">> => #{<<"protocol">> => <<"unknown">>}
        })
    ),
    ?assertMatch(
        {error, {_, 400, _}},
        emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, #{
            <<"traces">> => #{<<"filter">> => #{<<"unknown_filter">> => <<"foo">>}}
        })
    ),
    ?assertMatch(
        {error, {_, 400, _}},
        emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, #{
            <<"logs">> => #{<<"level">> => <<"foo">>}
        })
    ),
    ?assertMatch(
        {error, {_, 400, _}},
        emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, #{
            <<"metrics">> => #{<<"interval">> => <<"foo">>}
        })
    ),
    ?assertMatch(
        {error, {_, 400, _}},
        emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, #{
            <<"logs">> => #{<<"unknown_field">> => <<"foo">>}
        })
    ),
    ?assertMatch(
        {error, {_, 400, _}},
        emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, #{<<"unknown_field">> => <<"foo">>})
    ).

t_put_valid(Config) ->
    Auth = ?config(auth, Config),
    Path = ?OTEL_API_PATH,

    ?assertMatch(
        {ok, _},
        emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, #{
            <<"exporter">> => #{<<"endpoint">> => <<"nohost.com">>}
        })
    ),
    ?assertEqual(<<"http://nohost.com/">>, emqx:get_config(?CONF_PATH ++ [exporter, endpoint])),

    ?assertMatch(
        {ok, _}, emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, #{<<"exporter">> => #{}})
    ),
    ?assertMatch({ok, _}, emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, #{})),
    ?assertMatch(
        {ok, _}, emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, #{<<"traces">> => #{}})
    ),
    ?assertMatch(
        {ok, _}, emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, #{<<"logs">> => #{}})
    ),
    ?assertMatch(
        {ok, _}, emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, #{<<"metrics">> => #{}})
    ),
    ?assertMatch(
        {ok, _},
        emqx_mgmt_api_test_util:request_api(
            put,
            Path,
            "",
            Auth,
            #{<<"exporter">> => #{}, <<"traces">> => #{}, <<"logs">> => #{}, <<"metrics">> => #{}}
        )
    ),
    ?assertMatch(
        {ok, _},
        emqx_mgmt_api_test_util:request_api(
            put,
            Path,
            "",
            Auth,
            #{
                <<"exporter">> => #{
                    <<"endpoint">> => <<"https://localhost:4317">>, <<"protocol">> => <<"grpc">>
                },
                <<"traces">> => #{
                    <<"enable">> => true,
                    <<"max_queue_size">> => 10,
                    <<"exporting_timeout">> => <<"10s">>,
                    <<"scheduled_delay">> => <<"20s">>,
                    <<"filter">> => #{<<"trace_all">> => true}
                },
                <<"logs">> => #{
                    <<"level">> => <<"warning">>,
                    <<"max_queue_size">> => 100,
                    <<"exporting_timeout">> => <<"10s">>,
                    <<"scheduled_delay">> => <<"1s">>
                },
                <<"metrics">> => #{
                    %% alias for "interval"
                    <<"scheduled_delay">> => <<"15321ms">>
                }
            }
        ),
        %% alias check
        ?assertEqual(15_321, emqx:get_config(?CONF_PATH ++ [metrics, interval]))
    ).
