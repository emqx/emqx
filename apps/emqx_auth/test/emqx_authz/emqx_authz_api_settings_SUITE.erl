%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz_api_settings_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-import(emqx_mgmt_api_test_util, [request/3, uri/1]).

-include_lib("emqx/include/emqx.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx_conf, #{
                config => #{
                    authorization =>
                        #{
                            cache => #{enable => true},
                            no_match => allow,
                            sources => []
                        }
                }
            }},
            emqx_auth,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_api(_) ->
    Settings1Put = #{
        <<"no_match">> => <<"deny">>,
        <<"deny_action">> => <<"disconnect">>,
        <<"cache">> => #{
            <<"enable">> => false,
            <<"max_size">> => 32,
            <<"ttl">> => <<"60s">>,
            <<"excludes">> => [<<"nocache/#">>]
        }
    },
    Settings1Get = Settings1Put,

    {ok, 200, Result1} = request(put, uri(["authorization", "settings"]), Settings1Put),
    {ok, 200, Result1} = request(get, uri(["authorization", "settings"]), []),
    ?assertEqual(Settings1Get, emqx_utils_json:decode(Result1)),

    #{<<"cache">> := Cache} =
        Settings2Put = #{
            <<"no_match">> => <<"allow">>,
            <<"deny_action">> => <<"ignore">>,
            <<"cache">> => #{
                <<"enable">> => true,
                <<"max_size">> => 32,
                <<"ttl">> => <<"60s">>
            }
        },

    Settings2Get = Settings2Put#{
        <<"cache">> := Cache#{<<"excludes">> => []}
    },

    {ok, 200, Result2} = request(put, uri(["authorization", "settings"]), Settings2Put),
    {ok, 200, Result2} = request(get, uri(["authorization", "settings"]), []),
    ?assertEqual(Settings2Get, emqx_utils_json:decode(Result2)),

    ok.

stop_apps(Apps) ->
    lists:foreach(fun application:stop/1, Apps).
