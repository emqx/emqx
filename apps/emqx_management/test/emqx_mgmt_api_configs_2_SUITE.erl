%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mgmt_api_configs_2_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    _ = application:load(emqx),
    %% keep it the same as the dashboard defaults in apps/emqx_conf/etc/base.hocon
    Conf =
        [
            "dashboard.listeners.http { enable = true, bind = 18083 }",
            "dashboard.listeners.https { bind = 0 }\n"
        ],
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard(Conf)
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, Config) ->
    Config.

t_dashboard(_Config) ->
    {ok, Dashboard = #{<<"listeners">> := Listeners}} = get_config("dashboard"),
    Https1 = #{enable => true, bind => 18084},
    %% Ensure HTTPS listener can be enabled with just changing bind to a non-zero number
    %% i.e. the default certs should work
    ?assertMatch(
        {ok, _},
        update_config("dashboard", Dashboard#{<<"listeners">> => Listeners#{<<"https">> => Https1}})
    ),

    Https2 = #{
        <<"bind">> => 18084,
        <<"ssl_options">> =>
            #{
                %% Deliberately absent: the case asserts the update is refused.
                <<"keyfile">> => "/nonexistent/badkey.pem",
                <<"cacertfile">> => "/nonexistent/badcacert.pem",
                <<"certfile">> => "/nonexistent/badcert.pem"
            }
    },
    Dashboard2 = Dashboard#{<<"listeners">> => Listeners#{<<"https">> => Https2}},
    ?assertMatch(
        {error, {"HTTP/1.1", 400, _}},
        update_config("dashboard", Dashboard2)
    ),

    FilePath = fun(Name) ->
        iolist_to_binary(
            emqx_common_test_helpers:test_cert(Name)
        )
    end,
    KeyFile = FilePath("key.pem"),
    CertFile = FilePath("cert.pem"),
    CacertFile = FilePath("cacert.pem"),
    Https3 = #{
        <<"bind">> => 18084,
        <<"ssl_options">> => #{
            <<"keyfile">> => KeyFile,
            <<"cacertfile">> => CacertFile,
            <<"certfile">> => CertFile
        }
    },
    Dashboard3 = Dashboard#{<<"listeners">> => Listeners#{<<"https">> => Https3}},
    ?assertMatch({ok, _}, update_config("dashboard", Dashboard3)),

    Dashboard4 = Dashboard#{<<"listeners">> => Listeners#{<<"https">> => #{<<"bind">> => 0}}},
    ?assertMatch({ok, _}, update_config("dashboard", Dashboard4)),
    {ok, Dashboard41} = get_config("dashboard"),
    ?assertMatch(
        #{
            <<"bind">> := 0,
            <<"ssl_options">> :=
                #{
                    <<"keyfile">> := KeyFile,
                    <<"cacertfile">> := CacertFile,
                    <<"certfile">> := CertFile
                }
        },
        read_conf([<<"dashboard">>, <<"listeners">>, <<"https">>]),
        Dashboard41
    ),

    ?assertMatch({ok, _}, update_config("dashboard", Dashboard)),
    {ok, Dashboard1} = get_config("dashboard"),
    %% The update API deep-merges, and a deep merge cannot drop keys: the
    %% certificate files set earlier in this case survive putting the original
    %% config back.  Nothing defaults them, so the original carries none of them.
    ?assertEqual(without_https_cert_files(Dashboard), without_https_cert_files(Dashboard1)),
    timer:sleep(1500),
    ok.

%% Helpers

without_https_cert_files(Conf) ->
    lists:foldl(
        fun(Key, Acc) ->
            emqx_utils_maps:deep_remove(
                [<<"listeners">>, <<"https">>, <<"ssl_options">>, Key], Acc
            )
        end,
        Conf,
        [<<"certfile">>, <<"keyfile">>, <<"cacertfile">>]
    ).

get_config(Name) ->
    Path = emqx_mgmt_api_test_util:api_path(["configs", Name]),
    case emqx_mgmt_api_test_util:request_api(get, Path) of
        {ok, Res} ->
            {ok, emqx_utils_json:decode(Res)};
        Error ->
            Error
    end.

update_config(Name, Change) ->
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    UpdatePath = emqx_mgmt_api_test_util:api_path(["configs", Name]),
    case emqx_mgmt_api_test_util:request_api(put, UpdatePath, "", AuthHeader, Change) of
        {ok, Update} -> {ok, emqx_utils_json:decode(Update)};
        Error -> Error
    end.

read_conf(RootKeys) when is_list(RootKeys) ->
    case emqx_config:read_override_conf(#{override_to => cluster}) of
        undefined -> undefined;
        Conf -> emqx_utils_maps:deep_get(RootKeys, Conf, undefined)
    end;
read_conf(RootKey) ->
    read_conf([RootKey]).
