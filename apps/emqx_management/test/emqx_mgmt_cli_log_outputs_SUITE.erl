%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mgmt_cli_log_outputs_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    DashboardPort = emqx_common_test_helpers:select_free_port(tcp),
    Apps = emqx_cth_suite:start(
        [
            {emqx_conf, #{config => listeners_conf()}},
            emqx_modules,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard(dashboard_conf(DashboardPort))
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    ok = emqx_mgmt_cli:load(),
    [
        {apps, Apps},
        {api_host, "http://127.0.0.1:" ++ integer_to_list(DashboardPort)}
        | Config
    ].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)).

t_list_tracks_config(_Config) ->
    with_log_conf_restore(fun() ->
        Log0 = emqx_conf:get_raw([log], #{}),
        Log1 = emqx_utils_maps:deep_put(
            [<<"file">>, <<"default">>, <<"enable">>], Log0, false
        ),
        {ok, _} = update_log_conf(Log1),

        {ok, Output} = capture_ctl(fun() ->
            emqx_ctl:run_command(["log", "outputs", "list"])
        end),

        ?assertMatch(match, re:run(Output, <<"LogOutput\\(name=file">>, [{capture, none}])),
        ?assertMatch(match, re:run(Output, <<"status=disabled">>, [{capture, none}]))
    end).

t_update_cluster_config(_Config) ->
    with_log_conf_restore(fun() ->
        ok = emqx_ctl:run_command(["log", "outputs", "disable", "file"]),
        ?assertEqual(
            false,
            emqx_utils_maps:deep_get(
                [<<"file">>, <<"default">>, <<"enable">>],
                emqx_conf:get_raw([log], #{})
            )
        ),
        ?assertEqual({error, {not_found, default}}, logger:get_handler_config(default)),

        ok = emqx_ctl:run_command(["log", "outputs", "enable", "file"]),
        ?assertEqual(
            true,
            emqx_utils_maps:deep_get(
                [<<"file">>, <<"default">>, <<"enable">>],
                emqx_conf:get_raw([log], #{})
            )
        ),
        ?assertMatch({ok, _}, logger:get_handler_config(default)),

        ok = emqx_ctl:run_command(["log", "outputs", "set-level", "file", "error"]),
        ?assertEqual(
            <<"error">>,
            emqx_utils_maps:deep_get(
                [<<"file">>, <<"default">>, <<"level">>],
                emqx_conf:get_raw([log], #{})
            )
        ),
        ?assertMatch({ok, #{level := error}}, logger:get_handler_config(default)),

        ok = emqx_ctl:run_command(["log", "outputs", "disable", "console"]),
        ?assertEqual(
            false,
            emqx_utils_maps:deep_get([<<"console">>, <<"enable">>], emqx_conf:get_raw([log], #{}))
        ),
        ?assertEqual({error, {not_found, console}}, logger:get_handler_config(console)),

        ok = emqx_ctl:run_command(["log", "outputs", "enable", "console"]),
        ?assertEqual(
            true,
            emqx_utils_maps:deep_get([<<"console">>, <<"enable">>], emqx_conf:get_raw([log], #{}))
        ),
        ?assertMatch({ok, _}, logger:get_handler_config(console)),

        Log0 = emqx_conf:get_raw([log], #{}),
        DefaultFile = emqx_utils_maps:deep_get([<<"file">>, <<"default">>], Log0),
        NamedFile = DefaultFile#{
            <<"enable">> => true,
            <<"path">> => <<"log/emqx-test-new.log">>
        },
        {ok, _} = update_log_conf(
            emqx_utils_maps:deep_put([<<"file">>, <<"new">>], Log0, NamedFile)
        ),
        ?assertMatch({ok, _}, logger:get_handler_config(new)),

        ok = emqx_ctl:run_command(["log", "outputs", "set-level", "new", "debug"]),
        ?assertEqual(
            <<"debug">>,
            emqx_utils_maps:deep_get(
                [<<"file">>, <<"new">>, <<"level">>],
                emqx_conf:get_raw([log], #{})
            )
        ),
        ?assertMatch({ok, #{level := debug}}, logger:get_handler_config(new)),

        ok = emqx_ctl:run_command(["log", "outputs", "disable", "new"]),
        ?assertEqual(
            false,
            emqx_utils_maps:deep_get(
                [<<"file">>, <<"new">>, <<"enable">>],
                emqx_conf:get_raw([log], #{})
            )
        ),
        ?assertEqual({error, {not_found, new}}, logger:get_handler_config(new)),

        ok = emqx_ctl:run_command(["log", "outputs", "enable", "new"]),
        ?assertEqual(
            true,
            emqx_utils_maps:deep_get(
                [<<"file">>, <<"new">>, <<"enable">>],
                emqx_conf:get_raw([log], #{})
            )
        ),
        ?assertMatch({ok, _}, logger:get_handler_config(new))
    end).

t_http_api_cli_roundtrip(Config) ->
    Host = ?config(api_host, Config),
    with_log_conf_restore(fun() ->
        {ok, Log0} = http_get_log_config(Host),
        Log1 = emqx_utils_maps:deep_put([<<"file">>, <<"default">>, <<"enable">>], Log0, false),
        {ok, #{}} = http_update_log_config(Host, Log1),

        {ok, Output} = capture_ctl(fun() ->
            emqx_ctl:run_command(["log", "outputs", "list"])
        end),
        ?assertMatch(
            match,
            re:run(Output, <<"LogOutput\\(name=file[^\\n]*status=disabled">>, [{capture, none}])
        ),

        ok = emqx_ctl:run_command(["log", "outputs", "enable", "file"]),
        {ok, Log2} = http_get_log_config(Host),
        ?assertEqual(
            true,
            emqx_utils_maps:deep_get([<<"file">>, <<"default">>, <<"enable">>], Log2)
        ),

        ok = emqx_ctl:run_command(["log", "outputs", "set-level", "file", "error"]),
        {ok, Log3} = http_get_log_config(Host),
        ?assertEqual(
            <<"error">>,
            emqx_utils_maps:deep_get([<<"file">>, <<"default">>, <<"level">>], Log3)
        )
    end).

t_hide_legacy_handlers_usage(_Config) ->
    {ok, Output} = capture_ctl(fun() ->
        emqx_ctl:run_command(["log"])
    end),
    ?assertMatch(match, re:run(Output, <<"log outputs list">>, [{capture, none}])),
    ?assertMatch(match, re:run(Output, <<"log outputs enable <name>">>, [{capture, none}])),
    ?assertMatch(
        match, re:run(Output, <<"log outputs set-level <name> <Level>">>, [{capture, none}])
    ),
    ?assertEqual(nomatch, re:run(Output, <<"log handlers list">>, [{capture, none}])).

with_log_conf_restore(Fun) ->
    Log0 = emqx_conf:get_raw([log], #{}),
    try
        Fun()
    after
        _ = update_log_conf(Log0)
    end.

update_log_conf(Log) ->
    emqx_conf:update([log], Log, #{rawconf_with_defaults => true, override_to => cluster}).

capture_ctl(Fun) ->
    {Res, Prints} = emqx_common_test_helpers:capture_io_format(Fun),
    {Res, iolist_to_binary(Prints)}.

http_get_log_config(Host) ->
    Path = emqx_mgmt_api_test_util:api_path(Host, ["configs", "log"]),
    case emqx_mgmt_api_test_util:simple_request(get, Path, []) of
        {200, Log} -> {ok, Log};
        Error -> Error
    end.

http_update_log_config(Host, Log) ->
    Path = emqx_mgmt_api_test_util:api_path(Host, ["configs", "log"]),
    case emqx_mgmt_api_test_util:simple_request(put, Path, Log) of
        {200, Resp} -> {ok, Resp};
        Error -> Error
    end.

listeners_conf() ->
    #{
        listeners =>
            maps:from_list([
                {Type, #{default => #{bind => select_free_listener_port(Type)}}}
             || Type <- [tcp, ssl, ws, wss]
            ])
    }.

select_free_listener_port(ws) ->
    emqx_common_test_helpers:select_free_port(tcp);
select_free_listener_port(wss) ->
    emqx_common_test_helpers:select_free_port(tcp);
select_free_listener_port(Type) ->
    emqx_common_test_helpers:select_free_port(Type).

dashboard_conf(Port) ->
    io_lib:format(
        """
        dashboard {
            listeners.http { enable = true, bind = ~B }
            password_expired_time = "86400s"
        }
        """,
        [Port]
    ).
