%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_connector_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/emqx_config.hrl").

-define(CONNECTOR, emqx_connector_dummy_impl).

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [emqx, emqx_conf, emqx_connector],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(TestCase, Config) ->
    ?MODULE:TestCase({init, Config}).

end_per_testcase(TestCase, Config) ->
    ?MODULE:TestCase({'end', Config}).

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

mock_resource() ->
    meck:new(emqx_connector_resource, [passthrough]),
    meck:expect(emqx_connector_resource, connector_to_resource_type, 1, ?CONNECTOR),
    meck:new(?CONNECTOR, [non_strict]),
    meck:expect(?CONNECTOR, resource_type, 0, dummy),
    meck:expect(?CONNECTOR, callback_mode, 0, async_if_possible),
    meck:expect(?CONNECTOR, on_start, 2, {ok, connector_state}),
    meck:expect(?CONNECTOR, on_stop, 2, ok),
    meck:expect(?CONNECTOR, on_get_status, 2, connected),
    ok.

connector_config() ->
    #{
        <<"authentication">> => <<"none">>,
        <<"bootstrap_hosts">> => <<"127.0.0.1:9092">>,
        <<"connect_timeout">> => <<"5s">>,
        <<"enable">> => true,
        <<"metadata_request_timeout">> => <<"5s">>,
        <<"min_metadata_refresh_interval">> => <<"3s">>,
        <<"socket_opts">> =>
            #{
                <<"recbuf">> => <<"1024KB">>,
                <<"sndbuf">> => <<"1024KB">>,
                <<"tcp_keepalive">> => <<"none">>
            },
        <<"ssl">> =>
            #{
                <<"ciphers">> => [],
                <<"depth">> => 10,
                <<"enable">> => false,
                <<"hibernate_after">> => <<"5s">>,
                <<"log_level">> => <<"notice">>,
                <<"reuse_sessions">> => true,
                <<"secure_renegotiate">> => true,
                <<"verify">> => <<"verify_peer">>,
                <<"versions">> => [<<"tlsv1.3">>, <<"tlsv1.2">>]
            }
    }.

get_value(Key, TCConfig) ->
    emqx_bridge_v2_testlib:get_value(Key, TCConfig).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

%% the 2 test cases below are based on kafka connector which is ee only
t_connector_lifecycle({init, Config}) ->
    mock_resource(),
    Config;
t_connector_lifecycle({'end', Config}) ->
    meck:unload(),
    Config;
t_connector_lifecycle(_Config) ->
    ?assertEqual(
        [],
        emqx_connector:list(?global_ns)
    ),

    ?assertMatch(
        {ok, _},
        emqx_connector:create(?global_ns, kafka_producer, my_connector, connector_config())
    ),

    ?assertMatch(
        {ok, #{name := my_connector, type := kafka_producer}},
        emqx_connector:lookup(?global_ns, <<"kafka_producer">>, <<"my_connector">>)
    ),

    ?assertMatch(
        {ok, #{
            name := my_connector, type := kafka_producer, resource_data := #{status := connected}
        }},
        emqx_connector:lookup(?global_ns, kafka_producer, my_connector)
    ),

    ?assertMatch(
        [#{name := my_connector, type := kafka_producer}],
        emqx_connector:list(?global_ns)
    ),

    ?assertMatch(
        {ok, #{config := #{enable := false}}},
        emqx_connector:disable_enable(?global_ns, disable, kafka_producer, my_connector)
    ),

    ?assertMatch(
        {ok, #{resource_data := #{status := stopped}}},
        emqx_connector:lookup(?global_ns, kafka_producer, my_connector)
    ),

    ?assertMatch(
        {ok, #{config := #{enable := true}}},
        emqx_connector:disable_enable(?global_ns, enable, kafka_producer, my_connector)
    ),

    ?assertMatch(
        {ok, #{resource_data := #{status := connected}}},
        emqx_connector:lookup(?global_ns, kafka_producer, my_connector)
    ),

    ?assertMatch(
        ok,
        emqx_connector:remove(?global_ns, kafka_producer, my_connector)
    ),

    ?assertEqual(
        [],
        emqx_connector:list(?global_ns)
    ),

    ?assert(meck:validate(?CONNECTOR)),
    ?assertMatch(
        [
            %% Creation
            {_, {?CONNECTOR, on_start, [_, _]}, {ok, connector_state}},
            {_, {?CONNECTOR, on_get_status, [_, connector_state]}, connected},
            %% Disable
            {_, {?CONNECTOR, on_stop, [_, connector_state]}, ok},
            %% Enable (restart); it attempts to stop again
            {_, {?CONNECTOR, on_stop, [_, connector_state]}, ok},
            {_, {?CONNECTOR, on_start, [_, _]}, {ok, connector_state}},
            {_, {?CONNECTOR, on_get_status, [_, connector_state]}, connected},
            %% Remove
            {_, {?CONNECTOR, on_stop, [_, connector_state]}, ok}
        ],
        lists:filter(
            fun({_, {?CONNECTOR, Fun, _Args}, _}) ->
                lists:member(
                    Fun, [
                        on_start,
                        on_stop,
                        on_get_channels,
                        on_get_status,
                        on_add_channel
                    ]
                )
            end,
            meck:history(?CONNECTOR)
        )
    ),
    ok.

t_remove_fail({'init', Config}) ->
    mock_resource(),
    meck:expect(?CONNECTOR, on_get_channels, 1, [{<<"my_channel">>, #{enable => true}}]),
    meck:expect(?CONNECTOR, on_add_channel, 4, {ok, connector_state}),
    meck:expect(?CONNECTOR, query_mode, 1, simple_async_internal_buffer),
    Config;
t_remove_fail({'end', _Config}) ->
    meck:unload(),
    ok;
t_remove_fail(_Config) ->
    ?assertEqual(
        [],
        emqx_connector:list(?global_ns)
    ),

    ?assertMatch(
        {ok, _},
        emqx_connector:create(?global_ns, kafka_producer, my_failing_connector, connector_config())
    ),

    ?assertMatch(
        {error, {post_config_update, emqx_connector, {active_channels, [{<<"my_channel">>, _}]}}},
        emqx_connector:remove(?global_ns, kafka_producer, my_failing_connector)
    ),

    ?assertNotEqual(
        [],
        emqx_connector:list(?global_ns)
    ),

    ?assert(meck:validate(?CONNECTOR)),
    ok.

t_create_with_bad_name_direct_path({init, Config}) ->
    meck:new(emqx_connector_resource, [passthrough]),
    meck:expect(emqx_connector_resource, connector_to_resource_type, 1, ?CONNECTOR),
    meck:new(?CONNECTOR, [non_strict]),
    meck:expect(?CONNECTOR, resource_type, 0, dummy),
    meck:expect(?CONNECTOR, callback_mode, 0, async_if_possible),
    meck:expect(?CONNECTOR, on_start, 2, {ok, connector_state}),
    meck:expect(?CONNECTOR, on_stop, 2, ok),
    meck:expect(?CONNECTOR, on_get_status, 2, connected),
    Config;
t_create_with_bad_name_direct_path({'end', _Config}) ->
    meck:unload(),
    ok;
t_create_with_bad_name_direct_path(_Config) ->
    Path = [connectors, kafka_producer, 'test_哈哈'],
    ConnConfig0 = connector_config(),
    %% Note: must contain SSL options to trigger original bug.
    Cacertfile = emqx_common_test_helpers:app_path(
        emqx,
        filename:join(["etc", "certs", "cacert.pem"])
    ),
    ConnConfig = ConnConfig0#{<<"ssl">> => #{<<"cacertfile">> => Cacertfile}},
    ?assertMatch(
        {error,
            {pre_config_update, _ConfigHandlerMod, #{
                kind := validation_error,
                reason := <<"Invalid name format.", _/binary>>
            }}},
        emqx:update_config(Path, ConnConfig)
    ),
    ok.

t_create_with_bad_name_root_path({init, Config}) ->
    mock_resource(),
    Config;
t_create_with_bad_name_root_path({'end', _Config}) ->
    meck:unload(),
    ok;
t_create_with_bad_name_root_path(_Config) ->
    Path = [connectors],
    BadConnectorName = <<"test_哈哈"/utf8>>,
    ConnConfig0 = connector_config(),
    %% Note: must contain SSL options to trigger original bug.
    Cacertfile = emqx_common_test_helpers:app_path(
        emqx,
        filename:join(["etc", "certs", "cacert.pem"])
    ),
    ConnConfig = ConnConfig0#{<<"ssl">> => #{<<"cacertfile">> => Cacertfile}},
    Conf = #{<<"kafka_producer">> => #{BadConnectorName => ConnConfig}},
    ?assertMatch(
        {error,
            {pre_config_update, _ConfigHandlerMod, #{
                kind := validation_error,
                reason := bad_connector_names,
                bad_connectors := [#{type := <<"kafka_producer">>, name := BadConnectorName}]
            }}},
        emqx:update_config(Path, Conf)
    ),
    ok.

t_no_buffer_workers({'init', Config}) ->
    mock_resource(),
    meck:expect(?CONNECTOR, on_get_channels, 1, []),
    meck:expect(?CONNECTOR, on_add_channel, 4, {ok, connector_state}),
    meck:expect(?CONNECTOR, query_mode, 1, sync),
    [
        {path, [connectors, kafka_producer, no_bws]}
        | Config
    ];
t_no_buffer_workers({'end', Config}) ->
    Path = ?config(path, Config),
    {ok, _} = emqx:remove_config(Path),
    meck:unload(),
    ok;
t_no_buffer_workers(Config) ->
    Path = ?config(path, Config),
    ConnConfig = connector_config(),
    ?assertMatch({ok, _}, emqx:update_config(Path, ConnConfig)),
    ?assertEqual([], supervisor:which_children(emqx_resource_buffer_worker_sup)),
    ok.

%% Checks that the maximum timeout (currently) set by `resource_opts.health_check_timeout'
%% is respected when doing a dry run, even if the removal gets stuck because the resource
%% process is unresponsive.
t_dryrun_timeout({'init', Config}) ->
    mock_resource(),
    %% hang forever
    meck:expect(?CONNECTOR, on_start, fun(_ConnResId, _Opts) ->
        receive
            go -> ok
        end
    end),
    meck:expect(?CONNECTOR, on_get_channels, 1, []),
    meck:expect(?CONNECTOR, on_add_channel, 4, {ok, connector_state}),
    meck:expect(?CONNECTOR, query_mode, 1, sync),
    Config;
t_dryrun_timeout({'end', _Config}) ->
    meck:unload(),
    ok;
t_dryrun_timeout(Config) when is_list(Config) ->
    Type = kafka_producer,
    Conf0 = connector_config(),
    Timeout = 100,
    Conf = Conf0#{<<"resource_opts">> => #{<<"health_check_interval">> => Timeout}},
    %% Minimum timeout is capped at 5 s in `emqx_resource_manager'...  Plus, we need to
    %% wait for removal of stuck process, which itself has another 5 s timeout.
    ct:timetrap(15_000),
    %% Cache cleaner is triggered when the process initiating the dry run dies.
    Pid = spawn_link(fun() ->
        Res = emqx_connector_resource:create_dry_run(Type, Conf),
        ?assertEqual({error, timeout}, Res),
        ok
    end),
    MRef = monitor(process, Pid),
    receive
        {'DOWN', MRef, _, _, _} ->
            ok
    end,
    %% Should be removed asynchronously by cache cleaner.
    ?retry(1_000, 7, ?assertEqual([], emqx_resource:list_instances())),
    ok.

t_async_load_config_cli({init, Config}) ->
    mock_resource(),
    %% hang forever
    meck:expect(?CONNECTOR, on_start, fun(_ConnResId, _Opts) ->
        case persistent_term:get({?MODULE, continue}, false) of
            true ->
                {ok, connector_state};
            _ ->
                persistent_term:put({?MODULE, res_pid}, self()),
                receive
                    go ->
                        persistent_term:put({?MODULE, continue}, true),
                        {ok, connector_state}
                end
        end
    end),
    ContinueStart = fun() ->
        case persistent_term:get({?MODULE, res_pid}, undefined) of
            undefined ->
                ct:fail("resource didn't start");
            ResPid ->
                ResPid ! go,
                ok
        end
    end,
    meck:expect(?CONNECTOR, on_get_channels, 1, []),
    meck:expect(?CONNECTOR, on_add_channel, 4, {ok, connector_state}),
    meck:expect(?CONNECTOR, query_mode, 1, sync),
    [{continue_start, ContinueStart} | Config];
t_async_load_config_cli({'end', Config}) ->
    ContinueStart = ?config(continue_start, Config),
    ContinueStart(),
    emqx_bridge_v2_testlib:delete_all_connectors(),
    persistent_term:erase({?MODULE, continue}),
    persistent_term:erase({?MODULE, res_pid}),
    meck:unload(),
    ok;
t_async_load_config_cli(Config) when is_list(Config) ->
    ConnectorType = <<"kafka_producer">>,
    ConnectorName = <<"async_load_config_cli">>,
    RawConf = #{<<"connectors">> => #{ConnectorType => #{ConnectorName => connector_config()}}},
    ConfigToLoadBin = iolist_to_binary(hocon_pp:do(RawConf, #{})),
    ct:timetrap(5_000),
    ?assertMatch(ok, emqx_conf_cli:load_config(?global_ns, ConfigToLoadBin, #{mode => merge})),
    ok.
