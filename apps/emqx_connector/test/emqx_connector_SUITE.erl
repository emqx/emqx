%%--------------------------------------------------------------------
%% Copyright (c) 2022-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_connector_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(CONNECTOR, emqx_connector_dummy_impl).

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

%% the 2 test cases below are based on kafka connector which is ee only
-if(?EMQX_RELEASE_EDITION == ee).
t_connector_lifecycle({init, Config}) ->
    meck:new(emqx_connector_resource, [passthrough]),
    meck:expect(emqx_connector_resource, connector_to_resource_type, 1, ?CONNECTOR),
    meck:new(?CONNECTOR, [non_strict]),
    meck:expect(?CONNECTOR, resource_type, 0, dummy),
    meck:expect(?CONNECTOR, callback_mode, 0, async_if_possible),
    meck:expect(?CONNECTOR, on_start, 2, {ok, connector_state}),
    meck:expect(?CONNECTOR, on_stop, 2, ok),
    meck:expect(?CONNECTOR, on_get_status, 2, connected),
    [{mocked_mods, [?CONNECTOR, emqx_connector_resource]} | Config];
t_connector_lifecycle({'end', Config}) ->
    MockedMods = ?config(mocked_mods, Config),
    meck:unload(MockedMods),
    Config;
t_connector_lifecycle(_Config) ->
    ?assertEqual(
        [],
        emqx_connector:list()
    ),

    ?assertMatch(
        {ok, _},
        emqx_connector:create(kafka_producer, my_connector, connector_config())
    ),

    ?assertMatch(
        {ok, #{name := my_connector, type := kafka_producer}},
        emqx_connector:lookup(<<"connector:kafka_producer:my_connector">>)
    ),

    ?assertMatch(
        {ok, #{
            name := my_connector, type := kafka_producer, resource_data := #{status := connected}
        }},
        emqx_connector:lookup(<<"kafka_producer:my_connector">>)
    ),

    ?assertMatch(
        {ok, #{
            name := my_connector, type := kafka_producer, resource_data := #{status := connected}
        }},
        emqx_connector:lookup(kafka_producer, my_connector)
    ),

    ?assertMatch(
        [#{name := <<"my_connector">>, type := <<"kafka_producer">>}],
        emqx_connector:list()
    ),

    ?assertMatch(
        {ok, #{config := #{enable := false}}},
        emqx_connector:disable_enable(disable, kafka_producer, my_connector)
    ),

    ?assertMatch(
        {ok, #{resource_data := #{status := stopped}}},
        emqx_connector:lookup(kafka_producer, my_connector)
    ),

    ?assertMatch(
        {ok, #{config := #{enable := true}}},
        emqx_connector:disable_enable(enable, kafka_producer, my_connector)
    ),

    ?assertMatch(
        {ok, #{resource_data := #{status := connected}}},
        emqx_connector:lookup(kafka_producer, my_connector)
    ),

    ?assertMatch(
        {ok, #{config := #{connect_timeout := 10000}}},
        emqx_connector:update(kafka_producer, my_connector, (connector_config())#{
            <<"connect_timeout">> => <<"10s">>
        })
    ),

    ?assertMatch(
        {ok, #{resource_data := #{config := #{connect_timeout := 10000}}}},
        emqx_connector:lookup(kafka_producer, my_connector)
    ),

    ?assertMatch(
        ok,
        emqx_connector:remove(kafka_producer, my_connector)
    ),

    ?assertEqual(
        [],
        emqx_connector:list()
    ),

    ?assert(meck:validate(?CONNECTOR)),
    ?assertMatch(
        [
            {_, {?CONNECTOR, on_start, [_, _]}, {ok, connector_state}},
            {_, {?CONNECTOR, on_get_status, [_, connector_state]}, connected},
            {_, {?CONNECTOR, on_stop, [_, connector_state]}, ok},
            {_, {?CONNECTOR, on_start, [_, _]}, {ok, connector_state}},
            {_, {?CONNECTOR, on_get_status, [_, connector_state]}, connected},
            {_, {?CONNECTOR, on_stop, [_, connector_state]}, ok},
            {_, {?CONNECTOR, on_start, [_, _]}, {ok, connector_state}},
            {_, {?CONNECTOR, on_get_status, [_, connector_state]}, connected},
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
    meck:new(emqx_connector_resource, [passthrough]),
    meck:expect(emqx_connector_resource, connector_to_resource_type, 1, ?CONNECTOR),
    meck:new(?CONNECTOR, [non_strict]),
    meck:expect(?CONNECTOR, callback_mode, 0, async_if_possible),
    meck:expect(?CONNECTOR, resource_type, 0, dummy),
    meck:expect(?CONNECTOR, on_start, 2, {ok, connector_state}),
    meck:expect(?CONNECTOR, on_get_channels, 1, [{<<"my_channel">>, #{enable => true}}]),
    meck:expect(?CONNECTOR, on_add_channel, 4, {ok, connector_state}),
    meck:expect(?CONNECTOR, on_stop, 2, ok),
    meck:expect(?CONNECTOR, on_get_status, 2, connected),
    meck:expect(?CONNECTOR, query_mode, 1, simple_async_internal_buffer),
    Config;
t_remove_fail({'end', _Config}) ->
    meck:unload(),
    ok;
t_remove_fail(_Config) ->
    ?assertEqual(
        [],
        emqx_connector:list()
    ),

    ?assertMatch(
        {ok, _},
        emqx_connector:create(kafka_producer, my_failing_connector, connector_config())
    ),

    ?assertMatch(
        {error, {post_config_update, emqx_connector, {active_channels, [{<<"my_channel">>, _}]}}},
        emqx_connector:remove(kafka_producer, my_failing_connector)
    ),

    ?assertNotEqual(
        [],
        emqx_connector:list()
    ),

    ?assert(meck:validate(?CONNECTOR)),
    ?assertMatch(
        [
            {_, {?CONNECTOR, callback_mode, []}, _},
            {_, {?CONNECTOR, on_start, [_, _]}, {ok, connector_state}},
            {_, {?CONNECTOR, on_get_channels, [_]}, _},
            {_, {?CONNECTOR, on_get_status, [_, connector_state]}, connected},
            {_, {?CONNECTOR, on_get_channels, [_]}, _},
            {_, {?CONNECTOR, on_add_channel, _}, {ok, connector_state}},
            {_, {?CONNECTOR, on_get_channels, [_]}, _}
        ],
        lists:filter(
            fun({_, {?CONNECTOR, Fun, _Args}, _}) ->
                lists:member(
                    Fun, [
                        callback_mode,
                        on_start,
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
    meck:new(emqx_connector_resource, [passthrough]),
    meck:expect(emqx_connector_resource, connector_to_resource_type, 1, ?CONNECTOR),
    meck:new(?CONNECTOR, [non_strict]),
    meck:expect(?CONNECTOR, resource_type, 0, dummy),
    meck:expect(?CONNECTOR, callback_mode, 0, async_if_possible),
    meck:expect(?CONNECTOR, on_start, 2, {ok, connector_state}),
    meck:expect(?CONNECTOR, on_stop, 2, ok),
    meck:expect(?CONNECTOR, on_get_status, 2, connected),
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
    meck:new(emqx_connector_resource, [passthrough]),
    meck:expect(emqx_connector_resource, connector_to_resource_type, 1, ?CONNECTOR),
    meck:new(?CONNECTOR, [non_strict]),
    meck:expect(?CONNECTOR, resource_type, 0, dummy),
    meck:expect(?CONNECTOR, callback_mode, 0, async_if_possible),
    meck:expect(?CONNECTOR, on_start, 2, {ok, connector_state}),
    meck:expect(?CONNECTOR, on_get_channels, 1, []),
    meck:expect(?CONNECTOR, on_add_channel, 4, {ok, connector_state}),
    meck:expect(?CONNECTOR, on_stop, 2, ok),
    meck:expect(?CONNECTOR, on_get_status, 2, connected),
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
    meck:new(emqx_connector_resource, [passthrough]),
    meck:expect(emqx_connector_resource, connector_to_resource_type, 1, ?CONNECTOR),
    meck:new(?CONNECTOR, [non_strict]),
    meck:expect(?CONNECTOR, resource_type, 0, dummy),
    meck:expect(?CONNECTOR, callback_mode, 0, async_if_possible),
    %% hang forever
    meck:expect(?CONNECTOR, on_start, fun(_ConnResId, _Opts) ->
        receive
            go -> ok
        end
    end),
    meck:expect(?CONNECTOR, on_get_channels, 1, []),
    meck:expect(?CONNECTOR, on_add_channel, 4, {ok, connector_state}),
    meck:expect(?CONNECTOR, on_stop, 2, ok),
    meck:expect(?CONNECTOR, on_get_status, 2, connected),
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

%% helpers

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
-endif.
