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

-module(emqx_ft_conf_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("snabbkaffe/include/test_macros.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_Case, Config) ->
    _ = emqx_config:save_schema_mod_and_names(emqx_ft_schema),
    ok = emqx_common_test_helpers:start_apps(
        [emqx_conf, emqx_ft], fun
            (emqx_ft) ->
                emqx_ft_test_helpers:load_config(#{});
            (_) ->
                ok
        end
    ),
    {ok, _} = emqx:update_config([rpc, port_discovery], manual),
    Config.

end_per_testcase(_Case, _Config) ->
    ok = emqx_common_test_helpers:stop_apps([emqx_ft, emqx_conf]),
    ok = emqx_config:erase(file_transfer).

%%--------------------------------------------------------------------
%% Tests
%%--------------------------------------------------------------------

t_update_config(_Config) ->
    ?assertMatch(
        {error, #{kind := validation_error}},
        emqx_conf:update(
            [file_transfer],
            #{<<"storage">> => #{<<"unknown">> => #{<<"foo">> => 42}}},
            #{}
        )
    ),
    ?assertMatch(
        {ok, _},
        emqx_conf:update(
            [file_transfer],
            #{
                <<"enable">> => true,
                <<"storage">> => #{
                    <<"local">> => #{
                        <<"segments">> => #{
                            <<"root">> => <<"/tmp/path">>,
                            <<"gc">> => #{
                                <<"interval">> => <<"5m">>
                            }
                        },
                        <<"exporter">> => #{
                            <<"local">> => #{
                                <<"root">> => <<"/tmp/exports">>
                            }
                        }
                    }
                }
            },
            #{}
        )
    ),
    ?assertEqual(
        <<"/tmp/path">>,
        emqx_config:get([file_transfer, storage, local, segments, root])
    ),
    ?assertEqual(
        5 * 60 * 1000,
        emqx_ft_storage:with_storage_type(local, fun emqx_ft_conf:gc_interval/1)
    ),
    ?assertEqual(
        {5 * 60, 24 * 60 * 60},
        emqx_ft_storage:with_storage_type(local, fun emqx_ft_conf:segments_ttl/1)
    ).

t_disable_restore_config(Config) ->
    ?assertMatch(
        {ok, _},
        emqx_conf:update(
            [file_transfer],
            #{<<"enable">> => true, <<"storage">> => #{<<"local">> => #{}}},
            #{}
        )
    ),
    ?assertEqual(
        60 * 60 * 1000,
        emqx_ft_storage:with_storage_type(local, fun emqx_ft_conf:gc_interval/1)
    ),
    % Verify that transfers work
    ok = emqx_ft_test_helpers:upload_file(gen_clientid(), <<"f1">>, "f1", <<?MODULE_STRING>>),
    % Verify that clearing storage settings reverts config to defaults
    ?assertMatch(
        {ok, _},
        emqx_conf:update(
            [file_transfer],
            #{<<"enable">> => false, <<"storage">> => undefined},
            #{}
        )
    ),
    ?assertEqual(
        false,
        emqx_ft_conf:enabled()
    ),
    ?assertMatch(
        #{local := #{exporter := #{local := _}}},
        emqx_ft_conf:storage()
    ),
    ClientId = gen_clientid(),
    Client = emqx_ft_test_helpers:start_client(ClientId),
    % Verify that transfers fail cleanly when storage is disabled
    ?check_trace(
        ?assertMatch(
            {ok, #{reason_code_name := no_matching_subscribers}},
            emqtt:publish(
                Client,
                <<"$file/f2/init">>,
                emqx_utils_json:encode(emqx_ft:encode_filemeta(#{name => "f2", size => 42})),
                1
            )
        ),
        fun(Trace) ->
            ?assertMatch([], ?of_kind("file_transfer_init", Trace))
        end
    ),
    ok = emqtt:stop(Client),
    % Restore local storage backend
    Root = iolist_to_binary(emqx_ft_test_helpers:root(Config, node(), [segments])),
    ?assertMatch(
        {ok, _},
        emqx_conf:update(
            [file_transfer],
            #{
                <<"enable">> => true,
                <<"storage">> => #{
                    <<"local">> => #{
                        <<"segments">> => #{
                            <<"root">> => Root,
                            <<"gc">> => #{<<"interval">> => <<"1s">>}
                        }
                    }
                }
            },
            #{}
        )
    ),
    % Verify that GC is getting triggered eventually
    ?check_trace(
        ?block_until(#{?snk_kind := garbage_collection}, 5000, 0),
        fun(Trace) ->
            ?assertMatch(
                [
                    #{
                        ?snk_kind := garbage_collection,
                        storage := #{segments := #{root := Root}}
                    }
                ],
                ?of_kind(garbage_collection, Trace)
            )
        end
    ),
    % Verify that transfers work again
    ok = emqx_ft_test_helpers:upload_file(gen_clientid(), <<"f1">>, "f1", <<?MODULE_STRING>>).

t_switch_exporter(_Config) ->
    ?assertMatch(
        {ok, _},
        emqx_conf:update(
            [file_transfer],
            #{<<"enable">> => true},
            #{}
        )
    ),
    ?assertMatch(
        #{local := #{exporter := #{local := _}}},
        emqx_ft_conf:storage()
    ),
    % Verify that switching to a different exporter works
    ?assertMatch(
        {ok, _},
        emqx_conf:update(
            [file_transfer, storage, local, exporter],
            #{
                <<"s3">> => #{
                    <<"bucket">> => <<"emqx">>,
                    <<"host">> => <<"https://localhost">>,
                    <<"port">> => 9000,
                    <<"transport_options">> => #{
                        <<"ipv6_probe">> => false
                    }
                }
            },
            #{}
        )
    ),
    ?assertMatch(
        #{local := #{exporter := #{s3 := _}}},
        emqx_ft_conf:storage()
    ),
    % Verify that switching back to local exporter works
    ?assertMatch(
        {ok, _},
        emqx_conf:remove(
            [file_transfer, storage, local, exporter],
            #{}
        )
    ),
    ?assertMatch(
        {ok, _},
        emqx_conf:update(
            [file_transfer, storage, local, exporter],
            #{<<"local">> => #{}},
            #{}
        )
    ),
    ?assertMatch(
        #{local := #{exporter := #{local := #{}}}},
        emqx_ft_conf:storage()
    ),
    % Verify that transfers work
    ok = emqx_ft_test_helpers:upload_file(gen_clientid(), <<"f1">>, "f1", <<?MODULE_STRING>>).

gen_clientid() ->
    emqx_base62:encode(emqx_guid:gen()).
