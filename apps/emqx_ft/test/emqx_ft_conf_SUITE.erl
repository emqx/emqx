%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

init_per_testcase(Case, Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx_conf, #{}},
            {emqx_ft, #{config => "file_transfer {}"}}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Case, Config)}
    ),
    [{apps, Apps} | Config].

end_per_testcase(_Case, Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)),
    ok.

%%--------------------------------------------------------------------
%% Tests
%%--------------------------------------------------------------------

t_update_config(_Config) ->
    ?assertMatch(
        {error, #{kind := validation_error}},
        emqx_ft_conf:update(
            #{<<"storage">> => #{<<"unknown">> => #{<<"foo">> => 42}}}
        )
    ),
    ?assertMatch(
        {ok, _},
        emqx_ft_conf:update(
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
                                <<"enable">> => true,
                                <<"root">> => <<"/tmp/exports">>
                            }
                        }
                    }
                }
            }
        )
    ),
    ?assertEqual(
        "/tmp/path",
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
        emqx_ft_conf:update(
            #{<<"enable">> => true, <<"storage">> => #{<<"local">> => #{}}}
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
        emqx_ft_conf:update(#{<<"enable">> => false, <<"storage">> => undefined})
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
    Root = emqx_ft_test_helpers:root(Config, node(), [segments]),
    ?assertMatch(
        {ok, _},
        emqx_ft_conf:update(
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
            }
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
                        storage := #{segments := #{gc := #{interval := 1000}}}
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
        emqx_ft_conf:update(#{<<"enable">> => true})
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
                    <<"enable">> => true,
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
            #{<<"local">> => #{<<"enable">> => true}},
            #{}
        )
    ),
    ?assertMatch(
        #{local := #{exporter := #{local := #{}}}},
        emqx_ft_conf:storage()
    ),
    % Verify that transfers work
    ok = emqx_ft_test_helpers:upload_file(gen_clientid(), <<"f1">>, "f1", <<?MODULE_STRING>>).

t_persist_ssl_certfiles(Config) ->
    #{
        cert := Cert,
        key := Key
    } = emqx_ft_test_helpers:generate_pki_files(Config),
    ?assertMatch(
        {ok, _},
        emqx_ft_conf:update(mk_storage(true))
    ),
    ?assertEqual(
        [],
        list_ssl_certfiles(Config)
    ),
    ?assertMatch(
        {error, {pre_config_update, _, #{reason := <<"bad_ssl_config">>}}},
        emqx_ft_conf:update(
            mk_storage(true, #{
                <<"s3">> => mk_s3_config(#{
                    <<"transport_options">> => #{
                        <<"ssl">> => #{
                            <<"certfile">> => <<"cert.pem">>,
                            <<"keyfile">> => <<"key.pem">>
                        }
                    }
                })
            })
        )
    ),
    ?assertMatch(
        {ok, _},
        emqx_ft_conf:update(
            mk_storage(false, #{
                <<"s3">> => mk_s3_config(#{
                    <<"transport_options">> => #{
                        <<"ssl">> => #{
                            <<"certfile">> => Cert,
                            <<"keyfile">> => Key
                        }
                    }
                })
            })
        )
    ),
    ?assertMatch(
        #{
            local := #{
                exporter := #{
                    s3 := #{
                        transport_options := #{
                            ssl := #{
                                certfile := <<"/", _CertFilepath/binary>>,
                                keyfile := <<"/", _KeyFilepath/binary>>
                            }
                        }
                    }
                }
            }
        },
        emqx_ft_conf:storage()
    ),
    ?assertMatch(
        [_Certfile, _Keyfile],
        list_ssl_certfiles(Config)
    ),
    ?assertMatch(
        {ok, _},
        emqx_ft_conf:update(mk_storage(true))
    ).

t_import(Config) ->
    #{
        cert := Cert,
        key := Key
    } = emqx_ft_test_helpers:generate_pki_files(Config),
    {ok, _} =
        emqx_ft_conf:update(
            mk_storage(true, #{
                <<"s3">> => mk_s3_config(#{
                    <<"transport_options">> => #{
                        <<"ssl">> => #{
                            <<"certfile">> => Cert,
                            <<"keyfile">> => Key
                        }
                    }
                })
            })
        ),

    BackupConfig = emqx_config:get_raw([]),
    FTBackupConfig = maps:with([<<"file_transfer">>], BackupConfig),

    {ok, _} = emqx_ft_conf:update(mk_storage(true)),

    ?assertMatch(
        {ok, _},
        emqx_ft_conf:import_config(FTBackupConfig)
    ),

    ?assertMatch(
        #{local := #{exporter := #{s3 := #{enable := true}}}},
        emqx_ft_conf:storage()
    ).

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

mk_storage(Enabled) ->
    mk_storage(Enabled, #{<<"local">> => #{}}).

mk_storage(Enabled, Exporter) ->
    #{
        <<"enable">> => Enabled,
        <<"storage">> => #{
            <<"local">> => #{
                <<"exporter">> => Exporter
            }
        }
    }.

mk_s3_config(S3Config) ->
    BaseS3Config = #{
        <<"bucket">> => <<"emqx">>,
        <<"host">> => <<"https://localhost">>,
        <<"port">> => 9000
    },
    maps:merge(BaseS3Config, S3Config).

gen_clientid() ->
    emqx_utils:rand_id(16).

list_ssl_certfiles(_Config) ->
    CertDir = emqx:mutable_certs_dir(),
    filelib:fold_files(CertDir, ".*", true, fun(Filepath, Acc) -> [Filepath | Acc] end, []).
