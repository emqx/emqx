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

-module(emqx_ft_api_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(SECRET_ACCESS_KEY, <<"fake_secret_access_key">>).

-import(emqx_dashboard_api_test_helpers, [host/0, uri/1]).

all() -> emqx_common_test_helpers:all(?MODULE).

suite() ->
    [{timetrap, {seconds, 90}}].

init_per_suite(Config) ->
    WorkDir = ?config(priv_dir, Config),
    Cluster = mk_cluster_specs(Config),
    Nodes = [Node1 | _] = emqx_cth_cluster:start(Cluster, #{work_dir => WorkDir}),
    {ok, App} = erpc:call(Node1, emqx_common_test_http, create_default_app, []),
    [{cluster_nodes, Nodes}, {api, App} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_cluster:stop(?config(cluster_nodes, Config)).

mk_cluster_specs(_Config) ->
    Apps = [
        emqx_conf,
        {emqx, #{override_env => [{boot_modules, [broker, listeners]}]}},
        {emqx_ft, "file_transfer { enable = true }"},
        {emqx_management, #{}}
    ],
    DashboardConfig =
        "dashboard { \n"
        "    listeners.http { enable = true, bind = 0 } \n"
        "    default_username = \"\" \n"
        "    default_password = \"\" \n"
        "}\n",
    [
        {emqx_ft_api_SUITE1, #{
            role => core,
            apps => Apps ++
                [
                    {emqx_dashboard, DashboardConfig ++ "dashboard.listeners.http.bind = 18083"}
                ]
        }},
        {emqx_ft_api_SUITE2, #{
            role => core,
            apps => Apps ++ [{emqx_dashboard, DashboardConfig}]
        }},
        {emqx_ft_api_SUITE3, #{
            role => replicant,
            apps => Apps ++ [{emqx_dashboard, DashboardConfig}]
        }}
    ].

init_per_testcase(Case, Config) ->
    [{tc, Case} | Config].
end_per_testcase(_Case, Config) ->
    ok = reset_ft_config(Config, true),
    ok.

%%--------------------------------------------------------------------
%% Tests
%%--------------------------------------------------------------------

t_list_files(Config) ->
    ClientId = client_id(Config),
    FileId = <<"f1">>,

    Node = lists:last(test_nodes(Config)),
    ok = emqx_ft_test_helpers:upload_file(sync, ClientId, FileId, "f1", <<"data">>, Node),

    {ok, 200, #{<<"files">> := Files}} =
        request_json(get, uri(["file_transfer", "files"]), Config),

    ?assertMatch(
        [#{<<"clientid">> := ClientId, <<"fileid">> := <<"f1">>}],
        [File || File = #{<<"clientid">> := CId} <- Files, CId == ClientId]
    ),

    {ok, 200, #{<<"files">> := FilesTransfer}} =
        request_json(get, uri(["file_transfer", "files", ClientId, FileId]), Config),

    ?assertMatch(
        [#{<<"clientid">> := ClientId, <<"fileid">> := <<"f1">>}],
        FilesTransfer
    ),

    ?assertMatch(
        {ok, 404, #{<<"code">> := <<"FILES_NOT_FOUND">>}},
        request_json(get, uri(["file_transfer", "files", ClientId, <<"no-such-file">>]), Config)
    ).

t_download_transfer(Config) ->
    ClientId = client_id(Config),
    FileId = <<"f1">>,

    Nodes = [Node | _] = test_nodes(Config),
    NodeUpload = lists:last(Nodes),
    ok = emqx_ft_test_helpers:upload_file(sync, ClientId, FileId, "f1", <<"data">>, NodeUpload),

    ?assertMatch(
        {ok, 400, #{<<"code">> := <<"BAD_REQUEST">>}},
        request_json(
            get,
            uri(["file_transfer", "file"]) ++ query(#{fileref => FileId}),
            Config
        )
    ),

    ?assertMatch(
        {ok, 503, _},
        request(
            get,
            uri(["file_transfer", "file"]) ++
                query(#{fileref => FileId, node => <<"nonode@nohost">>}),
            Config
        )
    ),

    ?assertMatch(
        {ok, 404, _},
        request(
            get,
            uri(["file_transfer", "file"]) ++
                query(#{fileref => <<"unknown_file">>, node => Node}),
            Config
        )
    ),

    ?assertMatch(
        {ok, 404, #{<<"message">> := <<"Invalid query parameter", _/bytes>>}},
        request_json(
            get,
            uri(["file_transfer", "file"]) ++
                query(#{fileref => <<>>, node => Node}),
            Config
        )
    ),

    ?assertMatch(
        {ok, 404, #{<<"message">> := <<"Invalid query parameter", _/bytes>>}},
        request_json(
            get,
            uri(["file_transfer", "file"]) ++
                query(#{fileref => <<"/etc/passwd">>, node => Node}),
            Config
        )
    ),

    {ok, 200, #{<<"files">> := [File]}} =
        request_json(get, uri(["file_transfer", "files", ClientId, FileId]), Config),

    {ok, 200, Response} = request(get, host() ++ maps:get(<<"uri">>, File), Config),

    ?assertEqual(
        <<"data">>,
        Response
    ).

t_list_files_paging(Config) ->
    ClientId = client_id(Config),
    NFiles = 20,
    Nodes = test_nodes(Config),
    Uploads = [
        {mk_file_id("file:", N), mk_file_name(N), pick(N, Nodes)}
     || N <- lists:seq(1, NFiles)
    ],
    ok = lists:foreach(
        fun({FileId, Name, Node}) ->
            ok = emqx_ft_test_helpers:upload_file(sync, ClientId, FileId, Name, <<"data">>, Node)
        end,
        Uploads
    ),

    ?assertMatch(
        {ok, 200, #{<<"files">> := [_, _, _], <<"cursor">> := _}},
        request_json(get, uri(["file_transfer", "files"]) ++ query(#{limit => 3}), Config)
    ),

    {ok, 200, #{<<"files">> := Files}} =
        request_json(get, uri(["file_transfer", "files"]) ++ query(#{limit => 100}), Config),

    ?assert(length(Files) >= NFiles),

    ?assertNotMatch(
        {ok, 200, #{<<"cursor">> := _}},
        request_json(get, uri(["file_transfer", "files"]) ++ query(#{limit => 100}), Config)
    ),

    ?assertMatch(
        {ok, 400, #{<<"code">> := <<"BAD_REQUEST">>}},
        request_json(get, uri(["file_transfer", "files"]) ++ query(#{limit => 0}), Config)
    ),

    ?assertMatch(
        {ok, 400, #{<<"code">> := <<"BAD_REQUEST">>}},
        request_json(get, uri(["file_transfer", "files"]) ++ query(#{following => <<>>}), Config)
    ),

    ?assertMatch(
        {ok, 400, #{<<"code">> := <<"BAD_REQUEST">>}},
        request_json(
            get, uri(["file_transfer", "files"]) ++ query(#{following => <<"{\"\":}">>}), Config
        )
    ),

    ?assertMatch(
        {ok, 400, #{<<"code">> := <<"BAD_REQUEST">>}},
        request_json(
            get,
            uri(["file_transfer", "files"]) ++ query(#{following => <<"whatsthat!?">>}),
            Config
        )
    ),

    PageThrough = fun PageThrough(Query, Acc) ->
        case request_json(get, uri(["file_transfer", "files"]) ++ query(Query), Config) of
            {ok, 200, #{<<"files">> := FilesPage, <<"cursor">> := Cursor}} ->
                PageThrough(Query#{following => Cursor}, Acc ++ FilesPage);
            {ok, 200, #{<<"files">> := FilesPage}} ->
                Acc ++ FilesPage
        end
    end,

    ?assertEqual(Files, PageThrough(#{limit => 1}, [])),
    ?assertEqual(Files, PageThrough(#{limit => 8}, [])),
    ?assertEqual(Files, PageThrough(#{limit => NFiles}, [])).

t_ft_disabled(Config) ->
    ?assertMatch(
        {ok, 200, _},
        request_json(get, uri(["file_transfer", "files"]), Config)
    ),

    ?assertMatch(
        {ok, 400, _},
        request_json(
            get,
            uri(["file_transfer", "file"]) ++ query(#{fileref => <<"f1">>}),
            Config
        )
    ),

    ok = reset_ft_config(Config, false),

    ?assertMatch(
        {ok, 503, _},
        request_json(get, uri(["file_transfer", "files"]), Config)
    ),

    ?assertMatch(
        {ok, 503, _},
        request_json(
            get,
            uri(["file_transfer", "file"]) ++ query(#{fileref => <<"f1">>, node => node()}),
            Config
        )
    ).

t_configure_file_transfer(Config) ->
    Uri = uri(["file_transfer"]),
    test_configure(Uri, Config).

t_configure_config_file_transfer(Config) ->
    Uri = uri(["configs/file_transfer"]),
    test_configure(Uri, Config).

test_configure(Uri, Config) ->
    #{
        cert := Cert,
        key := Key
    } = emqx_ft_test_helpers:generate_pki_files(Config),
    ?assertMatch(
        {ok, 200, #{
            <<"enable">> := true,
            <<"storage">> :=
                #{
                    <<"local">> :=
                        #{
                            <<"enable">> := true,
                            <<"segments">> :=
                                #{
                                    <<"gc">> :=
                                        #{
                                            %% Match keep the raw conf
                                            %% 1h is not change to 3600000
                                            <<"interval">> := <<"1h">>,
                                            <<"maximum_segments_ttl">> := <<"24h">>,
                                            <<"minimum_segments_ttl">> := <<"5m">>
                                        }
                                }
                        }
                }
        }},
        request_json(get, Uri, Config)
    ),
    ?assertMatch(
        {ok, 200, #{<<"enable">> := false}},
        request_json(put, Uri, #{<<"enable">> => false}, Config)
    ),
    ?assertMatch(
        {ok, 200, #{<<"enable">> := false}},
        request_json(get, Uri, Config)
    ),
    Storage0 = emqx_ft_test_helpers:local_storage(Config),
    Storage = emqx_utils_maps:deep_put(
        [
            <<"local">>,
            <<"segments">>,
            <<"gc">>,
            <<"maximum_segments_ttl">>
        ],
        Storage0,
        <<"10m">>
    ),
    ?assertMatch(
        {ok, 200, #{
            <<"storage">> :=
                #{
                    <<"local">> :=
                        #{
                            <<"segments">> :=
                                #{
                                    <<"gc">> :=
                                        #{
                                            <<"interval">> := <<"1h">>,
                                            %% Match keep the raw conf
                                            %% 10m is not change to 600,000
                                            <<"maximum_segments_ttl">> := <<"10m">>,
                                            <<"minimum_segments_ttl">> := <<"5m">>
                                        }
                                }
                        }
                }
        }},
        request_json(
            put,
            Uri,
            #{
                <<"enable">> => true,
                <<"storage">> => Storage
            },
            Config
        )
    ),
    ?assertMatch(
        {ok, 400, _},
        request(
            put,
            Uri,
            #{
                <<"enable">> => true,
                <<"storage">> => #{
                    <<"local">> => #{},
                    <<"remote">> => #{}
                }
            },
            Config
        )
    ),
    ?assertMatch(
        {ok, 400, _},
        request(
            put,
            Uri,
            #{
                <<"enable">> => true,
                <<"storage">> => #{
                    <<"local">> => #{
                        <<"gc">> => #{<<"interval">> => -42}
                    }
                }
            },
            Config
        )
    ),
    S3Exporter = #{
        <<"host">> => <<"localhost">>,
        <<"port">> => 9000,
        <<"bucket">> => <<"emqx">>,
        <<"url_expire_time">> => <<"2h">>,
        <<"secret_access_key">> => ?SECRET_ACCESS_KEY,
        <<"transport_options">> => #{
            <<"ssl">> => #{
                <<"enable">> => true,
                <<"certfile">> => Cert,
                <<"keyfile">> => Key
            }
        }
    },
    {ok, 200, GetConfigJson} =
        request_json(
            put,
            Uri,
            #{
                <<"enable">> => true,
                <<"storage">> => #{
                    <<"local">> => #{
                        <<"exporter">> => #{
                            <<"s3">> => S3Exporter
                        }
                    }
                }
            },
            Config
        ),
    ?assertMatch(
        #{
            <<"enable">> := true,
            <<"storage">> := #{
                <<"local">> := #{
                    <<"exporter">> := #{
                        <<"s3">> := #{
                            <<"transport_options">> := #{
                                <<"ssl">> := SSL = #{
                                    <<"enable">> := true,
                                    <<"certfile">> := <<"/", _CertFilepath/bytes>>,
                                    <<"keyfile">> := <<"/", _KeyFilepath/bytes>>
                                }
                            },
                            %% ensure 2h is unchanged
                            <<"url_expire_time">> := <<"2h">>,
                            <<"secret_access_key">> := <<"******">>
                        }
                    }
                }
            }
        } when not is_map_key(<<"password">>, SSL),
        GetConfigJson
    ),
    ?assertMatch(
        {ok, 400, _},
        request_json(
            put,
            Uri,
            #{
                <<"enable">> => true,
                <<"storage">> => #{
                    <<"local">> => #{
                        <<"exporter">> => #{
                            <<"s3">> => emqx_utils_maps:deep_put(
                                [<<"transport_options">>, <<"ssl">>, <<"keyfile">>],
                                S3Exporter,
                                <<>>
                            )
                        }
                    }
                }
            },
            Config
        )
    ),
    ?assertMatch(
        {ok, 200, #{}},
        request_json(
            put,
            Uri,
            emqx_utils_maps:deep_merge(
                GetConfigJson,
                #{
                    <<"enable">> => true,
                    <<"storage">> => #{
                        <<"local">> => #{
                            <<"exporter">> => #{
                                <<"s3">> => emqx_utils_maps:deep_put(
                                    [<<"transport_options">>, <<"ssl">>, <<"enable">>],
                                    S3Exporter,
                                    false
                                )
                            }
                        }
                    }
                }
            ),
            Config
        )
    ),
    %% put secret as ******, check the secret is unchanged
    ?assertMatch(
        #{
            <<"storage">> :=
                #{
                    <<"local">> :=
                        #{
                            <<"enable">> := true,
                            <<"exporter">> :=
                                #{
                                    <<"s3">> :=
                                        #{
                                            <<"enable">> := true,
                                            <<"secret_access_key">> := ?SECRET_ACCESS_KEY
                                        }
                                }
                        }
                }
        },
        get_ft_config(Config)
    ),
    ok.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

test_nodes(Config) ->
    ?config(cluster_nodes, Config).

client_id(Config) ->
    iolist_to_binary(io_lib:format("~s.~s", [?config(group, Config), ?config(tc, Config)])).

mk_file_id(Prefix, N) ->
    iolist_to_binary([Prefix, integer_to_list(N)]).

mk_file_name(N) ->
    "file." ++ integer_to_list(N).

request(Method, Url, Config) ->
    request(Method, Url, [], Config).

request(Method, Url, Body, Config) ->
    Opts = #{compatible_mode => true, httpc_req_opts => [{body_format, binary}]},
    request(Method, Url, Body, Opts, Config).

request(Method, Url, Body, Opts, Config) ->
    emqx_mgmt_api_test_util:request_api(Method, Url, [], auth_header(Config), Body, Opts).

request_json(Method, Url, Body, Config) ->
    case request(Method, Url, Body, Config) of
        {ok, Code, RespBody} ->
            {ok, Code, json(RespBody)};
        Otherwise ->
            Otherwise
    end.

request_json(Method, Url, Config) ->
    request_json(Method, Url, [], Config).

json(Body) when is_binary(Body) ->
    try
        emqx_utils_json:decode(Body, [return_maps])
    catch
        _:_ ->
            error({bad_json, Body})
    end.

query(Params) ->
    KVs = lists:map(fun({K, V}) -> uri_encode(K) ++ "=" ++ uri_encode(V) end, maps:to_list(Params)),
    "?" ++ string:join(KVs, "&").

auth_header(Config) ->
    #{api_key := ApiKey, api_secret := Secret} = ?config(api, Config),
    emqx_common_test_http:auth_header(binary_to_list(ApiKey), binary_to_list(Secret)).

uri_encode(T) ->
    emqx_http_lib:uri_encode(to_list(T)).

to_list(A) when is_atom(A) ->
    atom_to_list(A);
to_list(A) when is_integer(A) ->
    integer_to_list(A);
to_list(B) when is_binary(B) ->
    binary_to_list(B);
to_list(L) when is_list(L) ->
    L.

pick(N, List) ->
    lists:nth(1 + (N rem length(List)), List).

reset_ft_config(Config, Enable) ->
    [Node | _] = test_nodes(Config),
    LocalConfig =
        #{
            <<"enable">> => Enable,
            <<"storage">> => #{
                <<"local">> => #{
                    <<"enable">> => true,
                    <<"segments">> => #{
                        <<"gc">> => #{
                            <<"interval">> => <<"1h">>,
                            <<"maximum_segments_ttl">> => "24h",
                            <<"minimum_segments_ttl">> => "5m"
                        }
                    }
                }
            }
        },
    {ok, _} = rpc:call(Node, emqx_ft_conf, update, [LocalConfig]),
    ok.

get_ft_config(Config) ->
    [Node | _] = test_nodes(Config),
    rpc:call(Node, emqx_ft_conf, get_raw, []).
