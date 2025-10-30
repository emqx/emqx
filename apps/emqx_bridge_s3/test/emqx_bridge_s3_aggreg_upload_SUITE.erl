%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_s3_aggreg_upload_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/test_macros.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").
-include_lib("emqx/include/emqx_config.hrl").

%%------------------------------------------------------------------------------
%% Type defs
%%------------------------------------------------------------------------------

-import(emqx_common_test_helpers, [on_exit/1]).
-import(emqx_utils_conv, [bin/1]).

%% See `emqx_bridge_s3.hrl`.
-define(BRIDGE_TYPE, <<"s3">>).
-define(CONNECTOR_TYPE, <<"s3">>).

-define(PROXY_NAME, "minio_tcp").

-define(PARQUET_QUERY_ENDPOINT, <<"http://query:8090">>).

-define(CONF_TIME_INTERVAL, 4000).
-define(CONF_MAX_RECORDS, 100).
-define(CONF_COLUMN_ORDER, ?CONF_COLUMN_ORDER([])).
-define(CONF_COLUMN_ORDER(T), [
    <<"publish_received_at">>,
    <<"clientid">>,
    <<"topic">>,
    <<"payload">>,
    <<"empty">>
    | T
]).

-define(LIMIT_TOLERANCE, 1.1).

-define(csv, csv).
-define(json_lines, json_lines).
-define(parquet, parquet).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all_with_matrix(?MODULE).

groups() ->
    emqx_common_test_helpers:groups_with_matrix(?MODULE).

init_per_suite(Config) ->
    % Setup toxiproxy
    ProxyHost = os:getenv("PROXY_HOST", "toxiproxy"),
    ProxyPort = list_to_integer(os:getenv("PROXY_PORT", "8474")),
    _ = emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_connector,
            emqx_bridge_s3,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [
        {apps, Apps},
        {proxy_host, ProxyHost},
        {proxy_port, ProxyPort},
        {proxy_name, ?PROXY_NAME}
        | Config
    ].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(TestCase, Config) ->
    ct:timetrap(timer:seconds(15)),
    ok = snabbkaffe:start_trace(),
    TS = erlang:system_time(),
    Name = iolist_to_binary(io_lib:format("~s-~p", [TestCase, TS])),
    Bucket = unicode:characters_to_list(string:replace(Name, "_", "-", all)),
    ConnectorConfig = connector_config(Name, Config),
    AggregContainerCfg =
        case aggregation_container_type(Config) of
            ?csv -> aggregation_container_config_csv(#{});
            ?json_lines -> aggregation_container_config_json_lines(#{});
            ?parquet -> aggregation_container_config_parquet_inline(#{})
        end,
    ActionConfig = action_config(#{
        <<"connector">> => Name,
        <<"parameters">> => #{
            <<"bucket">> => unicode:characters_to_binary(Bucket),
            <<"container">> => AggregContainerCfg
        }
    }),
    ok = emqx_bridge_s3_test_helpers:create_bucket(Bucket),
    [
        {connector_type, ?CONNECTOR_TYPE},
        {connector_name, Name},
        {connector_config, ConnectorConfig},
        {bridge_kind, action},
        {action_type, ?BRIDGE_TYPE},
        {action_name, Name},
        {action_config, ActionConfig},
        {s3_bucket, Bucket}
        | Config
    ].

end_per_testcase(_TestCase, _Config) ->
    ok = snabbkaffe:stop(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

connector_config(Name, _Config) ->
    BaseConf = emqx_s3_test_helpers:base_raw_config(tcp),
    emqx_bridge_s3_test_helpers:parse_and_check_config(
        <<"connectors">>, ?CONNECTOR_TYPE, Name, #{
            <<"enable">> => true,
            <<"description">> => <<"S3 Connector">>,
            <<"host">> => emqx_utils_conv:bin(maps:get(<<"host">>, BaseConf)),
            <<"port">> => maps:get(<<"port">>, BaseConf),
            <<"access_key_id">> => maps:get(<<"access_key_id">>, BaseConf),
            <<"secret_access_key">> => maps:get(<<"secret_access_key">>, BaseConf),
            <<"transport_options">> => #{
                <<"ssl">> => #{<<"enable">> => false},
                <<"connect_timeout">> => <<"500ms">>,
                <<"request_timeout">> => <<"1s">>,
                <<"pool_size">> => 4,
                <<"max_retries">> => 0
            },
            <<"resource_opts">> => #{
                <<"health_check_interval">> => <<"1s">>
            }
        }
    ).

action_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"connector">> => <<"please override">>,
        <<"parameters">> => #{
            <<"mode">> => <<"aggregated">>,
            <<"bucket">> => <<"please override">>,
            <<"key">> => <<"${action}/${node}/${datetime.rfc3339}">>,
            <<"acl">> => <<"public_read">>,
            <<"headers">> => #{
                <<"X-AMZ-Meta-Version">> => <<"42">>
            },
            <<"aggregation">> => #{
                <<"time_interval">> => <<"4s">>,
                <<"max_records">> => ?CONF_MAX_RECORDS
            },
            <<"container">> => <<"please override">>
        },
        <<"resource_opts">> => #{
            <<"health_check_interval">> => <<"1s">>,
            <<"max_buffer_bytes">> => <<"64MB">>,
            <<"query_mode">> => <<"async">>,
            <<"worker_pool_size">> => 4
        }
    },
    Config = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_s3_test_helpers:parse_and_check_config(
        <<"actions">>, ?BRIDGE_TYPE, <<"x">>, Config
    ).

get_config(K, TCConfig) -> emqx_bridge_v2_testlib:get_value(K, TCConfig).
get_config(K, TCConfig, Default) -> proplists:get_value(K, TCConfig, Default).

aggregation_container_type(TCConfig) ->
    emqx_common_test_helpers:get_matrix_prop(TCConfig, [?csv, ?json_lines, ?parquet], ?csv).

aggregation_container_config_csv(Overrides) ->
    emqx_utils_maps:deep_merge(
        #{
            <<"type">> => <<"csv">>,
            <<"column_order">> => ?CONF_COLUMN_ORDER
        },
        Overrides
    ).

aggregation_container_config_json_lines(Overrides) ->
    emqx_utils_maps:deep_merge(#{<<"type">> => <<"json_lines">>}, Overrides).

aggregation_container_config_parquet_inline(Overrides) ->
    emqx_connector_aggregator_test_helpers:aggregation_container_config_parquet_inline(Overrides).

sample_avro_schema1() ->
    #{
        <<"name">> => <<"root">>,
        <<"type">> => <<"record">>,
        <<"fields">> =>
            [
                #{
                    <<"field-id">> => 1,
                    <<"name">> => <<"clientid">>,
                    <<"type">> => <<"string">>
                },
                #{
                    <<"field-id">> => 2,
                    <<"name">> => <<"qos">>,
                    <<"type">> => <<"int">>
                },
                #{
                    <<"field-id">> => 3,
                    <<"name">> => <<"payload">>,
                    <<"default">> => null,
                    <<"type">> => [<<"null">>, <<"string">>]
                },
                #{
                    <<"field-id">> => 4,
                    <<"name">> => <<"publish_received_at">>,
                    <<"default">> => null,
                    <<"type">> => [
                        <<"null">>,
                        #{
                            <<"type">> => <<"long">>,
                            <<"adjust-to-utc">> => false,
                            <<"logicalType">> => <<"timestamp-micros">>
                        }
                    ]
                }
            ]
    }.

run_message_sender(BridgeName, N) ->
    ClientID = integer_to_binary(N),
    Topic = <<"a/b/c/", ClientID/binary>>,
    run_message_sender(BridgeName, N, ClientID, Topic, N, 0).

run_message_sender(BridgeName, N, ClientID, Topic, Delay, NSent) ->
    Payload = integer_to_binary(N * 1_000_000 + NSent),
    Message = emqx_bridge_s3_test_helpers:mk_message_event(ClientID, Topic, Payload),
    _ = send_message(BridgeName, Message),
    receive
        {stop, From} ->
            From ! {sent, self(), NSent + 1}
    after Delay ->
        run_message_sender(BridgeName, N, ClientID, Topic, Delay, NSent + 1)
    end.

receive_sender_reports([Sender | Rest]) ->
    receive
        {sent, Sender, NSent} -> NSent + receive_sender_reports(Rest)
    end;
receive_sender_reports([]) ->
    0.

%%

mk_message({ClientId, Topic, Payload}) ->
    emqx_message:make(bin(ClientId), bin(Topic), Payload).

mk_message_event({ClientID, Topic, Payload}) ->
    emqx_bridge_s3_test_helpers:mk_message_event(ClientID, Topic, Payload).

send_messages(BridgeName, MessageEvents) ->
    lists:foreach(
        fun(M) -> send_message(BridgeName, M) end,
        MessageEvents
    ).

send_messages_delayed(BridgeName, MessageEvents, Delay) ->
    lists:foreach(
        fun(M) ->
            send_message(BridgeName, M),
            timer:sleep(Delay)
        end,
        MessageEvents
    ).

%% todo: messages should be sent via rules in tests...
send_message(BridgeName, Message) ->
    ?assertEqual(
        ok, emqx_bridge_v2:send_message(?global_ns, ?BRIDGE_TYPE, BridgeName, Message, #{})
    ).

fetch_parse_csv(Bucket, Key) ->
    #{content := Content} = emqx_bridge_s3_test_helpers:get_object(Bucket, Key),
    {ok, CSV} = erl_csv:decode(Content),
    CSV.

aggreg_id(BridgeName) ->
    {?BRIDGE_TYPE, BridgeName}.

create_connector_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(Config, Overrides)
    ).

create_action_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_action_api(Config, Overrides)
    ).

simple_create_rule_api(TCConfig) ->
    emqx_bridge_v2_testlib:simple_create_rule_api(TCConfig).

start_client(Opts) ->
    {ok, C} = emqtt:start_link(Opts),
    on_exit(fun() -> emqtt:stop(C) end),
    {ok, _} = emqtt:connect(C),
    C.

unique_payload() ->
    integer_to_binary(erlang:unique_integer()).

read_parquet(Raw) ->
    Method = post,
    URI = iolist_to_binary(lists:join("/", [?PARQUET_QUERY_ENDPOINT, "read-parquet"])),
    {ok, {{_, 200, _}, _, Body}} =
        httpc:request(Method, {URI, [], "application/octet-stream", Raw}, [], [
            {body_format, binary}
        ]),
    emqx_utils_json:decode(Body).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_start_stop(Config) ->
    emqx_bridge_v2_testlib:t_start_stop(Config, s3_bridge_stopped).

t_create_via_http(Config) ->
    emqx_bridge_v2_testlib:t_create_via_http(Config).

t_on_get_status(Config) ->
    emqx_bridge_v2_testlib:t_on_get_status(Config, #{}).

t_create_invalid_config(Config) ->
    ?assertMatch(
        {error,
            {_Status, _, #{
                <<"code">> := <<"BAD_REQUEST">>,
                <<"message">> := #{
                    <<"kind">> := <<"validation_error">>,
                    <<"reason">> := <<"Inconsistent 'min_part_size'", _/bytes>>
                }
            }}},
        emqx_bridge_v2_testlib:create_bridge_api(
            Config,
            _Overrides = #{
                <<"parameters">> => #{
                    <<"min_part_size">> => <<"5GB">>,
                    <<"max_part_size">> => <<"100MB">>
                }
            }
        )
    ).

t_create_invalid_config_key_template(Config) ->
    ?assertMatch(
        {error,
            {_Status, _, #{
                <<"code">> := <<"BAD_REQUEST">>,
                <<"message">> := #{
                    <<"kind">> := <<"validation_error">>,
                    <<"reason">> := <<"Template placeholders are disallowed:", _/bytes>>,
                    <<"path">> := <<"root.parameters.key">>
                }
            }}},
        emqx_bridge_v2_testlib:create_bridge_api(
            Config,
            _Overrides = #{
                <<"parameters">> => #{
                    <<"key">> => <<"${action}/${foo}:${bar.rfc3339}">>
                }
            }
        )
    ).

t_update_invalid_config(Config) ->
    ?assertMatch({ok, _Bridge}, emqx_bridge_v2_testlib:create_bridge(Config)),
    ?assertMatch(
        {error,
            {_Status, _, #{
                <<"code">> := <<"BAD_REQUEST">>,
                <<"message">> := #{
                    <<"kind">> := <<"validation_error">>,
                    <<"reason">> := <<"Inconsistent 'min_part_size'", _/bytes>>
                }
            }}},
        emqx_bridge_v2_testlib:update_bridge_api(
            Config,
            _Overrides = #{
                <<"parameters">> => #{
                    <<"min_part_size">> => <<"5GB">>,
                    <<"max_part_size">> => <<"100MB">>
                }
            }
        )
    ).

t_aggreg_upload(Config) ->
    Bucket = ?config(s3_bucket, Config),
    BridgeName = ?config(action_name, Config),
    AggregId = aggreg_id(BridgeName),
    BridgeNameString = unicode:characters_to_list(BridgeName),
    NodeString = atom_to_list(node()),
    %% Create a bridge with the sample configuration.
    ?assertMatch({ok, _Bridge}, emqx_bridge_v2_testlib:create_bridge(Config)),
    %% Prepare some sample messages that look like Rule SQL productions.
    MessageEvents = lists:map(fun mk_message_event/1, [
        {<<"C1">>, T1 = <<"a/b/c">>, P1 = <<"{\"hello\":\"world\"}">>},
        {<<"C2">>, T2 = <<"foo/bar">>, P2 = <<"baz">>},
        {<<"C3">>, T3 = <<"t/42">>, P3 = <<"">>}
    ]),
    ok = send_messages(BridgeName, MessageEvents),
    %% Wait until the delivery is completed.
    ?block_until(#{?snk_kind := connector_aggreg_delivery_completed, action := AggregId}),
    %% Check the uploaded objects.
    _Uploads = [#{key := Key}] = emqx_bridge_s3_test_helpers:list_objects(Bucket),
    ?assertMatch(
        [BridgeNameString, NodeString, _Datetime, _Seq = "0"],
        string:split(Key, "/", all)
    ),
    Upload = #{content := Content} = emqx_bridge_s3_test_helpers:get_object(Bucket, Key),
    ?assertMatch(
        #{content_type := "text/csv", "x-amz-meta-version" := "42"},
        Upload
    ),
    %% Verify that column order is respected.
    ?assertMatch(
        {ok, [
            ?CONF_COLUMN_ORDER(_),
            [_TS1, <<"C1">>, T1, P1, <<>> | _],
            [_TS2, <<"C2">>, T2, P2, <<>> | _],
            [_TS3, <<"C3">>, T3, P3, <<>> | _]
        ]},
        erl_csv:decode(Content)
    ).

%% Smoke test for using JSON Lines container type.
t_aggreg_upload_json_lines(Config0) ->
    Bucket = ?config(s3_bucket, Config0),
    BridgeName = ?config(action_name, Config0),
    AggregId = aggreg_id(BridgeName),
    BridgeNameString = unicode:characters_to_list(BridgeName),
    NodeString = atom_to_list(node()),
    Config = emqx_bridge_v2_testlib:proplist_update(Config0, action_config, fun(Old) ->
        Cfg = emqx_utils_maps:deep_put(
            [<<"parameters">>, <<"container">>, <<"type">>],
            Old,
            <<"json_lines">>
        ),
        emqx_utils_maps:deep_remove(
            [<<"parameters">>, <<"container">>, <<"column_order">>],
            Cfg
        )
    end),
    %% Create a bridge with the sample configuration.
    ?assertMatch({ok, _Bridge}, emqx_bridge_v2_testlib:create_bridge(Config)),
    %% Prepare some sample messages that look like Rule SQL productions.
    MessageEvents = lists:map(fun mk_message_event/1, [
        {<<"C1">>, T1 = <<"a/b/c">>, P1 = <<"{\"hello\":\"world\"}">>},
        {<<"C2">>, T2 = <<"foo/bar">>, P2 = <<"baz">>},
        {<<"C3">>, T3 = <<"t/42">>, P3 = <<"">>}
    ]),
    ok = send_messages(BridgeName, MessageEvents),
    %% Wait until the delivery is completed.
    ?block_until(#{?snk_kind := connector_aggreg_delivery_completed, action := AggregId}),
    %% Check the uploaded objects.
    _Uploads = [#{key := Key}] = emqx_bridge_s3_test_helpers:list_objects(Bucket),
    ?assertMatch(
        [BridgeNameString, NodeString, _Datetime, _Seq = "0"],
        string:split(Key, "/", all)
    ),
    Upload = #{content := Content} = emqx_bridge_s3_test_helpers:get_object(Bucket, Key),
    ?assertMatch(
        #{content_type := "application/jsonl", "x-amz-meta-version" := "42"},
        Upload
    ),
    %% Verify that column order is respected.
    ?assertMatch(
        [
            #{
                <<"clientid">> := <<"C1">>,
                <<"payload">> := P1,
                <<"topic">> := T1
            },
            #{
                <<"clientid">> := <<"C2">>,
                <<"payload">> := P2,
                <<"topic">> := T2
            },
            #{
                <<"clientid">> := <<"C3">>,
                <<"payload">> := P3,
                <<"topic">> := T3
            }
        ],
        emqx_connector_aggreg_json_lines_test_utils:decode(Content)
    ).

%% Smoke test for using Parquet container type.
t_aggreg_upload_parquet() ->
    [{matrix, true}].
t_aggreg_upload_parquet(matrix) ->
    [[?parquet]];
t_aggreg_upload_parquet(TCConfig) ->
    Bucket = get_config(s3_bucket, TCConfig),
    %% Create a bridge with the sample configuration.
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{
            <<"aggregation">> => #{
                <<"time_interval">> => <<"1s">>,
                <<"max_records">> => 5
            },
            <<"container">> => aggregation_container_config_parquet_inline(#{})
        },
        %% To ensure message order below
        <<"resource_opts">> => #{<<"worker_pool_size">> => 1}
    }),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    ClientIds = [<<"C1">>, <<"C2">>, <<"C3">>],
    Payloads = lists:map(fun(_) -> unique_payload() end, ClientIds),
    {ok, {ok, _}} =
        ?wait_async_action(
            lists:foreach(
                fun({ClientId, Payload}) ->
                    C = start_client(#{clientid => ClientId}),
                    emqtt:publish(C, Topic, Payload)
                end,
                lists:zip(ClientIds, Payloads)
            ),
            #{?snk_kind := connector_aggreg_delivery_completed, transfer := T} when T /= empty,
            5_000
        ),
    %% Check the uploaded objects.
    _Uploads = [#{key := Key}] = emqx_bridge_s3_test_helpers:list_objects(Bucket),
    ?assertMatch(
        [_BridgeNameString, _NodeString, _Datetime, _Seq = "0"],
        string:split(Key, "/", all)
    ),
    Upload = #{content := Content} = emqx_bridge_s3_test_helpers:get_object(Bucket, Key),
    ?assertMatch(
        #{content_type := "application/octet-stream", "x-amz-meta-version" := "42"},
        Upload
    ),
    %% Verify that column order is respected.
    [P1, P2, P3] = Payloads,
    ?assertMatch(
        [
            #{
                <<"clientid">> := <<"C1">>,
                <<"payload">> := P1,
                <<"publish_received_at">> := _,
                <<"qos">> := 0
            },
            #{
                <<"clientid">> := <<"C2">>,
                <<"payload">> := P2,
                <<"publish_received_at">> := _,
                <<"qos">> := 0
            },
            #{
                <<"clientid">> := <<"C3">>,
                <<"payload">> := P3,
                <<"publish_received_at">> := _,
                <<"qos">> := 0
            }
        ],
        read_parquet(Content)
    ),
    ok.

t_aggreg_upload_rule(Config) ->
    Bucket = ?config(s3_bucket, Config),
    BridgeName = ?config(action_name, Config),
    AggregId = aggreg_id(BridgeName),
    ClientID = emqx_utils_conv:bin(?FUNCTION_NAME),
    %% Create a bridge with the sample configuration and a simple SQL rule.
    ?assertMatch({ok, _Bridge}, emqx_bridge_v2_testlib:create_bridge(Config)),
    ?assertMatch(
        {ok, _Rule},
        emqx_bridge_v2_testlib:create_rule_and_action_http(?BRIDGE_TYPE, <<>>, Config, #{
            sql => <<
                "SELECT"
                "  *,"
                "  strlen(payload) as psize,"
                "  unix_ts_to_rfc3339(publish_received_at, 'millisecond') as publish_received_at"
                "  FROM 's3/#'"
            >>
        })
    ),
    ok = lists:foreach(fun emqx:publish/1, [
        emqx_message:make(?FUNCTION_NAME, T1 = <<"s3/m1">>, P1 = <<"[HELLO]">>),
        emqx_message:make(?FUNCTION_NAME, T2 = <<"s3/m2">>, P2 = <<"[WORLD]">>),
        emqx_message:make(?FUNCTION_NAME, T3 = <<"s3/empty">>, P3 = <<>>),
        emqx_message:make(?FUNCTION_NAME, <<"not/s3">>, <<"should not be here">>)
    ]),
    ?block_until(#{?snk_kind := connector_aggreg_delivery_completed, action := AggregId}),
    %% Check the uploaded objects.
    _Uploads = [#{key := Key}] = emqx_bridge_s3_test_helpers:list_objects(Bucket),
    _CSV = [Header | Rows] = fetch_parse_csv(Bucket, Key),
    %% Verify that column order is respected and event fields are preserved.
    ?assertMatch(?CONF_COLUMN_ORDER(_), Header),
    ?assertEqual(
        [<<"event">>, <<"qos">>, <<"psize">>],
        [C || C <- [<<"event">>, <<"qos">>, <<"psize">>], lists:member(C, Header)]
    ),
    %% Verify that all the matching messages are present.
    ?assertMatch(
        [
            [_TS1, ClientID, T1, P1 | _],
            [_TS2, ClientID, T2, P2 | _],
            [_TS3, ClientID, T3, P3 | _]
        ],
        Rows
    ),
    %% Verify that timestamp column now has RFC3339 format.
    [_Row = [TS1 | _] | _Rest] = Rows,
    ?assert(
        is_integer(emqx_rule_funcs:rfc3339_to_unix_ts(TS1, millisecond)),
        TS1
    ).

t_aggreg_upload_restart(Config) ->
    %% NOTE
    %% This test verifies that the bridge will reuse existing aggregation buffer
    %% after a restart.
    Bucket = ?config(s3_bucket, Config),
    BridgeName = ?config(action_name, Config),
    AggregId = aggreg_id(BridgeName),
    %% Create a bridge with the sample configuration.
    ?assertMatch({ok, _Bridge}, emqx_bridge_v2_testlib:create_bridge_api(Config)),
    %% Send some sample messages that look like Rule SQL productions.
    MessageEvents = lists:map(fun mk_message_event/1, [
        {<<"C1">>, T1 = <<"a/b/c">>, P1 = <<"{\"hello\":\"world\"}">>},
        {<<"C2">>, T2 = <<"foo/bar">>, P2 = <<"baz">>},
        {<<"C3">>, T3 = <<"t/42">>, P3 = <<"">>}
    ]),
    ok = send_messages(BridgeName, MessageEvents),
    {ok, _} = ?block_until(#{?snk_kind := connector_aggreg_records_written, action := AggregId}),
    %% Restart the bridge.
    {204, _} = emqx_bridge_v2_testlib:disable_kind_api(action, ?BRIDGE_TYPE, BridgeName),
    {204, _} = emqx_bridge_v2_testlib:enable_kind_api(action, ?BRIDGE_TYPE, BridgeName),
    %% Send some more messages (wuth same timestamps though).
    ok = send_messages(BridgeName, MessageEvents),
    {ok, _} = ?block_until(#{?snk_kind := connector_aggreg_records_written, action := AggregId}),
    %% Wait until the delivery is completed.
    {ok, _} = ?block_until(#{?snk_kind := connector_aggreg_delivery_completed, action := AggregId}),
    %% Check there's still only one upload.
    _Uploads = [#{key := Key}] = emqx_bridge_s3_test_helpers:list_objects(Bucket),
    _Upload = #{content := Content} = emqx_bridge_s3_test_helpers:get_object(Bucket, Key),
    ?assertMatch(
        {ok, [
            _Header = [_ | _],
            [_TS1, <<"C1">>, T1, P1 | _],
            [_TS2, <<"C2">>, T2, P2 | _],
            [_TS3, <<"C3">>, T3, P3 | _],
            [_TS1, <<"C1">>, T1, P1 | _],
            [_TS2, <<"C2">>, T2, P2 | _],
            [_TS3, <<"C3">>, T3, P3 | _]
        ]},
        erl_csv:decode(Content)
    ).

%% NOTE
%% This test verifies that the bridge can recover from a buffer file corruption,
%% and does so while preserving uncompromised data.
t_aggreg_upload_restart_corrupted(Config) ->
    Bucket = ?config(s3_bucket, Config),
    BridgeName = ?config(action_name, Config),
    BatchSize = ?CONF_MAX_RECORDS div 2,
    Opts = #{
        aggreg_id => aggreg_id(BridgeName),
        batch_size => BatchSize,
        rule_sql => <<
            "SELECT"
            "  *,"
            "  strlen(payload) as psize,"
            "  unix_ts_to_rfc3339(publish_received_at, 'millisecond') as publish_received_at"
            "  FROM 's3/#'"
        >>,
        make_message_fn => fun(N) ->
            mk_message(
                {integer_to_binary(N), <<"s3/a/b/c">>, <<"{\"hello\":\"world\"}">>}
            )
        end,
        message_check_fn => fun(Context) ->
            #{
                messages_before := Messages1,
                messages_after := Messages2
            } = Context,

            _Uploads = [#{key := Key}] = emqx_bridge_s3_test_helpers:list_objects(Bucket),
            CSV = [_Header | Rows] = fetch_parse_csv(Bucket, Key),
            NRows = length(Rows),
            ?assert(
                NRows > BatchSize,
                CSV
            ),
            Expected = [
                {ClientId, Topic, Payload}
             || #message{
                    from = ClientId,
                    topic = Topic,
                    payload = Payload
                } <- lists:sublist(Messages1, NRows - BatchSize) ++ Messages2
            ],
            ?assertEqual(
                Expected,
                [{ClientID, Topic, Payload} || [_TS, ClientID, Topic, Payload | _] <- Rows],
                CSV
            ),

            ok
        end
    },
    emqx_bridge_v2_testlib:t_aggreg_upload_restart_corrupted(Config, Opts),
    ok.

t_aggreg_pending_upload_restart(Config) ->
    %% NOTE
    %% This test verifies that the bridge will finish uploading a buffer file after
    %% a restart.
    Bucket = ?config(s3_bucket, Config),
    BridgeName = ?config(action_name, Config),
    AggregId = aggreg_id(BridgeName),
    %% Create a bridge with the sample configuration.
    ?assertMatch({ok, _Bridge}, emqx_bridge_v2_testlib:create_bridge_api(Config)),
    %% Send few large messages that will require multipart upload.
    %% Ensure that they span multiple batch queries.
    Payload = iolist_to_binary(lists:duplicate(128 * 1024, "PAYLOAD!")),
    Messages = [{integer_to_binary(N), <<"a/b/c">>, Payload} || N <- lists:seq(1, 10)],
    ok = send_messages_delayed(BridgeName, lists:map(fun mk_message_event/1, Messages), 10),
    %% Wait until the multipart upload is started.
    {ok, #{key := ObjectKey}} =
        ?block_until(#{?snk_kind := s3_client_multipart_started, bucket := Bucket}),
    %% Stop the bridge.
    {204, _} = emqx_bridge_v2_testlib:disable_kind_api(action, ?BRIDGE_TYPE, BridgeName),
    %% Verify that pending uploads have been gracefully aborted.
    %% NOTE: Minio does not support multipart upload listing w/o prefix.
    ?assertEqual(
        [],
        emqx_bridge_s3_test_helpers:list_pending_uploads(Bucket, ObjectKey)
    ),
    %% Restart the bridge.
    {204, _} = emqx_bridge_v2_testlib:enable_kind_api(action, ?BRIDGE_TYPE, BridgeName),
    %% Wait until the delivery is completed.
    {ok, _} = ?block_until(#{?snk_kind := connector_aggreg_delivery_completed, action := AggregId}),
    %% Check that delivery contains all the messages.
    _Uploads = [#{key := Key}] = emqx_bridge_s3_test_helpers:list_objects(Bucket),
    [_Header | Rows] = fetch_parse_csv(Bucket, Key),
    ?assertEqual(
        Messages,
        [{CID, Topic, PL} || [_TS, CID, Topic, PL | _] <- Rows]
    ).

t_aggreg_next_rotate(Config) ->
    %% NOTE
    %% This is essentially a stress test that tries to verify that buffer rotation
    %% and windowing work correctly under high rate, high concurrency conditions.
    Bucket = ?config(s3_bucket, Config),
    BridgeName = ?config(action_name, Config),
    AggregId = aggreg_id(BridgeName),
    NSenders = 4,
    %% Create a bridge with the sample configuration.
    ?assertMatch({ok, _Bridge}, emqx_bridge_v2_testlib:create_bridge(Config)),
    %% Start separate processes to send messages.
    Senders = [
        spawn_link(fun() -> run_message_sender(BridgeName, N) end)
     || N <- lists:seq(1, NSenders)
    ],
    %% Give them some time to send messages so that rotation and windowing will happen.
    ok = timer:sleep(round(?CONF_TIME_INTERVAL * 1.5)),
    %% Stop the senders.
    _ = [Sender ! {stop, self()} || Sender <- Senders],
    NSent = receive_sender_reports(Senders),
    %% Wait for the last delivery to complete.
    ok = timer:sleep(round(?CONF_TIME_INTERVAL * 0.5)),
    ?block_until(
        #{?snk_kind := connector_aggreg_delivery_completed, action := AggregId}, infinity, 0
    ),
    %% There should be at least 2 time windows of aggregated records.
    Uploads = [K || #{key := K} <- emqx_bridge_s3_test_helpers:list_objects(Bucket)],
    DTs = [DT || K <- Uploads, [_Action, _Node, DT | _] <- [string:split(K, "/", all)]],
    ?assert(
        ordsets:size(ordsets:from_list(DTs)) > 1,
        Uploads
    ),
    %% Uploads should not contain more than max allowed records.
    CSVs = [{K, fetch_parse_csv(Bucket, K)} || K <- Uploads],
    NRecords = [{K, length(CSV) - 1} || {K, CSV} <- CSVs],
    ?assertEqual(
        [],
        [{K, NR} || {K, NR} <- NRecords, NR > ?CONF_MAX_RECORDS * ?LIMIT_TOLERANCE]
    ),
    %% No message should be lost.
    ?assertEqual(
        NSent,
        lists:sum([NR || {_, NR} <- NRecords])
    ).
