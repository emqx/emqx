%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_nats_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/file.hrl").
-include_lib("snabbkaffe/include/test_macros.hrl").
-include_lib("emqx/include/emqx_config.hrl").
-include("../src/emqx_bridge_nats.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

all() -> [{group, local}].

suite() -> [{timetrap, {seconds, 60}}].

groups() ->
    [
        {local, [], [
            t_core_publish,
            t_core_concurrent_publish,
            t_core_batch_callback_mode,
            t_invalid_query_shapes,
            t_error_classification,
            t_invalid_connector_config,
            t_core_batch_partial_failure,
            t_jetstream_publish,
            t_jetstream_batch_publish_all,
            t_jetstream_no_responders,
            t_template_error_details,
            t_publish_error_preserves_classification,
            t_reconnect,
            t_auth_user_password,
            t_auth_token,
            t_auth_nkey,
            t_tls,
            t_tls_first,
            t_creds_file_materialization,
            t_credentials_validation_edges,
            t_auth_jwt_creds,
            t_cluster_credentials_materialization
        ]}
    ].

init_per_suite(Config) ->
    case os:find_executable("nats-server") of
        false ->
            case os:getenv("IS_CI") of
                "yes" ->
                    ct:fail(nats_server_is_required_in_ci);
                _ ->
                    {skip, "nats-server executable is unavailable"}
            end;
        Executable ->
            Port = free_port(),
            Pid = start_nats(Executable, Port, true),
            wait_for_port(Port),
            Apps = emqx_cth_suite:start(
                [
                    {emqx,
                        "listeners.tcp.default.enable = false\n"
                        "listeners.ssl.default.enable = false\n"
                        "listeners.ws.default.enable = false\n"
                        "listeners.wss.default.enable = false\n"},
                    emqx_conf,
                    emqx_bridge_nats,
                    emqx_bridge,
                    emqx_rule_engine,
                    emqx_management,
                    emqx_mgmt_api_test_util:emqx_dashboard()
                ],
                #{work_dir => emqx_cth_suite:work_dir(Config)}
            ),
            [
                {apps, Apps},
                {nats_pid, Pid},
                {nats_executable, Executable},
                {nats_port, Port}
                | Config
            ]
    end.
end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)),
    stop_nats(?config(nats_pid, Config)),
    ok.
init_per_testcase(TestCase, Config) ->
    Name = atom_to_binary(TestCase),
    Port = integer_to_binary(?config(nats_port, Config)),
    ConnectorConfig = emqx_bridge_v2_testlib:parse_and_check_connector(
        ?CONNECTOR_TYPE_BIN,
        Name,
        #{
            <<"enable">> => true,
            <<"servers">> => <<"127.0.0.1:", Port/binary>>,
            <<"pool_size">> => 1,
            <<"connect_timeout">> => <<"2s">>,
            <<"authentication">> => <<"none">>,
            <<"resource_opts">> => #{<<"health_check_interval">> => <<"1s">>}
        }
    ),
    ActionConfig = emqx_bridge_v2_testlib:parse_and_check(
        action,
        ?ACTION_TYPE,
        Name,
        #{
            <<"enable">> => true,
            <<"connector">> => Name,
            <<"parameters">> => #{
                <<"subject">> => <<"emqx.events">>,
                <<"payload_template">> => <<"${.payload}">>,
                <<"headers">> => []
            },
            <<"resource_opts">> => #{
                <<"query_mode">> => <<"sync">>,
                <<"batch_size">> => 10,
                <<"batch_time">> => <<"100ms">>,
                <<"request_ttl">> => <<"60s">>
            }
        }
    ),
    on_exit(fun emqx_bridge_v2_testlib:delete_all_bridges_and_connectors/0),
    [
        {bridge_kind, action},
        {connector_type, ?CONNECTOR_TYPE},
        {connector_name, Name},
        {connector_config, ConnectorConfig},
        {action_type, ?ACTION_TYPE},
        {action_name, Name},
        {action_config, ActionConfig}
        | Config
    ].

end_per_testcase(_TestCase, _Config) ->
    emqx_common_test_helpers:call_janitor(),
    ok.

%%--------------------------------------------------------------------
%% Core NATS publishing
%%--------------------------------------------------------------------

t_core_publish(Config) ->
    {ok, Client} = nats_client(Config),
    {ok, _} = enats_client:subscribe(Client, <<"emqx.sensor/1/data">>, #{}),
    {201, _} = create_connector(Config),
    Action = #{
        <<"parameters">> => #{
            <<"subject">> => <<"emqx.${.topic}">>,
            <<"payload_template">> => <<"${.payload}">>,
            <<"headers">> => [
                #{<<"key">> => <<"x-topic">>, <<"value">> => <<"${.topic}">>}
            ]
        }
    },
    {201, _} = create_action(Config, Action),
    {ok, _} = create_rule(Config, <<"sensor/+/data">>),
    emqx:publish(emqx_message:make(<<"sensor/1/data">>, <<"hello-core">>)),
    emqx:publish(emqx_message:make(<<"sensor/1/data">>, <<"hello-core">>)),
    emqx:publish(emqx_message:make(<<"sensor/1/data">>, <<"hello-core">>)),
    lists:foreach(
        fun(_) ->
            ?assertMatch(
                {enats_client, Client,
                    {message, #{
                        subject := <<"emqx.sensor/1/data">>,
                        payload := <<"hello-core">>,
                        headers := [{<<"x-topic">>, <<"sensor/1/data">>}]
                    }}},
                receive_message()
            )
        end,
        lists:seq(1, 3)
    ),
    ok = enats_client:stop(Client).

t_core_concurrent_publish(Config) ->
    {ok, Client} = nats_client(Config),
    {ok, _} = enats_client:subscribe(Client, <<"emqx.concurrent">>, #{}),
    {201, _} = create_connector(Config),
    {201, _} = create_action(
        Config,
        #{
            <<"parameters">> => #{<<"subject">> => <<"emqx.concurrent">>},
            <<"resource_opts">> => #{
                <<"batch_size">> => 1,
                <<"batch_time">> => <<"0ms">>,
                <<"worker_pool_size">> => 16
            }
        }
    ),
    Payloads = [integer_to_binary(N) || N <- lists:seq(1, 32)],
    Parent = self(),
    _ = [
        spawn(fun() ->
            Result = emqx_bridge_v2:send_message(
                ?global_ns,
                ?ACTION_TYPE,
                proplists:get_value(action_name, Config),
                #{<<"payload">> => Payload},
                #{}
            ),
            Parent ! {sent, Payload, Result}
        end)
     || Payload <- Payloads
    ],
    Results = [
        receive
            {sent, Payload, Result} -> {Payload, Result}
        after 10000 ->
            ct:fail(concurrent_send_timeout)
        end
     || _ <- Payloads
    ],
    ?assertEqual(
        [],
        [{Payload, Result} || {Payload, Result} <- Results, not is_success(Result)]
    ),
    ?assertEqual(lists:sort(Payloads), lists:sort(receive_payloads(length(Payloads), []))),
    ok = enats_client:stop(Client).

t_core_batch_callback_mode(Config) ->
    {ok, Client} = nats_client(Config),
    {ok, _} = enats_client:subscribe(Client, <<"emqx.async">>, #{}),
    {201, _} = create_connector(Config),
    {201, _} = create_action(
        Config,
        #{
            <<"parameters">> => #{<<"subject">> => <<"emqx.async">>},
            <<"resource_opts">> => #{
                <<"query_mode">> => <<"async">>,
                <<"batch_size">> => 3,
                <<"batch_time">> => <<"1s">>,
                <<"worker_pool_size">> => 2
            }
        }
    ),
    {ok, _} = create_rule(Config, <<"sensor/+/async">>),
    ok = snabbkaffe:start_trace(),
    try
        lists:foreach(
            fun(N) ->
                emqx:publish(
                    emqx_message:make(<<"sensor/1/async">>, integer_to_binary(N))
                )
            end,
            lists:seq(1, 3)
        ),
        ?assertEqual(
            lists:sort([<<"1">>, <<"2">>, <<"3">>]),
            lists:sort(receive_payloads(3, []))
        ),
        Trace = snabbkaffe:collect_trace(),
        ?assert(
            lists:any(
                fun
                    (#{?snk_kind := call_batch_query}) -> true;
                    (_) -> false
                end,
                Trace
            )
        ),
        ?assertEqual([], ?of_kind(call_batch_query_async, Trace))
    after
        snabbkaffe:stop(),
        ok = enats_client:stop(Client)
    end.

t_invalid_query_shapes(_Config) ->
    EmptyState = #{channels => #{}},
    ?assertEqual(
        {error, {unrecoverable_error, {invalid_channel, missing}}},
        emqx_bridge_nats_connector:on_query(test, {missing, #{}}, EmptyState)
    ),
    ?assertEqual(
        {error, {unrecoverable_error, {invalid_query, malformed}}},
        emqx_bridge_nats_connector:on_query(test, malformed, EmptyState)
    ),
    ?assertEqual(
        {error, {unrecoverable_error, {invalid_batch, []}}},
        emqx_bridge_nats_connector:on_batch_query(test, [], EmptyState)
    ),
    ?assertEqual(
        {error, {unrecoverable_error, {invalid_channel, missing}}},
        emqx_bridge_nats_connector:on_batch_query(test, [{missing, #{}}], EmptyState)
    ),
    State = #{channels => #{channel => #{}}},
    ?assertEqual(
        {error, {unrecoverable_error, mixed_channels_in_batch}},
        emqx_bridge_nats_connector:on_batch_query(
            test, [{channel, #{}}, {other_channel, #{}}], State
        )
    ).

t_error_classification(_Config) ->
    meck:new(ecpool, [passthrough]),
    on_exit(fun() -> meck:unload(ecpool) end),
    meck:expect(ecpool, pick_and_do, fun(_, _, _) -> {error, get(nats_test_reason)} end),
    State = #{channels => #{channel => #{}}},
    Cases = [
        {disconnected, {error, {recoverable_error, disconnected}}},
        {reconnecting, {error, {recoverable_error, reconnecting}}},
        {closed, {error, {recoverable_error, closed}}},
        {stale_connection, {error, {recoverable_error, stale_connection}}},
        {timeout, {error, {recoverable_error, timeout}}},
        {econnrefused, {error, {recoverable_error, econnrefused}}},
        {{transport, closed}, {error, {recoverable_error, {transport, closed}}}},
        {
            {tls_upgrade_failed, bad_cert},
            {error, {recoverable_error, {tls_upgrade_failed, bad_cert}}}
        },
        {{client_exit, normal}, {error, {recoverable_error, {client_exit, normal}}}},
        {{auth, denied}, {error, {unrecoverable_error, {auth, denied}}}},
        {{protocol, bad_frame}, {error, {recoverable_error, {protocol, bad_frame}}}},
        {
            {invalid_batch_message, 1, invalid_subject},
            {error, {unrecoverable_error, {invalid_batch_message, 1, invalid_subject}}}
        },
        {
            {batch_too_large, bytes, 2, 1},
            {error, {unrecoverable_error, {batch_too_large, bytes, 2, 1}}}
        },
        {
            {disconnected, {server_error, denied}},
            {error, {unrecoverable_error, {server_error, denied}}}
        },
        {{disconnected, peer_closed}, {error, {recoverable_error, {disconnected, peer_closed}}}},
        {{payload_too_large, 10}, {error, {unrecoverable_error, {payload_too_large, 10}}}},
        {headers_not_supported, {error, {unrecoverable_error, headers_not_supported}}},
        {{invalid_subject, invalid}, {error, {unrecoverable_error, {invalid_subject, invalid}}}},
        {
            {template_error, bad_template},
            {error, {unrecoverable_error, {template_error, bad_template}}}
        },
        {{server_error, denied}, {error, {unrecoverable_error, {server_error, denied}}}},
        {
            {jetstream_unavailable, no_responders},
            {error, {recoverable_error, {jetstream_unavailable, no_responders}}}
        },
        {
            {jetstream_rejected, denied},
            {error, {unrecoverable_error, {jetstream_rejected, denied}}}
        },
        {{jetstream_error, bad_ack}, {error, {unrecoverable_error, {jetstream_error, bad_ack}}}},
        {{invalid_msg_id, bad_id}, {error, {unrecoverable_error, {invalid_msg_id, bad_id}}}},
        {
            {jetstream, unavailable, no_responders},
            {error, {recoverable_error, {jetstream, unavailable, no_responders}}}
        },
        {
            {jetstream, rejected, denied},
            {error, {unrecoverable_error, {jetstream, rejected, denied}}}
        },
        {
            {jetstream, invalid_ack, bad_ack},
            {error, {unrecoverable_error, {jetstream, invalid_ack, bad_ack}}}
        },
        {other_error, {error, {unrecoverable_error, other_error}}},
        {ecpool_empty, {error, {recoverable_error, disconnected}}}
    ],
    lists:foreach(
        fun({Reason, Expected}) ->
            put(nats_test_reason, Reason),
            ?assertEqual(Expected, emqx_bridge_nats_connector:on_query(test, {channel, #{}}, State))
        end,
        Cases
    ).

t_invalid_connector_config(_Config) ->
    ?assertMatch(
        {error, {invalid_config, {invalid_authentication, _}}},
        emqx_bridge_nats_connector:on_start(
            test,
            #{
                servers => <<"127.0.0.1:4222">>,
                authentication => #{mechanism => unsupported}
            }
        )
    ).

t_core_batch_partial_failure(Config) ->
    {ok, Client} = nats_client(Config),
    {ok, _} = enats_client:subscribe(Client, <<"emqx.>">>, #{}),
    {201, _} = create_connector(Config),
    {201, _} = create_action(
        Config,
        #{
            <<"parameters">> => #{
                <<"subject">> => <<"${.topic}">>,
                <<"payload_template">> => <<"${.payload}">>
            },
            <<"resource_opts">> => #{
                <<"batch_size">> => 3,
                <<"batch_time">> => <<"1s">>,
                <<"worker_pool_size">> => 1
            }
        }
    ),
    ActionName = proplists:get_value(action_name, Config),
    Parent = self(),
    Messages = [
        {<<"emqx.batch.one">>, <<"batch-one">>},
        {<<"bad subject">>, <<"batch-bad">>},
        {<<"emqx.batch.two">>, <<"batch-two">>}
    ],
    _ = [
        spawn(fun() ->
            Result = emqx_bridge_v2:send_message(
                ?global_ns,
                ?ACTION_TYPE,
                ActionName,
                #{<<"topic">> => Topic, <<"payload">> => Payload},
                #{}
            ),
            Parent ! {batch_result, Topic, Result}
        end)
     || {Topic, Payload} <- Messages
    ],
    Results = [
        receive
            {batch_result, Topic, Result} -> {Topic, Result}
        after 10000 ->
            ct:fail(batch_partial_failure_timeout)
        end
     || _ <- Messages
    ],
    ?assertMatch(
        {<<"bad subject">>, {error, {unrecoverable_error, {invalid_subject, _}}}},
        lists:keyfind(<<"bad subject">>, 1, Results)
    ),
    ?assertEqual(ok, proplists:get_value(<<"emqx.batch.one">>, Results)),
    ?assertEqual(ok, proplists:get_value(<<"emqx.batch.two">>, Results)),
    ?assertEqual(
        lists:sort([<<"batch-one">>, <<"batch-two">>]),
        lists:sort(receive_payloads(2, []))
    ),
    ok = enats_client:stop(Client).

is_success(ok) ->
    true;
is_success({ok, _}) ->
    true;
is_success(_) ->
    false.

%%--------------------------------------------------------------------
%% JetStream publishing
%%--------------------------------------------------------------------

t_jetstream_publish(Config) ->
    {ok, Client} = nats_client(Config),
    ok = create_stream(Client),
    {201, _} = create_connector(Config),
    Action = #{
        <<"parameters">> => #{
            <<"subject">> => <<"emqx.events">>,
            <<"payload_template">> => <<"${.payload}">>,
            <<"delivery_mode">> => <<"jetstream">>,
            <<"msg_id_template">> => <<"fixed-test-id">>,
            <<"headers">> => []
        },
        <<"resource_opts">> => #{
            <<"batch_size">> => 1,
            <<"batch_time">> => <<"0ms">>
        }
    },
    {201, _} = create_action(Config, Action),
    {ok, _} = create_rule(Config, <<"sensor/+/data">>),
    emqx:publish(emqx_message:make(<<"sensor/1/data">>, <<"hello-js">>)),
    emqx:publish(emqx_message:make(<<"sensor/1/data">>, <<"hello-js">>)),
    ok = wait_until(fun() -> stream_last_sequence(Client) =:= {ok, 1} end, 5000),
    ?assertEqual({ok, 1}, stream_last_sequence(Client)),
    ok = enats_client:stop(Client).

t_jetstream_batch_publish_all(Config) ->
    {ok, Client} = nats_client(Config),
    ok = create_stream(Client),
    {ok, InitialCount} = stream_last_sequence(Client),
    {ok, _} = enats_client:subscribe(Client, <<"emqx.events">>, #{}),
    {201, _} = create_connector(Config),
    Action = #{
        <<"parameters">> => #{
            <<"subject">> => <<"emqx.events">>,
            <<"payload_template">> => <<"${.payload}">>,
            <<"delivery_mode">> => <<"jetstream">>,
            <<"msg_id_template">> => <<>>,
            <<"headers">> => []
        },
        <<"resource_opts">> => #{
            <<"batch_size">> => 3,
            <<"batch_time">> => <<"1s">>,
            <<"worker_pool_size">> => 1
        }
    },
    {201, _} = create_action(Config, Action),
    ok = snabbkaffe:start_trace(),
    try
        Parent = self(),
        _ = [
            spawn(fun() ->
                Result = emqx_bridge_v2:send_message(
                    ?global_ns,
                    ?ACTION_TYPE,
                    proplists:get_value(action_name, Config),
                    #{<<"topic">> => <<"sensor/1/data">>, <<"payload">> => Payload},
                    #{}
                ),
                Parent ! {sent, Result}
            end)
         || Payload <- [<<"js-batch-1">>, <<"js-batch-2">>, <<"js-batch-3">>]
        ],
        [
            receive
                {sent, {ok, _}} -> ok;
                {sent, ok} -> ok
            after 5000 -> ct:fail(batch_send_timeout)
            end
         || _ <- lists:seq(1, 3)
        ],
        ok = wait_until(fun() -> stream_last_sequence(Client) =:= {ok, InitialCount + 3} end, 5000),
        ?assertEqual(
            lists:sort([<<"js-batch-1">>, <<"js-batch-2">>, <<"js-batch-3">>]),
            lists:sort(receive_payloads(3, []))
        ),
        Trace = snabbkaffe:collect_trace(),
        ?assert(
            lists:any(
                fun(#{batch := Batch}) -> length(Batch) =:= 3 end,
                ?of_kind(call_batch_query, Trace) ++ ?of_kind(call_batch_query_async, Trace)
            )
        )
    after
        snabbkaffe:stop(),
        ok = enats_client:stop(Client)
    end.

%%--------------------------------------------------------------------
%% Error and recovery handling
%%--------------------------------------------------------------------

t_jetstream_no_responders(Config) ->
    Port = free_port(),
    Nats = start_nats(?config(nats_executable, Config), Port, false),
    try
        wait_for_port(Port),
        ConnectorOverrides = #{
            <<"servers">> => iolist_to_binary(["127.0.0.1:", integer_to_list(Port)])
        },
        {ok, 201, _} = create_connector_silent(Config, ConnectorOverrides),
        {201, _} = create_action(
            Config,
            #{
                <<"parameters">> => #{
                    <<"delivery_mode">> => <<"jetstream">>,
                    <<"msg_id_template">> => <<"stable-id">>
                },
                <<"resource_opts">> => #{
                    <<"batch_size">> => 1,
                    <<"batch_time">> => <<"0ms">>,
                    <<"request_ttl">> => <<"5s">>
                }
            }
        ),
        ok = snabbkaffe:start_trace(),
        try
            Result = emqx_bridge_v2:send_message(
                ?global_ns,
                ?ACTION_TYPE,
                proplists:get_value(action_name, Config),
                #{<<"payload">> => <<"no-js">>},
                #{}
            ),
            Trace = snabbkaffe:collect_trace(),
            ?assertMatch({error, {resource_error, #{reason := timeout}}}, Result),
            ?assert(
                lists:any(
                    fun
                        (
                            #{
                                ?snk_kind := nats_connector_query_return,
                                result :=
                                    {error,
                                        {recoverable_error, {jetstream, unavailable, <<"503">>}}}
                            }
                        ) ->
                            true;
                        (_) ->
                            false
                    end,
                    Trace
                )
            )
        after
            snabbkaffe:stop()
        end
    after
        stop_nats(Nats)
    end.

t_template_error_details(Config) ->
    {201, _} = create_connector(Config),
    {201, _} = create_action(
        Config,
        #{
            <<"parameters">> => #{<<"payload_template">> => <<"${.missing.payload}">>},
            <<"resource_opts">> => #{<<"batch_size">> => 1}
        }
    ),
    Result = emqx_bridge_v2:send_message(
        ?global_ns,
        ?ACTION_TYPE,
        proplists:get_value(action_name, Config),
        #{<<"payload">> => <<"hello">>},
        #{}
    ),
    ?assertMatch(
        {error, {unrecoverable_error, {template_error, #{class := error, reason := _}}}},
        Result
    ).

t_publish_error_preserves_classification(Config) ->
    {201, _} = create_connector(Config),
    {201, _} = create_action(
        Config,
        #{<<"parameters">> => #{<<"subject">> => <<"bad subject">>}}
    ),
    Result = emqx_bridge_v2:send_message(
        ?global_ns,
        ?ACTION_TYPE,
        proplists:get_value(action_name, Config),
        #{<<"payload">> => <<"hello">>},
        #{}
    ),
    ?assertMatch(
        {error, {unrecoverable_error, {invalid_subject, _}}},
        Result
    ).

t_reconnect(Config) ->
    {ok, Client} = nats_client(Config),
    {ok, _} = enats_client:subscribe(Client, <<"emqx.events">>, #{}),
    {201, _} = create_connector(Config),
    {201, _} = create_action(Config),
    {ok, _} = create_rule(Config, <<"sensor/+/data">>),
    stop_nats(?config(nats_pid, Config)),
    receive
        {enats_client, Client, disconnected, _Reason} -> ok
    after 2000 -> ct:fail(nats_disconnect_not_observed)
    end,
    Parent = self(),
    Restart = spawn(fun() ->
        timer:sleep(200),
        Nats = start_nats(?config(nats_executable, Config), ?config(nats_port, Config), false),
        wait_for_port(?config(nats_port, Config)),
        Parent ! {nats_restarted, self()},
        receive
            stop -> stop_nats(Nats)
        end
    end),
    try
        emqx:publish(emqx_message:make(<<"sensor/1/data">>, <<"hello-during-outage">>)),
        receive
            {nats_restarted, Restart} -> ok
        after 10000 -> ct:fail(nats_restart_not_observed)
        end,
        ?assertMatch(
            {enats_client, Client, {message, #{payload := <<"hello-during-outage">>}}},
            receive_message(5000)
        )
    after
        Restart ! stop
    end,
    ok = enats_client:stop(Client).

%%--------------------------------------------------------------------
%% Authentication and TLS
%%--------------------------------------------------------------------

t_auth_user_password(Config) ->
    auth_publish_case(
        Config,
        ["--user", "alice", "--pass", "secret"],
        #{
            <<"mechanism">> => <<"user_password">>,
            <<"username">> => <<"alice">>,
            <<"password">> => <<"secret">>
        },
        #{
            mechanism => user_password,
            username => <<"alice">>,
            password => fun() -> <<"secret">> end
        }
    ).

t_auth_token(Config) ->
    auth_publish_case(
        Config,
        ["--auth", "token"],
        #{
            <<"mechanism">> => <<"token">>,
            <<"token">> => <<"token">>
        },
        #{mechanism => token, token => fun() -> <<"token">> end}
    ).

t_auth_nkey(Config) ->
    {PublicKey, PrivateKey} = crypto:generate_key(eddsa, ed25519),
    PublicNKey = enats_auth:encode_nkey_public(PublicKey),
    ConfigFile = filename:join(?config(priv_dir, Config), "nats-connector-nkey.conf"),
    ConfigText = iolist_to_binary([
        "authorization { users = [{nkey: \"", PublicNKey, "\"}] }\n"
    ]),
    ok = file:write_file(ConfigFile, ConfigText),
    Seed = encode_seed(PrivateKey),
    auth_publish_case(
        Config,
        ["-c", ConfigFile],
        #{
            <<"mechanism">> => <<"nkey">>,
            <<"nkey_seed">> => Seed
        },
        #{
            mechanism => nkey,
            public_key => PublicNKey,
            sign_fun => enats_auth:nkey_signer(PublicKey, PrivateKey)
        },
        #{},
        #{},
        fun(ConnectorBody) ->
            ?assertEqual(nomatch, binary:match(ConnectorBody, Seed)),
            ok
        end
    ).

t_tls(Config) ->
    PrivDir = ?config(priv_dir, Config),
    CertFile = filename:join(PrivDir, "nats-connector-tls.crt"),
    KeyFile = filename:join(PrivDir, "nats-connector-tls.key"),
    generate_test_certificate(CertFile, KeyFile),
    auth_publish_case(
        Config,
        ["--tls", "--tlscert", CertFile, "--tlskey", KeyFile],
        none,
        none,
        #{tls => true, ssl_opts => [{verify, verify_none}]},
        #{
            <<"ssl">> => #{
                <<"enable">> => true,
                <<"verify">> => <<"verify_peer">>,
                <<"cacertfile">> => CertFile
            }
        }
    ).

t_tls_first(Config) ->
    PrivDir = ?config(priv_dir, Config),
    CertFile = filename:join(PrivDir, "nats-connector-tls-first.crt"),
    KeyFile = filename:join(PrivDir, "nats-connector-tls-first.key"),
    ConfigFile = filename:join(PrivDir, "nats-connector-tls-first.conf"),
    generate_test_certificate(CertFile, KeyFile),
    ConfigText = iolist_to_binary([
        "tls {\n",
        "  cert_file: \"",
        CertFile,
        "\"\n",
        "  key_file: \"",
        KeyFile,
        "\"\n",
        "  handshake_first: true\n",
        "}\n"
    ]),
    ok = file:write_file(ConfigFile, ConfigText),
    auth_publish_case(
        Config,
        ["-c", ConfigFile],
        none,
        none,
        #{tls => true, tls_handshake => first, ssl_opts => [{verify, verify_none}]},
        #{
            <<"ssl">> => #{
                <<"enable">> => true,
                <<"verify">> => <<"verify_peer">>,
                <<"cacertfile">> => CertFile
            },
            <<"tls_handshake">> => <<"first">>
        }
    ).

%%--------------------------------------------------------------------
%% Credentials materialization and cluster behavior
%%--------------------------------------------------------------------

t_creds_file_materialization(_Config) ->
    Seed = encode_seed(<<1:256>>),
    Contents = iolist_to_binary([
        "-----BEGIN NATS USER JWT-----\njwt\n------END NATS USER JWT------\n",
        "-----BEGIN USER NKEY SEED-----\n",
        Seed,
        "\n------END USER NKEY SEED------\n"
    ]),
    RawConfig = #{
        <<"authentication">> => #{
            <<"mechanism">> => <<"jwt">>,
            <<"credentials_file">> => Contents
        }
    },
    Path = [<<"connectors">>, <<"nats">>, <<"creds_materialization">>],
    {ok, StoredConfig} = emqx_bridge_nats_connector:pre_config_update(
        Path, <<"creds_materialization">>, RawConfig, undefined
    ),
    StoredAuth = maps:get(<<"authentication">>, StoredConfig),
    Filename = maps:get(<<"credentials_file">>, StoredAuth),
    ?assert(is_binary(Filename)),
    ?assertEqual({ok, Contents}, file:read_file(Filename)),
    {ok, #file_info{mode = Mode}} = file:read_file_info(Filename),
    ?assertEqual(8#600, Mode band 8#777),
    {ok, StoredConfig} = emqx_bridge_nats_connector:pre_config_update(
        Path, <<"creds_materialization">>, StoredConfig, undefined
    ),
    ok = file:delete(Filename).

t_credentials_validation_edges(Config) ->
    Seed = encode_seed(<<1:256>>),
    Contents = iolist_to_binary([
        "-----BEGIN NATS USER JWT-----\njwt\n------END NATS USER JWT------\n",
        "-----BEGIN USER NKEY SEED-----\n",
        Seed,
        "\n------END USER NKEY SEED------\n"
    ]),
    Path = [<<"connectors">>, <<"nats">>, <<"credentials_edges">>],
    {ok, StoredConfig} = emqx_bridge_nats_connector:pre_config_update(
        Path,
        <<"credentials_edges">>,
        #{authentication => #{mechanism => jwt, credentials_file => Contents}},
        undefined
    ),
    #{authentication := #{credentials_file := Filename}} = StoredConfig,
    ?assert(is_list(Filename) orelse is_binary(Filename)),
    ?assertEqual({ok, Contents}, file:read_file(Filename)),
    ok = file:delete(Filename),
    InvalidContents = <<
        "-----BEGIN NATS USER JWT-----\njwt\n------END NATS USER JWT------\n",
        "-----BEGIN USER NKEY SEED-----\ninvalid\n------END USER NKEY SEED------\n"
    >>,
    ?assertMatch(
        {error, #{reason := invalid_credentials}},
        emqx_bridge_nats_connector:pre_config_update(
            Path,
            <<"credentials_edges">>,
            #{authentication => #{mechanism => jwt, credentials_file => InvalidContents}},
            undefined
        )
    ),
    ?assertMatch(
        {error, #{reason := invalid_credentials}},
        emqx_bridge_nats_connector:pre_config_update(
            Path,
            <<"credentials_edges">>,
            #{
                <<"authentication">> => #{
                    <<"mechanism">> => <<"jwt">>,
                    <<"credentials_file">> => InvalidContents
                }
            },
            undefined
        )
    ),
    ExistingPath = filename:join(?config(priv_dir, Config), "existing.creds"),
    ok = file:write_file(ExistingPath, Contents),
    ?assertEqual(
        {ok, #{authentication => #{mechanism => jwt, credentials_file => ExistingPath}}},
        emqx_bridge_nats_connector:pre_config_update(
            Path,
            <<"credentials_edges">>,
            #{authentication => #{mechanism => jwt, credentials_file => ExistingPath}},
            undefined
        )
    ),
    ok = file:delete(ExistingPath),
    ?assertEqual(
        {ok, #{other => value}},
        emqx_bridge_nats_connector:pre_config_update(
            Path, <<"credentials_edges">>, #{other => value}, undefined
        )
    ).

t_auth_jwt_creds(Config) ->
    {Fixture, ConfigFile, Credentials} = jwt_credentials_fixture(Config),
    #{user_public := UserPublic, user_private := UserPrivate, user_jwt := UserJWT} = Fixture,
    auth_publish_case(
        Config,
        ["-c", ConfigFile],
        #{
            <<"mechanism">> => <<"jwt">>,
            <<"credentials_file">> => Credentials
        },
        #{
            mechanism => jwt,
            public_key => UserPublic,
            jwt => fun() -> UserJWT end,
            sign_fun => enats_auth:nkey_signer(UserPublic, UserPrivate)
        },
        #{},
        #{},
        fun(ConnectorBody) ->
            assert_credentials_not_exposed(ConnectorBody),
            assert_credentials_file_is_gc_safe(Config, Credentials)
        end
    ).

t_cluster_credentials_materialization(Config) ->
    {_Fixture, _NatsConfigFile, Credentials} = jwt_credentials_fixture(Config),
    Name = atom_to_binary(?FUNCTION_NAME),
    ConnectorConfig = #{
        <<"enable">> => false,
        <<"servers">> => <<"127.0.0.1:4222">>,
        <<"pool_size">> => 1,
        <<"connect_timeout">> => <<"2s">>,
        <<"authentication">> => #{
            <<"mechanism">> => <<"jwt">>,
            <<"credentials_file">> => Credentials
        },
        <<"resource_opts">> => #{<<"health_check_interval">> => <<"1s">>}
    },
    Cluster = [
        {nats_credentials_1, #{apps => cluster_app_specs()}},
        {nats_credentials_2, #{apps => cluster_app_specs()}}
    ],
    Nodes = emqx_cth_cluster:start(
        Cluster,
        #{work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, Config)}
    ),
    [N1, N2] = Nodes,
    try
        ?assertEqual(
            emqx_bridge_nats_connector, ?ON(N1, emqx_connector_info:config_transform_module(nats))
        ),
        {ok, _} = ?ON(N1, emqx_connector:create(?global_ns, nats, Name, ConnectorConfig)),
        Filename1 = assert_cluster_credentials_file(N1, Name, Credentials),
        Filename2 = assert_cluster_credentials_file(N2, Name, Credentials),
        ?assertNotEqual(Filename1, Filename2),
        ok
    after
        ok = emqx_cth_cluster:stop(Nodes)
    end.

%%--------------------------------------------------------------------
%% Test helpers
%%--------------------------------------------------------------------

auth_publish_case(Config, ServerArgs, Authentication, ClientAuth) ->
    auth_publish_case(Config, ServerArgs, Authentication, ClientAuth, #{}, #{}, fun(_Body) -> ok end).

auth_publish_case(
    Config,
    ServerArgs,
    Authentication,
    ClientAuth,
    ClientOptions,
    ConnectorOverrides0
) ->
    auth_publish_case(
        Config,
        ServerArgs,
        Authentication,
        ClientAuth,
        ClientOptions,
        ConnectorOverrides0,
        fun(_Body) -> ok end
    ).

auth_publish_case(
    Config,
    ServerArgs,
    Authentication,
    ClientAuth,
    ClientOptions,
    ConnectorOverrides0,
    AfterConnector
) ->
    Port = free_port(),
    Pid = start_nats(?config(nats_executable, Config), Port, false, ServerArgs),
    wait_for_port(Port),
    try
        {ok, Client} = connect_client(
            enats_client:start_link(
                maps:merge(
                    #{
                        host => "127.0.0.1",
                        port => Port,
                        auth => ClientAuth,
                        owner => self()
                    },
                    ClientOptions
                )
            )
        ),
        ConnectorOverrides = emqx_utils_maps:deep_merge(
            #{
                <<"servers">> => iolist_to_binary(["127.0.0.1:", integer_to_list(Port)]),
                <<"authentication">> => Authentication
            },
            ConnectorOverrides0
        ),
        {ok, 201, ConnectorBody} = create_connector_silent(Config, ConnectorOverrides),
        #{<<"status">> := <<"connected">>} = emqx_utils_json:decode(ConnectorBody),
        ok = AfterConnector(ConnectorBody),
        {201, _} = create_action(Config),
        {ok, _} = enats_client:subscribe(Client, <<"emqx.events">>, #{}),
        {ok, _} = create_rule(Config, <<"sensor/+/data">>),
        emqx:publish(emqx_message:make(<<"sensor/1/data">>, <<"hello-auth">>)),
        ?assertMatch(
            {enats_client, Client, {message, #{payload := <<"hello-auth">>}}},
            receive_message(5000)
        ),
        ok = enats_client:stop(Client)
    after
        stop_nats(Pid)
    end.

assert_credentials_file_is_gc_safe(Config, Contents) ->
    Name = proplists:get_value(connector_name, Config),
    Filename = emqx:get_raw_config(
        [connectors, nats, Name, authentication, credentials_file], undefined
    ),
    ?assert(is_binary(Filename) orelse is_list(Filename)),
    ?assert(Filename =/= Contents),
    {ok, _} = emqx_tls_certfile_gc:force(),
    ?assertMatch({ok, _}, file:read_file_info(Filename)),
    ok.

assert_credentials_not_exposed(ConnectorBody) ->
    ?assertEqual(nomatch, binary:match(ConnectorBody, <<"BEGIN NATS USER JWT">>)),
    ?assertEqual(nomatch, binary:match(ConnectorBody, <<"BEGIN USER NKEY SEED">>)),
    ok.

assert_cluster_credentials_file(Node, Name, Contents) ->
    Filename = ?ON(
        Node,
        emqx:get_raw_config(
            [connectors, nats, Name, authentication, credentials_file], undefined
        )
    ),
    ?assert(is_binary(Filename) orelse is_list(Filename)),
    ?assert(Filename =/= Contents),
    ?assertEqual({ok, Contents}, ?ON(Node, file:read_file(Filename))),
    {ok, #file_info{mode = Mode}} = ?ON(Node, file:read_file_info(Filename)),
    ?assertEqual(8#600, Mode band 8#777),
    {ok, _} = ?ON(Node, emqx_tls_certfile_gc:force()),
    ?assertMatch({ok, _}, ?ON(Node, file:read_file_info(Filename))),
    Filename.

cluster_app_specs() ->
    [
        {emqx, #{before_start => fun cluster_emqx_before_start/2}},
        emqx_conf,
        emqx_auth,
        emqx_connector,
        emqx_bridge_nats,
        emqx_bridge,
        emqx_rule_engine,
        emqx_management
    ].

cluster_emqx_before_start(App, AppConfig) ->
    emqx_config:init_load(emqx_connector_schema, <<>>),
    emqx_config:add_allowed_namespaced_config_root(<<"connectors">>),
    emqx_cth_suite:inhibit_config_loader(App, AppConfig).

create_connector_silent(Config, Overrides) ->
    ConnectorConfig0 = proplists:get_value(connector_config, Config),
    ConnectorConfig = emqx_utils_maps:deep_merge(ConnectorConfig0, Overrides),
    Params = ConnectorConfig#{
        <<"type">> => ?CONNECTOR_TYPE_BIN,
        <<"name">> => proplists:get_value(connector_name, Config)
    },
    Path = emqx_mgmt_api_test_util:api_path(["connectors"]),
    Body = emqx_utils_json:encode(Params),
    Headers = [
        {"authorization", "Basic ZGVmYXVsdF9hcHBfa2V5OmRlZmF1bHRfYXBwX3NlY3JldA=="},
        {"content-type", "application/json"}
    ],
    case
        httpc:request(post, {Path, Headers, "application/json", Body}, [], [{body_format, binary}])
    of
        {ok, {{_, Code, _}, _ResponseHeaders, ResponseBody}} ->
            {ok, Code, ResponseBody};
        Error ->
            Error
    end.

create_connector(Config) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(Config, #{})
    ).

create_action(Config) -> create_action(Config, #{}).
create_action(Config, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_kind_api(Config, Overrides)
    ).

create_rule(Config, Topic) ->
    emqx_bridge_v2_testlib:create_rule_and_action_http(?ACTION_TYPE_BIN, Topic, Config, #{}).

nats_client(Config) ->
    connect_client(
        enats_client:start_link(#{
            host => "127.0.0.1",
            port => ?config(nats_port, Config),
            owner => self(),
            reconnect => true,
            reconnect_delay => 100
        })
    ).

connect_client({ok, Client}) ->
    ok = enats_client:connect(Client),
    {ok, Client}.

create_stream(Client) ->
    Body = jsx:encode(#{<<"name">> => <<"EMQX">>, <<"subjects">> => [<<"emqx.events">>]}),
    {ok, _} = enats_client:request(Client, <<"$JS.API.STREAM.CREATE.EMQX">>, Body, #{
        timeout => 2000
    }),
    ok.

stream_last_sequence(Client) ->
    {ok, #{payload := Payload}} = enats_client:request(
        Client, <<"$JS.API.STREAM.INFO.EMQX">>, <<>>, #{timeout => 2000}
    ),
    #{<<"state">> := #{<<"messages">> := Count}} = jsx:decode(Payload, [return_maps]),
    {ok, Count}.

receive_message() -> receive_message(2000).
receive_message(Timeout) ->
    receive
        {enats_client, Client, {message, _} = Message} -> {enats_client, Client, Message};
        _Other -> receive_message(Timeout)
    after Timeout -> ct:fail(nats_message_timeout)
    end.

receive_payloads(0, Acc) ->
    Acc;
receive_payloads(N, Acc) ->
    {enats_client, _Client, {message, #{payload := Payload}}} = receive_message(5000),
    receive_payloads(N - 1, [Payload | Acc]).

free_port() ->
    {ok, Socket} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}]),
    {ok, {_Address, Port}} = inet:sockname(Socket),
    gen_tcp:close(Socket),
    Port.

generate_test_certificate(CertFile, KeyFile) ->
    Dir = filename:dirname(CertFile),
    CAKeyFile = filename:join(Dir, "nats-test-ca.key"),
    CACertFile = filename:join(Dir, "nats-test-ca.crt"),
    CSRFile = filename:join(Dir, "nats-test-server.csr"),
    ExtFile = filename:join(Dir, "nats-test-server.ext"),
    ok = file:write_file(
        ExtFile,
        <<"basicConstraints=critical,CA:FALSE\n",
            "keyUsage=critical,digitalSignature,keyEncipherment\n", "extendedKeyUsage=serverAuth\n",
            "subjectAltName=IP:127.0.0.1\n">>
    ),
    Command = lists:flatten(
        io_lib:format(
            "openssl req -x509 -newkey rsa:2048 -nodes -keyout ~ts -out ~ts "
            "-subj /CN=nats-test-ca -addext basicConstraints=critical,CA:TRUE "
            "-addext keyUsage=critical,keyCertSign,cRLSign -days 1 >/dev/null 2>&1 && "
            "openssl req -new -newkey rsa:2048 -nodes -keyout ~ts -out ~ts "
            "-subj /CN=127.0.0.1 >/dev/null 2>&1 && "
            "openssl x509 -req -in ~ts -CA ~ts -CAkey ~ts -CAcreateserial "
            "-out ~ts -days 1 -sha256 -extfile ~ts >/dev/null 2>&1",
            [
                CAKeyFile,
                CACertFile,
                KeyFile,
                CSRFile,
                CSRFile,
                CACertFile,
                CAKeyFile,
                CertFile,
                ExtFile
            ]
        )
    ),
    _ = os:cmd(Command),
    {ok, ServerCert} = file:read_file(CertFile),
    {ok, CACert} = file:read_file(CACertFile),
    ok = file:write_file(CertFile, [ServerCert, CACert]),
    ok.

encode_seed(PrivateSeed) ->
    Prefix = <<(16#90 bor (16#A0 bsr 5)), ((16#A0 band 31) bsl 3), PrivateSeed/binary>>,
    encode_base32(<<Prefix/binary, (test_crc16(Prefix)):16/little>>).

test_crc16(Bin) ->
    test_crc16(Bin, 0).
test_crc16(<<>>, Crc) ->
    Crc;
test_crc16(<<Byte, Rest/binary>>, Crc0) ->
    Crc1 = Crc0 bxor (Byte bsl 8),
    test_crc16(Rest, test_crc_byte(Crc1, 8)).

test_crc_byte(Crc, 0) ->
    Crc band 16#FFFF;
test_crc_byte(Crc, N) when Crc band 16#8000 =/= 0 ->
    test_crc_byte(((Crc bsl 1) bxor 16#1021) band 16#FFFF, N - 1);
test_crc_byte(Crc, N) ->
    test_crc_byte((Crc bsl 1) band 16#FFFF, N - 1).

encode_base32(Bits) ->
    encode_base32(Bits, []).

encode_base32(<<Value:5, Rest/bitstring>>, Acc) ->
    encode_base32(Rest, [lists:nth(Value + 1, "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567") | Acc]);
encode_base32(Bits, Acc) when bit_size(Bits) > 0 ->
    Size = bit_size(Bits),
    <<Value:Size>> = Bits,
    Padded = Value bsl (5 - Size),
    encode_base32(<<>>, [lists:nth(Padded + 1, "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567") | Acc]);
encode_base32(<<>>, Acc) ->
    list_to_binary(lists:reverse(Acc)).

jwt_credentials_fixture(Config) ->
    OperatorPrivate =
        <<1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
            26, 27, 28, 29, 30, 31, 32>>,
    AccountPrivate =
        <<33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54,
            55, 56, 57, 58, 59, 60, 61, 62, 63, 64>>,
    OperatorPublic = public_nkey(16#70, OperatorPrivate),
    AccountPublic = public_nkey(16#00, AccountPrivate),
    AccountJWT = sign_jwt(
        #{
            <<"iss">> => OperatorPublic,
            <<"sub">> => AccountPublic,
            <<"iat">> => erlang:system_time(second),
            <<"nats">> => #{
                <<"type">> => <<"account">>,
                <<"version">> => 2,
                <<"limits">> => #{
                    <<"subs">> => -1,
                    <<"data">> => -1,
                    <<"payload">> => -1,
                    <<"imports">> => -1,
                    <<"exports">> => -1,
                    <<"conn">> => -1,
                    <<"leaf">> => -1,
                    <<"wildcards">> => true
                },
                <<"default_permissions">> => #{
                    <<"pub">> => #{},
                    <<"sub">> => #{}
                }
            }
        },
        OperatorPrivate
    ),
    {_UserPublicRaw, UserPrivate} = crypto:generate_key(eddsa, ed25519),
    UserPublic = public_nkey(16#A0, UserPrivate),
    UserJWT = sign_jwt(
        #{
            <<"iss">> => AccountPublic,
            <<"sub">> => UserPublic,
            <<"iat">> => erlang:system_time(second),
            <<"nats">> => #{
                <<"type">> => <<"user">>,
                <<"version">> => 2,
                <<"subs">> => -1,
                <<"data">> => -1,
                <<"payload">> => -1,
                <<"pub">> => #{<<"allow">> => [<<"emqx.events">>]},
                <<"sub">> => #{<<"allow">> => [<<"emqx.events">>, <<"_INBOX.>">>]}
            }
        },
        AccountPrivate
    ),
    Seed = encode_seed(UserPrivate),
    Credentials = iolist_to_binary([
        "-----BEGIN NATS USER JWT-----\n",
        UserJWT,
        "\n------END NATS USER JWT------\n",
        "-----BEGIN USER NKEY SEED-----\n",
        Seed,
        "\n------END USER NKEY SEED------\n"
    ]),
    ConfigFile = filename:join(?config(priv_dir, Config), "nats-connector-jwt.conf"),
    ConfigText = iolist_to_binary([
        "operator: ",
        build_operator_jwt(OperatorPublic, OperatorPrivate),
        "\n",
        "resolver: MEMORY\n",
        "resolver_preload: {\n  ",
        AccountPublic,
        ": ",
        AccountJWT,
        "\n}\n"
    ]),
    ok = file:write_file(ConfigFile, ConfigText),
    {
        #{
            operator_public => OperatorPublic,
            account_public => AccountPublic,
            user_public => UserPublic,
            user_private => UserPrivate,
            user_jwt => UserJWT
        },
        ConfigFile,
        Credentials
    }.

build_operator_jwt(OperatorPublic, OperatorPrivate) ->
    sign_jwt(
        #{
            <<"iss">> => OperatorPublic,
            <<"sub">> => OperatorPublic,
            <<"iat">> => erlang:system_time(second),
            <<"nats">> => #{<<"type">> => <<"operator">>, <<"version">> => 2}
        },
        OperatorPrivate
    ).

public_nkey(Prefix, PrivateKey) ->
    {PublicKey, _} = crypto:generate_key(eddsa, ed25519, PrivateKey),
    Payload = <<Prefix:8, PublicKey/binary>>,
    encode_nkey(<<Payload/binary, (test_crc16(Payload)):16/little>>).

encode_nkey(Bin) ->
    encode_base32(Bin).

sign_jwt(Claims, PrivateKey) ->
    Header = base64url_encode(
        emqx_utils_json:encode(#{alg => <<"ed25519-nkey">>, typ => <<"JWT">>})
    ),
    Payload = base64url_encode(emqx_utils_json:encode(Claims)),
    SigningInput = <<Header/binary, ".", Payload/binary>>,
    Signature = base64url_encode(crypto:sign(eddsa, none, SigningInput, [PrivateKey, ed25519])),
    <<SigningInput/binary, ".", Signature/binary>>.

base64url_encode(Bin) ->
    base64:encode(Bin, #{mode => urlsafe, padding => false}).

start_nats(Executable, Port, JetStream) ->
    start_nats(Executable, Port, JetStream, []).

start_nats(Executable, Port, JetStream, ExtraArgs) ->
    PidFile = filename:join(
        "/tmp", "emqx-nats-" ++ integer_to_list(erlang:unique_integer([positive])) ++ ".pid"
    ),
    Args =
        ["-a", "127.0.0.1", "-p", integer_to_list(Port), "-P", PidFile] ++ ExtraArgs ++
            case JetStream of
                true ->
                    StoreDir = filename:join(
                        "/tmp", "emqx-nats-" ++ integer_to_list(erlang:unique_integer([positive]))
                    ),
                    filelib:ensure_dir(filename:join(StoreDir, "placeholder")),
                    ["-js", "-sd", StoreDir];
                false ->
                    []
            end,
    {open_port({spawn_executable, Executable}, [{args, Args}, exit_status]), Port, PidFile}.

stop_nats({PortHandle, _Port, PidFile}) when is_port(PortHandle) ->
    case file:read_file(PidFile) of
        {ok, PidBin} -> os:cmd("kill -KILL " ++ string:trim(binary_to_list(PidBin)));
        {error, _} -> ok
    end,
    %% Do not synchronously close the open_port here.  On some systems
    %% port_close/1 waits for the nats-server port program and can delay the
    %% restart until the action request TTL expires.
    _ = file:delete(PidFile),
    ok;
stop_nats(_) ->
    ok.

wait_for_port(Port) ->
    wait_for_port(Port, 100).

wait_for_port(_Port, 0) ->
    ct:fail(nats_port_not_ready);
wait_for_port(Port, Attempts) ->
    case gen_tcp:connect("127.0.0.1", Port, [], 100) of
        {ok, Socket} ->
            gen_tcp:close(Socket),
            ok;
        {error, _} ->
            timer:sleep(50),
            wait_for_port(Port, Attempts - 1)
    end.

wait_until(_Pred, Timeout) when Timeout =< 0 ->
    ct:fail(wait_until_timeout);
wait_until(Pred, Timeout) ->
    case catch Pred() of
        true ->
            ok;
        _ ->
            timer:sleep(50),
            wait_until(Pred, Timeout - 50)
    end.
