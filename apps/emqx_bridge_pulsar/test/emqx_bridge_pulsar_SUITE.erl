%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_pulsar_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx/include/emqx_config.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-import(emqx_common_test_helpers, [on_exit/1]).

-define(CONNECTOR_TYPE, pulsar).
-define(CONNECTOR_TYPE_BIN, <<"pulsar">>).
-define(ACTION_TYPE, pulsar).
-define(ACTION_TYPE_BIN, <<"pulsar">>).

-define(PROXY_NAME_TCP, "pulsar_plain").
-define(PROXY_NAME_TLS, "pulsar_tls").
-define(PROXY_HOST, "toxiproxy").
-define(PROXY_PORT, 8474).

-define(local, local).
-define(cluster, cluster).
-define(tcp, tcp).
-define(tls, tls).
-define(sync, sync).
-define(async, async).

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    [
        {group, ?cluster},
        {group, ?local}
    ].

groups() ->
    emqx_bridge_v2_testlib:local_and_cluster_groups(?MODULE, ?local, ?cluster).

init_per_suite(TCConfig) ->
    TCConfig.

end_per_suite(_TCConfig) ->
    ok.

init_per_group(?local, TCConfig) ->
    reset_proxy(),
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_bridge_pulsar,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(TCConfig)}
    ),
    [
        {apps, Apps}
        | TCConfig
    ];
init_per_group(?cluster, TCConfig) ->
    reset_proxy(),
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_bridge_pulsar,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management
        ],
        #{work_dir => emqx_cth_suite:work_dir(TCConfig)}
    ),
    [
        {apps, Apps}
        | TCConfig
    ];
init_per_group(?tcp, TCConfig) ->
    Server = <<"pulsar://toxiproxy:6652">>,
    [
        {server, Server},
        {enable_tls, false},
        {proxy_name, ?PROXY_NAME_TCP},
        {proxy_host, ?PROXY_HOST},
        {proxy_port, ?PROXY_PORT}
        | TCConfig
    ];
init_per_group(?tls, TCConfig) ->
    Server = <<"pulsar+ssl://toxiproxy:6653">>,
    [
        {server, Server},
        {enable_tls, true},
        {proxy_name, ?PROXY_NAME_TLS},
        {proxy_host, ?PROXY_HOST},
        {proxy_port, ?PROXY_PORT}
        | TCConfig
    ];
init_per_group(?sync, TCConfig) ->
    [{query_mode, ?sync} | TCConfig];
init_per_group(?async, TCConfig) ->
    [{query_mode, ?async} | TCConfig];
init_per_group(_Group, TCConfig) ->
    TCConfig.

end_per_group(?local, TCConfig) ->
    Apps = get_config(apps, TCConfig),
    emqx_cth_suite:stop(Apps),
    reset_proxy(),
    ok;
end_per_group(?cluster, TCConfig) ->
    Apps = get_config(apps, TCConfig),
    emqx_cth_suite:stop(Apps),
    reset_proxy(),
    ok;
end_per_group(_Group, _TCConfig) ->
    ok.

init_per_testcase(TestCase, TCConfig) ->
    reset_proxy(),
    Path = group_path(TCConfig, no_groups),
    ct:pal(asciiart:visible($%, "~p - ~s", [Path, TestCase])),
    UniqueNum = integer_to_binary(erlang:unique_integer()),
    ConnectorName = atom_to_binary(TestCase),
    ConnectorConfig = connector_config(#{
        <<"servers">> => get_config(server, TCConfig, <<"pulsar://toxiproxy:6652">>),
        <<"ssl">> => #{
            <<"enable">> => get_config(enable_tls, TCConfig, false),
            <<"verify">> => <<"verify_none">>,
            <<"server_name_indication">> => <<"auto">>
        }
    }),
    ActionName = ConnectorName,
    PulsarTopic = <<(atom_to_binary(TestCase))/binary, UniqueNum/binary>>,
    ActionConfig = action_config(#{
        <<"connector">> => ConnectorName,
        <<"parameters">> => #{<<"pulsar_topic">> => PulsarTopic},
        <<"resource_opts">> => #{
            <<"query_mode">> => bin(get_config(query_mode, TCConfig, <<"sync">>))
        }
    }),
    Consumer = start_consumer(TestCase, PulsarTopic, TCConfig),
    snabbkaffe:start_trace(),
    [
        {bridge_kind, action},
        {connector_type, ?CONNECTOR_TYPE},
        {connector_name, ConnectorName},
        {connector_config, ConnectorConfig},
        {action_type, ?ACTION_TYPE},
        {action_name, ActionName},
        {action_config, ActionConfig},
        {consumer, Consumer}
        | TCConfig
    ].

end_per_testcase(_TestCase, TCConfig) ->
    snabbkaffe:stop(),
    stop_consumer(TCConfig),
    emqx_bridge_v2_testlib:delete_all_rules(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

connector_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my connector">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"servers">> => <<"pulsar://toxiproxy:6652">>,
        <<"authentication">> => <<"none">>,
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_connector_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check_connector(?CONNECTOR_TYPE_BIN, <<"x">>, InnerConfigMap).

action_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my action">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"parameters">> => #{
            <<"retention_period">> => <<"infinity">>,
            <<"max_batch_bytes">> => <<"1MB">>,
            <<"max_inflight">> => 100,
            <<"batch_size">> => 100,
            <<"strategy">> => <<"random">>,
            <<"buffer">> => #{
                <<"mode">> => <<"memory">>,
                <<"per_partition_limit">> => <<"10MB">>,
                <<"segment_bytes">> => <<"5MB">>,
                <<"memory_overload_protection">> => true
            },
            <<"message">> => #{
                <<"key">> => <<"${.clientid}">>,
                <<"value">> => <<"${.}">>
            },
            <<"pulsar_topic">> => <<"pulsar-topic">>
        },
        <<"resource_opts">> =>
            maps:without(
                [
                    <<"batch_size">>,
                    <<"batch_time">>,
                    <<"inflight_window">>,
                    <<"max_buffer_bytes">>,
                    <<"worker_pool_size">>
                ],
                emqx_bridge_v2_testlib:common_action_resource_opts()
            )
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check(action, ?ACTION_TYPE_BIN, <<"x">>, InnerConfigMap).

get_config(K, TCConfig) -> emqx_bridge_v2_testlib:get_value(K, TCConfig).
get_config(K, TCConfig, Default) -> proplists:get_value(K, TCConfig, Default).

bin(X) -> emqx_utils_conv:bin(X).

group_path(TCConfig, Default) ->
    case emqx_common_test_helpers:group_path(TCConfig) of
        [] -> Default;
        Path -> Path
    end.

get_tc_prop(TestCase, Key, Default) ->
    maybe
        true ?= erlang:function_exported(?MODULE, TestCase, 0),
        {Key, Val} ?= proplists:lookup(Key, ?MODULE:TestCase()),
        Val
    else
        _ -> Default
    end.

reset_proxy() ->
    emqx_common_test_helpers:reset_proxy(?PROXY_HOST, ?PROXY_PORT).

with_failure(FailureType, TCConfig, Fn) ->
    ProxyName = get_config(proxy_name, TCConfig),
    emqx_common_test_helpers:with_failure(FailureType, ProxyName, ?PROXY_HOST, ?PROXY_PORT, Fn).

enable_failure(FailureType, TCConfig) ->
    ProxyName = get_config(proxy_name, TCConfig),
    emqx_common_test_helpers:enable_failure(FailureType, ProxyName, ?PROXY_HOST, ?PROXY_PORT).

heal_failure(FailureType, TCConfig) ->
    ProxyName = get_config(proxy_name, TCConfig),
    emqx_common_test_helpers:heal_failure(FailureType, ProxyName, ?PROXY_HOST, ?PROXY_PORT).

start_consumer(TestCase, PulsarTopic, TCConfig) ->
    Server = get_config(server, TCConfig, <<"pulsar://toxiproxy:6652">>),
    IsTLS = get_config(enable_tls, TCConfig, false),
    ConsumerClientId = list_to_atom(
        atom_to_list(TestCase) ++ integer_to_list(erlang:unique_integer())
    ),
    CertsPath = emqx_common_test_helpers:deps_path(emqx, "etc/certs"),
    SSLOpts = #{
        enable => IsTLS,
        keyfile => filename:join([CertsPath, "key.pem"]),
        certfile => filename:join([CertsPath, "cert.pem"]),
        cacertfile => filename:join([CertsPath, "cacert.pem"])
    },
    Opts = #{enable_ssl => IsTLS, ssl_opts => emqx_tls_lib:to_client_opts(SSLOpts)},
    {ok, _} = pulsar:ensure_supervised_client(ConsumerClientId, [Server], Opts),
    ConsumerOpts = Opts#{
        cb_init_args => #{send_to => self()},
        cb_module => pulsar_echo_consumer,
        sub_type => 'Shared',
        subscription => atom_to_list(TestCase) ++ integer_to_list(erlang:unique_integer()),
        max_consumer_num => 1,
        %% Note!  This must not coincide with the client
        %% id, or else weird bugs will happen, like the
        %% consumer never starts...
        name => list_to_atom("test_consumer" ++ integer_to_list(erlang:unique_integer())),
        consumer_id => 1
    },
    {ok, Consumer} = pulsar:ensure_supervised_consumers(
        ConsumerClientId,
        PulsarTopic,
        ConsumerOpts
    ),
    %% since connection is async, and there's currently no way to
    %% specify the subscription initial position as `Earliest', we
    %% need to wait until the consumer is connected to avoid
    %% flakiness.
    ok = wait_until_consumer_connected(Consumer),
    #{consumer_client_id => ConsumerClientId, consumer => Consumer}.

stop_consumer(TCConfig) ->
    #{
        consumer_client_id := ConsumerClientId,
        consumer := Consumer
    } = get_config(consumer, TCConfig),
    ok = pulsar:stop_and_delete_supervised_consumers(Consumer),
    ok = pulsar:stop_and_delete_supervised_client(ConsumerClientId),
    ok.

wait_until_consumer_connected(Consumer) ->
    ?retry(
        _Sleep = 300,
        _Attempts0 = 20,
        true = pulsar_consumers:all_connected(Consumer)
    ),
    ok.

receive_consumed() ->
    receive_consumed(_Timeout = 5_000).

receive_consumed(Timeout) ->
    receive
        {pulsar_message, #{payloads := Payloads}} ->
            lists:map(fun try_decode_json/1, Payloads)
    after Timeout ->
        ct:pal("mailbox: ~p", [process_info(self(), messages)]),
        ct:fail("no message consumed")
    end.

receive_consumed_until_seqno(SeqNo, Timeout) ->
    do_receive_consumed_until_seqno(SeqNo, Timeout, _Acc = #{}).

do_receive_consumed_until_seqno(SeqNo, _Timeout, Acc) when map_size(Acc) == SeqNo ->
    maps:values(Acc);
do_receive_consumed_until_seqno(SeqNo, Timeout, Acc0) ->
    receive
        {pulsar_message, #{payloads := Payloads0}} ->
            Payloads = lists:map(fun try_decode_json/1, Payloads0),
            %% Pulsar apparently may resend some duplicate messages during/after
            %% connectivity problems.
            Acc = lists:foldl(
                fun(#{<<"payload">> := N} = P, AccIn) ->
                    AccIn#{N => P}
                end,
                Acc0,
                Payloads
            ),
            do_receive_consumed_until_seqno(SeqNo, Timeout, Acc)
    after Timeout ->
        maps:values(Acc0)
    end.

flush_consumed() ->
    receive
        {pulsar_message, _} -> flush_consumed()
    after 0 -> ok
    end.

try_decode_json(Payload) ->
    case emqx_utils_json:safe_decode(Payload) of
        {error, _} ->
            Payload;
        {ok, JSON} ->
            JSON
    end.

get_pulsar_producers() ->
    SupMod = pulsar_producers_sup,
    Mod = pulsar_producer,
    [
        P
     || {_Name, SupPid, _Type, _Mods} <- supervisor:which_children(SupMod),
        P <- element(2, process_info(SupPid, links)),
        case proc_lib:initial_call(P) of
            {Mod, init, _} -> true;
            _ -> false
        end
    ].

wait_until_producer_connected() ->
    Pids = get_pulsar_producers(),
    ?retry(
        _Sleep = 300,
        _Attempts0 = 20,
        begin
            true = length(Pids) > 0,
            lists:foreach(fun(P) -> {connected, _} = sys:get_state(P) end, Pids)
        end
    ),
    ok.

kill_resource_managers() ->
    ct:pal("gonna kill resource managers"),
    lists:foreach(
        fun({_, Pid, _, _}) ->
            ct:pal("terminating resource manager ~p", [Pid]),
            Ref = monitor(process, Pid),
            exit(Pid, kill),
            receive
                {'DOWN', Ref, process, Pid, killed} ->
                    ok
            after 500 ->
                ct:fail("pid ~p didn't die!", [Pid])
            end,
            ok
        end,
        supervisor:which_children(emqx_resource_manager_sup)
    ).

cluster(TestCase, TCConfig) ->
    AppSpecs = [
        emqx,
        emqx_conf,
        emqx_bridge_pulsar,
        emqx_bridge,
        emqx_rule_engine,
        emqx_management
    ],
    Nodes = emqx_cth_cluster:start(
        [
            {emqx_bridge_pulsar_impl_producer1, #{
                apps => AppSpecs ++ [emqx_mgmt_api_test_util:emqx_dashboard()]
            }},
            {emqx_bridge_pulsar_impl_producer2, #{apps => AppSpecs}}
        ],
        #{work_dir => emqx_cth_suite:work_dir(TestCase, TCConfig)}
    ),
    ok = on_exit(fun() -> emqx_cth_cluster:stop(Nodes) end),
    Nodes.

get_mqtt_port(Node) ->
    {_IP, Port} = ?ON(Node, emqx_config:get([listeners, tcp, default, bind])),
    Port.

create_connector_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(TCConfig, Overrides)
    ).

create_action_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_action_api(TCConfig, Overrides)
    ).

get_action_metrics_api(TCConfig) ->
    emqx_bridge_v2_testlib:get_action_metrics_api(TCConfig).

get_combined_metrics(TCConfig, RuleId) ->
    ActionResId = emqx_bridge_v2_testlib:resource_id(TCConfig),
    ct:pal("action res id: ~s", [ActionResId]),
    Metrics = emqx_resource:get_metrics(ActionResId),
    RuleMetrics = emqx_metrics_worker:get_counters(rule_metrics, RuleId),
    Metrics#{rule => RuleMetrics}.

reset_combined_metrics(TCConfig, RuleId) ->
    #{
        kind := action,
        type := Type,
        name := Name
    } = emqx_bridge_v2_testlib:get_common_values(TCConfig),
    ok = emqx_bridge_v2:reset_metrics(?global_ns, actions, Type, Name),
    ok = emqx_rule_engine:reset_metrics_for_rule(RuleId),
    ok.

get_connector_api(TCConfig) ->
    #{connector_type := ConnectorType, connector_name := ConnectorName} =
        emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:get_connector_api(
            ConnectorType, ConnectorName
        )
    ).

get_action_api(TCConfig) ->
    emqx_bridge_v2_testlib:get_action_api2(TCConfig).

delete_action_api(TCConfig) ->
    #{kind := Kind, type := Type, name := Name} =
        emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2_testlib:delete_kind_api(Kind, Type, Name).

probe_action_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:probe_bridge_api(TCConfig, Overrides)
    ).

update_action_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:update_bridge_api2(Config, Overrides).

disable_action_api(TCConfig) ->
    #{kind := Kind, type := Type, name := Name} =
        emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2_testlib:disable_kind_api(Kind, Type, Name).

enable_action_api(TCConfig) ->
    #{kind := Kind, type := Type, name := Name} =
        emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2_testlib:enable_kind_api(Kind, Type, Name).

simple_create_rule_api(TCConfig) ->
    emqx_bridge_v2_testlib:simple_create_rule_api(TCConfig).

assert_status_api(Line, Name, Status, TCConfig) ->
    ?assertMatch(
        {200, #{
            <<"status">> := Status,
            <<"node_status">> := [#{<<"status">> := Status}]
        }},
        get_action_api([{action_name, Name} | TCConfig]),
        #{line => Line, name => Name, expected_status => Status}
    ).
-define(assertStatusAPI(NAME, STATUS, TCCONFIG), assert_status_api(?LINE, NAME, STATUS, TCCONFIG)).

start_client() ->
    start_client(_Opts = #{}).

start_client(Opts0) ->
    Opts = maps:merge(#{proto_ver => v5}, Opts0),
    {ok, C} = emqtt:start_link(Opts),
    on_exit(fun() -> emqtt:stop(C) end),
    {ok, _} = emqtt:connect(C),
    C.

unique_payload() ->
    integer_to_binary(erlang:unique_integer()).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_start_stop() ->
    [{matrix, true}].
t_start_stop(matrix) ->
    [[?tcp], [?tls]];
t_start_stop(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_start_stop(TCConfig, pulsar_bridge_stopped).

t_on_get_status() ->
    [{matrix, true}].
t_on_get_status(matrix) ->
    [[?tcp], [?tls]];
t_on_get_status(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_on_get_status(TCConfig, #{failure_status => ?status_connecting}).

t_rule_action() ->
    [{matrix, true}].
t_rule_action(matrix) ->
    [[?tcp, ?sync], [?tcp, ?async], [?tls, ?sync]];
t_rule_action(TCConfig) when is_list(TCConfig) ->
    PostPublishFn = fun(Context) ->
        #{payload := Payload} = Context,
        Consumed = receive_consumed(),
        ?assertMatch([#{<<"payload">> := Payload}], Consumed),
        ?assertMatch(
            {200, #{
                <<"metrics">> := #{
                    <<"matched">> := 1,
                    <<"success">> := 1,
                    <<"failed">> := 0,
                    <<"dropped">> := 0,
                    <<"late_reply">> := 0,
                    <<"received">> := 0
                }
            }},
            get_action_metrics_api(TCConfig)
        )
    end,
    Opts = #{
        post_publish_fn => PostPublishFn
    },
    emqx_bridge_v2_testlib:t_rule_action(TCConfig, Opts).

t_start_when_down() ->
    [{matrix, true}].
t_start_when_down(matrix) ->
    [[?tcp]];
t_start_when_down(TCConfig) ->
    ?check_trace(
        begin
            with_failure(down, TCConfig, fun() ->
                {201, #{<<"status">> := <<"disconnected">>}} =
                    create_connector_api(TCConfig, #{}),
                ok
            end),
            %% Should recover given enough time.
            ?retry(
                _Sleep = 500,
                _Attempts = 20,
                ?assertMatch(
                    {200, #{<<"status">> := <<"connected">>}},
                    get_connector_api(TCConfig)
                )
            ),
            ok
        end,
        []
    ),
    ok.

t_send_when_down() ->
    [{matrix, true}].
t_send_when_down(matrix) ->
    [[?tcp, ?sync], [?tcp, ?async]];
t_send_when_down(TCConfig) ->
    do_t_send_with_failure(TCConfig, down).

t_send_when_timeout() ->
    [{matrix, true}].
t_send_when_timeout(matrix) ->
    [[?tcp, ?async]];
t_send_when_timeout(TCConfig) ->
    do_t_send_with_failure(TCConfig, timeout).

do_t_send_with_failure(TCConfig, FailureType) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, #{}),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    Payload = unique_payload(),
    ?check_trace(
        begin
            with_failure(FailureType, TCConfig, fun() ->
                {_, {ok, _}} =
                    ?wait_async_action(
                        emqtt:publish(C, Topic, Payload, [{qos, 0}]),
                        #{
                            ?snk_kind := "pulsar_producer_query_enter",
                            mode := async,
                            ?snk_span := {complete, _}
                        },
                        5_000
                    ),
                ok
            end),
            ok
        end,
        fun(_Trace) ->
            %% Should recover given enough time.
            Data0 = receive_consumed(20_000),
            ?assertMatch(
                [
                    #{
                        <<"clientid">> := _ClientId,
                        <<"event">> := <<"message.publish">>,
                        <<"payload">> := Payload,
                        <<"topic">> := _MQTTTopic
                    }
                ],
                Data0
            ),
            ok
        end
    ),
    ok.

%% Check that we correctly terminate the pulsar client when the pulsar
%% producer processes fail to start for whatever reason.
t_failure_to_start_producer(TCConfig) ->
    ?check_trace(
        begin
            ?force_ordering(
                #{?snk_kind := name_registered},
                #{?snk_kind := pulsar_producer_about_to_start_producers}
            ),
            spawn_link(fun() ->
                ?tp(will_register_name, #{}),
                {ok, #{producer_name := ProducerName}} = ?block_until(
                    #{?snk_kind := pulsar_producer_capture_name}, 10_000
                ),
                true = register(ProducerName, self()),
                ?tp(name_registered, #{name => ProducerName}),
                %% Just simulating another process so that starting the
                %% producers fail.  Currently it does a gen_server:call
                %% with `infinity' timeout, so this is just to avoid
                %% hanging.
                receive
                    {'$gen_call', From, _Request} ->
                        gen_server:reply(From, {error, im_not, your_producer})
                end,
                receive
                    die -> ok
                end
            end),
            {_, {ok, _}} =
                ?wait_async_action(
                    begin
                        {201, _} = create_connector_api(TCConfig, #{}),
                        {201, _} = create_action_api(TCConfig, #{})
                    end,
                    #{?snk_kind := pulsar_bridge_producer_stopped},
                    20_000
                ),
            ok
        end,
        []
    ),
    ok.

%% Check the driver recovers itself if one of the producer processes
%% die for whatever reason.
t_producer_process_crash(TCConfig) ->
    ?check_trace(
        begin
            {201, _} = create_connector_api(TCConfig, #{}),
            {201, _} = create_action_api(TCConfig, #{
                <<"parameters">> => #{<<"buffer">> => #{<<"mode">> => <<"disk">>}}
            }),
            #{topic := Topic} = simple_create_rule_api(TCConfig),
            C = start_client(),
            [ProducerPid | _] = get_pulsar_producers(),
            Ref = monitor(process, ProducerPid),
            exit(ProducerPid, kill),
            receive
                {'DOWN', Ref, process, ProducerPid, _Killed} ->
                    ok
            after 1_000 -> ct:fail("pid didn't die")
            end,
            ?retry(
                _Sleep0 = 50,
                _Attempts0 = 50,
                ?assertEqual(
                    #{error => <<"Not connected for unknown reason">>, status => connecting},
                    emqx_bridge_v2_testlib:force_health_check(
                        emqx_bridge_v2_testlib:get_common_values(TCConfig)
                    )
                )
            ),
            ?assertMatch(
                {200, #{<<"status">> := <<"connected">>}},
                get_connector_api(TCConfig)
            ),
            %% Should recover given enough time.
            ?retry(
                _Sleep = 1_000,
                _Attempts = 20,
                ?assertMatch(
                    {200, #{<<"status">> := <<"connected">>}},
                    get_action_api(TCConfig)
                )
            ),
            Payload = unique_payload(),
            {_, {ok, _}} =
                ?wait_async_action(
                    emqtt:publish(C, Topic, Payload),
                    #{
                        ?snk_kind := "pulsar_producer_query_enter",
                        mode := async,
                        ?snk_span := {complete, _}
                    },
                    5_000
                ),
            Data0 = receive_consumed(20_000),
            ?assertMatch(
                [
                    #{
                        <<"clientid">> := _ClientId,
                        <<"event">> := <<"message.publish">>,
                        <<"payload">> := Payload,
                        <<"topic">> := Topic
                    }
                ],
                Data0
            ),
            ok
        end,
        []
    ),
    ok.

t_resource_manager_crash_after_producers_started(TCConfig) ->
    ?check_trace(
        begin
            ?force_ordering(
                #{?snk_kind := pulsar_producer_producers_allocated},
                #{?snk_kind := will_kill_resource_manager}
            ),
            ?force_ordering(
                #{?snk_kind := resource_manager_killed},
                #{?snk_kind := pulsar_producer_bridge_started}
            ),
            spawn_link(fun() ->
                ?tp(will_kill_resource_manager, #{}),
                kill_resource_managers(),
                ?tp(resource_manager_killed, #{}),
                ok
            end),
            %% even if the resource manager is dead, we can still
            %% clear the allocated resources.
            {_, {ok, _}} =
                ?wait_async_action(
                    begin
                        {201, _} = create_connector_api(TCConfig, #{}),
                        {201, _} = create_action_api(TCConfig, #{})
                    end,
                    #{?snk_kind := pulsar_bridge_stopped, instance_id := InstanceId} when
                        InstanceId =/= undefined,
                    10_000
                ),
            ok
        end,
        fun(Trace) ->
            ?assertMatch([_ | _], ?of_kind(pulsar_bridge_client_stopped, Trace)),
            ?assertMatch([_ | _], ?of_kind(pulsar_bridge_producer_stopped, Trace)),
            ok
        end
    ),
    ok.

t_resource_manager_crash_before_producers_started(TCConfig) ->
    ?check_trace(
        begin
            ?force_ordering(
                #{?snk_kind := pulsar_producer_capture_name},
                #{?snk_kind := will_kill_resource_manager}
            ),
            ?force_ordering(
                #{?snk_kind := resource_manager_killed},
                #{?snk_kind := pulsar_producer_about_to_start_producers}
            ),
            spawn_link(fun() ->
                ?tp(will_kill_resource_manager, #{}),
                kill_resource_managers(),
                ?tp(resource_manager_killed, #{}),
                ok
            end),
            %% even if the resource manager is dead, we can still
            %% clear the allocated resources.
            {_, {ok, _}} =
                ?wait_async_action(
                    begin
                        {201, _} = create_connector_api(TCConfig, #{}),
                        {201, _} = create_action_api(TCConfig, #{})
                    end,
                    #{?snk_kind := pulsar_bridge_stopped},
                    10_000
                ),
            ok
        end,
        fun(Trace) ->
            ?assertMatch([_ | _], ?of_kind(pulsar_bridge_client_stopped, Trace)),
            ok
        end
    ),
    ok.

t_strategy_key_validation(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    ?assertMatch(
        {400, #{
            <<"message">> :=
                #{
                    <<"kind">> := <<"validation_error">>,
                    <<"reason">> := <<"Message key cannot be empty", _/binary>>
                }
        }},
        create_action_api(
            TCConfig,
            #{
                <<"parameters">> => #{
                    <<"strategy">> => <<"key_dispatch">>, <<"message">> => #{<<"key">> => <<>>}
                }
            }
        )
    ),
    ?assertMatch(
        {400, #{
            <<"message">> :=
                #{
                    <<"kind">> := <<"validation_error">>,
                    <<"reason">> := <<"Message key cannot be empty", _/binary>>
                }
        }},
        probe_action_api(
            TCConfig,
            #{
                <<"parameters">> => #{
                    <<"strategy">> => <<"key_dispatch">>, <<"message">> => #{<<"key">> => <<>>}
                }
            }
        )
    ),
    ok.

t_cluster() ->
    [{?cluster, true}].
t_cluster(TCConfig) ->
    ct:timetrap({seconds, 120}),
    ?check_trace(
        begin
            Nodes = [_N1, N2 | _] = cluster(?FUNCTION_NAME, TCConfig),
            {ok, SRef1} = snabbkaffe:subscribe(
                ?match_event(#{?snk_kind := pulsar_producer_bridge_started}),
                length(Nodes),
                25_000
            ),
            Fun = fun() -> ?ON(N2, emqx_mgmt_api_test_util:auth_header_()) end,
            emqx_bridge_v2_testlib:set_auth_header_getter(Fun),
            {201, #{<<"status">> := <<"connected">>}} = create_connector_api(TCConfig, #{}),
            {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, #{}),
            {ok, _} = snabbkaffe:receive_events(SRef1),
            erpc:multicall(Nodes, fun wait_until_producer_connected/0),
            ?tp(publishing_message, #{}),
            #{topic := Topic} = simple_create_rule_api(TCConfig),
            Port = get_mqtt_port(N2),
            C = start_client(#{port => Port}),
            Payload = unique_payload(),
            emqtt:publish(C, Topic, Payload),
            Data0 = receive_consumed(30_000),
            ?assertMatch(
                [
                    #{
                        <<"clientid">> := _ClientId,
                        <<"event">> := <<"message.publish">>,
                        <<"payload">> := Payload,
                        <<"topic">> := Topic
                    }
                ],
                Data0
            ),
            ok
        end,
        []
    ),
    ok.

t_resilience() ->
    [{matrix, true}].
t_resilience(matrix) ->
    [[?tcp]];
t_resilience(TCConfig) ->
    ?check_trace(
        begin
            {201, #{<<"status">> := <<"connected">>}} = create_connector_api(TCConfig, #{}),
            {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, #{}),
            #{topic := Topic} = simple_create_rule_api(TCConfig),
            C = start_client(),
            ProduceInterval = 100,
            TestPid = self(),
            StartSequentialProducer =
                fun Go(SeqNo0) ->
                    receive
                        stop -> TestPid ! {done, SeqNo0}
                    after 0 ->
                        SeqNo = SeqNo0 + 1,
                        emqtt:publish(C, Topic, integer_to_binary(SeqNo)),
                        SeqNo rem 10 =:= 0 andalso (TestPid ! {sent, SeqNo}),
                        timer:sleep(ProduceInterval),
                        Go(SeqNo)
                    end
                end,
            SequentialProducer = spawn_link(fun() -> StartSequentialProducer(0) end),
            ct:sleep(2 * ProduceInterval),
            {ok, _} = enable_failure(down, TCConfig),
            ?retry(
                _Sleep1 = 500,
                _Attempts1 = 20,
                ?assertMatch(
                    {200, #{<<"status">> := <<"connecting">>}},
                    get_connector_api(TCConfig)
                )
            ),
            %% Note: we don't check for timeouts here because:
            %%   a) If we do trigger auto reconnect, that means that the producers were
            %%   killed and the `receive_consumed' below will fail.
            %%   b) If there's a timeout, that's the correct path; we just need to give the
            %%   resource manager a chance to do so.
            ?block_until(#{?snk_kind := resource_auto_reconnect}, 5_000),
            {ok, _} = heal_failure(down, TCConfig),
            ?retry(
                _Sleep2 = 500,
                _Attempts2 = 20,
                ?assertMatch(
                    {200, #{<<"status">> := <<"connected">>}},
                    get_connector_api(TCConfig)
                )
            ),
            ?retry(
                _Sleep2 = 500,
                _Attempts2 = 20,
                ?assertMatch(
                    {200, #{<<"status">> := <<"connected">>}},
                    get_action_api(TCConfig)
                )
            ),
            SequentialProducer ! stop,
            NumProduced =
                receive
                    {done, SeqNo} -> SeqNo
                after 1_000 -> ct:fail("producer didn't stop!")
                end,
            Consumed = receive_consumed_until_seqno(NumProduced, _Timeout = 10_000),
            ?assertEqual(
                NumProduced,
                length(Consumed),
                #{
                    num_produced => NumProduced,
                    consumed => Consumed
                }
            ),
            ExpectedPayloads = lists:map(fun integer_to_binary/1, lists:seq(1, NumProduced)),
            ConsumedPayloads = lists:sort(
                fun(ABin, BBin) -> binary_to_integer(ABin) =< binary_to_integer(BBin) end,
                lists:map(
                    fun(#{<<"payload">> := P}) -> P end,
                    Consumed
                )
            ),
            ?assertEqual(
                ExpectedPayloads,
                ConsumedPayloads,
                #{
                    missing => ExpectedPayloads -- ConsumedPayloads,
                    unexpected => ConsumedPayloads -- ExpectedPayloads
                }
            ),
            ok
        end,
        []
    ),
    ok.

%% Tests that deleting/disabling an action that share the same Pulsar topic with other
%% actions do not disturb the latter.
t_multiple_actions_sharing_topic(TCConfig) when is_list(TCConfig) ->
    ?check_trace(
        begin
            ActionName1 = <<"a1">>,
            ActionName2 = <<"a2">>,
            {201, _} = create_connector_api(TCConfig, #{}),
            {201, _} = create_action_api([{action_name, ActionName1} | TCConfig], #{}),
            {201, _} = create_action_api([{action_name, ActionName2} | TCConfig], #{}),

            ?assertStatusAPI(ActionName1, <<"connected">>, TCConfig),
            ?assertStatusAPI(ActionName2, <<"connected">>, TCConfig),

            #{topic := RuleTopic} = simple_create_rule_api([{action_name, ActionName2} | TCConfig]),
            C = start_client(),
            SendMessage = fun() ->
                ReqPayload = unique_payload(),
                {ok, _} = emqtt:publish(C, RuleTopic, #{}, ReqPayload, [
                    {qos, 1}, {retain, false}
                ]),
                ok
            end,

            %% Disabling a1 shouldn't disturb a2.
            ?assertMatch({204, _}, disable_action_api([{action_name, ActionName1} | TCConfig])),

            ?assertStatusAPI(ActionName1, <<"disconnected">>, TCConfig),
            ?assertStatusAPI(ActionName2, <<"connected">>, TCConfig),

            ?assertMatch(ok, SendMessage()),
            ?assertStatusAPI(ActionName2, <<"connected">>, TCConfig),

            ?assertMatch({204, _}, enable_action_api([{action_name, ActionName1} | TCConfig])),
            ?assertStatusAPI(ActionName1, <<"connected">>, TCConfig),
            ?assertStatusAPI(ActionName2, <<"connected">>, TCConfig),
            ?assertMatch(ok, SendMessage()),

            %% Deleting also shouldn't disrupt a2.
            ?assertMatch({204, _}, delete_action_api([{action_name, ActionName1} | TCConfig])),
            ?assertStatusAPI(ActionName2, <<"connected">>, TCConfig),
            ?assertMatch(ok, SendMessage()),

            ok
        end,
        []
    ),
    ok.

t_sync_query_down() ->
    [{matrix, true}].
t_sync_query_down(matrix) ->
    [[?tcp]];
t_sync_query_down(TCConfig) when is_list(TCConfig) ->
    ct:timetrap({seconds, 15}),
    Payload = #{<<"x">> => <<"some data">>},
    PayloadBin = emqx_utils_json:encode(Payload),
    ClientId = <<"some_client">>,
    Opts = #{
        make_message_fn => fun(Topic) -> emqx_message:make(ClientId, Topic, PayloadBin) end,
        enter_tp_filter =>
            ?match_event(#{?snk_kind := "pulsar_producer_send"}),
        error_tp_filter =>
            ?match_event(#{?snk_kind := "resource_simple_sync_internal_buffer_query_timeout"}),
        success_tp_filter =>
            ?match_event(#{?snk_kind := pulsar_echo_consumer_message})
    },
    emqx_bridge_v2_testlib:t_sync_query_down(TCConfig, Opts),
    ok.

%% Checks that we correctly handle telemetry events emitted by pulsar.
t_telemetry_metrics() ->
    [{matrix, true}].
t_telemetry_metrics(matrix) ->
    [[?tcp]];
t_telemetry_metrics(TCConfig) when is_list(TCConfig) ->
    ActionName1 = <<"a1">>,
    ActionName2 = <<"a2">>,
    TCConfigAction1 = [{action_name, ActionName1} | TCConfig],
    TCConfigAction2 = [{action_name, ActionName2} | TCConfig],
    ?check_trace(
        begin
            {201, _} = create_connector_api(TCConfig, #{}),
            {201, _} =
                create_action_api(
                    TCConfigAction1,
                    %% Initially, this will overflow on small messages
                    #{
                        <<"parameters">> => #{
                            <<"buffer">> => #{
                                <<"mode">> => <<"disk">>,
                                <<"per_partition_limit">> => <<"2B">>,
                                <<"segment_bytes">> => <<"1B">>
                            }
                        }
                    }
                ),
            {201, _} = create_action_api(TCConfigAction2, #{}),
            #{id := RuleId, topic := RuleTopic} = simple_create_rule_api(TCConfigAction1),
            C = start_client(),
            SendMessage = fun() ->
                ReqPayload = unique_payload(),
                {ok, _} = emqtt:publish(C, RuleTopic, #{}, ReqPayload, [
                    {qos, 1}, {retain, false}
                ]),
                ok
            end,
            SendMessage(),
            ?retry(
                100,
                10,
                ?assertMatch(
                    #{
                        counters := #{
                            'dropped.queue_full' := 1,
                            'dropped.expired' := 0,
                            success := 0,
                            matched := 1,
                            failed := 0,
                            received := 0
                        },
                        gauges := #{
                            inflight := 0,
                            queuing := 0,
                            queuing_bytes := 0
                        },
                        rule := #{
                            matched := 1,
                            'actions.failed' := 1,
                            'actions.failed.unknown' := 1,
                            'actions.success' := 0
                        }
                    },
                    get_combined_metrics(TCConfigAction1, RuleId)
                )
            ),
            reset_combined_metrics(TCConfigAction1, RuleId),
            %% Now to make it drop expired messages
            {200, _} =
                update_action_api(TCConfigAction1, #{
                    <<"parameters">> => #{
                        <<"retention_period">> => <<"10ms">>
                    }
                }),
            with_failure(down, TCConfig, fun() ->
                ct:sleep(500),
                SendMessage(),
                ?retry(
                    500,
                    10,
                    ?assertMatch(
                        #{
                            counters := #{
                                'dropped.queue_full' := 0,
                                'dropped.expired' := 0,
                                success := 0,
                                matched := 1,
                                failed := 0,
                                received := 0
                            },
                            gauges := #{
                                inflight := 0,
                                queuing := 1,
                                queuing_bytes := QueuingBytes1
                            }
                        } when QueuingBytes1 > 0,
                        get_combined_metrics(TCConfigAction1, RuleId)
                    )
                ),
                %% Other action is not affected by telemetry events for first action.
                ?assertMatch(
                    {200, #{
                        <<"metrics">> := #{
                            <<"dropped.queue_full">> := 0,
                            <<"dropped.expired">> := 0,
                            <<"success">> := 0,
                            <<"matched">> := 0,
                            <<"failed">> := 0,
                            <<"received">> := 0,
                            <<"inflight">> := 0,
                            <<"queuing">> := 0,
                            <<"queuing_bytes">> := 0
                        }
                    }},
                    get_action_metrics_api(TCConfigAction2)
                ),
                ok
            end),
            %% After connection is restored, the request is already expired
            ?retry(
                500,
                20,
                ?assertMatch(
                    #{
                        counters := #{
                            'dropped.queue_full' := 0,
                            'dropped.expired' := 1,
                            success := 0,
                            matched := 1,
                            failed := 0,
                            received := 0
                        },
                        gauges := #{
                            inflight := 0,
                            queuing := 0,
                            queuing_bytes := 0
                        },
                        rule := #{
                            matched := 1,
                            'actions.failed' := 1,
                            'actions.failed.unknown' := 1,
                            'actions.success' := 0
                        }
                    },
                    get_combined_metrics(TCConfigAction1, RuleId)
                )
            ),
            reset_combined_metrics(TCConfigAction1, RuleId),

            %% Now, a success.
            SendMessage(),
            ?retry(
                500,
                20,
                ?assertMatch(
                    #{
                        counters := #{
                            'dropped.queue_full' := 0,
                            'dropped.expired' := 0,
                            success := 1,
                            matched := 1,
                            failed := 0,
                            received := 0
                        },
                        gauges := #{
                            inflight := 0,
                            queuing := 0,
                            queuing_bytes := 0
                        },
                        rule := #{
                            matched := 1,
                            'actions.failed' := 0,
                            'actions.failed.unknown' := 0,
                            'actions.success' := 1
                        }
                    },
                    get_combined_metrics(TCConfigAction1, RuleId)
                )
            ),

            %% Other action is not affected by telemetry events for first action.
            ?retry(
                100,
                10,
                ?assertMatch(
                    {200, #{
                        <<"metrics">> := #{
                            <<"dropped.queue_full">> := 0,
                            <<"dropped.expired">> := 0,
                            <<"success">> := 0,
                            <<"matched">> := 0,
                            <<"failed">> := 0,
                            <<"received">> := 0,
                            <<"inflight">> := 0,
                            <<"queuing">> := 0,
                            <<"queuing_bytes">> := 0
                        }
                    }},
                    get_action_metrics_api(TCConfigAction2)
                )
            ),

            ok
        end,
        []
    ),
    ok.

%% Verifies that the `actions.failed' and `actions.failed.unknown' counters are bumped
%% when a message is dropped due to buffer overflow (both sync and async).
t_overflow_rule_metrics(TCConfig) when is_list(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} =
        create_action_api(
            TCConfig,
            #{
                <<"parameters">> => #{
                    <<"buffer">> => #{
                        <<"per_partition_limit">> => <<"2B">>,
                        <<"segment_bytes">> => <<"1B">>
                    }
                },
                <<"resource_opts">> => #{<<"query_mode">> => <<"async">>}
            }
        ),
    #{id := RuleId, topic := RuleTopic} = simple_create_rule_api(TCConfig),
    %% Async
    emqx:publish(emqx_message:make(RuleTopic, <<"aaaaaaaaaaaaaa">>)),
    ?retry(
        100,
        10,
        ?assertMatch(
            #{
                counters := #{
                    'dropped' := 1,
                    'dropped.queue_full' := 1,
                    'dropped.expired' := 0,
                    'dropped.other' := 0,
                    'success' := 0,
                    'matched' := 1,
                    'failed' := 0,
                    'received' := 0
                },
                rule := #{
                    'matched' := 1,
                    'failed' := 0,
                    'passed' := 1,
                    'actions.success' := 0,
                    'actions.failed' := 1,
                    'actions.failed.out_of_service' := 0,
                    'actions.failed.unknown' := 1,
                    'actions.discarded' := 0
                }
            },
            get_combined_metrics(TCConfig, RuleId)
        )
    ),

    %% Sync
    {200, _} =
        update_action_api(TCConfig, #{
            <<"parameters">> => #{
                <<"buffer">> => #{
                    <<"per_partition_limit">> => <<"2B">>,
                    <<"segment_bytes">> => <<"1B">>
                }
            },
            <<"resource_opts">> => #{<<"query_mode">> => <<"sync">>}
        }),
    reset_combined_metrics(TCConfig, RuleId),

    emqx:publish(emqx_message:make(RuleTopic, <<"aaaaaaaaaaaaaa">>)),
    ?retry(
        100,
        10,
        ?assertMatch(
            #{
                counters := #{
                    'dropped' := 1,
                    'dropped.queue_full' := 1,
                    'dropped.expired' := 0,
                    'dropped.other' := 0,
                    'success' := 0,
                    'matched' := 1,
                    'failed' := 0,
                    'received' := 0
                },
                rule := #{
                    'matched' := 1,
                    'failed' := 0,
                    'passed' := 1,
                    'actions.success' := 0,
                    'actions.failed' := 1,
                    'actions.failed.out_of_service' := 0,
                    'actions.failed.unknown' := 1,
                    'actions.discarded' := 0
                }
            },
            get_combined_metrics(TCConfig, RuleId)
        )
    ),

    ok.

%% Verifies that the `actions.failed' and `actions.failed.unknown' counters are bumped
%% when a message is dropped due to reaching its TTL (both sync and async).
t_expired_rule_metrics() ->
    [{matrix, true}].
t_expired_rule_metrics(matrix) ->
    [[?tcp]];
t_expired_rule_metrics(TCConfig) when is_list(TCConfig) ->
    {201, _} =
        create_connector_api(
            TCConfig,
            #{<<"resource_opts">> => #{<<"health_check_interval">> => <<"1s">>}}
        ),
    {201, _} =
        create_action_api(
            TCConfig,
            #{
                <<"parameters">> => #{<<"retention_period">> => <<"1ms">>},
                <<"resource_opts">> => #{<<"query_mode">> => <<"async">>}
            }
        ),
    #{id := RuleId, topic := RuleTopic} = simple_create_rule_api(TCConfig),
    TelemetryId = <<"expired_queue">>,
    TestPid = self(),
    telemetry:attach(
        TelemetryId,
        [pulsar, queuing],
        fun(_EventId, #{gauge_set := N}, _Metadata, _HandlerCfg) ->
            case N > 0 of
                true -> TestPid ! enqueued;
                false -> ok
            end
        end,
        unused
    ),
    on_exit(fun() -> telemetry:detach(TelemetryId) end),
    %% Helper to ensure a message is enqueued before letting it be sent to Pulsar.
    Enqueue = fun() ->
        with_failure(down, TCConfig, fun() ->
            ct:sleep(500),
            emqx:publish(emqx_message:make(RuleTopic, <<"aaaaaaaaaaaaaa">>)),
            receive
                enqueued -> ok
            after 5_000 -> ct:fail("didn't enqueue message!")
            end
        end)
    end,

    %% Async
    Enqueue(),
    ?retry(
        500,
        20,
        ?assertMatch(
            #{
                counters := #{
                    'dropped' := 1,
                    'dropped.queue_full' := 0,
                    'dropped.expired' := 1,
                    'dropped.other' := 0,
                    'success' := 0,
                    'matched' := 1,
                    'failed' := 0,
                    'received' := 0
                },
                rule := #{
                    'matched' := 1,
                    'failed' := 0,
                    'passed' := 1,
                    'actions.success' := 0,
                    'actions.failed' := 1,
                    'actions.failed.out_of_service' := 0,
                    'actions.failed.unknown' := 1,
                    'actions.discarded' := 0
                }
            },
            get_combined_metrics(TCConfig, RuleId)
        )
    ),

    %% Sync
    {200, _} =
        update_action_api(TCConfig, #{
            <<"parameters">> => #{<<"retention_period">> => <<"1ms">>},
            <<"resource_opts">> => #{<<"query_mode">> => <<"sync">>}
        }),
    reset_combined_metrics(TCConfig, RuleId),

    Enqueue(),
    ?retry(
        500,
        20,
        ?assertMatch(
            #{
                counters := #{
                    'dropped' := 1,
                    'dropped.queue_full' := 0,
                    'dropped.expired' := 1,
                    'dropped.other' := 0,
                    'success' := 0,
                    'matched' := 1,
                    'failed' := 0,
                    'received' := 0
                },
                rule := #{
                    'matched' := 1,
                    'failed' := 0,
                    'passed' := 1,
                    'actions.success' := 0,
                    'actions.failed' := 1,
                    'actions.failed.out_of_service' := 0,
                    'actions.failed.unknown' := 1,
                    'actions.discarded' := 0
                }
            },
            get_combined_metrics(TCConfig, RuleId)
        )
    ),

    ok.
