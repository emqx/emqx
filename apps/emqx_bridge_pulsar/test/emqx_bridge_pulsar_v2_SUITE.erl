%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_pulsar_v2_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

-define(TYPE, <<"pulsar">>).
-define(APPS, [emqx_conf, emqx_resource, emqx_bridge, emqx_rule_engine, emqx_bridge_pulsar]).
-define(RULE_TOPIC, "pulsar/rule").
-define(RULE_TOPIC_BIN, <<?RULE_TOPIC>>).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    [
        {group, plain},
        {group, tls}
    ].

groups() ->
    AllTCs = emqx_common_test_helpers:all(?MODULE),
    [
        {plain, AllTCs},
        {tls, AllTCs}
    ].

init_per_suite(Config) ->
    %% Ensure enterprise bridge module is loaded
    _ = emqx_bridge_enterprise:module_info(),
    {ok, Cwd} = file:get_cwd(),
    PrivDir = ?config(priv_dir, Config),
    WorkDir = emqx_utils_fs:find_relpath(filename:join(PrivDir, "ebp"), Cwd),
    Apps = emqx_cth_suite:start(
        lists:flatten([
            ?APPS,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ]),
        #{work_dir => WorkDir}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_group(plain = Type, Config) ->
    PulsarHost = os:getenv("PULSAR_PLAIN_HOST", "toxiproxy"),
    PulsarPort = list_to_integer(os:getenv("PULSAR_PLAIN_PORT", "6652")),
    ProxyName = "pulsar_plain",
    case emqx_common_test_helpers:is_tcp_server_available(PulsarHost, PulsarPort) of
        true ->
            Config1 = common_init_per_group(),
            NewConfig =
                [
                    {proxy_name, ProxyName},
                    {pulsar_host, PulsarHost},
                    {pulsar_port, PulsarPort},
                    {pulsar_type, Type},
                    {use_tls, false}
                    | Config1 ++ Config
                ],
            create_connector(?MODULE, NewConfig),
            NewConfig;
        false ->
            maybe_skip_without_ci()
    end;
init_per_group(tls = Type, Config) ->
    PulsarHost = os:getenv("PULSAR_TLS_HOST", "toxiproxy"),
    PulsarPort = list_to_integer(os:getenv("PULSAR_TLS_PORT", "6653")),
    ProxyName = "pulsar_tls",
    case emqx_common_test_helpers:is_tcp_server_available(PulsarHost, PulsarPort) of
        true ->
            Config1 = common_init_per_group(),
            NewConfig =
                [
                    {proxy_name, ProxyName},
                    {pulsar_host, PulsarHost},
                    {pulsar_port, PulsarPort},
                    {pulsar_type, Type},
                    {use_tls, true}
                    | Config1 ++ Config
                ],
            create_connector(?MODULE, NewConfig),
            NewConfig;
        false ->
            maybe_skip_without_ci()
    end.

end_per_group(Group, Config) when
    Group =:= plain;
    Group =:= tls
->
    common_end_per_group(Config),
    ok.

common_init_per_group() ->
    ProxyHost = os:getenv("PROXY_HOST", "toxiproxy"),
    ProxyPort = list_to_integer(os:getenv("PROXY_PORT", "8474")),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    UniqueNum = integer_to_binary(erlang:unique_integer()),
    MQTTTopic = <<"mqtt/topic/", UniqueNum/binary>>,
    [
        {proxy_host, ProxyHost},
        {proxy_port, ProxyPort},
        {mqtt_topic, MQTTTopic}
    ].

common_end_per_group(Config) ->
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    ok.

init_per_testcase(TestCase, Config) ->
    common_init_per_testcase(TestCase, Config).

end_per_testcase(_Testcase, Config) ->
    case proplists:get_bool(skip_does_not_apply, Config) of
        true ->
            ok;
        false ->
            ok = emqx_config:delete_override_conf_files(),
            ProxyHost = ?config(proxy_host, Config),
            ProxyPort = ?config(proxy_port, Config),
            emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
            emqx_bridge_v2_testlib:delete_all_bridges(),
            stop_consumer(Config),
            %% in CI, apparently this needs more time since the
            %% machines struggle with all the containers running...
            emqx_common_test_helpers:call_janitor(60_000),
            ok = snabbkaffe:stop(),
            flush_consumed(),
            ok
    end.

common_init_per_testcase(TestCase, Config0) ->
    ct:timetrap(timer:seconds(60)),
    emqx_bridge_v2_testlib:delete_all_bridges(),
    UniqueNum = integer_to_binary(erlang:unique_integer()),
    PulsarTopic =
        <<
            (atom_to_binary(TestCase))/binary,
            UniqueNum/binary
        >>,
    Config1 = [{pulsar_topic, PulsarTopic} | Config0],
    ConsumerConfig = start_consumer(TestCase, Config1),
    Config = ConsumerConfig ++ Config1,
    ok = snabbkaffe:start_trace(),
    Config.

create_connector(Name, Config) ->
    Connector = pulsar_connector(Config),
    {ok, _} = emqx_connector:create(?TYPE, Name, Connector).

delete_connector(Name) ->
    ok = emqx_connector:remove(?TYPE, Name).

create_action(Name, Config) ->
    Action = pulsar_action(Config),
    {ok, _} = emqx_bridge_v2:create(actions, ?TYPE, Name, Action).

delete_action(Name) ->
    ok = emqx_bridge_v2:remove(actions, ?TYPE, Name).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_action_probe(Config) ->
    Name = atom_to_binary(?FUNCTION_NAME),
    Action = pulsar_action(Config),
    {ok, Res0} = emqx_bridge_v2_testlib:probe_bridge_api(action, ?TYPE, Name, Action),
    ?assertMatch({{_, 204, _}, _, _}, Res0),
    ok.

t_action(Config) ->
    Name = atom_to_binary(?FUNCTION_NAME),
    create_action(Name, Config),
    Actions = emqx_bridge_v2:list(actions),
    Any = fun(#{name := BName}) -> BName =:= Name end,
    ?assert(lists:any(Any, Actions), Actions),
    Topic = <<"lkadfdaction">>,
    {ok, #{id := RuleId}} = emqx_rule_engine:create_rule(
        #{
            sql => <<"select * from \"", Topic/binary, "\"">>,
            id => atom_to_binary(?FUNCTION_NAME),
            actions => [<<"pulsar:", Name/binary>>],
            description => <<"bridge_v2 send msg to pulsar action">>
        }
    ),
    on_exit(fun() -> emqx_rule_engine:delete_rule(RuleId) end),
    MQTTClientID = <<"pulsar_mqtt_clientid">>,
    {ok, C1} = emqtt:start_link([{clean_start, true}, {clientid, MQTTClientID}]),
    {ok, _} = emqtt:connect(C1),
    ReqPayload = payload(),
    ReqPayloadBin = emqx_utils_json:encode(ReqPayload),
    {ok, _} = emqtt:publish(C1, Topic, #{}, ReqPayloadBin, [{qos, 1}, {retain, false}]),
    [#{<<"clientid">> := ClientID, <<"payload">> := RespPayload}] = receive_consumed(5000),
    ?assertEqual(MQTTClientID, ClientID),
    ?assertEqual(ReqPayload, emqx_utils_json:decode(RespPayload)),
    ok = emqtt:disconnect(C1),
    InstanceId = instance_id(actions, Name),
    ?retry(
        100,
        20,
        ?assertMatch(
            #{
                counters := #{
                    dropped := 0,
                    success := 1,
                    matched := 1,
                    failed := 0,
                    received := 0
                }
            },
            emqx_resource:get_metrics(InstanceId)
        )
    ),
    ok = delete_action(Name),
    ActionsAfterDelete = emqx_bridge_v2:list(actions),
    ?assertNot(lists:any(Any, ActionsAfterDelete), ActionsAfterDelete),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

pulsar_connector(Config) ->
    PulsarHost = ?config(pulsar_host, Config),
    PulsarPort = ?config(pulsar_port, Config),
    UseTLS = proplists:get_value(use_tls, Config, false),
    Name = atom_to_binary(?MODULE),
    Prefix =
        case UseTLS of
            true -> <<"pulsar+ssl://">>;
            false -> <<"pulsar://">>
        end,
    ServerURL = iolist_to_binary([
        Prefix,
        PulsarHost,
        ":",
        integer_to_binary(PulsarPort)
    ]),
    Connector = #{
        <<"connectors">> => #{
            <<"pulsar">> => #{
                Name => #{
                    <<"enable">> => true,
                    <<"ssl">> => #{
                        <<"enable">> => UseTLS,
                        <<"verify">> => <<"verify_none">>,
                        <<"server_name_indication">> => <<"auto">>
                    },
                    <<"authentication">> => <<"none">>,
                    <<"servers">> => ServerURL
                }
            }
        }
    },
    parse_and_check(<<"connectors">>, emqx_connector_schema, Connector, Name).

pulsar_action(Config) ->
    Name = atom_to_binary(?MODULE),
    Action = #{
        <<"actions">> => #{
            <<"pulsar">> => #{
                Name => #{
                    <<"connector">> => Name,
                    <<"enable">> => true,
                    <<"parameters">> => #{
                        <<"retention_period">> => <<"infinity">>,
                        <<"max_batch_bytes">> => <<"1MB">>,
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
                        <<"pulsar_topic">> => ?config(pulsar_topic, Config)
                    },
                    <<"resource_opts">> => #{
                        <<"health_check_interval">> => <<"1s">>,
                        <<"metrics_flush_interval">> => <<"300ms">>
                    }
                }
            }
        }
    },
    parse_and_check(<<"actions">>, emqx_bridge_v2_schema, Action, Name).

parse_and_check(Key, Mod, Conf, Name) ->
    ConfStr = hocon_pp:do(Conf, #{}),
    ct:pal(ConfStr),
    {ok, RawConf} = hocon:binary(ConfStr, #{format => map}),
    hocon_tconf:check_plain(Mod, RawConf, #{required => false, atom_key => false}),
    #{Key := #{<<"pulsar">> := #{Name := RetConf}}} = RawConf,
    RetConf.

instance_id(Type, Name) ->
    ConnectorId = emqx_bridge_resource:resource_id(Type, ?TYPE, Name),
    BridgeId = emqx_bridge_resource:bridge_id(?TYPE, Name),
    TypeBin =
        case Type of
            sources -> <<"source:">>;
            actions -> <<"action:">>
        end,
    <<TypeBin/binary, BridgeId/binary, ":", ConnectorId/binary>>.

start_consumer(TestCase, Config) ->
    PulsarHost = ?config(pulsar_host, Config),
    PulsarPort = ?config(pulsar_port, Config),
    PulsarTopic = ?config(pulsar_topic, Config),
    UseTLS = ?config(use_tls, Config),
    Scheme =
        case UseTLS of
            true -> <<"pulsar+ssl://">>;
            false -> <<"pulsar://">>
        end,
    URL =
        binary_to_list(
            <<Scheme/binary, (list_to_binary(PulsarHost))/binary, ":",
                (integer_to_binary(PulsarPort))/binary>>
        ),
    ConsumerClientId = list_to_atom(
        atom_to_list(TestCase) ++ integer_to_list(erlang:unique_integer())
    ),
    CertsPath = emqx_common_test_helpers:deps_path(emqx, "etc/certs"),
    SSLOpts = #{
        enable => UseTLS,
        keyfile => filename:join([CertsPath, "key.pem"]),
        certfile => filename:join([CertsPath, "cert.pem"]),
        cacertfile => filename:join([CertsPath, "cacert.pem"])
    },
    Opts = #{enable_ssl => UseTLS, ssl_opts => emqx_tls_lib:to_client_opts(SSLOpts)},
    {ok, _ClientPid} = pulsar:ensure_supervised_client(ConsumerClientId, [URL], Opts),
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
    [
        {consumer_client_id, ConsumerClientId},
        {pulsar_consumer, Consumer}
    ].

stop_consumer(Config) ->
    ConsumerClientId = ?config(consumer_client_id, Config),
    Consumer = ?config(pulsar_consumer, Config),
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

wait_until_producer_connected() ->
    wait_until_connected(pulsar_producers_sup, pulsar_producer).

wait_until_connected(SupMod, Mod) ->
    Pids = get_pids(SupMod, Mod),
    ?retry(
        _Sleep = 300,
        _Attempts0 = 20,
        begin
            true = length(Pids) > 0,
            lists:foreach(fun(P) -> {connected, _} = sys:get_state(P) end, Pids)
        end
    ),
    ok.

get_pulsar_producers() ->
    get_pids(pulsar_producers_sup, pulsar_producer).

get_pids(SupMod, Mod) ->
    [
        P
     || {_Name, SupPid, _Type, _Mods} <- supervisor:which_children(SupMod),
        P <- element(2, process_info(SupPid, links)),
        case proc_lib:initial_call(P) of
            {Mod, init, _} -> true;
            _ -> false
        end
    ].

receive_consumed(Timeout) ->
    receive
        {pulsar_message, #{payloads := Payloads}} ->
            lists:map(fun try_decode_json/1, Payloads)
    after Timeout ->
        ct:pal("mailbox: ~p", [process_info(self(), messages)]),
        ct:fail("no message consumed")
    end.

flush_consumed() ->
    receive
        {pulsar_message, _} -> flush_consumed()
    after 0 -> ok
    end.

try_decode_json(Payload) ->
    case emqx_utils_json:safe_decode(Payload, [return_maps]) of
        {error, _} ->
            Payload;
        {ok, JSON} ->
            JSON
    end.

payload() ->
    #{<<"key">> => 42, <<"data">> => <<"pulsar">>, <<"timestamp">> => 10000}.

maybe_skip_without_ci() ->
    case os:getenv("IS_CI") of
        "yes" ->
            throw(no_pulsar);
        _ ->
            {skip, no_pulsar}
    end.
