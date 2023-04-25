%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_kafka_impl_producer_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("brod/include/brod.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

-define(PRODUCER, emqx_bridge_kafka_impl_producer).

%%------------------------------------------------------------------------------
%% Things for REST API tests
%%------------------------------------------------------------------------------

-import(
    emqx_common_test_http,
    [
        request_api/3,
        request_api/5,
        get_http_data/1
    ]
).

-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx.hrl").
-include("emqx_dashboard.hrl").

-define(HOST, "http://127.0.0.1:18083").

%% -define(API_VERSION, "v5").

-define(BASE_PATH, "/api/v5").

-define(BRIDGE_TYPE, "kafka_producer").

-define(APPS, [emqx_resource, emqx_bridge, emqx_rule_engine, emqx_bridge_kafka]).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    [
        {group, on_query},
        {group, on_query_async}
    ].

groups() ->
    All = emqx_common_test_helpers:all(?MODULE),
    [{on_query, All}, {on_query_async, All}].

wait_until_kafka_is_up() ->
    wait_until_kafka_is_up(0).

wait_until_kafka_is_up(300) ->
    ct:fail("Kafka is not up even though we have waited for a while");
wait_until_kafka_is_up(Attempts) ->
    KafkaTopic = "test-topic-one-partition",
    case resolve_kafka_offset(kafka_hosts(), KafkaTopic, 0) of
        {ok, _} ->
            ok;
        _ ->
            timer:sleep(1000),
            wait_until_kafka_is_up(Attempts + 1)
    end.

init_per_suite(Config) ->
    %% ensure loaded
    _ = application:load(emqx_ee_bridge),
    _ = emqx_ee_bridge:module_info(),
    application:load(emqx_bridge),
    ok = emqx_common_test_helpers:start_apps([emqx_conf]),
    ok = emqx_connector_test_helpers:start_apps(?APPS),
    {ok, _} = application:ensure_all_started(emqx_connector),
    emqx_mgmt_api_test_util:init_suite(),
    wait_until_kafka_is_up(),
    %% Wait until bridges API is up
    (fun WaitUntilRestApiUp() ->
        case show(http_get(["bridges"])) of
            {ok, 200, _Res} ->
                ok;
            Val ->
                ct:pal("REST API for bridges not up. Wait and try again. Response: ~p", [Val]),
                timer:sleep(1000),
                WaitUntilRestApiUp()
        end
    end)(),
    Config.

end_per_suite(_Config) ->
    emqx_mgmt_api_test_util:end_suite(),
    ok = emqx_common_test_helpers:stop_apps([emqx_conf]),
    ok = emqx_connector_test_helpers:stop_apps(lists:reverse(?APPS)),
    _ = application:stop(emqx_connector),
    ok.

init_per_group(GroupName, Config) ->
    [{query_api, GroupName} | Config].

end_per_group(_, _) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    delete_all_bridges(),
    emqx_common_test_helpers:call_janitor(60_000),
    ok.

set_special_configs(emqx_management) ->
    Listeners = #{http => #{port => 8081}},
    Config = #{
        listeners => Listeners,
        applications => [#{id => "admin", secret => "public"}]
    },
    emqx_config:put([emqx_management], Config),
    ok;
set_special_configs(emqx_dashboard) ->
    emqx_dashboard_api_test_helpers:set_default_config(),
    ok;
set_special_configs(_) ->
    ok.
%%------------------------------------------------------------------------------
%% Test cases for all combinations of SSL, no SSL and authentication types
%%------------------------------------------------------------------------------

t_publish_no_auth(CtConfig) ->
    publish_with_and_without_ssl(CtConfig, "none").

t_publish_no_auth_key_dispatch(CtConfig) ->
    publish_with_and_without_ssl(CtConfig, "none", #{"partition_strategy" => "key_dispatch"}).

t_publish_sasl_plain(CtConfig) ->
    publish_with_and_without_ssl(CtConfig, valid_sasl_plain_settings()).

t_publish_sasl_scram256(CtConfig) ->
    publish_with_and_without_ssl(CtConfig, valid_sasl_scram256_settings()).

t_publish_sasl_scram512(CtConfig) ->
    publish_with_and_without_ssl(CtConfig, valid_sasl_scram512_settings()).

t_publish_sasl_kerberos(CtConfig) ->
    publish_with_and_without_ssl(CtConfig, valid_sasl_kerberos_settings()).

%%------------------------------------------------------------------------------
%% Test cases for REST api
%%------------------------------------------------------------------------------

show(X) ->
    % erlang:display('______________ SHOW ______________:'),
    % erlang:display(X),
    X.

t_kafka_bridge_rest_api_plain_text(_CtConfig) ->
    kafka_bridge_rest_api_all_auth_methods(false).

t_kafka_bridge_rest_api_ssl(_CtConfig) ->
    kafka_bridge_rest_api_all_auth_methods(true).

kafka_bridge_rest_api_all_auth_methods(UseSSL) ->
    NormalHostsString =
        case UseSSL of
            true -> kafka_hosts_string_ssl();
            false -> kafka_hosts_string()
        end,
    SASLHostsString =
        case UseSSL of
            true -> kafka_hosts_string_ssl_sasl();
            false -> kafka_hosts_string_sasl()
        end,
    BinifyMap = fun(Map) ->
        maps:from_list([
            {erlang:iolist_to_binary(K), erlang:iolist_to_binary(V)}
         || {K, V} <- maps:to_list(Map)
        ])
    end,
    SSLSettings =
        case UseSSL of
            true -> #{<<"ssl">> => BinifyMap(valid_ssl_settings())};
            false -> #{}
        end,
    kafka_bridge_rest_api_helper(
        maps:merge(
            #{
                <<"bootstrap_hosts">> => NormalHostsString,
                <<"authentication">> => <<"none">>
            },
            SSLSettings
        )
    ),
    kafka_bridge_rest_api_helper(
        maps:merge(
            #{
                <<"bootstrap_hosts">> => SASLHostsString,
                <<"authentication">> => BinifyMap(valid_sasl_plain_settings())
            },
            SSLSettings
        )
    ),
    kafka_bridge_rest_api_helper(
        maps:merge(
            #{
                <<"bootstrap_hosts">> => SASLHostsString,
                <<"authentication">> => BinifyMap(valid_sasl_scram256_settings())
            },
            SSLSettings
        )
    ),
    kafka_bridge_rest_api_helper(
        maps:merge(
            #{
                <<"bootstrap_hosts">> => SASLHostsString,
                <<"authentication">> => BinifyMap(valid_sasl_scram512_settings())
            },
            SSLSettings
        )
    ),
    kafka_bridge_rest_api_helper(
        maps:merge(
            #{
                <<"bootstrap_hosts">> => SASLHostsString,
                <<"authentication">> => BinifyMap(valid_sasl_kerberos_settings())
            },
            SSLSettings
        )
    ),
    ok.

kafka_bridge_rest_api_helper(Config) ->
    BridgeType = ?BRIDGE_TYPE,
    BridgeName = "my_kafka_bridge",
    BridgeID = emqx_bridge_resource:bridge_id(
        erlang:list_to_binary(BridgeType),
        erlang:list_to_binary(BridgeName)
    ),
    ResourceId = emqx_bridge_resource:resource_id(
        erlang:list_to_binary(BridgeType),
        erlang:list_to_binary(BridgeName)
    ),
    UrlEscColon = "%3A",
    BridgesProbeParts = ["bridges_probe"],
    BridgeIdUrlEnc = BridgeType ++ UrlEscColon ++ BridgeName,
    BridgesParts = ["bridges"],
    BridgesPartsIdDeleteAlsoActions = ["bridges", BridgeIdUrlEnc ++ "?also_delete_dep_actions"],
    OpUrlFun = fun(OpName) -> ["bridges", BridgeIdUrlEnc, OpName] end,
    EnableFun = fun(Enable) -> ["bridges", BridgeIdUrlEnc, "enable", Enable] end,
    BridgesPartsOpDisable = EnableFun("false"),
    BridgesPartsOpEnable = EnableFun("true"),
    BridgesPartsOpRestart = OpUrlFun("restart"),
    BridgesPartsOpStop = OpUrlFun("stop"),
    %% List bridges
    MyKafkaBridgeExists = fun() ->
        {ok, _Code, BridgesData} = show(http_get(BridgesParts)),
        Bridges = show(json(BridgesData)),
        lists:any(
            fun
                (#{<<"name">> := <<"my_kafka_bridge">>}) -> true;
                (_) -> false
            end,
            Bridges
        )
    end,
    %% Delete if my_kafka_bridge exists
    case MyKafkaBridgeExists() of
        true ->
            %% Delete the bridge my_kafka_bridge
            {ok, 204, <<>>} = show(http_delete(BridgesPartsIdDeleteAlsoActions));
        false ->
            ok
    end,
    false = MyKafkaBridgeExists(),
    %% Create new Kafka bridge
    KafkaTopic = "test-topic-one-partition",
    CreateBodyTmp = #{
        <<"type">> => <<?BRIDGE_TYPE>>,
        <<"name">> => <<"my_kafka_bridge">>,
        <<"bootstrap_hosts">> => iolist_to_binary(maps:get(<<"bootstrap_hosts">>, Config)),
        <<"enable">> => true,
        <<"authentication">> => maps:get(<<"authentication">>, Config),
        <<"local_topic">> => <<"t/#">>,
        <<"kafka">> => #{
            <<"topic">> => iolist_to_binary(KafkaTopic),
            <<"buffer">> => #{<<"memory_overload_protection">> => <<"false">>},
            <<"message">> => #{
                <<"key">> => <<"${clientid}">>,
                <<"value">> => <<"${.payload}">>
            }
        }
    },
    CreateBody =
        case maps:is_key(<<"ssl">>, Config) of
            true -> CreateBodyTmp#{<<"ssl">> => maps:get(<<"ssl">>, Config)};
            false -> CreateBodyTmp
        end,
    {ok, 201, _Data} = show(http_post(BridgesParts, show(CreateBody))),
    %% Check that the new bridge is in the list of bridges
    true = MyKafkaBridgeExists(),
    %% Probe should work
    {ok, 204, _} = http_post(BridgesProbeParts, CreateBody),
    %% no extra atoms should be created when probing
    AtomsBefore = erlang:system_info(atom_count),
    {ok, 204, _} = http_post(BridgesProbeParts, CreateBody),
    AtomsAfter = erlang:system_info(atom_count),
    ?assertEqual(AtomsBefore, AtomsAfter),
    %% Create a rule that uses the bridge
    {ok, 201, Rule} = http_post(
        ["rules"],
        #{
            <<"name">> => <<"kafka_bridge_rest_api_helper_rule">>,
            <<"enable">> => true,
            <<"actions">> => [BridgeID],
            <<"sql">> => <<"SELECT * from \"kafka_bridge_topic/#\"">>
        }
    ),
    #{<<"id">> := RuleId} = emqx_utils_json:decode(Rule, [return_maps]),
    %% counters should be empty before
    ?assertEqual(0, emqx_resource_metrics:matched_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:success_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:dropped_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:failed_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:inflight_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:queuing_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:dropped_other_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:dropped_queue_full_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:dropped_resource_not_found_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:dropped_resource_stopped_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:retried_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:retried_failed_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:retried_success_get(ResourceId)),
    %% Get offset before sending message
    {ok, Offset} = resolve_kafka_offset(kafka_hosts(), KafkaTopic, 0),
    %% Send message to topic and check that it got forwarded to Kafka
    Body = <<"message from EMQX">>,
    emqx:publish(emqx_message:make(<<"kafka_bridge_topic/1">>, Body)),
    %% Give Kafka some time to get message
    timer:sleep(100),
    %% Check that Kafka got message
    BrodOut = brod:fetch(kafka_hosts(), KafkaTopic, 0, Offset),
    {ok, {_, [KafkaMsg]}} = show(BrodOut),
    Body = KafkaMsg#kafka_message.value,
    %% Check crucial counters and gauges
    ?assertEqual(1, emqx_resource_metrics:matched_get(ResourceId)),
    ?assertEqual(1, emqx_resource_metrics:success_get(ResourceId)),
    ?assertEqual(1, emqx_metrics_worker:get(rule_metrics, RuleId, 'actions.success')),
    ?assertEqual(0, emqx_metrics_worker:get(rule_metrics, RuleId, 'actions.failed')),
    ?assertEqual(0, emqx_resource_metrics:dropped_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:failed_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:inflight_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:queuing_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:dropped_other_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:dropped_queue_full_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:dropped_resource_not_found_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:dropped_resource_stopped_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:retried_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:retried_failed_get(ResourceId)),
    ?assertEqual(0, emqx_resource_metrics:retried_success_get(ResourceId)),
    %% Perform operations
    {ok, 204, _} = show(http_put(show(BridgesPartsOpDisable), #{})),
    {ok, 204, _} = show(http_put(show(BridgesPartsOpDisable), #{})),
    {ok, 204, _} = show(http_put(show(BridgesPartsOpEnable), #{})),
    {ok, 204, _} = show(http_put(show(BridgesPartsOpEnable), #{})),
    {ok, 204, _} = show(http_post(show(BridgesPartsOpStop), #{})),
    {ok, 204, _} = show(http_post(show(BridgesPartsOpStop), #{})),
    {ok, 204, _} = show(http_post(show(BridgesPartsOpRestart), #{})),
    %% Cleanup
    {ok, 204, _} = show(http_delete(BridgesPartsIdDeleteAlsoActions)),
    false = MyKafkaBridgeExists(),
    delete_all_bridges(),
    ok.

%%------------------------------------------------------------------------------
%% Other tests
%%------------------------------------------------------------------------------

%% Need to stop the already running client; otherwise, the
%% next `on_start' call will try to ensure the client
%% exists and it will.  This is specially bad if the
%% original crash was due to misconfiguration and we are
%% trying to fix it...
t_failed_creation_then_fix(Config) ->
    HostsString = kafka_hosts_string_sasl(),
    ValidAuthSettings = valid_sasl_plain_settings(),
    WrongAuthSettings = ValidAuthSettings#{"password" := "wrong"},
    Hash = erlang:phash2([HostsString, ?FUNCTION_NAME]),
    Type = ?BRIDGE_TYPE,
    Name = "kafka_bridge_name_" ++ erlang:integer_to_list(Hash),
    ResourceId = emqx_bridge_resource:resource_id(Type, Name),
    BridgeId = emqx_bridge_resource:bridge_id(Type, Name),
    KafkaTopic = "test-topic-one-partition",
    WrongConf = config(#{
        "authentication" => WrongAuthSettings,
        "kafka_hosts_string" => HostsString,
        "kafka_topic" => KafkaTopic,
        "instance_id" => ResourceId,
        "ssl" => #{}
    }),
    ValidConf = config(#{
        "authentication" => ValidAuthSettings,
        "kafka_hosts_string" => HostsString,
        "kafka_topic" => KafkaTopic,
        "instance_id" => ResourceId,
        "producer" => #{
            "kafka" => #{
                "buffer" => #{
                    "memory_overload_protection" => false
                }
            }
        },
        "ssl" => #{}
    }),
    %% creates, but fails to start producers
    {ok, #{config := WrongConfigAtom1}} = emqx_bridge:create(
        Type, erlang:list_to_atom(Name), WrongConf
    ),
    WrongConfigAtom = WrongConfigAtom1#{bridge_name => Name},
    ?assertThrow(Reason when is_list(Reason), ?PRODUCER:on_start(ResourceId, WrongConfigAtom)),
    %% before throwing, it should cleanup the client process.  we
    %% retry because the supervisor might need some time to really
    %% remove it from its tree.
    ?retry(50, 10, ?assertEqual([], supervisor:which_children(wolff_client_sup))),
    %% must succeed with correct config
    {ok, #{config := ValidConfigAtom1}} = emqx_bridge:create(
        Type, erlang:list_to_atom(Name), ValidConf
    ),
    ValidConfigAtom = ValidConfigAtom1#{bridge_name => Name},
    {ok, State} = ?PRODUCER:on_start(ResourceId, ValidConfigAtom),
    Time = erlang:unique_integer(),
    BinTime = integer_to_binary(Time),
    Msg = #{
        clientid => BinTime,
        payload => <<"payload">>,
        timestamp => Time
    },
    {ok, Offset} = resolve_kafka_offset(kafka_hosts(), KafkaTopic, 0),
    ct:pal("base offset before testing ~p", [Offset]),
    ok = send(Config, ResourceId, Msg, State),
    {ok, {_, [KafkaMsg]}} = brod:fetch(kafka_hosts(), KafkaTopic, 0, Offset),
    ?assertMatch(#kafka_message{key = BinTime}, KafkaMsg),
    %% TODO: refactor those into init/end per testcase
    ok = ?PRODUCER:on_stop(ResourceId, State),
    ok = emqx_bridge_resource:remove(BridgeId),
    delete_all_bridges(),
    ok.

t_load_configuration_file_with_legacy_name(Config) ->
    Cluster = cluster_1_node(Config),
    ?check_trace(
        begin
            [Node] = start_cluster(Cluster),
            erpc:call(Node, emqx_common_test_http, create_default_app, []),
            ?assertMatch(
                #{kafka_producer := #{kproducer := #{}}},
                erpc:call(Node, emqx_config, get, [[bridges]])
            ),
            ?assertMatch(
                #{<<"kafka_producer">> := #{<<"kproducer">> := #{}}},
                erpc:call(Node, emqx_config, get_raw, [[bridges]])
            ),
            ?retry(
                _Interval = 500,
                _NAttempts = 20,
                ?assertMatch(
                    [
                        #{
                            type := <<"kafka_producer">>,
                            name := <<"kproducer">>,
                            resource_data := #{status := connected}
                        }
                    ],
                    erpc:call(Node, emqx_bridge, list, [])
                )
            ),
            ?assertMatch(
                {ok, #{<<"type">> := <<"kafka_producer">>}},
                get_bridge_api(Node)
            ),
            ok
        end,
        []
    ),
    ok.

%%------------------------------------------------------------------------------
%% Helper functions
%%------------------------------------------------------------------------------

send(Config, ResourceId, Msg, State) when is_list(Config) ->
    Ref = make_ref(),
    ok = do_send(Ref, Config, ResourceId, Msg, State),
    receive
        {ack, Ref} ->
            ok
    after 10000 ->
        error(timeout)
    end.

do_send(Ref, Config, ResourceId, Msg, State) when is_list(Config) ->
    Caller = self(),
    F = fun(ok) ->
        Caller ! {ack, Ref},
        ok
    end,
    case proplists:get_value(query_api, Config) of
        on_query ->
            ok = ?PRODUCER:on_query(ResourceId, {send_message, Msg}, State),
            F(ok);
        on_query_async ->
            {ok, _} = ?PRODUCER:on_query_async(ResourceId, {send_message, Msg}, {F, []}, State),
            ok
    end.

publish_with_and_without_ssl(CtConfig, AuthSettings) ->
    publish_with_and_without_ssl(CtConfig, AuthSettings, #{}).

publish_with_and_without_ssl(CtConfig, AuthSettings, Config) ->
    publish_helper(
        CtConfig,
        #{
            auth_settings => AuthSettings,
            ssl_settings => #{}
        },
        Config
    ),
    publish_helper(
        CtConfig,
        #{
            auth_settings => AuthSettings,
            ssl_settings => valid_ssl_settings()
        },
        Config
    ),
    ok.

publish_helper(CtConfig, AuthSettings) ->
    publish_helper(CtConfig, AuthSettings, #{}).

publish_helper(
    CtConfig,
    #{
        auth_settings := AuthSettings,
        ssl_settings := SSLSettings
    },
    Conf0
) ->
    delete_all_bridges(),
    HostsString =
        case {AuthSettings, SSLSettings} of
            {"none", Map} when map_size(Map) =:= 0 ->
                kafka_hosts_string();
            {"none", Map} when map_size(Map) =/= 0 ->
                kafka_hosts_string_ssl();
            {_, Map} when map_size(Map) =:= 0 ->
                kafka_hosts_string_sasl();
            {_, _} ->
                kafka_hosts_string_ssl_sasl()
        end,
    Hash = erlang:phash2([HostsString, AuthSettings, SSLSettings]),
    Name = "kafka_bridge_name_" ++ erlang:integer_to_list(Hash),
    Type = ?BRIDGE_TYPE,
    InstId = emqx_bridge_resource:resource_id(Type, Name),
    KafkaTopic = "test-topic-one-partition",
    Conf = config(
        #{
            "authentication" => AuthSettings,
            "kafka_hosts_string" => HostsString,
            "kafka_topic" => KafkaTopic,
            "instance_id" => InstId,
            "local_topic" => <<"mqtt/local">>,
            "ssl" => SSLSettings
        },
        Conf0
    ),
    {ok, _} = emqx_bridge:create(
        <<?BRIDGE_TYPE>>, list_to_binary(Name), Conf
    ),
    Time = erlang:unique_integer(),
    BinTime = integer_to_binary(Time),
    Partition = 0,
    Msg = #{
        clientid => BinTime,
        payload => <<"payload">>,
        timestamp => Time
    },
    {ok, Offset0} = resolve_kafka_offset(kafka_hosts(), KafkaTopic, Partition),
    ct:pal("base offset before testing ~p", [Offset0]),
    {ok, _Group, #{state := State}} = emqx_resource:get_instance(InstId),
    ok = send(CtConfig, InstId, Msg, State),
    {ok, {_, [KafkaMsg0]}} = brod:fetch(kafka_hosts(), KafkaTopic, Partition, Offset0),
    ?assertMatch(#kafka_message{key = BinTime}, KafkaMsg0),

    %% test that it forwards from local mqtt topic as well
    {ok, Offset1} = resolve_kafka_offset(kafka_hosts(), KafkaTopic, Partition),
    ct:pal("base offset before testing (2) ~p", [Offset1]),
    emqx:publish(emqx_message:make(<<"mqtt/local">>, <<"payload">>)),
    ct:sleep(2_000),
    {ok, {_, [KafkaMsg1]}} = brod:fetch(kafka_hosts(), KafkaTopic, Partition, Offset1),
    ?assertMatch(#kafka_message{value = <<"payload">>}, KafkaMsg1),

    delete_all_bridges(),
    ok.

default_config() ->
    #{"partition_strategy" => "random"}.

config(Args) ->
    config(Args, #{}).

config(Args0, More) ->
    Args1 = maps:merge(default_config(), Args0),
    Args = maps:merge(Args1, More),
    ConfText = hocon_config(Args),
    {ok, Conf} = hocon:binary(ConfText, #{format => map}),
    ct:pal("Running tests with conf:\n~p", [Conf]),
    InstId = maps:get("instance_id", Args),
    <<"bridge:", BridgeId/binary>> = InstId,
    {Type, Name} = emqx_bridge_resource:parse_bridge_id(BridgeId),
    TypeBin = atom_to_binary(Type),
    hocon_tconf:check_plain(
        emqx_bridge_schema,
        Conf,
        #{atom_key => false, required => false}
    ),
    #{<<"bridges">> := #{TypeBin := #{Name := Parsed}}} = Conf,
    Parsed.

hocon_config(Args) ->
    InstId = maps:get("instance_id", Args),
    <<"bridge:", BridgeId/binary>> = InstId,
    {_Type, Name} = emqx_bridge_resource:parse_bridge_id(BridgeId),
    AuthConf = maps:get("authentication", Args),
    AuthTemplate = iolist_to_binary(hocon_config_template_authentication(AuthConf)),
    AuthConfRendered = bbmustache:render(AuthTemplate, AuthConf),
    SSLConf = maps:get("ssl", Args, #{}),
    SSLTemplate = iolist_to_binary(hocon_config_template_ssl(SSLConf)),
    SSLConfRendered = bbmustache:render(SSLTemplate, SSLConf),
    Hocon = bbmustache:render(
        iolist_to_binary(hocon_config_template()),
        Args#{
            "authentication" => AuthConfRendered,
            "bridge_name" => Name,
            "ssl" => SSLConfRendered
        }
    ),
    Hocon.

%% erlfmt-ignore
hocon_config_template() ->
"""
bridges.kafka_producer.{{ bridge_name }} {
  bootstrap_hosts = \"{{ kafka_hosts_string }}\"
  enable = true
  authentication = {{{ authentication }}}
  ssl = {{{ ssl }}}
  local_topic = \"{{ local_topic }}\"
  kafka = {
    message = {
      key = \"${clientid}\"
      value = \"${.payload}\"
      timestamp = \"${timestamp}\"
    }
    buffer = {
      memory_overload_protection = false
    }
    partition_strategy = {{ partition_strategy }}
    topic = \"{{ kafka_topic }}\"
  }
  metadata_request_timeout = 5s
  min_metadata_refresh_interval = 3s
  socket_opts {
    nodelay = true
  }
  connect_timeout = 5s
}
""".

%% erlfmt-ignore
hocon_config_template_authentication("none") ->
    "none";
hocon_config_template_authentication(#{"mechanism" := _}) ->
"""
{
    mechanism = {{ mechanism }}
    password = {{ password }}
    username = {{ username }}
}
""";
hocon_config_template_authentication(#{"kerberos_principal" := _}) ->
"""
{
    kerberos_principal = \"{{ kerberos_principal }}\"
    kerberos_keytab_file = \"{{ kerberos_keytab_file }}\"
}
""".

%% erlfmt-ignore
hocon_config_template_ssl(Map) when map_size(Map) =:= 0 ->
"""
{
    enable = false
}
""";
hocon_config_template_ssl(_) ->
"""
{
    enable = true
    cacertfile = \"{{{cacertfile}}}\"
    certfile = \"{{{certfile}}}\"
    keyfile = \"{{{keyfile}}}\"
}
""".

kafka_hosts_string() ->
    KafkaHost = os:getenv("KAFKA_PLAIN_HOST", "kafka-1.emqx.net"),
    KafkaPort = os:getenv("KAFKA_PLAIN_PORT", "9092"),
    KafkaHost ++ ":" ++ KafkaPort ++ ",".

kafka_hosts_string_sasl() ->
    KafkaHost = os:getenv("KAFKA_SASL_PLAIN_HOST", "kafka-1.emqx.net"),
    KafkaPort = os:getenv("KAFKA_SASL_PLAIN_PORT", "9093"),
    KafkaHost ++ ":" ++ KafkaPort ++ ",".

kafka_hosts_string_ssl() ->
    KafkaHost = os:getenv("KAFKA_SSL_HOST", "kafka-1.emqx.net"),
    KafkaPort = os:getenv("KAFKA_SSL_PORT", "9094"),
    KafkaHost ++ ":" ++ KafkaPort ++ ",".

kafka_hosts_string_ssl_sasl() ->
    KafkaHost = os:getenv("KAFKA_SASL_SSL_HOST", "kafka-1.emqx.net"),
    KafkaPort = os:getenv("KAFKA_SASL_SSL_PORT", "9095"),
    KafkaHost ++ ":" ++ KafkaPort ++ ",".

shared_secret_path() ->
    os:getenv("CI_SHARED_SECRET_PATH", "/var/lib/secret").

shared_secret(client_keyfile) ->
    filename:join([shared_secret_path(), "client.key"]);
shared_secret(client_certfile) ->
    filename:join([shared_secret_path(), "client.crt"]);
shared_secret(client_cacertfile) ->
    filename:join([shared_secret_path(), "ca.crt"]);
shared_secret(rig_keytab) ->
    filename:join([shared_secret_path(), "rig.keytab"]).

valid_ssl_settings() ->
    #{
        "cacertfile" => shared_secret(client_cacertfile),
        "certfile" => shared_secret(client_certfile),
        "keyfile" => shared_secret(client_keyfile),
        "enable" => <<"true">>
    }.

valid_sasl_plain_settings() ->
    #{
        "mechanism" => "plain",
        "username" => "emqxuser",
        "password" => "password"
    }.

valid_sasl_scram256_settings() ->
    (valid_sasl_plain_settings())#{
        "mechanism" => "scram_sha_256"
    }.

valid_sasl_scram512_settings() ->
    (valid_sasl_plain_settings())#{
        "mechanism" => "scram_sha_512"
    }.

valid_sasl_kerberos_settings() ->
    #{
        "kerberos_principal" => "rig@KDC.EMQX.NET",
        "kerberos_keytab_file" => shared_secret(rig_keytab)
    }.

kafka_hosts() ->
    kpro:parse_endpoints(kafka_hosts_string()).

resolve_kafka_offset(Hosts, Topic, Partition) ->
    brod:resolve_offset(Hosts, Topic, Partition, latest).

%%------------------------------------------------------------------------------
%% Internal functions rest API helpers
%%------------------------------------------------------------------------------

bin(X) -> iolist_to_binary(X).

random_num() ->
    erlang:system_time(nanosecond).

http_get(Parts) ->
    request_api(get, api_path(Parts), auth_header_()).

http_delete(Parts) ->
    request_api(delete, api_path(Parts), auth_header_()).

http_post(Parts, Body) ->
    request_api(post, api_path(Parts), [], auth_header_(), Body).

http_put(Parts, Body) ->
    request_api(put, api_path(Parts), [], auth_header_(), Body).

request_dashboard(Method, Url, Auth) ->
    Request = {Url, [Auth]},
    do_request_dashboard(Method, Request).
request_dashboard(Method, Url, QueryParams, Auth) ->
    Request = {Url ++ "?" ++ QueryParams, [Auth]},
    do_request_dashboard(Method, Request).
do_request_dashboard(Method, Request) ->
    ct:pal("Method: ~p, Request: ~p", [Method, Request]),
    case httpc:request(Method, Request, [], []) of
        {error, socket_closed_remotely} ->
            {error, socket_closed_remotely};
        {ok, {{"HTTP/1.1", Code, _}, _Headers, Return}} when
            Code >= 200 andalso Code =< 299
        ->
            {ok, Return};
        {ok, {Reason, _, _}} ->
            {error, Reason}
    end.

auth_header_() ->
    auth_header_(<<"admin">>, <<"public">>).

auth_header_(Username, Password) ->
    {ok, Token} = emqx_dashboard_admin:sign_token(Username, Password),
    {"Authorization", "Bearer " ++ binary_to_list(Token)}.

api_path(Parts) ->
    ?HOST ++ filename:join([?BASE_PATH | Parts]).

json(Data) ->
    {ok, Jsx} = emqx_utils_json:safe_decode(Data, [return_maps]),
    Jsx.

delete_all_bridges() ->
    lists:foreach(
        fun(#{name := Name, type := Type}) ->
            emqx_bridge:remove(Type, Name)
        end,
        emqx_bridge:list()
    ),
    %% at some point during the tests, sometimes `emqx_bridge:list()'
    %% returns an empty list, but `emqx:get_config([bridges])' returns
    %% a bunch of orphan test bridges...
    lists:foreach(fun emqx_resource:remove/1, emqx_resource:list_instances()),
    emqx_config:put([bridges], #{}),
    ok.

cluster_1_node(Config) ->
    DataDir = ?config(data_dir, Config),
    PrivDataDir = ?config(priv_dir, Config),
    LegacyConf = filename:join([DataDir, "kafka_producer_legacy.hocon"]),
    PeerModule =
        case os:getenv("IS_CI") of
            false ->
                slave;
            _ ->
                ct_slave
        end,
    StartAppsExtraArgs =
        #{
            extra_mustache_vars => #{
                test_priv_dir => PrivDataDir,
                bootstrap_hosts => kafka_hosts_string()
            },
            conf_file_path => LegacyConf,
            force_schema => emqx_ee_conf_schema
        },
    Apps = [emqx_conf, emqx_dashboard, emqx_bridge],
    Cluster = emqx_common_test_helpers:emqx_cluster(
        [core],
        [
            %% Note: emqx_bridge must be last for this test to avoid
            %% clobbering the initialization changes the app makes.
            {apps, Apps},
            {load_apps, [emqx_management | Apps]},
            {listener_ports, []},
            {peer_mod, PeerModule},
            {priv_data_dir, PrivDataDir},
            {load_schema, true},
            {start_autocluster, true},
            {start_apps_extra_args, StartAppsExtraArgs},
            {schema_mod, emqx_ee_conf_schema},
            {env_handler, fun
                (emqx) ->
                    application:set_env(emqx, boot_modules, [broker, router]),
                    ok;
                (emqx_conf) ->
                    ok;
                (emqx_dashboard) ->
                    ok;
                (_App) ->
                    ok
            end}
        ]
    ),
    ct:pal("cluster: ~p", [Cluster]),
    Cluster.

start_cluster(Cluster) ->
    Nodes =
        [
            emqx_common_test_helpers:start_slave(Name, Opts)
         || {Name, Opts} <- Cluster
        ],
    on_exit(fun() ->
        emqx_utils:pmap(
            fun(N) ->
                ct:pal("stopping ~p", [N]),
                ok = emqx_common_test_helpers:stop_slave(N)
            end,
            Nodes
        )
    end),
    Nodes.

get_bridge_api(Node) ->
    TypeBin = <<?BRIDGE_TYPE>>,
    Name = <<"kproducer">>,
    BridgeId = emqx_bridge_resource:bridge_id(TypeBin, Name),
    Params = [],
    %% using custom port because it's running on the peer node.
    Path = "http://127.0.0.1:28083" ++ filename:join(["/", "api", "v5", "bridges", BridgeId]),
    AuthHeader = erpc:call(Node, emqx_mgmt_api_test_util, auth_header_, []),
    Opts = #{return_all => true},
    ct:pal("getting bridge (via http): ~p", [Params]),
    Res =
        case emqx_mgmt_api_test_util:request_api(get, Path, "", AuthHeader, Params, Opts) of
            {ok, {_Status, _Headers, Body0}} -> {ok, emqx_utils_json:decode(Body0, [return_maps])};
            Error -> Error
        end,
    ct:pal("bridge get result: ~p", [Res]),
    Res.
