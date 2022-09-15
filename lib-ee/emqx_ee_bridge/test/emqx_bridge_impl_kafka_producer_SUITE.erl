%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_impl_kafka_producer_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("brod/include/brod.hrl").

-define(PRODUCER, emqx_bridge_impl_kafka).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(brod),
    {ok, _} = application:ensure_all_started(wolff),
    Config.

end_per_suite(_) ->
    ok.

do_publish(Conf, KafkaTopic, InstId) ->
    Time = erlang:system_time(millisecond),
    BinTime = integer_to_binary(Time),
    Msg = #{
        clientid => BinTime,
        payload => <<"payload">>,
        timestamp => Time
    },
    {ok, Offset} = resolve_kafka_offset(kafka_hosts(), KafkaTopic, 0),
    ct:pal("base offset before testing ~p", [Offset]),
    StartRes = ?PRODUCER:on_start(InstId, Conf),
    {ok, State} = StartRes,
    OnQueryRes = ?PRODUCER:on_query(InstId, {send_message, Msg}, State),
    ok = OnQueryRes,
    {ok, {_, [KafkaMsg]}} = brod:fetch(kafka_hosts(), KafkaTopic, 0, Offset),
    ?assertMatch(#kafka_message{key = BinTime}, KafkaMsg),
    ok = ?PRODUCER:on_stop(InstId, State),
    ok.

t_publish(_CtConfig) ->
    InstId = emqx_bridge_resource:resource_id("kafka", "NoAuthInst"),
    KafkaTopic = "test-topic-one-partition",
    Conf = config(#{
        "authentication" => "none",
        "kafka_hosts_string" => kafka_hosts_string(),
        "kafka_topic" => KafkaTopic,
        "instance_id" => InstId
    }),
    do_publish(Conf, KafkaTopic, InstId).

t_publish_sasl_plain(_CtConfig) ->
    InstId = emqx_bridge_resource:resource_id("kafka", "SASLPlainInst"),
    KafkaTopic = "test-topic-one-partition",
    Conf = config(#{
        "authentication" => #{
            "mechanism" => "plain",
            "username" => "emqxuser",
            "password" => "password"
        },
        "kafka_hosts_string" => kafka_hosts_string_sasl(),
        "kafka_topic" => KafkaTopic,
        "instance_id" => InstId
    }),
    do_publish(Conf, KafkaTopic, InstId).

t_publish_sasl_scram256(_CtConfig) ->
    InstId = emqx_bridge_resource:resource_id("kafka", "SASLScram256Inst"),
    KafkaTopic = "test-topic-one-partition",
    KafkaTopic = "test-topic-one-partition",
    Conf = config(#{
        "authentication" => #{
            "mechanism" => "scram_sha_256",
            "username" => "emqxuser",
            "password" => "password"
        },
        "kafka_hosts_string" => kafka_hosts_string_sasl(),
        "kafka_topic" => KafkaTopic,
        "instance_id" => InstId
    }),
    do_publish(Conf, KafkaTopic, InstId).

t_publish_sasl_scram512(_CtConfig) ->
    InstId = emqx_bridge_resource:resource_id("kafka", "SASLScram512Inst"),
    KafkaTopic = "test-topic-one-partition",
    Conf = config(#{
        "authentication" => #{
            "mechanism" => "scram_sha_512",
            "username" => "emqxuser",
            "password" => "password"
        },
        "kafka_hosts_string" => kafka_hosts_string_sasl(),
        "kafka_topic" => KafkaTopic,
        "instance_id" => InstId
    }),
    do_publish(Conf, KafkaTopic, InstId).

t_publish_sasl_kerberos(_CtConfig) ->
    InstId = emqx_bridge_resource:resource_id("kafka", "SASLKerberosInst"),
    KafkaTopic = "test-topic-one-partition",
    Conf = config(#{
        "authentication" => #{
            "kerberos_principal" => "rig@KDC.EMQX.NET",
            "kerberos_keytab_file" => "/var/lib/secret/rig.key"
        },
        "kafka_hosts_string" => kafka_hosts_string_sasl(),
        "kafka_topic" => KafkaTopic,
        "instance_id" => InstId
    }),
    do_publish(Conf, KafkaTopic, InstId).

config(Args) ->
    {ok, Conf} = hocon:binary(hocon_config(Args)),
    #{config := Parsed} = hocon_tconf:check_plain(
        emqx_ee_bridge_kafka,
        #{<<"config">> => Conf},
        #{atom_key => true}
    ),
    InstId = maps:get("instance_id", Args),
    Parsed#{bridge_name => erlang:element(2, emqx_bridge_resource:parse_bridge_id(InstId))}.

hocon_config(Args) ->
    AuthConf = maps:get("authentication", Args),
    AuthTemplate = iolist_to_binary(hocon_config_template_authentication(AuthConf)),
    AuthConfRendered = bbmustache:render(AuthTemplate, AuthConf),
    Hocon = bbmustache:render(
        iolist_to_binary(hocon_config_template()),
        Args#{"authentication" => AuthConfRendered}
    ),
    Hocon.

%% erlfmt-ignore
hocon_config_template() ->
"""
bootstrap_hosts = \"{{ kafka_hosts_string }}\"
enable = true
authentication = {{{ authentication }}} 
producer = {
    mqtt {
       topic = \"t/#\"
    }
    kafka = {
        topic = \"{{ kafka_topic }}\"
    }
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

kafka_hosts_string() ->
    "kafka-1.emqx.net:9092,".

kafka_hosts_string_sasl() ->
    "kafka-1.emqx.net:9093,".

kafka_hosts() ->
    kpro:parse_endpoints(kafka_hosts_string()).

resolve_kafka_offset(Hosts, Topic, Partition) ->
    brod:resolve_offset(Hosts, Topic, Partition, latest).
