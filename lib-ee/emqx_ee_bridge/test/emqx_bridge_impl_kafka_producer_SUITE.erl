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
    {ok, State} = ?PRODUCER:on_start(InstId, Conf),
    ok = ?PRODUCER:on_query(InstId, {send_message, Msg}, State),
    {ok, {_, [KafkaMsg]}} = brod:fetch(kafka_hosts(), KafkaTopic, 0, Offset),
    ?assertMatch(#kafka_message{key = BinTime}, KafkaMsg),
    ok = ?PRODUCER:on_stop(InstId, State),
    ok.

t_publish(_CtConfig) ->
    KafkaTopic = "test-topic-one-partition",
    Conf = config(#{
        "authentication" => "none",
        "kafka_hosts_string" => kafka_hosts_string(),
        "kafka_topic" => KafkaTopic
    }),
    do_publish(Conf, KafkaTopic, <<"NoAuthInst">>).

t_publish_sasl_plain(_CtConfig) ->
    KafkaTopic = "test-topic-one-partition",
    Conf = config(#{
        "authentication" => #{
            "mechanism" => "plain",
            "username" => "emqxuser",
            "password" => "password"
        },
        "kafka_hosts_string" => kafka_hosts_string_sasl(),
        "kafka_topic" => KafkaTopic
    }),
    do_publish(Conf, KafkaTopic, <<"SASLPlainInst">>).

t_publish_sasl_scram256(_CtConfig) ->
    KafkaTopic = "test-topic-one-partition",
    Conf = config(#{
        "authentication" => #{
            "mechanism" => "scram_sha_256",
            "username" => "emqxuser",
            "password" => "password"
        },
        "kafka_hosts_string" => kafka_hosts_string_sasl(),
        "kafka_topic" => KafkaTopic
    }),
    do_publish(Conf, KafkaTopic, <<"SASLScram256Inst">>).

t_publish_sasl_scram512(_CtConfig) ->
    KafkaTopic = "test-topic-one-partition",
    Conf = config(#{
        "authentication" => #{
            "mechanism" => "scram_sha_512",
            "username" => "emqxuser",
            "password" => "password"
        },
        "kafka_hosts_string" => kafka_hosts_string_sasl(),
        "kafka_topic" => KafkaTopic
    }),
    do_publish(Conf, KafkaTopic, <<"SASLScram512Inst">>).

t_publish_sasl_kerberos(_CtConfig) ->
    KafkaTopic = "test-topic-one-partition",
    Conf = config(#{
        "authentication" => #{
            "kerberos_principal" => "rig@KDC.EMQX.NET",
            "kerberos_keytab_file" => "/var/lib/secret/rig.key"
        },
        "kafka_hosts_string" => kafka_hosts_string_sasl(),
        "kafka_topic" => KafkaTopic
    }),
    do_publish(Conf, KafkaTopic, <<"SASLKerberosInst">>).

config(Args) ->
    {ok, Conf} = hocon:binary(hocon_config(Args)),
    #{config := Parsed} = hocon_tconf:check_plain(
        emqx_ee_bridge_kafka,
        #{<<"config">> => Conf},
        #{atom_key => true}
    ),
    Parsed#{bridge_name => "testbridge"}.

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
