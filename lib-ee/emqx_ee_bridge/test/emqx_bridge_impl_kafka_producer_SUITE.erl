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

wait_until_kafka_is_up() ->
    wait_until_kafka_is_up(0).

wait_until_kafka_is_up(90) ->
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
    {ok, _} = application:ensure_all_started(brod),
    {ok, _} = application:ensure_all_started(wolff),
    wait_until_kafka_is_up(),
    Config.

end_per_suite(_) ->
    ok.

t_publish_no_auth(_CtConfig) ->
    publish_with_and_without_ssl("none").

t_publish_sasl_plain(_CtConfig) ->
    publish_with_and_without_ssl(valid_sasl_plain_settings()).

t_publish_sasl_scram256(_CtConfig) ->
    publish_with_and_without_ssl(valid_sasl_scram256_settings()).

t_publish_sasl_scram512(_CtConfig) ->
    publish_with_and_without_ssl(valid_sasl_scram512_settings()).

t_publish_sasl_kerberos(_CtConfig) ->
    publish_with_and_without_ssl(valid_sasl_kerberos_settings()).

publish_with_and_without_ssl(AuthSettings) ->
    publish_helper(#{
        auth_settings => AuthSettings,
        ssl_settings => #{}
    }),
    publish_helper(#{
        auth_settings => AuthSettings,
        ssl_settings => valid_ssl_settings()
    }).

publish_helper(#{
    auth_settings := AuthSettings,
    ssl_settings := SSLSettings
}) ->
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
    InstId = emqx_bridge_resource:resource_id("kafka", Name),
    KafkaTopic = "test-topic-one-partition",
    Conf = config(#{
        "authentication" => AuthSettings,
        "kafka_hosts_string" => HostsString,
        "kafka_topic" => KafkaTopic,
        "instance_id" => InstId,
        "ssl" => SSLSettings
    }),
    %% To make sure we get unique value
    timer:sleep(1),
    Time = erlang:monotonic_time(),
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

config(Args) ->
    ConfText = hocon_config(Args),
    ct:pal("Running tests with conf:\n~s", [ConfText]),
    {ok, Conf} = hocon:binary(ConfText),
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
    SSLConf = maps:get("ssl", Args, #{}),
    SSLTemplate = iolist_to_binary(hocon_config_template_ssl(SSLConf)),
    SSLConfRendered = bbmustache:render(SSLTemplate, SSLConf),
    Hocon = bbmustache:render(
        iolist_to_binary(hocon_config_template()),
        Args#{
            "authentication" => AuthConfRendered,
            "ssl" => SSLConfRendered
        }
    ),
    Hocon.

%% erlfmt-ignore
hocon_config_template() ->
"""
bootstrap_hosts = \"{{ kafka_hosts_string }}\"
enable = true
authentication = {{{ authentication }}} 
ssl = {{{ ssl }}}
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
    "kafka-1.emqx.net:9092,".

kafka_hosts_string_sasl() ->
    "kafka-1.emqx.net:9093,".

kafka_hosts_string_ssl() ->
    "kafka-1.emqx.net:9094,".

kafka_hosts_string_ssl_sasl() ->
    "kafka-1.emqx.net:9095,".

valid_ssl_settings() ->
    #{
        "cacertfile" => <<"/var/lib/secret/ca.crt">>,
        "certfile" => <<"/var/lib/secret/client.crt">>,
        "keyfile" => <<"/var/lib/secret/client.key">>
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
        "kerberos_keytab_file" => "/var/lib/secret/rig.keytab"
    }.

kafka_hosts() ->
    kpro:parse_endpoints(kafka_hosts_string()).

resolve_kafka_offset(Hosts, Topic, Partition) ->
    brod:resolve_offset(Hosts, Topic, Partition, latest).
