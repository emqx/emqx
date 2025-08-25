%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_kafka_testlib).

-compile(nowarn_export_all).
-compile(export_all).

-export([
    wait_until_kafka_is_up/0,

    shared_secret_path/0,
    shared_secret/1,

    valid_ssl_settings/0,

    no_auth/0,
    plain_auth/0,
    scram_sha256_auth/0,
    scram_sha512_auth/0,
    kerberos_auth/0,

    kafka_hosts_direct/0,

    ensure_kafka_topic/1,
    ensure_kafka_topic/2,
    delete_kafka_topic/1,
    test_topic_one_partition/0
]).

-import(emqx_common_test_helpers, [on_exit/1]).

shared_secret_path() ->
    os:getenv("CI_SHARED_SECRET_PATH", "/var/lib/secret").

shared_secret(client_keyfile) ->
    bin(filename:join([shared_secret_path(), "client.key"]));
shared_secret(client_certfile) ->
    bin(filename:join([shared_secret_path(), "client.crt"]));
shared_secret(client_cacertfile) ->
    bin(filename:join([shared_secret_path(), "ca.crt"]));
shared_secret(rig_keytab) ->
    bin(filename:join([shared_secret_path(), "rig.keytab"])).

valid_ssl_settings() ->
    #{
        <<"verify">> => <<"verify_none">>,
        <<"cacertfile">> => shared_secret(client_cacertfile),
        <<"certfile">> => shared_secret(client_certfile),
        <<"keyfile">> => shared_secret(client_keyfile),
        <<"enable">> => true
    }.

test_topic_one_partition() ->
    <<"test-topic-one-partition">>.

%% In CI, Kafka may take a while longer to reach an usable state.  If we run the test
%% suites too soon, we may get some flaky failures with error messages like these:
%% "The coordinator is not available."
wait_until_kafka_is_up() ->
    wait_until_kafka_is_up(0).

wait_until_kafka_is_up(300) ->
    ct:fail("Kafka is not up even though we have waited for a while");
wait_until_kafka_is_up(Attempts) ->
    KafkaTopic = test_topic_one_partition(),
    case brod:resolve_offset(kafka_hosts_direct(), KafkaTopic, _Partition = 0, latest) of
        {ok, _} ->
            ok;
        _ ->
            timer:sleep(1_000),
            wait_until_kafka_is_up(Attempts + 1)
    end.

kafka_hosts_direct() ->
    kpro:parse_endpoints("kafka-1.emqx.net:9092").

ensure_kafka_topic(KafkaTopic) ->
    ensure_kafka_topic(KafkaTopic, _Opts = #{}).

ensure_kafka_topic(KafkaTopic, Opts) ->
    NumPartitions = maps:get(num_partitions, Opts, 1),
    on_exit(fun() -> delete_kafka_topic(KafkaTopic) end),
    TopicConfigs = [
        #{
            name => KafkaTopic,
            num_partitions => NumPartitions,
            replication_factor => 1,
            assignments => [],
            configs => []
        }
    ],
    RequestConfig = #{timeout => 5_000},
    ConnConfig = #{},
    Endpoints = kafka_hosts_direct(),
    case brod:create_topics(Endpoints, TopicConfigs, RequestConfig, ConnConfig) of
        ok ->
            ok;
        {error, topic_already_exists} ->
            ok;
        {error, Reason} when is_binary(Reason) ->
            {match, Reason} = {re:run(Reason, <<"already exists">>, [{capture, none}]), Reason},
            ok
    end.

delete_kafka_topic(KafkaTopic) ->
    Timeout = 1_000,
    ConnConfig = #{},
    Endpoints = kafka_hosts_direct(),
    case brod:delete_topics(Endpoints, [KafkaTopic], Timeout, ConnConfig) of
        ok -> ok;
        {error, unknown_topic_or_partition} -> ok
    end.

no_auth() ->
    <<"none">>.

plain_auth() ->
    #{
        <<"mechanism">> => <<"plain">>,
        <<"username">> => <<"emqxuser">>,
        <<"password">> => <<"password">>
    }.

scram_sha256_auth() ->
    #{
        <<"mechanism">> => <<"scram_sha_256">>,
        <<"username">> => <<"emqxuser">>,
        <<"password">> => <<"password">>
    }.

scram_sha512_auth() ->
    #{
        <<"mechanism">> => <<"scram_sha_512">>,
        <<"username">> => <<"emqxuser">>,
        <<"password">> => <<"password">>
    }.

kerberos_auth() ->
    #{
        <<"kerberos_principal">> => <<"rig@KDC.EMQX.NET">>,
        <<"kerberos_keytab_file">> => shared_secret(rig_keytab)
    }.

msk_iam_auth() ->
    <<"msk_iam">>.

bin(X) -> emqx_utils_conv:bin(X).
