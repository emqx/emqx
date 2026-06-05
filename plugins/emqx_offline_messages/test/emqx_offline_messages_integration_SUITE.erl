%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_offline_messages_integration_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/asserts.hrl").

-define(MYSQL_HOST, "mysql").
-define(MYSQL_PORT, 3306).
-define(MYSQL_TLS_HOST, "mysql-tls").
-define(MYSQL_TLS_PORT, 3306).
-define(MYSQL_DATABASE, "mqtt").
-define(MYSQL_USERNAME, "root").
-define(MYSQL_PASSWORD, "public").

-define(REDIS_HOST, "redis").
-define(REDIS_PORT, 6379).
-define(REDIS_TLS_HOST, "redis-tls").
-define(REDIS_TLS_PORT, 6380).
-define(REDIS_SENTINEL_HOST, "redis-sentinel").
-define(REDIS_SENTINEL_PORT, 26379).
-define(REDIS_CLUSTER_SERVERS,
    <<"redis-cluster-1:6379,redis-cluster-2:6379,redis-cluster-3:6379">>
).
-define(REDIS_PASSWORD, "public").

-define(set_config(KEY, VALUE, CONFIG), lists:keyreplace(KEY, 1, CONFIG, {KEY, VALUE})).

all() ->
    [
        {group, mysql_tcp},
        {group, mysql_ssl},
        {group, redis_tcp},
        {group, redis_ssl},
        {group, redis_sentinel},
        {group, redis_cluster},
        {group, mysql_cleanup},
        {group, redis_cleanup}
    ].

groups() ->
    emqx_common_test_helpers:nested_groups([
        [redis_sentinel, redis_cluster, redis_tcp, redis_ssl, mysql_tcp, mysql_ssl],
        [buffered, unbuffered],
        [
            t_different_subscribers,
            t_subscription_persistence,
            t_restored_subscription_replay_is_not_duplicated,
            t_message_order
        ]
    ]) ++
        [
            {mysql_cleanup, [], [t_mysql_table_cleanup]},
            {redis_cleanup, [], [t_redis_table_cleanup]}
        ].

init_per_suite(Config) ->
    ok = wait_for_tcp_servers([
        {?MYSQL_HOST, ?MYSQL_PORT},
        {?MYSQL_TLS_HOST, ?MYSQL_TLS_PORT},
        {?REDIS_HOST, ?REDIS_PORT},
        {?REDIS_TLS_HOST, ?REDIS_TLS_PORT},
        {?REDIS_SENTINEL_HOST, ?REDIS_SENTINEL_PORT},
        {"redis-cluster-1", 6379},
        {"redis-cluster-2", 6379},
        {"redis-cluster-3", 6379}
    ]),
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx,
            emqx_resource,
            emqx_connector,
            emqx_mysql,
            emqx_redis,
            emqx_bridge_mysql,
            emqx_offline_messages
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps}, {plugin_config, plugin_config()} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)).

init_per_group(mysql_tcp, Config) ->
    set_backend(mysql_tcp, Config);
init_per_group(mysql_ssl, Config) ->
    set_backend(mysql_ssl, Config);
init_per_group(mysql_cleanup, Config) ->
    set_backend(mysql_tcp, Config);
init_per_group(redis_tcp, Config) ->
    set_backend(redis_tcp, Config);
init_per_group(redis_ssl, Config) ->
    set_backend(redis_ssl, Config);
init_per_group(redis_sentinel, Config) ->
    set_backend(redis_sentinel, Config);
init_per_group(redis_cluster, Config) ->
    set_backend(redis_cluster, Config);
init_per_group(redis_cleanup, Config) ->
    set_backend(redis_tcp, Config);
init_per_group(buffered, Config) ->
    set_batch_size(10, Config);
init_per_group(unbuffered, Config) ->
    set_batch_size(1, Config);
init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_Case, Config) ->
    PluginConfig = ?config(plugin_config, Config),
    ok = emqx_offline_messages:on_config_changed(empty_plugin_config(), PluginConfig),
    ok = cleanup_backend(?config(backend, Config)),
    Config.

end_per_testcase(_Case, Config) ->
    PluginConfig = ?config(plugin_config, Config),
    ok = emqx_offline_messages:on_config_changed(PluginConfig, empty_plugin_config()).

t_different_subscribers(_Config) ->
    Topic = unique_topic(),
    Payload = unique_payload(),

    ClientPub = emqtt_connect(),
    {ok, _} = emqtt:publish(ClientPub, Topic, Payload, 1),
    ok = emqtt:stop(ClientPub),
    ct:sleep(500),

    ClientSub0 = emqtt_connect(),
    {ok, _, [1]} = emqtt:subscribe(ClientSub0, Topic, 1),
    ?assertReceive({publish, #{payload := Payload}}, 1000),
    ok = emqtt:stop(ClientSub0),
    ct:sleep(500),

    ClientSub1 = emqtt_connect(),
    {ok, _, [1]} = emqtt:subscribe(ClientSub1, Topic, 1),
    ?assertNotReceive({publish, #{payload := Payload}}, 1000),
    ok = emqtt:stop(ClientSub1).

t_subscription_persistence(_Config) ->
    ClientId = unique_clientid(),
    Topic = unique_topic(),
    SubscriberOpts = [{clientid, ClientId}, {clean_start, true}],

    ClientSub0 = emqtt_connect(SubscriberOpts),
    {ok, _, [1]} = emqtt:subscribe(ClientSub0, Topic, 1),
    ok = emqtt:stop(ClientSub0),
    ct:sleep(500),

    Payload0 = unique_payload(),
    ClientPub = emqtt_connect(),
    {ok, _} = emqtt:publish(ClientPub, Topic, Payload0, 1),
    ct:sleep(500),

    ClientSub1 = emqtt_connect(SubscriberOpts),
    ?assertReceive({publish, #{payload := Payload0}}, 1000),
    ok = emqtt:stop(ClientSub1),
    ct:sleep(500),

    ClientSub2 = emqtt_connect(SubscriberOpts),
    ?assertNotReceive({publish, #{payload := Payload0}}, 1000),
    Payload1 = unique_payload(),
    {ok, _} = emqtt:publish(ClientPub, Topic, Payload1, 1),
    ?assertReceive({publish, #{payload := Payload1}}, 1000),

    ok = emqtt:stop(ClientPub),
    ok = emqtt:stop(ClientSub2).

t_restored_subscription_replay_is_not_duplicated(_Config) ->
    ClientId = unique_clientid(),
    Topic = unique_topic(),
    SubscriberOpts = [{clientid, ClientId}, {clean_start, true}],

    ClientSub0 = emqtt_connect(SubscriberOpts),
    {ok, _, [1]} = emqtt:subscribe(ClientSub0, Topic, 1),
    ok = emqtt:stop(ClientSub0),

    Payload = unique_payload(),
    ClientPub = emqtt_connect(),
    {ok, _} = emqtt:publish(ClientPub, Topic, Payload, 1),
    ct:sleep(500),

    ClientSub1 = emqtt_connect(SubscriberOpts),
    ?assertEqual([Payload], receive_payloads(1000)),

    ok = emqtt:stop(ClientPub),
    ok = emqtt:stop(ClientSub1).

t_message_order(_Config) ->
    Topic = unique_topic(),
    ClientPub = emqtt_connect(),
    lists:foreach(
        fun(I) ->
            {ok, _} = emqtt:publish(ClientPub, Topic, integer_to_binary(I), 1)
        end,
        lists:seq(1, 200)
    ),
    ok = emqtt:stop(ClientPub),
    ct:sleep(500),

    ClientSub = emqtt_connect(),
    {ok, _, [1]} = emqtt:subscribe(ClientSub, Topic, 1),
    Messages = receive_messages(),
    ok = emqtt:stop(ClientSub),

    Expected = lists:seq(1, 200),
    ?assertEqual([], Expected -- Messages, "Not all messages were received"),
    ?assertEqual([], Messages -- Expected, "Duplicate messages were received"),
    InvalidOrder = lists:filter(fun({A, B}) -> A =/= B end, lists:zip(Expected, Messages)),
    ?assertEqual([], InvalidOrder, "Messages were received in the wrong order").

t_mysql_table_cleanup(_Config) ->
    ClientId = unique_clientid(),
    Topic = unique_topic(),
    Payload = unique_payload(),
    SubscriberOpts = [{clientid, ClientId}, {clean_start, true}],

    ClientSub0 = emqtt_connect(SubscriberOpts),
    {ok, _, [1]} = emqtt:subscribe(ClientSub0, Topic, 1),
    ok = emqtt:stop(ClientSub0),

    ClientPub = emqtt_connect(),
    {ok, _} = emqtt:publish(ClientPub, Topic, Payload, 1),
    ok = emqtt:stop(ClientPub),
    ct:sleep(500),

    ClientSub1 = emqtt_connect(SubscriberOpts),
    ?assertReceive({publish, #{payload := Payload}}, 1000),
    {ok, _, _} = emqtt:unsubscribe(ClientSub1, Topic),
    ok = emqtt:stop(ClientSub1),
    ct:sleep(500),

    ?assertEqual([#{<<"COUNT(*)">> => 0}], mysql_query(<<"SELECT COUNT(*) FROM mqtt_msg">>)),
    ?assertEqual([#{<<"COUNT(*)">> => 0}], mysql_query(<<"SELECT COUNT(*) FROM mqtt_sub">>)).

t_redis_table_cleanup(_Config) ->
    ClientId = unique_clientid(),
    Topic = unique_topic(),
    Payload = unique_payload(),
    SubscriberOpts = [{clientid, ClientId}, {clean_start, true}],

    ClientSub0 = emqtt_connect(SubscriberOpts),
    {ok, _, [1]} = emqtt:subscribe(ClientSub0, Topic, 1),
    ok = emqtt:stop(ClientSub0),

    ClientPub = emqtt_connect(),
    {ok, _} = emqtt:publish(ClientPub, Topic, Payload, 1),
    ok = emqtt:stop(ClientPub),
    ct:sleep(500),

    ClientSub1 = emqtt_connect(SubscriberOpts),
    ?assertReceive({publish, #{payload := Payload}}, 1000),
    {ok, _, _} = emqtt:unsubscribe(ClientSub1, Topic),
    ok = emqtt:stop(ClientSub1),
    ct:sleep(500),

    {ok, Pid} = redis_client(),
    ?assertEqual({ok, []}, eredis:q(Pid, [<<"KEYS">>, <<"mqtt:msg:*">>])),
    ?assertEqual({ok, []}, eredis:q(Pid, [<<"KEYS">>, <<"mqtt:sub:*">>])).

set_backend(mysql_tcp, Config) ->
    PluginConfig0 = ?config(plugin_config, Config),
    PluginConfig1 = emqx_utils_maps:deep_put([<<"mysql">>, <<"enable">>], PluginConfig0, true),
    PluginConfig2 = emqx_utils_maps:deep_put(
        [<<"mysql">>, <<"server">>], PluginConfig1, <<"mysql:3306">>
    ),
    PluginConfig3 = emqx_utils_maps:deep_put(
        [<<"mysql">>, <<"ssl">>, <<"enable">>], PluginConfig2, false
    ),
    [{backend, mysql} | ?set_config(plugin_config, PluginConfig3, Config)];
set_backend(mysql_ssl, Config) ->
    PluginConfig0 = ?config(plugin_config, Config),
    PluginConfig1 = emqx_utils_maps:deep_put([<<"mysql">>, <<"enable">>], PluginConfig0, true),
    PluginConfig2 = emqx_utils_maps:deep_put(
        [<<"mysql">>, <<"server">>], PluginConfig1, <<"mysql-tls:3306">>
    ),
    PluginConfig3 = set_ssl_enabled([<<"mysql">>], PluginConfig2),
    [{backend, mysql} | ?set_config(plugin_config, PluginConfig3, Config)];
set_backend(redis_tcp, Config) ->
    PluginConfig0 = ?config(plugin_config, Config),
    PluginConfig1 = emqx_utils_maps:deep_put([<<"redis">>, <<"enable">>], PluginConfig0, true),
    PluginConfig2 = emqx_utils_maps:deep_put(
        [<<"redis">>, <<"servers">>], PluginConfig1, <<"redis:6379">>
    ),
    PluginConfig3 = emqx_utils_maps:deep_put(
        [<<"redis">>, <<"redis_type">>], PluginConfig2, <<"single">>
    ),
    PluginConfig4 = emqx_utils_maps:deep_put(
        [<<"redis">>, <<"ssl">>, <<"enable">>], PluginConfig3, false
    ),
    [{backend, redis} | ?set_config(plugin_config, PluginConfig4, Config)];
set_backend(redis_ssl, Config) ->
    PluginConfig0 = ?config(plugin_config, Config),
    PluginConfig1 = emqx_utils_maps:deep_put([<<"redis">>, <<"enable">>], PluginConfig0, true),
    PluginConfig2 = emqx_utils_maps:deep_put(
        [<<"redis">>, <<"servers">>], PluginConfig1, <<"redis-tls:6380">>
    ),
    PluginConfig3 = emqx_utils_maps:deep_put(
        [<<"redis">>, <<"redis_type">>], PluginConfig2, <<"single">>
    ),
    PluginConfig4 = set_redis_ssl_enabled(PluginConfig3),
    [{backend, redis} | ?set_config(plugin_config, PluginConfig4, Config)];
set_backend(redis_sentinel, Config) ->
    PluginConfig0 = ?config(plugin_config, Config),
    PluginConfig1 = emqx_utils_maps:deep_put([<<"redis">>, <<"enable">>], PluginConfig0, true),
    PluginConfig2 = emqx_utils_maps:deep_put(
        [<<"redis">>, <<"redis_type">>], PluginConfig1, <<"sentinel">>
    ),
    PluginConfig3 = emqx_utils_maps:deep_put(
        [<<"redis">>, <<"servers">>], PluginConfig2, <<"redis-sentinel:26379">>
    ),
    PluginConfig4 = emqx_utils_maps:deep_put(
        [<<"redis">>, <<"sentinel">>], PluginConfig3, <<"mytcpmaster">>
    ),
    PluginConfig5 = emqx_utils_maps:deep_put(
        [<<"redis">>, <<"sentinel_password">>], PluginConfig4, <<"public">>
    ),
    PluginConfig6 = emqx_utils_maps:deep_put(
        [<<"redis">>, <<"ssl">>, <<"enable">>], PluginConfig5, false
    ),
    [{backend, redis} | ?set_config(plugin_config, PluginConfig6, Config)];
set_backend(redis_cluster, Config) ->
    PluginConfig0 = ?config(plugin_config, Config),
    PluginConfig1 = emqx_utils_maps:deep_put([<<"redis">>, <<"enable">>], PluginConfig0, true),
    PluginConfig2 = emqx_utils_maps:deep_put(
        [<<"redis">>, <<"redis_type">>], PluginConfig1, <<"cluster">>
    ),
    PluginConfig3 = emqx_utils_maps:deep_put(
        [<<"redis">>, <<"servers">>], PluginConfig2, ?REDIS_CLUSTER_SERVERS
    ),
    PluginConfig4 = emqx_utils_maps:deep_put(
        [<<"redis">>, <<"ssl">>, <<"enable">>], PluginConfig3, false
    ),
    [{backend, redis} | ?set_config(plugin_config, PluginConfig4, Config)].

set_ssl_enabled(Path, Config0) ->
    Config1 = emqx_utils_maps:deep_put(Path ++ [<<"ssl">>, <<"enable">>], Config0, true),
    Config2 = emqx_utils_maps:deep_put(
        Path ++ [<<"ssl">>, <<"verify">>], Config1, <<"verify_none">>
    ),
    Config3 = emqx_utils_maps:deep_put(
        Path ++ [<<"ssl">>, <<"server_name_indication">>], Config2, <<"disable">>
    ),
    Config4 = emqx_utils_maps:deep_put(Path ++ [<<"ssl">>, <<"cacertfile">>], Config3, <<>>),
    Config5 = emqx_utils_maps:deep_put(Path ++ [<<"ssl">>, <<"certfile">>], Config4, <<>>),
    emqx_utils_maps:deep_put(Path ++ [<<"ssl">>, <<"keyfile">>], Config5, <<>>).

set_redis_ssl_enabled(Config0) ->
    Config1 = set_ssl_enabled([<<"redis">>], Config0),
    CertsDir = filename:join([code:lib_dir(emqx_auth), "test/data/certs"]),
    Config2 = emqx_utils_maps:deep_put(
        [<<"redis">>, <<"ssl">>, <<"cacertfile">>], Config1, filename:join(CertsDir, "ca.crt")
    ),
    Config3 = emqx_utils_maps:deep_put(
        [<<"redis">>, <<"ssl">>, <<"certfile">>], Config2, filename:join(CertsDir, "client.crt")
    ),
    emqx_utils_maps:deep_put(
        [<<"redis">>, <<"ssl">>, <<"keyfile">>], Config3, filename:join(CertsDir, "client.key")
    ).

set_batch_size(BatchSize, Config) ->
    Backend = ?config(backend, Config),
    PluginConfig0 = ?config(plugin_config, Config),
    PluginConfig = emqx_utils_maps:deep_put(
        [atom_to_binary(Backend), <<"batch_size">>], PluginConfig0, BatchSize
    ),
    ?set_config(plugin_config, PluginConfig, Config).

cleanup_backend(mysql) ->
    ok = mysql_query_ok(<<"DELETE FROM mqtt_msg">>),
    ok = mysql_query_ok(<<"DELETE FROM mqtt_sub">>);
cleanup_backend(redis) ->
    {ok, Pid} = redis_client(),
    try
        {ok, <<"OK">>} = eredis:q(Pid, [<<"FLUSHALL">>]),
        ok
    after
        eredis:stop(Pid)
    end.

plugin_config() ->
    DefaultSSLConfig = #{
        <<"enable">> => false,
        <<"verify">> => <<"verify_peer">>,
        <<"cacertfile">> => <<"/certs/ca.crt">>,
        <<"certfile">> => <<"/certs/mysql-client.crt">>,
        <<"keyfile">> => <<"/certs/mysql-client.key">>
    },
    #{
        <<"mysql">> => #{
            <<"enable">> => false,
            <<"ssl">> => DefaultSSLConfig#{<<"server_name_indication">> => <<"mysql-server">>},
            <<"topics">> => [<<"t/#">>],
            <<"server">> => <<"mysql:3306">>,
            <<"username">> => <<"root">>,
            <<"password">> => <<"public">>,
            <<"pool_size">> => 8,
            <<"database">> => <<"mqtt">>,
            <<"init_default_schema">> => true,
            <<"select_message_sql">> => <<"select * from mqtt_msg where topic = ${topic}">>,
            <<"delete_message_sql">> => <<"delete from mqtt_msg where msgid = ${id}">>,
            <<"insert_message_sql">> => <<
                "insert into mqtt_msg(msgid, sender, topic, qos, retain, payload, arrived) "
                "values(${id}, ${from}, ${topic}, ${qos}, ${flags.retain}, "
                "${payload}, FROM_UNIXTIME(${timestamp}/1000))"
            >>,
            <<"insert_subscription_sql">> => <<
                "insert into mqtt_sub(clientid, topic, qos) "
                "values(${clientid}, ${topic}, ${qos}) on duplicate key update qos = ${qos}"
            >>,
            <<"select_subscriptions_sql">> => <<
                "select topic, qos from mqtt_sub where clientid = ${clientid}"
            >>,
            <<"delete_subscription_sql">> => <<
                "delete from mqtt_sub where clientid = ${clientid} and topic = ${topic}"
            >>,
            <<"batch_size">> => 1,
            <<"batch_time">> => 50
        },
        <<"redis">> => #{
            <<"enable">> => false,
            <<"ssl">> => DefaultSSLConfig#{<<"server_name_indication">> => <<"redis-server">>},
            <<"servers">> => <<"redis:6379">>,
            <<"topics">> => [<<"t/#">>],
            <<"redis_type">> => <<"single">>,
            <<"sentinel">> => <<>>,
            <<"pool_size">> => 8,
            <<"username">> => <<>>,
            <<"password">> => <<"public">>,
            <<"sentinel_username">> => <<>>,
            <<"sentinel_password">> => <<>>,
            <<"database">> => 0,
            <<"message_key_prefix">> => <<"mqtt:msg">>,
            <<"subscription_key_prefix">> => <<"mqtt:sub">>,
            <<"message_ttl">> => 7200,
            <<"batch_size">> => 1,
            <<"batch_time">> => 50
        }
    }.

empty_plugin_config() ->
    PluginConfig0 = plugin_config(),
    PluginConfig1 = emqx_utils_maps:deep_put([<<"mysql">>, <<"enable">>], PluginConfig0, false),
    emqx_utils_maps:deep_put([<<"redis">>, <<"enable">>], PluginConfig1, false).

emqtt_connect() ->
    emqtt_connect([]).

emqtt_connect(Opts) ->
    {ok, Pid} = emqtt:start_link(Opts ++ [{proto_ver, v5}]),
    {ok, _} = emqtt:connect(Pid),
    Pid.

unique_id() ->
    binary:encode_hex(crypto:strong_rand_bytes(16)).

unique_topic() ->
    <<"t/", (unique_id())/binary>>.

unique_clientid() ->
    <<"c/", (unique_id())/binary>>.

unique_payload() ->
    <<"p/", (unique_id())/binary>>.

receive_messages() ->
    receive
        {publish, #{payload := Payload}} ->
            [binary_to_integer(Payload) | receive_messages()]
    after 500 ->
        []
    end.

receive_payloads(Timeout) ->
    receive_payloads(Timeout, []).

receive_payloads(Timeout, Acc) ->
    receive
        {publish, #{payload := Payload}} ->
            receive_payloads(Timeout, [Payload | Acc])
    after Timeout ->
        lists:reverse(Acc)
    end.

mysql_query_ok(Query) ->
    case mysql_query_raw(Query) of
        ok -> ok;
        {ok, _Headers, _Rows} -> ok
    end.

mysql_query(Query) ->
    {ok, Header, Rows} = mysql_query_raw(Query),
    [maps:from_list(lists:zip(Header, Row)) || Row <- Rows].

mysql_query_raw(Query) ->
    Pid = mysql_client(),
    try
        mysql:query(Pid, Query)
    after
        mysql:stop(Pid)
    end.

mysql_client() ->
    Opts = [
        {host, ?MYSQL_HOST},
        {port, ?MYSQL_PORT},
        {user, ?MYSQL_USERNAME},
        {password, ?MYSQL_PASSWORD},
        {database, ?MYSQL_DATABASE}
    ],
    {ok, Pid} = mysql:start_link(Opts),
    Pid.

redis_client() ->
    Opts = [
        {host, ?REDIS_HOST},
        {port, ?REDIS_PORT},
        {password, ?REDIS_PASSWORD}
    ],
    eredis:start_link(Opts).

wait_for_tcp_servers(Servers) ->
    wait_for_tcp_servers(Servers, 10).

wait_for_tcp_servers(_Servers, 0) ->
    ct:fail(tcp_servers_unavailable);
wait_for_tcp_servers(Servers, Attempts) ->
    case emqx_common_test_helpers:is_all_tcp_servers_available(Servers) of
        true ->
            ok;
        false ->
            timer:sleep(1000),
            wait_for_tcp_servers(Servers, Attempts - 1)
    end.
