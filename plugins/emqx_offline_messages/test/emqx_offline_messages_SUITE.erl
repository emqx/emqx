%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_offline_messages_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/asserts.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([mria], #{
        work_dir => emqx_cth_suite:work_dir(Config)
    }),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    meck:unload(),
    ok = emqx_cth_suite:stop(?config(apps, Config)).

end_per_testcase(_Case, Config) ->
    meck:unload(),
    Config.

t_fix_ssl_config_removes_empty_files(_Config) ->
    Config0 = #{
        <<"ssl">> => #{
            <<"enable">> => true,
            <<"cacertfile">> => <<>>,
            <<"certfile">> => <<"/tmp/client.crt">>
        }
    },
    ?assertEqual(
        #{
            <<"ssl">> => #{
                <<"enable">> => true,
                <<"certfile">> => <<"/tmp/client.crt">>
            }
        },
        emqx_offline_messages_utils:fix_ssl_config(Config0)
    ),
    ?assertEqual(
        #{<<"ssl">> => #{<<"enable">> => false}},
        emqx_offline_messages_utils:fix_ssl_config(#{})
    ).

t_make_resource_opts_uses_sync_mode(_Config) ->
    Opts = emqx_offline_messages_utils:make_resource_opts(#{
        <<"batch_size">> => 10,
        <<"batch_time">> => 250
    }),
    ?assertMatch(
        #{
            start_after_created := true,
            batch_size := 10,
            batch_time := 250,
            query_mode := sync,
            owner_id := <<"omp">>
        },
        Opts
    ).

t_need_persist_message_requires_qos_and_topic_match(_Config) ->
    Filters = emqx_offline_messages_utils:topic_filters(#{<<"topics">> => [<<"devices/+/events">>]}),
    Matching = emqx_message:make(<<"client-a">>, 1, <<"devices/d1/events">>, <<"payload">>),
    Qos0 = emqx_message:make(<<"client-a">>, 0, <<"devices/d1/events">>, <<"payload">>),
    OtherTopic = emqx_message:make(<<"client-a">>, 1, <<"devices/d1/state">>, <<"payload">>),
    ?assert(emqx_offline_messages_utils:need_persist_message(Matching, Filters)),
    ?assertNot(emqx_offline_messages_utils:need_persist_message(Qos0, Filters)),
    ?assertNot(emqx_offline_messages_utils:need_persist_message(OtherTopic, Filters)).

t_main_callbacks_handle_stopped_server(_Config) ->
    ?assertEqual(ok, emqx_offline_messages:on_config_changed(#{}, #{})),
    ?assertEqual({error, <<"Plugin is not running">>}, emqx_offline_messages:on_health_check()).

t_resource_health_status_formats_results(_Config) ->
    ok = meck:new(emqx_resource, [non_strict]),
    ok = meck:expect(emqx_resource, health_check, fun
        (<<"redis">>) -> {ok, connected};
        (<<"mysql">>) -> {ok, disconnected};
        (<<"broken">>) -> {error, timeout}
    end),
    ?assertEqual(ok, emqx_offline_messages_utils:resource_health_status(<<"Redis">>, <<"redis">>)),
    ?assertMatch(
        {error, <<"Resource MySQL is not connected", _/binary>>},
        emqx_offline_messages_utils:resource_health_status(<<"MySQL">>, <<"mysql">>)
    ),
    ?assertMatch(
        {error, <<"Resource Backend health check failed", _/binary>>},
        emqx_offline_messages_utils:resource_health_status(<<"Backend">>, <<"broken">>)
    ).

t_redis_restored_subscription_replay_is_consumed(_Config) ->
    Self = self(),
    ClientId = <<"client1">>,
    Topic = <<"offline/redis/distinct/client1">>,
    SubTable = <<"mqtt:sub:", ClientId/binary>>,
    Context = #{
        subscription_key_prefix => <<"mqtt:sub">>,
        message_key_prefix => <<"mqtt:msg">>,
        message_ttl => 7200
    },
    ok = meck:new(emqx_resource, [non_strict]),
    ok = meck:new(emqx_metrics_worker, [non_strict]),
    ok = meck:expect(emqx_metrics_worker, inc, fun(_, _, _) -> ok end),
    ok = meck:expect(emqx_resource, simple_sync_query, fun
        (<<"offline_messages_redis">>, {cmd, [<<"HGETALL">>, SubTable0]}) when
            SubTable0 =:= SubTable
        ->
            {ok, [Topic, <<"1">>]};
        (<<"offline_messages_redis">>, {cmd, [<<"HSET">>, _, _, _]}) ->
            {ok, 1};
        (<<"offline_messages_redis">>, {cmd, [<<"ZRANGE">>, _, 0, -1, <<"WITHSCORES">>]}) ->
            Self ! redis_replay,
            {ok, []}
    end),

    ok = emqx_offline_messages_redis:on_client_connected(#{clientid => ClientId}, #{}, Context),
    ?assertReceive({subscribe, [{Topic, #{qos := 1}}]}),
    ok = emqx_offline_messages_redis:on_session_subscribed(
        #{clientid => ClientId}, Topic, #{qos => 1, is_new => false}, Context
    ),
    ?assertReceive(redis_replay),
    ok = emqx_offline_messages_redis:on_session_subscribed(
        #{clientid => ClientId}, Topic, #{qos => 1, is_new => false}, Context
    ),
    ?assertNotReceive(redis_replay, 100).

t_mysql_restored_subscription_replay_is_consumed(_Config) ->
    Self = self(),
    ClientId = <<"client1">>,
    Topic = <<"offline/mysql/distinct/client1">>,
    Context = #{
        statements => #{
            select_subscriptions_sql => {select_subscriptions_sql, []},
            insert_subscription_sql => {insert_subscription_sql, []},
            select_message_sql => {select_message_sql, []}
        }
    },
    ok = meck:new(emqx_resource, [non_strict]),
    ok = meck:new(emqx_metrics_worker, [non_strict]),
    ok = meck:expect(emqx_metrics_worker, inc, fun(_, _, _) -> ok end),
    ok = meck:expect(emqx_resource, simple_sync_query, fun
        (<<"offline_messages_mysql">>, {sql, select_subscriptions_sql, [], 1000}) ->
            {ok, [<<"topic">>, <<"qos">>], [[Topic, 1]]};
        (<<"offline_messages_mysql">>, {sql, insert_subscription_sql, [], 1000}) ->
            ok;
        (<<"offline_messages_mysql">>, {sql, select_message_sql, [], 1000}) ->
            Self ! mysql_replay,
            {ok, [], []}
    end),

    ok = emqx_offline_messages_mysql:on_client_connected(#{clientid => ClientId}, #{}, Context),
    ?assertReceive({subscribe, [{Topic, #{qos := 1}}]}),
    ok = emqx_offline_messages_mysql:on_session_subscribed(
        #{clientid => ClientId}, Topic, #{qos => 1, is_new => false}, Context
    ),
    ?assertReceive(mysql_replay),
    ok = emqx_offline_messages_mysql:on_session_subscribed(
        #{clientid => ClientId}, Topic, #{qos => 1, is_new => false}, Context
    ),
    ?assertNotReceive(mysql_replay, 100).

t_redis_connector_batches_queries(_Config) ->
    ok = meck:new(emqx_redis, [non_strict]),
    ok = meck:expect(emqx_redis, on_query, fun
        (inst, {cmds, [[<<"PING">>], [<<"SET">>, <<"k">>, <<"v">>]]}, state) ->
            flattened;
        (_Inst, Query, _State) ->
            {unexpected, Query}
    end),
    ?assertEqual(
        flattened,
        emqx_offline_messages_redis_connector:on_batch_query(
            inst,
            [{cmd, [<<"PING">>]}, {cmds, [[<<"SET">>, <<"k">>, <<"v">>]]}],
            state
        )
    ).
