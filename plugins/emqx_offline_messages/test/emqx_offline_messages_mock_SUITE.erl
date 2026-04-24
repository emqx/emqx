%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_offline_messages_mock_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(TEST_PID_KEY, {?MODULE, test_pid}).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Port = emqx_common_test_helpers:select_free_port(tcp),
    EmqxConfig = #{
        listeners => #{
            tcp => #{default => #{bind => Port}},
            ssl => #{default => #{bind => 0}},
            ws => #{default => #{bind => 0}},
            wss => #{default => #{bind => 0}}
        }
    },
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            {emqx, #{config => EmqxConfig}},
            emqx_offline_messages
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps}, {mqtt_port, Port} | Config].

end_per_suite(Config) ->
    meck:unload(),
    ok = emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_Case, Config) ->
    persistent_term:put(?TEST_PID_KEY, self()),
    ok = meck:new(emqx_resource, [non_strict]),
    ok = meck:expect(emqx_resource, create_local, fun(
        <<"offline_messages_redis">>, <<"omp">>, emqx_offline_messages_redis_connector, Conf, Opts
    ) ->
        notify({create_local, Conf, Opts}),
        {ok, started}
    end),
    ok = meck:expect(emqx_resource, remove_local, fun(<<"offline_messages_redis">>) ->
        notify(remove_local),
        ok
    end),
    ok = meck:expect(emqx_resource, health_check, fun(<<"offline_messages_redis">>) ->
        notify(health_check),
        {ok, connected}
    end),
    ok = meck:expect(emqx_resource, simple_sync_query, fun
        (<<"offline_messages_redis">>, {cmd, Cmd}) ->
            notify({cmd, Cmd}),
            reply_for_cmd(Cmd);
        (<<"offline_messages_redis">>, {cmds, Cmds}) ->
            notify({cmds, Cmds}),
            {ok, lists:duplicate(length(Cmds), ok)}
    end),
    Config.

end_per_testcase(_Case, Config) ->
    catch emqx_offline_messages:on_config_changed(current_plugin_config(), #{}),
    persistent_term:erase(?TEST_PID_KEY),
    meck:unload(),
    Config.

t_redis_smoke_persists_matching_publish_and_subscription_fetches(Config) ->
    RedisConf = redis_config(),
    Port = ?config(mqtt_port, Config),
    ok = emqx_offline_messages:on_config_changed(#{}, #{<<"redis">> => RedisConf}),
    ?assertMatch({create_local, _, _}, wait_for_event()),

    {ok, Pub} = connect_client(Port, <<"smoke_pub">>),
    clear_events(),
    {ok, _} = emqtt:publish(Pub, <<"offline/demo">>, <<"payload">>, 1),
    PublishEvents = collect_events(400),
    ?assert(has_redis_store_batch(PublishEvents)),

    {ok, Sub} = connect_client(Port, <<"smoke_sub">>),
    clear_events(),
    {ok, _, [1]} = emqtt:subscribe(Sub, <<"offline/demo">>, 1),
    SubscribeEvents = collect_events(400),
    ?assert(has_subscription_insert(SubscribeEvents)),
    ?assert(has_fetch_attempt(SubscribeEvents)),

    stop_client(Pub),
    stop_client(Sub).

t_redis_smoke_ignores_nonmatching_or_qos0_publish(Config) ->
    RedisConf = redis_config(),
    Port = ?config(mqtt_port, Config),
    ok = emqx_offline_messages:on_config_changed(#{}, #{<<"redis">> => RedisConf}),
    ?assertMatch({create_local, _, _}, wait_for_event()),

    {ok, Pub} = connect_client(Port, <<"smoke_pub_ignored">>),

    clear_events(),
    ok = emqtt:publish(Pub, <<"offline/qos0">>, <<"payload">>, 0),
    EventsQos0 = collect_events(400),
    ?assertNot(has_redis_store_batch(EventsQos0)),

    clear_events(),
    {ok, _} = emqtt:publish(Pub, <<"other/topic">>, <<"payload">>, 1),
    EventsOtherTopic = collect_events(400),
    ?assertNot(has_redis_store_batch(EventsOtherTopic)),

    stop_client(Pub),
    ok = emqx_offline_messages:on_config_changed(#{<<"redis">> => RedisConf}, #{}),
    ?assertEqual(remove_local, wait_for_event()).

redis_config() ->
    #{
        <<"enable">> => true,
        <<"servers">> => <<"127.0.0.1:6379">>,
        <<"redis_type">> => <<"single">>,
        <<"pool_size">> => 1,
        <<"topics">> => [<<"offline/#">>],
        <<"batch_size">> => 1,
        <<"batch_time">> => 0
    }.

current_plugin_config() ->
    emqx_offline_messages:current_config().

connect_client(Port, ClientId) ->
    {ok, Pid} = emqtt:start_link(#{
        host => "127.0.0.1",
        port => Port,
        proto_ver => v5,
        clientid => ClientId
    }),
    monitor(process, Pid),
    unlink(Pid),
    case emqtt:connect(Pid) of
        {ok, _} ->
            {ok, Pid};
        {error, Reason} ->
            stop_client(Pid),
            erlang:error({connect_failed, Reason})
    end.

stop_client(Pid) ->
    catch emqtt:stop(Pid),
    receive
        {'DOWN', _, process, Pid, _Reason} -> ok
    after 1000 ->
        ok
    end.

notify(Event) ->
    case persistent_term:get(?TEST_PID_KEY, undefined) of
        undefined -> ok;
        Pid -> Pid ! {resource_event, Event}
    end.

reply_for_cmd([<<"HGETALL">> | _]) ->
    {ok, []};
reply_for_cmd([<<"ZRANGE">> | _]) ->
    {ok, []};
reply_for_cmd([<<"HSET">> | _]) ->
    {ok, 1};
reply_for_cmd([<<"HDEL">> | _]) ->
    {ok, 1};
reply_for_cmd([<<"DEL">> | _]) ->
    {ok, 1};
reply_for_cmd([<<"ZREM">> | _]) ->
    {ok, 1};
reply_for_cmd(Cmd) ->
    erlang:error({unexpected_cmd, Cmd}).

wait_for_event() ->
    receive
        {resource_event, Event} -> Event
    after 1000 ->
        erlang:error(timeout_waiting_for_resource_event)
    end.

collect_events(Timeout) ->
    collect_events([], Timeout).

collect_events(Acc, Timeout) ->
    receive
        {resource_event, Event} ->
            collect_events([Event | Acc], Timeout)
    after Timeout ->
        lists:reverse(Acc)
    end.

clear_events() ->
    _ = collect_events(0),
    ok.

has_redis_store_batch(Events) ->
    lists:any(
        fun
            ({cmds, Cmds}) ->
                has_cmd_prefix(Cmds, <<"HMSET">>) andalso
                    has_cmd_prefix(Cmds, <<"ZADD">>) andalso
                    has_cmd_prefix(Cmds, <<"EXPIRE">>);
            (_) ->
                false
        end,
        Events
    ).

has_subscription_insert(Events) ->
    lists:any(
        fun
            ({cmd, [<<"HSET">>, _, <<"offline/demo">>, _]}) -> true;
            (_) -> false
        end,
        Events
    ).

has_fetch_attempt(Events) ->
    lists:any(
        fun
            ({cmd, [<<"ZRANGE">>, _, 0, -1, <<"WITHSCORES">>]}) -> true;
            (_) -> false
        end,
        Events
    ).

has_cmd_prefix(Cmds, ExpectedPrefix) ->
    lists:any(
        fun
            ([Prefix | _]) when Prefix =:= ExpectedPrefix -> true;
            (_) -> false
        end,
        Cmds
    ).
