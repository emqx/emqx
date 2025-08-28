%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_limiter_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("asserts.hrl").

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

-define(EMQX_CONFIG, #{
    <<"listeners">> => #{
        <<"tcp">> => #{
            <<"default">> => #{
                <<"acceptors">> => 1
            }
        }
    }
}).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Config.
end_per_suite(_Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    erlang:process_flag(trap_exit, true),
    Apps = emqx_cth_suite:start([{emqx, ?EMQX_CONFIG}], #{
        work_dir => emqx_cth_suite:work_dir(TestCase, Config)
    }),
    snabbkaffe:start_trace(),
    [{apps, Apps} | Config].

end_per_testcase(_TestCase, Config) ->
    snabbkaffe:stop(),
    emqx_cth_suite:stop(?config(apps, Config)).

%%--------------------------------------------------------------------
%% Tests
%%--------------------------------------------------------------------

t_max_conn_listener(_Config) ->
    ?assertWaitEvent(
        begin
            set_limiter_for_listener(max_conn_rate, <<"2/500ms">>),
            ct:sleep(550),
            spawn_connector()
        end,
        #{?snk_kind := esockd_limiter_consume_pause},
        10_000
    ).

t_max_conn_zone(_Config) ->
    ?assertWaitEvent(
        begin
            set_limiter_for_zone(max_conn_rate, <<"2/500ms">>),
            ct:sleep(550),
            spawn_connector()
        end,
        #{?snk_kind := esockd_limiter_consume_pause},
        10_000
    ).

t_max_message_rate_listener(_Config) ->
    ?assertWaitEvent(
        begin
            set_limiter_for_listener(messages_rate, <<"2/500ms">>),
            ct:sleep(550),
            spawn_publisher(100, 1)
        end,
        #{?snk_kind := limiter_exclusive_try_consume, success := false},
        10_000
    ).

t_max_message_rate_zone(_Config) ->
    ?assertWaitEvent(
        begin
            set_limiter_for_zone(messages_rate, <<"2/500ms">>),
            ct:sleep(550),
            spawn_publisher(100, 1)
        end,
        #{?snk_kind := limiter_shared_try_consume, success := false},
        10_000
    ).

t_bytes_rate_listener(_Config) ->
    ?assertWaitEvent(
        begin
            set_limiter_for_listener(bytes_rate, <<"5kb/1m">>),
            ct:sleep(550),
            spawn_publisher(100, 1)
        end,
        #{?snk_kind := limiter_exclusive_try_consume, success := false},
        10_000
    ).

t_bytes_rate_zone(_Config) ->
    ?assertWaitEvent(
        begin
            set_limiter_for_zone(bytes_rate, <<"5kb/1m">>),
            ct:sleep(550),
            spawn_publisher(100, 1)
        end,
        #{?snk_kind := limiter_shared_try_consume, success := false},
        10_000
    ).

t_metrics_quota_exceeded(_Config) ->
    ?assertWaitEvent(
        begin
            set_limiter_for_zone(messages_rate, <<"2/500ms">>),
            ct:sleep(550),
            spawn_publisher(100, 0)
        end,
        #{?snk_kind := limiter_shared_try_consume, success := false},
        10_000
    ),
    ?retry(
        _Inteval = 100,
        _Attempts = 10,
        ?assert(
            emqx_metrics:val('packets.publish.quota_exceeded') > 0
        )
    ),
    ?retry(
        _Inteval = 100,
        _Attempts = 10,
        ?assert(
            emqx_metrics:val('messages.dropped.quota_exceeded') > 0
        )
    ).

t_add_new_zone_and_listener(_Config) ->
    %% Add a new zone
    emqx:update_config([mqtt, limiter], #{<<"messages_rate">> => <<"1/500ms">>}),
    OldZoneConfig = emqx_config:get_raw([zones]),
    NewZoneConfig = OldZoneConfig#{
        <<"myzone1">> => #{
            <<"mqtt">> => #{<<"limiter">> => #{<<"messages_rate">> => <<"100/500ms">>}}
        },
        <<"myzone2">> => #{}
    },
    {ok, _} = emqx:update_config([zones], NewZoneConfig),

    %% Check that limiters are configured for the new zone
    %% For zone `myzone1`, the limiter is inherited from the default zone.
    {_, LimiterOptions1} = emqx_limiter_registry:find_group({zone, myzone1}),
    ?assertMatch(
        #{capacity := 100},
        proplists:get_value(messages, LimiterOptions1)
    ),
    %% For zone `myzone2`, the limiter is overridden by.
    {_, LimiterOptions2} = emqx_limiter_registry:find_group({zone, myzone2}),
    ?assertMatch(
        #{capacity := 1},
        proplists:get_value(messages, LimiterOptions2)
    ),

    %% Check that a listener is created successfully,
    %% i.e. is able to connect to zone's limiters.
    OldListenerConfig = emqx_config:get_raw([listeners]),
    NewListenerConfig0 = emqx_utils_maps:deep_put(
        [<<"tcp">>, <<"mylistener">>],
        OldListenerConfig,
        #{
            <<"zone">> => <<"myzone1">>,
            <<"bind">> => random_bind()
        }
    ),
    NewListenerConfig = emqx_utils_maps:deep_put(
        [<<"tcp">>, <<"mylistener">>],
        NewListenerConfig0,
        #{
            <<"zone">> => <<"myzone2">>,
            <<"bind">> => random_bind()
        }
    ),
    ?assertMatch(
        {ok, _},
        emqx:update_config([listeners], NewListenerConfig)
    ),

    %% Remove the listeners and zone
    {ok, _} = emqx:update_config([listeners], OldListenerConfig),
    {ok, _} = emqx:update_config([zones], OldZoneConfig),

    %% Check that limiters are removed
    ?assertEqual(
        undefined,
        emqx_limiter_registry:find_group({zone, myzone1})
    ),
    ?assertEqual(
        undefined,
        emqx_limiter_registry:find_group({zone, myzone2})
    ).

t_handle_global_zone_change(_Config) ->
    %% Add a new zone without explicit limits (thus inherited from the global defaults)
    emqx:update_config([mqtt, limiter], #{}),
    OldZoneConfig = emqx_config:get_raw([zones]),
    NewZoneConfig = OldZoneConfig#{<<"myzone">> => #{}},
    {ok, _} = emqx:update_config([zones], NewZoneConfig),
    {_, LimiterOptions0} = emqx_limiter_registry:find_group({zone, myzone}),
    ?assertMatch(
        #{capacity := infinity},
        proplists:get_value(messages, LimiterOptions0)
    ),

    %% Update the global defaults to have limits
    emqx:update_config([mqtt, limiter], #{<<"messages_rate">> => <<"1/500ms">>}),

    %% Check that the zone's limiter has applied the new limits
    {_, LimiterOptions1} = emqx_limiter_registry:find_group({zone, myzone}),
    ?assertMatch(
        #{capacity := 1},
        proplists:get_value(messages, LimiterOptions1)
    ).

t_update_group_no_change(_Config) ->
    %% Ensure that limiter impl is not asked to be updated if config hasn't changed.
    Group = ?MODULE,
    LConf = #{
        capacity => 42,
        interval => 100,
        burst_capacity => 42_000,
        burst_interval => 100_000
    },
    ok = emqx_limiter:create_group(?MODULE, Group, [{conn, LConf}, {byte, LConf}]),
    ?assertEqual(
        ok,
        emqx_limiter:update_group(Group, [{byte, LConf}, {conn, LConf}])
    ).

%%--------------------------------------------------------------------
%% Limiter implementation (see `t_update_group_no_change/1`)
%%--------------------------------------------------------------------

-spec create_group(emqx_limiter:group(), [{emqx_limiter:name(), emqx_limiter:options()}]) -> ok.
create_group(_Group, _LimiterConfigs) ->
    ok.

-spec delete_group(emqx_limiter:group()) -> ok.
delete_group(_Group) ->
    ok.

-spec update_group(emqx_limiter:group(), [{emqx_limiter:name(), emqx_limiter:options()}]) ->
    ok.
update_group(_Group, _LimiterConfigs) ->
    error(should_not_happen).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

set_limiter_for_zone(Key, Value) ->
    KeyBin = atom_to_binary(Key, utf8),
    MqttConf0 = emqx_config:fill_defaults(#{<<"mqtt">> => emqx:get_raw_config([mqtt])}),
    MqttConf1 = emqx_utils_maps:deep_put([<<"mqtt">>, <<"limiter">>, KeyBin], MqttConf0, Value),
    {ok, _} = emqx:update_config([mqtt], maps:get(<<"mqtt">>, MqttConf1)).

set_limiter_for_listener(Key, Value) ->
    KeyBin = atom_to_binary(Key, utf8),
    emqx:update_config(
        [listeners, tcp, default],
        {update, #{
            KeyBin => Value
        }}
    ),
    ok.

spawn_connector() ->
    spawn_link(fun() ->
        run_connector()
    end).

run_connector() ->
    {ok, C} = emqtt:start_link([{host, "127.0.0.1"}, {port, 1883}]),
    case emqtt:connect(C) of
        {ok, _} ->
            {ok, _} = emqtt:publish(C, <<"test">>, <<"a">>, 1),
            ok = emqtt:stop(C);
        {error, _Reason} ->
            ok
    end,
    ct:sleep(10),
    run_connector().

spawn_publisher(PayloadSize, QoS) ->
    spawn_link(fun() ->
        {ok, C} = emqtt:start_link([{host, "127.0.0.1"}, {port, 1883}]),
        {ok, _} = emqtt:connect(C),
        run_publisher(C, PayloadSize, QoS)
    end).

run_publisher(C, PayloadSize, QoS) ->
    _ = emqtt:publish(C, <<"test">>, binary:copy(<<"a">>, PayloadSize), QoS),
    ct:sleep(10),
    run_publisher(C, PayloadSize, QoS).

random_bind() ->
    Host = "127.0.0.1",
    Port = emqx_common_test_helpers:select_free_port(tcp),
    iolist_to_binary(emqx_listeners:format_bind({Host, Port})).
