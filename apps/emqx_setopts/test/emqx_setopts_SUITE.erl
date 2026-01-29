%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_setopts_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(KEEPALIVE_BULK_TOPIC, <<"$SETOPTS/mqtt/keepalive-bulk">>).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx, """
                listeners {
                    tcp.default.bind = 1883
                    ssl.default = marked_for_deletion
                    quic.default = marked_for_deletion
                    ws.default = marked_for_deletion
                    wss.default = marked_for_deletion
                }
            """},
            emqx_setopts
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

t_dynamic_keepalive_update_extend(_) ->
    emqx_config:put_zone_conf(default, [mqtt, keepalive_multiplier], 1.5),
    emqx_config:put_zone_conf(default, [mqtt, keepalive_check_interval], 1000),
    erlang:process_flag(trap_exit, true),
    ClientID = <<"dynamic_extend">>,
    KeepaliveSec = 2,
    {ok, C} = emqtt:start_link([
        {keepalive, KeepaliveSec},
        {clientid, binary_to_list(ClientID)}
    ]),
    {ok, _} = emqtt:connect(C),
    emqtt:pause(C),
    [ChannelPid] = emqx_cm:lookup_channels(ClientID),
    erlang:link(ChannelPid),
    ct:sleep(1100),
    ok = publish_keepalive_self(C, 10),
    ?assertMatch(#{conninfo := #{keepalive := 10}}, wait_for_keepalive(ClientID, 10, 2000)),
    ?assertMatch(
        no_keepalive_timeout_received,
        receive_msg_in_time(ChannelPid, C, 2500),
        2500
    ),
    ok = emqtt:stop(C).

t_dynamic_keepalive_update_shorten(_) ->
    emqx_config:put_zone_conf(default, [mqtt, keepalive_multiplier], 1.5),
    emqx_config:put_zone_conf(default, [mqtt, keepalive_check_interval], 1000),
    erlang:process_flag(trap_exit, true),
    ClientID = <<"dynamic_shorten">>,
    KeepaliveSec = 10,
    {ok, C} = emqtt:start_link([
        {keepalive, KeepaliveSec},
        {clientid, binary_to_list(ClientID)}
    ]),
    {ok, _} = emqtt:connect(C),
    emqtt:pause(C),
    [ChannelPid] = emqx_cm:lookup_channels(ClientID),
    erlang:link(ChannelPid),
    ct:sleep(2500),
    ok = publish_keepalive_self(C, 1),
    ?assertMatch(#{conninfo := #{keepalive := 1}}, wait_for_keepalive(ClientID, 1, 2000)),
    ?assertMatch(ok, receive_msg_in_time(ChannelPid, C, 4000)).

t_dynamic_keepalive_update_batch(_) ->
    emqx_config:put_zone_conf(default, [mqtt, keepalive_multiplier], 1.5),
    emqx_config:put_zone_conf(default, [mqtt, keepalive_check_interval], 1000),
    ClientId1 = <<"dynamic_batch_1">>,
    ClientId2 = <<"dynamic_batch_2">>,
    {ok, C1} = emqtt:start_link([{keepalive, 5}, {clientid, binary_to_list(ClientId1)}]),
    {ok, C2} = emqtt:start_link([{keepalive, 5}, {clientid, binary_to_list(ClientId2)}]),
    {ok, _} = emqtt:connect(C1),
    {ok, _} = emqtt:connect(C2),
    ok = publish_keepalive_update([
        #{clientid => ClientId1, keepalive => 7},
        #{clientid => <<"missing_client">>, keepalive => 3}
    ]),
    ?assertMatch(#{conninfo := #{keepalive := 7}}, wait_for_keepalive(ClientId1, 7, 2000)),
    ok = emqtt:stop(C1),
    ok = emqtt:stop(C2).

t_dynamic_keepalive_update_invalid_payload(_) ->
    {ok, Sub} = emqtt:start_link([]),
    {ok, _} = emqtt:connect(Sub),
    {ok, _, [0]} = emqtt:subscribe(Sub, <<"t/invalid">>, []),
    ClientId = <<"dynamic_invalid">>,
    {ok, Pub} = emqtt:start_link([{clientid, binary_to_list(ClientId)}]),
    {ok, _} = emqtt:connect(Pub),
    _ = emqtt:publish(Pub, <<"$SETOPTS/mqtt/keepalive">>, <<"not_int">>),
    ok = emqtt:stop(Pub),
    {ok, Pub2} = emqtt:start_link([]),
    {ok, _} = emqtt:connect(Pub2),
    _ = emqtt:publish(Pub2, <<"t/invalid">>, <<"ok">>),
    receive
        {publish, #{payload := <<"ok">>}} ->
            ok
    after 1000 ->
        error(no_message_received_after_invalid_payload)
    end,
    ok = emqtt:stop(Pub2),
    ok = emqtt:stop(Sub).

t_dynamic_keepalive_update_timeout(_) ->
    ClientID = <<"dynamic_timeout">>,
    {ok, C} = emqtt:start_link([
        {keepalive, 5},
        {clientid, binary_to_list(ClientID)}
    ]),
    {ok, _} = emqtt:connect(C),
    [ChannelPid] = emqx_cm:lookup_channels(ClientID),
    ok = sys:suspend(ChannelPid),
    Result =
        try call_in_time(fun() -> emqx_setopts:set_keepalive({ClientID, 9}) end, 500) of
            CallResult -> CallResult
        after
            ok = sys:resume(ChannelPid),
            ok = emqtt:stop(C)
        end,
    ?assertMatch({ok, ok}, Result).

t_dynamic_keepalive_update_timeout_batch_call(_) ->
    ClientID = <<"dynamic_timeout_batch_call">>,
    {ok, C} = emqtt:start_link([
        {keepalive, 5},
        {clientid, binary_to_list(ClientID)}
    ]),
    {ok, _} = emqtt:connect(C),
    ok = sys:suspend(whereis(emqx_setopts)),
    Result = emqx_setopts:set_keepalive({ClientID, 9}),
    ok = sys:resume(whereis(emqx_setopts)),
    ok = emqtt:stop(C),
    ?assertMatch({error, timeout}, Result).

t_dynamic_keepalive_batch_missing_client(_) ->
    ClientId = <<"dynamic_missing_client">>,
    ?assertMatch(
        {error, not_found},
        emqx_setopts:set_keepalive({ClientId, 9})
    ).

t_dynamic_keepalive_batch_deadline_expired(_) ->
    Alias = erlang:alias([reply]),
    Past = erlang:monotonic_time(millisecond) - 1,
    gen_server:cast(whereis(emqx_setopts), {keepalive_sync, <<"deadline">>, 1, Past, Alias}),
    receive
        {Alias, _Reply} ->
            erlang:unalias(Alias),
            error(unexpected_reply)
    after 100 ->
        erlang:unalias(Alias),
        ok
    end.

t_dynamic_keepalive_registry_disabled_lookup(_) ->
    Prev = emqx:get_config([broker, enable_session_registry]),
    ok = emqx_config:put([broker, enable_session_registry], false),
    ClientId = <<"dynamic_registry_disabled">>,
    ?assertMatch(
        {error, not_found},
        emqx_setopts:set_keepalive({ClientId, 9})
    ),
    ok = emqx_config:put([broker, enable_session_registry], Prev).

t_dynamic_keepalive_bulk_invalid_payloads(_) ->
    ClientId = <<"dynamic_bulk_invalid">>,
    {ok, C} = emqtt:start_link([{keepalive, 5}, {clientid, binary_to_list(ClientId)}]),
    {ok, _} = emqtt:connect(C),
    _ = emqtt:publish(C, ?KEEPALIVE_BULK_TOPIC, <<"not_json">>),
    _ = emqtt:publish(C, ?KEEPALIVE_BULK_TOPIC, <<"{\"foo\":1}">>),
    _ = emqtt:publish(C, ?KEEPALIVE_BULK_TOPIC, <<"[{\"clientid\":\"", ClientId/binary, "\"}]">>),
    _ = emqtt:publish(
        C,
        ?KEEPALIVE_BULK_TOPIC,
        <<"[{\"clientid\":\"", ClientId/binary, "\",\"keepalive\":\"bad\"}]">>
    ),
    ?assertMatch(#{conninfo := #{keepalive := 5}}, wait_for_keepalive(ClientId, 5, 2000)),
    ok = emqtt:stop(C).

t_dynamic_keepalive_non_client_publish_forbidden(_) ->
    ClientId = <<"dynamic_forbidden">>,
    {ok, C} = emqtt:start_link([{keepalive, 5}, {clientid, binary_to_list(ClientId)}]),
    {ok, _} = emqtt:connect(C),
    Msg = emqx_message:make(http_api, 0, <<"$SETOPTS/mqtt/keepalive">>, <<"10">>, #{}, #{}),
    _ = emqx:publish(Msg),
    ?assertMatch(#{conninfo := #{keepalive := 5}}, wait_for_keepalive(ClientId, 5, 2000)),
    ok = emqtt:stop(C).

t_dynamic_keepalive_channel_not_connected(_) ->
    ClientId = <<"dynamic_not_connected">>,
    {ok, C} = emqtt:start_link([{keepalive, 5}, {clientid, binary_to_list(ClientId)}]),
    {ok, _} = emqtt:connect(C),
    [ChannelPid] = emqx_cm:lookup_channels(ClientId),
    true = ets:delete(emqx_channel_live, ChannelPid),
    ?assertMatch(
        [{ClientId, {error, not_found}}],
        emqx_setopts:do_call_keepalive_clients([{ClientId, 9}])
    ),
    ok = emqtt:stop(C).

t_extract_batch_client_result_branches(_) ->
    ClientId = <<"branchy">>,
    ?assertEqual(
        ok,
        emqx_setopts:test_extract_batch_client_result(
            ClientId,
            [
                [{ClientId, ok}],
                [{ClientId, {error, e1}}],
                [{<<"other">>, ok}]
            ]
        )
    ),
    ?assertEqual(
        {error, e1},
        emqx_setopts:test_extract_batch_client_result(
            ClientId,
            [
                [{ClientId, {error, e1}}],
                [{ClientId, {error, e2}}]
            ]
        )
    ),
    ?assertEqual(
        {error, e1},
        emqx_setopts:test_extract_batch_client_result(
            ClientId,
            [
                {error, e1},
                {error, e2},
                foo
            ]
        )
    ).

t_setopts_internal_misc(_) ->
    ?assertEqual(ignored, gen_server:call(whereis(emqx_setopts), ping)),
    ok = gen_server:cast(whereis(emqx_setopts), ping),
    whereis(emqx_setopts) ! ping,
    ?assertMatch(#{}, sys:get_state(whereis(emqx_setopts))),
    ok.

t_dynamic_keepalive_update_clamped(_) ->
    emqx_config:put_zone_conf(default, [mqtt, server_keepalive], 6),
    ClientId1 = <<"dynamic_clamp_1">>,
    ClientId2 = <<"dynamic_clamp_2">>,
    {ok, C1} = emqtt:start_link([{keepalive, 5}, {clientid, binary_to_list(ClientId1)}]),
    {ok, C2} = emqtt:start_link([{keepalive, 5}, {clientid, binary_to_list(ClientId2)}]),
    {ok, _} = emqtt:connect(C1),
    {ok, _} = emqtt:connect(C2),
    ok = publish_keepalive_self(C1, 30),
    ?assertMatch(#{conninfo := #{keepalive := 6}}, wait_for_keepalive(ClientId1, 6, 2000)),
    ok = publish_keepalive_update([#{clientid => ClientId2, keepalive => 30}]),
    ?assertMatch(#{conninfo := #{keepalive := 6}}, wait_for_keepalive(ClientId2, 6, 2000)),
    ok = emqtt:stop(C1),
    ok = emqtt:stop(C2),
    emqx_config:put_zone_conf(default, [mqtt, server_keepalive], disabled).

t_keepalive_update_not_routed_to_subscribers(_) ->
    {ok, Sub} = emqtt:start_link([]),
    {ok, _} = emqtt:connect(Sub),
    {ok, _, [0, 0]} = emqtt:subscribe(Sub, [
        {<<"$SETOPTS/#">>, []},
        {<<"$SETOPTS2/#">>, []}
    ]),
    {ok, Pub} = emqtt:start_link([]),
    {ok, _} = emqtt:connect(Pub),
    _ = emqtt:publish(Pub, <<"$SETOPTS2/1">>, <<"allowed">>),
    receive
        {publish, #{topic := <<"$SETOPTS2/1">>, payload := <<"allowed">>}} ->
            ok
    after 500 ->
        error(no_keepalive2_message_received)
    end,
    ok = publish_keepalive_update(#{clientid => <<"dynamic_no_route">>, keepalive => 10}),
    receive
        {publish, #{topic := Topic}} ->
            error({unexpected_keepalive_delivery, Topic})
    after 100 ->
        ok
    end,
    ok = emqtt:stop(Pub),
    ok = emqtt:stop(Sub).

t_setopts_unknown_topic_not_routed(_) ->
    {ok, Sub} = emqtt:start_link([]),
    {ok, _} = emqtt:connect(Sub),
    {ok, _, [0, 0]} = emqtt:subscribe(Sub, [
        {<<"$SETOPTS/#">>, []},
        {<<"$SETOPTS2/#">>, []}
    ]),
    {ok, Pub} = emqtt:start_link([]),
    {ok, _} = emqtt:connect(Pub),
    _ = emqtt:publish(Pub, <<"$SETOPTS2/unknown">>, <<"allowed">>),
    receive
        {publish, #{topic := <<"$SETOPTS2/unknown">>, payload := <<"allowed">>}} ->
            ok
    after 500 ->
        error(no_setopts2_message_received)
    end,
    _ = emqtt:publish(Pub, <<"$SETOPTS/foobar/1">>, <<"ignored">>),
    receive
        {publish, #{topic := Topic}} ->
            error({unexpected_setopts_delivery, Topic})
    after 200 ->
        ok
    end,
    ok = emqtt:stop(Pub),
    ok = emqtt:stop(Sub).

t_drop_overloaded_batches(_) ->
    emqx_config:put_zone_conf(default, [mqtt, keepalive_multiplier], 1.5),
    emqx_config:put_zone_conf(default, [mqtt, keepalive_check_interval], 1000),
    ClientIds = [list_to_binary(io_lib:format("overload_~b", [N])) || N <- lists:seq(1, 12)],
    Clients = [
        begin
            {ok, C} = emqtt:start_link([{keepalive, 5}, {clientid, binary_to_list(ClientId)}]),
            {ok, _} = emqtt:connect(C),
            {ClientId, C}
        end
     || ClientId <- ClientIds
    ],
    ok = sys:suspend(whereis(emqx_setopts)),
    lists:foreach(
        fun({ClientId, _C}) ->
            ok = publish_keepalive_update([#{clientid => ClientId, keepalive => 9}])
        end,
        Clients
    ),
    ct:sleep(100),
    ok = sys:resume(whereis(emqx_setopts)),
    [_First | Rest] = ClientIds,
    [Second | _] = Rest,
    ?assertMatch(#{conninfo := #{keepalive := 9}}, wait_for_keepalive(Second, 9, 2000)),
    ok = ensure_keepalive_unchanged(hd(ClientIds), 5, 1000),
    lists:foreach(fun({_Id, C}) -> ok = emqtt:stop(C) end, Clients).

receive_msg_in_time(ChannelPid, C, Timeout) ->
    receive
        {'EXIT', ChannelPid, {shutdown, keepalive_timeout}} ->
            receive
                {'EXIT', C, {shutdown, tcp_closed}} ->
                    ok
            after 500 ->
                throw(no_tcp_closed_from_mqtt_client)
            end
    after Timeout ->
        no_keepalive_timeout_received
    end.

publish_keepalive_update(Update) ->
    publish_keepalive_update(Update, undefined).

publish_keepalive_update(Update, _Opts) when is_list(Update) ->
    {ok, Pub} = emqtt:start_link([]),
    {ok, _} = emqtt:connect(Pub),
    Payload = emqx_utils_json:encode(Update),
    _ = emqtt:publish(Pub, ?KEEPALIVE_BULK_TOPIC, Payload),
    ok = emqtt:stop(Pub),
    ok;
publish_keepalive_update(#{clientid := ClientId, keepalive := Interval}, _Opts) ->
    publish_keepalive_single(ClientId, Interval);
publish_keepalive_update(#{<<"clientid">> := ClientId, <<"keepalive">> := Interval}, _Opts) ->
    publish_keepalive_single(ClientId, Interval).

publish_keepalive_self(Client, Interval) ->
    Payload = integer_to_binary(to_int(Interval)),
    Topic = <<"$SETOPTS/mqtt/keepalive">>,
    _ = emqtt:publish(Client, Topic, Payload),
    ok.

publish_keepalive_single(ClientId, Interval) ->
    {ok, Pub} = emqtt:start_link([{clientid, binary_to_list(ClientId)}]),
    {ok, _} = emqtt:connect(Pub),
    Payload = integer_to_binary(to_int(Interval)),
    Topic = <<"$SETOPTS/mqtt/keepalive">>,
    _ = emqtt:publish(Pub, Topic, Payload),
    ok = emqtt:stop(Pub),
    ok.

publish_keepalive_update_raw(Topic, Payload) ->
    {ok, Pub} = emqtt:start_link([]),
    {ok, _} = emqtt:connect(Pub),
    _ = emqtt:publish(Pub, Topic, Payload),
    ok = emqtt:stop(Pub),
    ok.

to_int(Value) when is_integer(Value) -> Value;
to_int(Value) ->
    try emqx_utils_conv:int(Value) of
        Int -> Int
    catch
        _:_ -> error({invalid_keepalive, Value})
    end.

wait_for_keepalive(ClientId, Expected, TimeoutMs) ->
    Info = emqx_cm:get_chan_info(ClientId),
    case Info of
        #{conninfo := #{keepalive := Expected}} ->
            Info;
        _ when TimeoutMs > 0 ->
            ct:sleep(50),
            wait_for_keepalive(ClientId, Expected, TimeoutMs - 50);
        _ ->
            Info
    end.

ensure_keepalive_unchanged(ClientId, Expected, TimeoutMs) ->
    Info = emqx_cm:get_chan_info(ClientId),
    case Info of
        #{conninfo := #{keepalive := Expected}} when TimeoutMs =< 0 ->
            ok;
        #{conninfo := #{keepalive := Expected}} ->
            ct:sleep(50),
            ensure_keepalive_unchanged(ClientId, Expected, TimeoutMs - 50);
        #{conninfo := #{keepalive := Other}} ->
            error({keepalive_changed, ClientId, Expected, Other});
        _ when TimeoutMs =< 0 ->
            ok;
        _ ->
            ct:sleep(50),
            ensure_keepalive_unchanged(ClientId, Expected, TimeoutMs - 50)
    end.

call_in_time(Fun, TimeoutMs) ->
    Parent = self(),
    Ref = make_ref(),
    _Pid = spawn(fun() -> Parent ! {Ref, Fun()} end),
    receive
        {Ref, Result} -> {ok, Result}
    after TimeoutMs ->
        {error, timeout}
    end.
