%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_offline_messages_pg_SUITE).
-compile(export_all).
-compile(nowarn_export_all).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("../../../apps/emqx_extsub/src/emqx_extsub_internal.hrl").
-include("../include/emqx_offline_messages.hrl").
-include("../include/emqx_omp_pg.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Port = emqx_common_test_helpers:select_free_port(tcp),
    Apps = emqx_cth_suite:start(
        [
            {emqx_conf, #{
                config => #{
                    authorization => #{no_match => allow},
                    listeners => #{
                        tcp => #{default => #{bind => Port}},
                        ssl => #{default => #{bind => 0}},
                        ws => #{default => #{bind => 0}},
                        wss => #{default => #{bind => 0}}
                    }
                }
            }},
            {emqx, #{config => false}},
            emqx_extsub,
            emqx_offline_messages
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    ok = drop_schema(),
    [{apps, Apps}, {port, Port} | Config].

end_per_suite(Config) -> emqx_cth_suite:stop(?config(apps, Config)).

%% Development databases are recreated when the schema definitions change.
drop_schema() ->
    Parsed = emqx_omp_pg_config:parse(#{
        <<"host">> => <<"pgsql">>, <<"ssl">> => #{<<"enable">> => false}
    }),
    {ok, Conn} = emqx_omp_pg_store:connect(Parsed),
    _ = epgsql:squery(Conn, """
    DROP TABLE IF EXISTS mqtt_pg_subscription, mqtt_pg_message, mqtt_pg_topic, mqtt_pg_schema CASCADE;
    DROP FUNCTION IF EXISTS omp_append(text, text, text, jsonb);
    DROP FUNCTION IF EXISTS omp_append(text, text, jsonb);
    """),
    ok = epgsql:close(Conn).

init_per_testcase(Case, Config) ->
    ok = snabbkaffe:start_trace(),
    Namespace = atom_to_binary(Case),
    Raw = #{
        <<"enable">> => true,
        <<"host">> => <<"pgsql">>,
        <<"namespace">> => Namespace,
        <<"topics">> => [<<"offline/#">>],
        <<"init_default_schema">> => true,
        <<"read">> => #{
            <<"page_max_messages">> => 2, <<"window">> => 4, <<"idle_poll_interval">> => 200
        },
        <<"checkpoint">> => #{<<"interval">> => 50, <<"max_pending">> => 2}
    },
    Parsed = emqx_omp_pg_config:parse(Raw),
    {ok, Conn} = emqx_omp_pg_store:connect(Parsed),
    ok = emqx_omp_pg_store:install(Parsed),
    _ = sql(
        Conn, "TRUNCATE mqtt_pg_subscription, mqtt_pg_message, mqtt_pg_topic RESTART IDENTITY", []
    ),
    ok = emqx_offline_messages:on_config_changed(#{}, #{<<"postgresql">> => Raw}),
    [{namespace, Namespace}, {raw, Raw}, {db, Conn}, {pg_config, Parsed} | Config].

end_per_testcase(_Case, Config) ->
    emqx_common_test_helpers:call_janitor(),
    ok = emqx_offline_messages_pg:stop(),
    epgsql:close(?config(db, Config)),
    meck:unload(),
    snabbkaffe:stop(),
    ok.

%% Stored publishes receive a success acknowledgement and send one live copy after commit.
-doc "Check successful QoS 1/2 publishes dispatch committed positions instead of routing.".
t_publish_success(Config) ->
    Topic = <<"offline/success">>,
    Namespace = ?config(namespace, Config),
    ok = emqx:subscribe(Topic),
    Pub = client(Config, <<"publisher">>, []),
    {ok, #{reason_code := 0}} = emqtt:publish(Pub, Topic, <<"one">>, 1),
    {ok, #{reason_code := 0}} = emqtt:publish(Pub, Topic, <<"two">>, 2),
    ?assertEqual([{1, <<"one">>}, {2, <<"two">>}], messages(Config, Topic)),
    ?assertReceive(?OMP_LIVE(Namespace, _, 1, #message{payload = <<"one">>})),
    ?assertReceive(?OMP_LIVE(Namespace, _, 2, #message{payload = <<"two">>})),
    %% The broker does not route the original message to subscribers.
    ?assertNotReceive({deliver, Topic, _}),
    ok = emqx:unsubscribe(Topic),
    ok = emqtt:disconnect(Pub).

%% QoS 0 publishes keep hook changes and count the publish once.
-doc "Check QoS 0 batch dispatch after commit with one publish count and trace.".
t_qos0_publish(Config) ->
    Topic = <<"offline/continuation">>,
    Parent = self(),
    Action = {?MODULE, transform_publish, []},
    ok = emqx_hooks:add('message.publish', Action, 0),
    emqx_common_test_helpers:on_exit(fun() -> emqx_hooks:del('message.publish', Action) end),
    ok = meck:new(emqx_trace, [passthrough, no_link]),
    ok = meck:expect(emqx_trace, publish, fun(Msg) ->
        case Msg#message.topic of
            Topic -> Parent ! publish_observed;
            _ -> ok
        end,
        meck:passthrough([Msg])
    end),
    ok = emqx:subscribe(Topic),
    emqx_common_test_helpers:on_exit(fun() -> emqx:unsubscribe(Topic) end),
    Before = emqx_metrics:val_global('messages.publish'),
    {ok, [], _} = emqx_broker:publish2(emqx_message:make(<<"p">>, 0, Topic, <<"original">>), #{}),
    ?assertReceive(?OMP_LIVE(_, _, 1, #message{payload = <<"transformed">>})),
    ?assertNotReceive({deliver, Topic, _}),
    ?assertEqual([{1, <<"transformed">>}], messages(Config, Topic)),
    ?assertEqual(Before + 1, emqx_metrics:val_global('messages.publish')),
    ?assertReceive(publish_observed),
    ?assertNotReceive(publish_observed).

%% Earlier publish rejections must not create PostgreSQL rows at any QoS.
-doc "Check rejected and disconnecting publishes are not stored.".
t_rejected_publish_not_stored(Config) ->
    Topic = <<"offline/rejected">>,
    ok = emqx:subscribe(Topic),
    emqx_common_test_helpers:on_exit(fun() -> emqx:unsubscribe(Topic) end),
    lists:foreach(
        fun(QoS) ->
            Msg = emqx_message:make(<<"p">>, QoS, Topic, <<"rejected">>),
            ?assertMatch(
                {ok, [], _},
                emqx_broker:publish2(emqx_message:set_header(allow_publish, false, Msg))
            ),
            ?assertMatch(
                {error, disconnect, _},
                emqx_broker:publish2(emqx_message:set_header(should_disconnect, true, Msg))
            )
        end,
        [0, 1, 2]
    ),
    {ok, [], _} = emqx_broker:publish2(emqx_message:make(<<"p">>, 0, Topic, <<"accepted">>)),
    ?assertReceive(?OMP_LIVE(_, _, 1, #message{payload = <<"accepted">>})),
    ?assertEqual([{1, <<"accepted">>}], messages(Config, Topic)).

transform_publish(Msg) ->
    {ok, Msg#message{payload = <<"transformed">>}}.

%% Each client's cursor follows its latest acknowledgement independently.
-doc "Check ordered pagination and independent progress with ordered acknowledgements.".
t_independent_progress(Config) ->
    Topic = <<"offline/progress">>,
    append(Config, Topic, [integer_to_binary(N) || N <- lists:seq(1, 6)]),
    Slow = client(Config, <<"slow">>, [{auto_ack, false}]),
    Fast = client(Config, <<"fast">>, []),
    {ok, _, [1]} = emqtt:subscribe(Slow, Topic, 1),
    SlowMessages = receive_messages(4),
    ?assertEqual([<<"1">>, <<"2">>, <<"3">>, <<"4">>], payloads(SlowMessages)),
    {ok, _, [1]} = emqtt:subscribe(Fast, Topic, 1),
    ?assertEqual([integer_to_binary(N) || N <- lists:seq(1, 6)], payloads(receive_messages(6))),
    ?retry(50, 100, ?assertEqual(6, cursor(Config, <<"fast">>, Topic))),
    ?assertEqual(0, cursor(Config, <<"slow">>, Topic)),
    [First | Later] = SlowMessages,
    emqtt:puback(Slow, maps:get(packet_id, First)),
    ?retry(50, 100, ?assertEqual(1, cursor(Config, <<"slow">>, Topic))),
    lists:foreach(fun(#{packet_id := Id}) -> emqtt:puback(Slow, Id) end, Later),
    Last = receive_messages(2),
    ?assertEqual([<<"5">>, <<"6">>], payloads(Last)),
    lists:foreach(fun(#{packet_id := Id}) -> emqtt:puback(Slow, Id) end, Last),
    ?retry(50, 100, ?assertEqual(6, cursor(Config, <<"slow">>, Topic))),
    ?assertEqual(6, length(messages(Config, Topic))),
    ok = emqtt:disconnect(Slow),
    ok = emqtt:disconnect(Fast).

%% Database subscriptions survive clean start and explicit unsubscribe removes them.
-doc "Check clean-start restoration, repeated subscribe and explicit unsubscribe.".
t_restore_and_unsubscribe(Config) ->
    Topic = <<"offline/restore">>,
    Sub0 = client(Config, <<"restored">>, []),
    {ok, _, [1]} = emqtt:subscribe(Sub0, Topic, 1),
    append(Config, Topic, [<<"first">>]),
    ?assertEqual([<<"first">>], payloads(receive_messages(1))),
    ?retry(50, 100, ?assertEqual(1, cursor(Config, <<"restored">>, Topic))),
    {ok, _, [1]} = emqtt:subscribe(Sub0, Topic, 1),
    no_messages(),
    ok = emqtt:disconnect(Sub0),
    append(Config, Topic, [<<"second">>]),
    Sub1 = client(Config, <<"restored">>, [{clean_start, true}]),
    ?assertEqual([<<"second">>], payloads(receive_messages(1))),
    {ok, _, _} = emqtt:unsubscribe(Sub1, Topic),
    ?retry(50, 100, ?assertEqual(undefined, cursor(Config, <<"restored">>, Topic))),
    ok = emqtt:disconnect(Sub1),
    Sub2 = client(Config, <<"restored">>, []),
    no_messages(),
    ok = emqtt:disconnect(Sub2).

%% Transaction rollback preserves consecutive offsets and releases blocked writers.
-doc "Check counter locks, rollback, independent topics and pagination larger than a page.".
t_transaction_ordering(Config) ->
    Conn0 = ?config(db, Config),
    {ok, Conn1} = emqx_omp_pg_store:connect(?config(pg_config, Config)),
    emqx_common_test_helpers:on_exit(fun() -> epgsql:close(Conn1) end),
    Topic = <<"offline/transactions">>,
    sql(Conn0, "BEGIN", []),
    {ok, {_, 1}} = store_append(Conn0, Config, Topic, [<<"rolled-back">>]),
    Parent = self(),
    spawn_link(fun() ->
        Parent !
            {writer,
                store_append(Conn1, Config, Topic, [<<"1">>, <<"2">>, <<"3">>, <<"4">>, <<"5">>])}
    end),
    %% A separate topic can commit while the first topic's counter remains locked.
    append(Config, <<"offline/other">>, [<<"independent">>]),
    receive
        {writer, _} -> ct:fail(writer_did_not_wait)
    after 100 -> ok
    end,
    sql(Conn0, "ROLLBACK", []),
    receive
        {writer, Result} -> ?assertMatch({ok, {_, 1}}, Result)
    after 5000 -> ct:fail(writer_timeout)
    end,
    ?assertEqual(lists:seq(1, 5), [N || {N, _} <- messages(Config, Topic)]),
    %% An unrelated transaction does not hide committed rows from the reader.
    sql(Conn0, "BEGIN", []),
    Sub = client(Config, <<"pages">>, []),
    {ok, _, [1]} = emqtt:subscribe(Sub, Topic, 1),
    ?assertEqual([<<"1">>, <<"2">>, <<"3">>, <<"4">>, <<"5">>], payloads(receive_messages(5))),
    sql(Conn0, "ROLLBACK", []),
    ok = emqtt:disconnect(Sub).

%% Failed inserts roll back the counter and message rows.
-doc "Check atomic append rollback and the empty batch check.".
t_append_failure(Config) ->
    Conn = ?config(db, Config),
    Namespace = ?config(namespace, Config),
    Topic = <<"offline/invalid">>,
    Message = emqx_omp_pg_store:encode(emqx_message:make(<<"p">>, 1, Topic, <<"data">>)),
    ?assertException(
        error, _, emqx_omp_pg_store:run(Conn, {append, Namespace, Topic, [Message#{qos := 3}]})
    ),
    ?assertException(error, _, emqx_omp_pg_store:run(Conn, {append, Namespace, Topic, []})),
    ?assertEqual([], messages(Config, Topic)),
    ?assertMatch({ok, {_, 1}}, store_append(Conn, Config, Topic, [<<"valid">>])).

%% The plugin sets the session statement timeout only when the option is enabled.
-doc "Check the `set_statement_timeout` option.".
t_statement_timeout_option(Config) ->
    PgConfig = ?config(pg_config, Config),
    lists:foreach(
        fun(#{set := Set, expected := Expected}) ->
            {ok, Conn} = emqx_omp_pg_store:connect(PgConfig#{set_statement_timeout := Set}),
            ?assertEqual([{Expected}], sql(Conn, "SHOW statement_timeout", [])),
            ok = epgsql:close(Conn)
        end,
        [
            #{set => true, expected => <<"5s">>},
            #{set => false, expected => <<"0">>}
        ]
    ).

%% Idle polls recover a SQL commit without subsequent broker traffic.
-doc "Check idle recovery without notification connections.".
t_idle_recovery(Config) ->
    Topic = <<"offline/idle">>,
    Sub = client(Config, <<"idle">>, []),
    {ok, _, [1]} = emqtt:subscribe(Sub, Topic, 1),
    append(Config, Topic, [<<"before">>]),
    ?assertEqual([<<"before">>], payloads(receive_messages(1))),
    append(Config, Topic, [<<"during">>]),
    ?assertEqual([<<"during">>], payloads(receive_messages(1))),
    ?assertEqual(
        [{0}],
        sql(
            ?config(db, Config),
            "SELECT count(*) FROM pg_stat_activity WHERE query LIKE 'LISTEN omp_%'",
            []
        )
    ),
    ok = emqtt:disconnect(Sub).

%% Messages published at different QoS share one sequence and are delivered at QoS 1.
-doc "Check one ordered sequence, QoS 1 delivery and in-order completion for mixed publish QoS.".
t_mixed_qos(Config) ->
    Topic = <<"offline/mixed">>,
    Sub = client(Config, <<"mixed">>, [{auto_ack, false}]),
    {ok, _, [1]} = emqtt:subscribe(Sub, Topic, 1),
    Pub = client(Config, <<"mixed-pub">>, []),
    {ok, _} = emqtt:publish(Pub, Topic, <<"one">>, 1),
    [#{packet_id := First, qos := 1, payload := <<"one">>}] = receive_messages(1),
    ok = emqtt:publish(Pub, Topic, <<"zero">>, 0),
    [#{packet_id := Second, qos := 1, payload := <<"zero">>}] = receive_messages(1),
    {ok, _} = emqtt:publish(Pub, Topic, <<"two">>, 2),
    [#{packet_id := Third, qos := 1, payload := <<"two">>}] = receive_messages(1),
    ?assertEqual(
        [{1, 1}, {2, 0}, {3, 2}],
        sql(?config(db, Config), "SELECT msg_offset, qos FROM mqtt_pg_message ORDER BY 1", [])
    ),
    %% The checkpoint waits for the in-order acknowledgements.
    ?assertEqual(0, cursor(Config, <<"mixed">>, Topic)),
    lists:foreach(fun(PacketId) -> emqtt:puback(Sub, PacketId) end, [First, Second, Third]),
    ?retry(50, 100, ?assertEqual(3, cursor(Config, <<"mixed">>, Topic))),
    ok = emqtt:disconnect(Sub),
    ok = emqtt:disconnect(Pub).

%% Managed topics accept only QoS 1 subscriptions and deliver at QoS 1 even with QoS upgrade.
-doc "Check the subscription QoS rule and QoS 1 delivery with `upgrade_qos`.".
t_delivery_qos(Config) ->
    Topic = <<"offline/qos">>,
    Sub0 = client(Config, <<"qos-denied">>, []),
    ?assertMatch({ok, _, [16#87]}, emqtt:subscribe(Sub0, Topic, 0)),
    ?assertMatch({ok, _, [16#87]}, emqtt:subscribe(Sub0, Topic, 2)),
    ok = emqtt:disconnect(Sub0),
    Mqtt = emqx_config:get_zone_conf(default, [mqtt]),
    ok = emqx_config:put_zone_conf(default, [mqtt], Mqtt#{upgrade_qos := true}),
    emqx_common_test_helpers:on_exit(fun() ->
        emqx_config:put_zone_conf(default, [mqtt], Mqtt)
    end),
    Pub = client(Config, <<"qos-pub">>, []),
    ok = emqtt:publish(Pub, Topic, <<"zero">>, 0),
    ?retry(20, 100, ?assertEqual([{1, <<"zero">>}], messages(Config, Topic))),
    Sub1 = client(Config, <<"qos-sub">>, []),
    {ok, _, [1]} = emqtt:subscribe(Sub1, Topic, 1),
    ?assertMatch([#{qos := 1, payload := <<"zero">>}], receive_messages(1)),
    {ok, _} = emqtt:publish(Pub, Topic, <<"two">>, 2),
    ?assertMatch([#{qos := 1, payload := <<"two">>}], receive_messages(1)),
    ok = emqtt:disconnect(Sub1),
    ok = emqtt:disconnect(Pub).

%% Policy skips and invalid rows cannot block later deliverable messages.
-doc "Check no-local, expiry, malformed row and negative acknowledgement settlement.".
t_settlement(Config) ->
    Topic = <<"offline/settlement">>,
    Conn = ?config(db, Config),
    Now = erlang:system_time(millisecond),
    Own = emqx_message:make(<<"settler">>, 1, Topic, <<"own">>),
    Expired = Own#message{
        from = <<"publisher">>,
        timestamp = Now - 5000,
        headers = #{properties => #{'Message-Expiry-Interval' => 1}}
    },
    {ok, {_, 1}} = emqx_omp_pg_store:run(
        Conn,
        {append, ?config(namespace, Config), Topic, [
            emqx_omp_pg_store:encode(Msg)
         || Msg <- [Own, Expired]
        ]}
    ),
    append(Config, Topic, [<<"invalid">>, <<"rejected">>, <<"valid">>]),
    sql(Conn, "UPDATE mqtt_pg_message SET properties = '{}' WHERE msg_offset = 3", []),
    Sub = client(Config, <<"settler">>, [{auto_ack, false}]),
    {ok, _, [1]} = emqtt:subscribe(Sub, Topic, [{qos, 1}, {nl, true}]),
    [#{packet_id := Rejected}] = receive_messages(1),
    emqtt:puback(Sub, Rejected, 16#80, #{}),
    [#{packet_id := Valid, payload := <<"valid">>}] = receive_messages(1),
    emqtt:puback(Sub, Valid),
    ?retry(50, 100, ?assertEqual(5, cursor(Config, <<"settler">>, Topic))),
    ok = emqtt:disconnect(Sub).

%% Replay skips messages deleted by retention or by hand and continues with the next row.
-doc "Check replay after count retention and manual row deletion.".
t_retention_and_gaps(Config) ->
    Topic = <<"offline/gaps">>,
    Conn = ?config(db, Config),
    {ok, _} = emqx_omp_pg_store:run(
        Conn,
        {subscribe, ?config(namespace, Config), <<"gaps">>, Topic, #{qos => 1}, earliest}
    ),
    {ok, {Id, 1}} = store_append(
        Conn, Config, Topic, [integer_to_binary(N) || N <- lists:seq(1, 5)]
    ),
    ?assertEqual({ok, 2}, emqx_omp_pg_store:run(Conn, {trim, Id, 3, 1000})),
    ?assertEqual([{3, <<"3">>}, {4, <<"4">>}, {5, <<"5">>}], messages(Config, Topic)),
    sql(Conn, "DELETE FROM mqtt_pg_message WHERE msg_offset = 4", []),
    Sub = client(Config, <<"gaps">>, []),
    ?assertEqual([<<"3">>, <<"5">>], payloads(receive_messages(2))),
    ?retry(50, 100, ?assertEqual(5, cursor(Config, <<"gaps">>, Topic))),
    ok = emqtt:disconnect(Sub).

%% A new subscription with the latest start skips history and receives only later messages.
-doc "Check the latest starting position.".
t_latest_start(Config) ->
    Topic = <<"offline/latest">>,
    Conn = ?config(db, Config),
    Namespace = ?config(namespace, Config),
    append(Config, Topic, [<<"old">>]),
    {ok, #{cursor := 1}} =
        emqx_omp_pg_store:run(
            Conn, {subscribe, Namespace, <<"latest">>, Topic, #{qos => 1}, latest}
        ),
    Sub = client(Config, <<"latest">>, []),
    append(Config, Topic, [<<"new">>]),
    ?assertEqual([<<"new">>], payloads(receive_messages(1))),
    ok = emqtt:disconnect(Sub).

%% Wildcard, non-QoS 1 and oversized subscriptions to a managed feed are denied.
-doc "Check unsupported subscriptions are denied and shared subscriptions receive nothing.".
t_subscription_rejections(Config) ->
    Sub = client(Config, <<"denied">>, []),
    lists:foreach(
        fun({Topic, QoS}) ->
            ?assertMatch({ok, _, [16#87]}, emqtt:subscribe(Sub, Topic, QoS))
        end,
        [
            {<<"offline/#">>, 1},
            {<<"#">>, 1},
            {<<"$share/group/offline/#">>, 1},
            {<<"offline/", (binary:copy(<<"x">>, 1024))/binary>>, 1},
            {<<"offline/exact">>, 0},
            {<<"offline/exact">>, 2}
        ]
    ),
    ?assertMatch({ok, _, [0]}, emqtt:subscribe(Sub, <<"ordinary/topic">>, 0)),
    ?assertMatch({ok, _, [1]}, emqtt:subscribe(Sub, <<"offline/exact">>, 1)),
    ok = emqtt:disconnect(Sub),
    %% Authorization cannot see the share prefix. The subscription succeeds and gets no messages.
    Shared = client(Config, <<"shared">>, []),
    ?assertMatch({ok, _, [1]}, emqtt:subscribe(Shared, <<"$share/group/offline/shared">>, 1)),
    append(Config, <<"offline/shared">>, [<<"history">>]),
    Pub = client(Config, <<"shared-pub">>, []),
    {ok, _} = emqtt:publish(Pub, <<"offline/shared">>, <<"live">>, 1),
    no_messages(),
    ok = emqtt:disconnect(Shared),
    ok = emqtt:disconnect(Pub).

%% Binary correlation data and duplicate user properties survive SQL encoding.
-doc "Check the external MQTT property encoding.".
t_properties(Config) ->
    Topic = <<"offline/properties">>,
    Props = #{
        'Correlation-Data' => <<0, 255, 128>>,
        'Content-Type' => <<"application/octet-stream">>,
        'User-Property' => [{<<"a">>, <<"1">>}, {<<"a">>, <<"2">>}]
    },
    Msg = emqx_message:set_header(
        properties, Props, emqx_message:make(<<"p">>, 1, Topic, <<0, 255>>)
    ),
    {ok, {_, 1}} = emqx_omp_pg_store:run(
        ?config(db, Config),
        {append, ?config(namespace, Config), Topic, [emqx_omp_pg_store:encode(Msg)]}
    ),
    Sub = client(Config, <<"properties">>, []),
    {ok, _, [1]} = emqtt:subscribe(Sub, Topic, 1),
    [#{payload := Payload, properties := Received}] = receive_messages(1),
    ?assertEqual(<<0, 255>>, Payload),
    ?assertEqual(Props, maps:with(maps:keys(Props), Received)),
    ok = emqtt:disconnect(Sub).

%% PUBACK cannot arrive while another transaction holds the append counter lock.
-doc "Check the commit boundary for publisher acknowledgements.".
t_ack_after_commit(Config) ->
    Topic = <<"offline/commit">>,
    append(Config, Topic, [<<"initial">>]),
    Pub = client(Config, <<"commit-publisher">>, []),
    Conn = ?config(db, Config),
    sql(Conn, "BEGIN", []),
    sql(Conn, "SELECT topic_id FROM mqtt_pg_topic WHERE topic = $1 FOR UPDATE", [Topic]),
    Parent = self(),
    spawn_link(fun() -> Parent ! {published, emqtt:publish(Pub, Topic, <<"committed">>, 1)} end),
    receive
        {published, _} -> ct:fail(ack_before_commit)
    after 100 -> ok
    end,
    ?assertEqual([{1, <<"initial">>}], messages(Config, Topic)),
    sql(Conn, "COMMIT", []),
    receive
        {published, Result} -> ?assertMatch({ok, #{reason_code := 0}}, Result)
    after 5000 -> ct:fail(no_ack)
    end,
    ?assertEqual([{1, <<"initial">>}, {2, <<"committed">>}], messages(Config, Topic)),
    ok = emqtt:disconnect(Pub).

%% Database statement timeouts disconnect MQTT 3.1.1 and MQTT 5 publishers.
-doc "Check write failure without a success acknowledgement.".
t_write_failure(Config) ->
    Raw = ?config(raw, Config),
    ok = emqx_offline_messages:on_config_changed(
        #{<<"postgresql">> => Raw},
        #{<<"postgresql">> => Raw#{<<"query_timeout">> => 200}}
    ),
    lists:foreach(
        fun(Version) ->
            Pub = client(Config, atom_to_binary(Version), [{proto_ver, Version}]),
            unlink(Pub),
            Monitor = monitor(process, Pub),
            Conn = ?config(db, Config),
            sql(Conn, "BEGIN", []),
            sql(Conn, "LOCK TABLE mqtt_pg_topic IN ACCESS EXCLUSIVE MODE", []),
            ?assertNotMatch(
                {ok, _}, catch emqtt:publish(Pub, <<"offline/failure">>, <<"lost">>, 1)
            ),
            receive
                {'DOWN', Monitor, process, Pub, _} -> ok
            after 5000 -> ct:fail(not_disconnected)
            end,
            sql(Conn, "ROLLBACK", [])
        end,
        [v4, v5]
    ),
    ?assertEqual([], messages(Config, <<"offline/failure">>)).

%% An unreachable database at start keeps managed publishes failing instead of unstored.
-doc "Check start failure keeps the hooks and rejects managed QoS 1 publishes.".
t_start_failure(Config) ->
    Raw = ?config(raw, Config),
    Bad = Raw#{<<"port">> => 1, <<"init_default_schema">> => false},
    ok = emqx_offline_messages_pg:on_config_changed(Raw, Bad),
    ?assertMatch({error, _}, emqx_offline_messages_pg:on_health_check(Bad)),
    Topic = <<"offline/start-failure">>,
    ok = emqx:subscribe(Topic),
    emqx_common_test_helpers:on_exit(fun() -> emqx:unsubscribe(Topic) end),
    Pub = client(Config, <<"start-failure">>, []),
    unlink(Pub),
    Monitor = monitor(process, Pub),
    ?assertNotMatch({ok, _}, catch emqtt:publish(Pub, Topic, <<"lost">>, 1)),
    receive
        {'DOWN', Monitor, process, Pub, _} -> ok
    after 5000 -> ct:fail(not_disconnected)
    end,
    ?assertNotReceive({deliver, Topic, _}),
    ?assertNotReceive(?OMP_LIVE(_, _, _, _)),
    %% A later config change starts the backend.
    ok = emqx_offline_messages_pg:on_config_changed(Bad, Raw),
    ?assertEqual(ok, emqx_offline_messages_pg:on_health_check(Raw)),
    ?assertEqual([], messages(Config, Topic)).

%% QoS 0 count and byte thresholds flush before the configured idle timeout.
-doc "Check QoS 0 flush thresholds independently of timers.".
t_qos0_thresholds(Config) ->
    lists:foreach(
        fun(#{name := Name, count := Count, bytes := Bytes}) ->
            Raw = ?config(raw, Config),
            Writer = #{
                <<"pool_size">> => 1,
                <<"min_flush_timeout">> => 10000,
                <<"max_flush_timeout">> => 10000,
                <<"flush_max_messages">> => Count,
                <<"flush_max_bytes">> => Bytes
            },
            ok = emqx_offline_messages_pg:on_config_changed(#{}, Raw#{<<"writer">> => Writer}),
            Pub = client(Config, Name, []),
            Topic = <<"offline/", Name/binary>>,
            ok = emqtt:publish(Pub, Topic, <<"abc">>, 0),
            ?assertEqual([], messages(Config, Topic)),
            ok = emqtt:publish(Pub, Topic, <<"def">>, 0),
            ?retry(
                20, 100, ?assertEqual([{1, <<"abc">>}, {2, <<"def">>}], messages(Config, Topic))
            ),
            ok = emqtt:disconnect(Pub)
        end,
        [
            #{name => <<"count">>, count => 2, bytes => 10000},
            #{name => <<"bytes">>, count => 100, bytes => 6}
        ]
    ).

%% Stopping the backend flushes buffered QoS 0 messages once.
-doc "Check writer shutdown stores and dispatches buffered QoS 0 messages.".
t_writer_shutdown(Config) ->
    Raw = ?config(raw, Config),
    Writer = #{
        <<"pool_size">> => 1, <<"min_flush_timeout">> => 60000, <<"max_flush_timeout">> => 60000
    },
    ok = emqx_offline_messages_pg:on_config_changed(#{}, Raw#{<<"writer">> => Writer}),
    Topic = <<"offline/shutdown">>,
    ok = emqx:subscribe(Topic),
    emqx_common_test_helpers:on_exit(fun() -> emqx:unsubscribe(Topic) end),
    {ok, [], _} = emqx_broker:publish2(emqx_message:make(<<"p">>, 0, Topic, <<"buffered">>)),
    ?assertEqual([], messages(Config, Topic)),
    ok = emqx_offline_messages_pg:stop(),
    ?assertEqual([{1, <<"buffered">>}], messages(Config, Topic)),
    ?assertReceive(?OMP_LIVE(_, _, 1, #message{payload = <<"buffered">>})),
    ?assertNotReceive(?OMP_LIVE(_, _, _, _)).

%% A crash during a flush drops the rest of the buffer instead of appending batches twice.
-doc "Check a writer crash counts buffered messages as failed and stores no duplicate.".
t_writer_crash(Config) ->
    Raw = ?config(raw, Config),
    Writer = #{
        <<"pool_size">> => 1,
        <<"min_flush_timeout">> => 60000,
        <<"max_flush_timeout">> => 60000,
        <<"flush_max_messages">> => 2
    },
    ok = emqx_offline_messages_pg:on_config_changed(#{}, Raw#{<<"writer">> => Writer}),
    %% The second append of the flush crashes the writer after the first one committed.
    Appends = counters:new(1, []),
    ok = meck:new(emqx_omp_pg_store, [passthrough, no_link]),
    ok = meck:expect(emqx_omp_pg_store, call, fun
        (omp_pg_write, Key, {append, _, _, _} = Operation, PgConfig) ->
            counters:add(Appends, 1, 1),
            case counters:get(Appends, 1) of
                2 -> error(test_crash);
                _ -> meck:passthrough([omp_pg_write, Key, Operation, PgConfig])
            end;
        (Pool, Key, Operation, PgConfig) ->
            meck:passthrough([Pool, Key, Operation, PgConfig])
    end),
    Failed = write_failed(),
    {ok, [], _} = emqx_broker:publish2(
        emqx_message:make(<<"p">>, 0, <<"offline/crash-a">>, <<"a">>)
    ),
    {ok, [], _} = emqx_broker:publish2(
        emqx_message:make(<<"p">>, 0, <<"offline/crash-b">>, <<"b">>)
    ),
    ?retry(20, 100, ?assertEqual(Failed + 2, write_failed())),
    Stored = messages(Config, <<"offline/crash-a">>) ++ messages(Config, <<"offline/crash-b">>),
    ?assertMatch([{1, _}], Stored).

write_failed() ->
    emqx_metrics_worker:get(?METRICS_WORKER, postgresql, write_failed).

%% A new handler resumes from durable progress after a session takeover.
-doc "Check takeover preserves unacknowledged history and later delivery.".
t_takeover(Config) ->
    Topic = <<"offline/takeover">>,
    Session = [{clean_start, false}, {properties, #{'Session-Expiry-Interval' => 60}}],
    Sub0 = client(Config, <<"takeover">>, [{auto_ack, false} | Session]),
    unlink(Sub0),
    {ok, _, [1]} = emqtt:subscribe(Sub0, Topic, 1),
    append(Config, Topic, [<<"first">>, <<"second">>]),
    ?assertEqual([<<"first">>, <<"second">>], payloads(receive_messages(2))),
    Sub1 = client(Config, <<"takeover">>, Session),
    Replayed = payloads(receive_messages(4)),
    ?assertEqual([<<"first">>, <<"first">>, <<"second">>, <<"second">>], lists:sort(Replayed)),
    ?retry(50, 100, ?assertEqual(2, cursor(Config, <<"takeover">>, Topic))),
    append(Config, Topic, [<<"third">>]),
    ?assertEqual([<<"third">>], payloads(receive_messages(1))),
    ok = emqtt:disconnect(Sub1).

%% Live copies that arrive during takeover are recovered by the new handler.
-doc "Check live traffic during takeover reaches the client and completes every offset.".
t_takeover_live(Config) ->
    Topic = <<"offline/takeover-live">>,
    Session = [{clean_start, false}, {properties, #{'Session-Expiry-Interval' => 60}}],
    Sub0 = client(Config, <<"takeover-live">>, Session),
    unlink(Sub0),
    {ok, _, [1]} = emqtt:subscribe(Sub0, Topic, 1),
    Pub = client(Config, <<"takeover-pub">>, []),
    Parent = self(),
    spawn_link(fun() ->
        lists:foreach(
            fun(N) -> {ok, _} = emqtt:publish(Pub, Topic, integer_to_binary(N), 1) end,
            lists:seq(1, 30)
        ),
        Parent ! published
    end),
    Sub1 = client(Config, <<"takeover-live">>, Session),
    receive
        published -> ok
    after 10000 -> ct:fail(publisher_timeout)
    end,
    ?retry(50, 100, ?assertEqual(30, cursor(Config, <<"takeover-live">>, Topic))),
    Received = [binary_to_integer(P) || P <- payloads(drain_messages())],
    %% Every offset reaches one of the clients, and the new handler completes all of them.
    ?assertEqual(lists:seq(1, 30), lists:usort(Received)),
    ok = emqtt:disconnect(Sub1),
    ok = emqtt:disconnect(Pub).

%% A request to a pool that is not running fails at once, so the handler retries without a timeout.
-doc "Check an asynchronous request reports a missing pool immediately.".
t_async_missing_pool(_Config) ->
    Ref = make_ref(),
    Target = emqx_extsub_handler:message_target(make_ref()),
    ok = emqx_omp_pg_store:async(omp_pg_missing, key, health, Target, Ref),
    ?assertReceive(#info_to_extsub{info = {pg_result, Ref, {error, no_such_pool}}}).

%% A configuration reload recreates active readers against the new connection pools.
-doc "Check backend reload keeps durable progress and resumes live delivery.".
t_reload(Config) ->
    Topic = <<"offline/reload">>,
    Sub = client(Config, <<"reload">>, []),
    {ok, _, [1]} = emqtt:subscribe(Sub, Topic, 1),
    append(Config, Topic, [<<"before">>]),
    ?assertEqual([<<"before">>], payloads(receive_messages(1))),
    ?retry(50, 100, ?assertEqual(1, cursor(Config, <<"reload">>, Topic))),
    Raw = ?config(raw, Config),
    ok = emqx_offline_messages_pg:on_config_changed(Raw, Raw#{<<"read_pool_size">> => 2}),
    append(Config, Topic, [<<"after">>]),
    ?assertEqual([<<"after">>], payloads(receive_messages(1))),
    ?assertEqual(ok, emqx_offline_messages_pg:on_health_check(Raw)),
    ok = emqtt:disconnect(Sub).

%% HTTP publishes report committed messages as persisted.
-doc "Check the HTTP publish handler's stored-message response.".
t_http_publish(Config) ->
    Topic = <<"offline/http">>,
    ?assertMatch(
        {200, _},
        emqx_mgmt_api_publish:publish(
            post,
            #{body => #{<<"topic">> => Topic, <<"payload">> => <<"http">>, <<"qos">> => 1}}
        )
    ),
    ?assertEqual([{1, <<"http">>}], messages(Config, Topic)).

%% Unsupported session types are denied before a PostgreSQL handler can start.
-doc "Check the durable-session authorization policy.".
t_durable_session_policy(_Config) ->
    ClientInfo = #{clientid => <<"durable">>, protocol => mqtt},
    ok = emqx_offline_messages_pg:on_session(ClientInfo, #{impl => emqx_persistent_session_ds}),
    ?assertMatch(
        {stop, #{result := deny}},
        emqx_offline_messages_pg:on_authorize(
            ClientInfo,
            #{action_type => subscribe, qos => 1},
            <<"offline/durable">>,
            #{result => allow}
        )
    ),
    ok = emqx_offline_messages_pg:on_session(ClientInfo, #{impl => emqx_session_mem}),
    ?assertMatch(
        {ok, #{result := allow}},
        emqx_offline_messages_pg:on_authorize(
            ClientInfo,
            #{action_type => subscribe, qos => 1},
            <<"offline/durable">>,
            #{result => allow}
        )
    ).

%% Gateway channels must be rejected even when they use an in-memory session.
-doc "Check managed-topic authorization rejects gateway protocols without caching the denial.".
t_gateway_policy(_Config) ->
    ok = emqx_offline_messages_pg:on_session(#{}, #{impl => emqx_session_mem}),
    lists:foreach(
        fun(Protocol) ->
            ?assertMatch(
                {stop, #{result := deny, is_cacheable := false}},
                emqx_offline_messages_pg:on_authorize(
                    #{protocol => Protocol},
                    #{action_type => subscribe, qos => 1},
                    <<"offline/gateway">>,
                    #{result => allow}
                )
            )
        end,
        [stomp, mqttsn, coap, nats]
    ).

%% Retention drains more than one SQL batch without waiting for another cleanup interval.
-doc "Check the periodic retention job removes a large expired prefix.".
t_retention_batches(Config) ->
    Topic = <<"offline/retention-batches">>,
    append(Config, Topic, lists:duplicate(2100, <<"payload">>)),
    Raw = ?config(raw, Config),
    Retention = #{<<"max_messages">> => 2, <<"interval">> => 100},
    ok = emqx_offline_messages_pg:on_config_changed(Raw, Raw#{<<"retention">> => Retention}),
    ?retry(50, 100, ?assertEqual([2099, 2100], [N || {N, _} <- messages(Config, Topic)])).

%% Age retention removes the expired prefix and stops at the first recent row.
-doc "Check age retention and an out-of-order publish time.".
t_age_retention(Config) ->
    Conn = ?config(db, Config),
    Namespace = ?config(namespace, Config),
    Topic = <<"offline/age">>,
    Now = erlang:system_time(millisecond),
    Messages = [
        emqx_omp_pg_store:encode(
            published_at(emqx_message:make(<<"p">>, 1, Topic, integer_to_binary(N)), T)
        )
     || {N, T} <- [{1, Now - 10000}, {2, Now}, {3, Now - 10000}, {4, Now}]
    ],
    {ok, {Id, 1}} = emqx_omp_pg_store:run(Conn, {append, Namespace, Topic, Messages}),
    {ok, Cutoff} = emqx_omp_pg_store:run(Conn, {age_cutoff, Id, 5, Now - 5000}),
    ?assertEqual(2, Cutoff),
    {ok, 1} = emqx_omp_pg_store:run(Conn, {trim, Id, Cutoff, 1000}),
    ?assertEqual([2, 3, 4], [N || {N, _} <- messages(Config, Topic)]),
    ?assertEqual({ok, 5}, emqx_omp_pg_store:run(Conn, {age_cutoff, Id, 5, Now + 5000})).

published_at(Msg, Timestamp) ->
    Msg#message{timestamp = Timestamp}.

%% Cleanup keeps completed history with `earliest` and removes it with `latest`.
-doc "Check the consumed cutoff depends on the subscription start mode.".
t_consumed_cleanup(Config) ->
    Conn = ?config(db, Config),
    Namespace = ?config(namespace, Config),
    Topic = <<"offline/consumed">>,
    {ok, {Id, 1}} = store_append(Conn, Config, Topic, [<<"1">>, <<"2">>, <<"3">>]),
    {ok, _} = emqx_omp_pg_store:run(
        Conn, {subscribe, Namespace, <<"a">>, Topic, #{qos => 1}, earliest}
    ),
    {ok, _} = emqx_omp_pg_store:run(
        Conn, {subscribe, Namespace, <<"b">>, Topic, #{qos => 1}, earliest}
    ),
    {ok, _} = emqx_omp_pg_store:run(Conn, {checkpoint, Namespace, <<"a">>, Id, 3}),
    {ok, _} = emqx_omp_pg_store:run(Conn, {checkpoint, Namespace, <<"b">>, Id, 1}),
    Raw = ?config(raw, Config),
    Retention = #{<<"interval">> => 100},
    %% With `earliest`, completed messages within retention stay readable.
    ok = emqx_offline_messages_pg:on_config_changed(Raw, Raw#{<<"retention">> => Retention}),
    timer:sleep(500),
    ?assertEqual([1, 2, 3], [N || {N, _} <- messages(Config, Topic)]),
    %% With `latest`, messages completed by every subscriber are removed.
    Latest = Raw#{<<"retention">> => Retention, <<"new_subscription_start">> => <<"latest">>},
    ok = emqx_offline_messages_pg:on_config_changed(Raw, Latest),
    ?retry(50, 100, ?assertEqual([2, 3], [N || {N, _} <- messages(Config, Topic)])),
    %% A topic without subscriptions keeps no messages with `latest`.
    sql(Conn, "DELETE FROM mqtt_pg_subscription", []),
    ?retry(50, 100, ?assertEqual([], messages(Config, Topic))),
    ?assertEqual({ok, 4}, emqx_omp_pg_store:run(Conn, {consumed_cutoff, Id, 4})).

%% A subscribe transaction blocks appends, so completions cannot pass a new start offset.
-doc "Check the topic share lock orders a latest subscription before later appends.".
t_consumed_cleanup_race(Config) ->
    Conn0 = ?config(db, Config),
    {ok, Conn1} = emqx_omp_pg_store:connect(?config(pg_config, Config)),
    emqx_common_test_helpers:on_exit(fun() -> epgsql:close(Conn1) end),
    Namespace = ?config(namespace, Config),
    Topic = <<"offline/race">>,
    {ok, {Id, 1}} = store_append(Conn0, Config, Topic, [<<"1">>, <<"2">>]),
    {ok, _} = emqx_omp_pg_store:run(
        Conn0, {subscribe, Namespace, <<"old">>, Topic, #{qos => 1}, earliest}
    ),
    {ok, _} = emqx_omp_pg_store:run(Conn0, {checkpoint, Namespace, <<"old">>, Id, 2}),
    %% Hold the share lock as a subscribe transaction does after its tip read.
    sql(Conn1, "BEGIN", []),
    [{3}] = sql(Conn1, "SELECT next_offset FROM mqtt_pg_topic WHERE topic_id = $1 FOR SHARE", [Id]),
    Parent = self(),
    spawn_link(fun() -> Parent ! {appended, store_append(Conn0, Config, Topic, [<<"3">>])} end),
    receive
        {appended, _} -> ct:fail(append_did_not_wait)
    after 200 -> ok
    end,
    sql(
        Conn1,
        """
        INSERT INTO mqtt_pg_subscription(namespace, client_id, topic_id, subopts, ack_offset)
        VALUES ($1, 'new', $2, '{}', 2)
        """,
        [Namespace, Id]
    ),
    sql(Conn1, "COMMIT", []),
    receive
        {appended, Result} -> ?assertEqual({ok, {Id, 3}}, Result)
    after 5000 -> ct:fail(append_timeout)
    end,
    %% The old subscriber completes the new offset, and the cutoff stays at the new start.
    {ok, _} = emqx_omp_pg_store:run(Conn0, {checkpoint, Namespace, <<"old">>, Id, 3}),
    ?assertEqual({ok, 3}, emqx_omp_pg_store:run(Conn0, {consumed_cutoff, Id, 4})).

%% Inactive subscriptions expire while active subscriptions renew their row.
-doc "Check the subscription expiry sweep distinguishes active and inactive clients.".
t_subscription_expiry(Config) ->
    Raw = ?config(raw, Config),
    ok = emqx_offline_messages_pg:on_config_changed(Raw, Raw#{<<"subscription_expiry">> => 600}),
    Topic = <<"offline/expiry">>,
    Namespace = ?config(namespace, Config),
    Conn = ?config(db, Config),
    Sub = client(Config, <<"active">>, []),
    {ok, _, [1]} = emqtt:subscribe(Sub, Topic, 1),
    ?retry(20, 100, ?assertEqual(0, cursor(Config, <<"active">>, Topic))),
    {ok, _} = emqx_omp_pg_store:run(
        Conn, {subscribe, Namespace, <<"inactive">>, Topic, #{qos => 1}, earliest}
    ),
    timer:sleep(700),
    {ok, 1} = emqx_omp_pg_store:run(Conn, {expire, Namespace, 600, 1000}),
    ?assertEqual(undefined, cursor(Config, <<"inactive">>, Topic)),
    ?assertEqual(0, cursor(Config, <<"active">>, Topic)),
    ?assertEqual(
        [],
        sql(
            Conn,
            "SELECT indexname FROM pg_indexes WHERE indexname = 'mqtt_pg_subscription_updated'",
            []
        )
    ),
    append(Config, Topic, [<<"still-active">>]),
    ?assertEqual([<<"still-active">>], payloads(receive_messages(1))),
    ok = emqtt:disconnect(Sub).

%% Session inflight limits return unsent messages to ExtSub until acknowledgements free capacity.
-doc "Check PostgreSQL replay preserves order under an MQTT receive maximum of one.".
t_inflight_backpressure(Config) ->
    Topic = <<"offline/inflight">>,
    Sub = client(Config, <<"inflight">>, [
        {auto_ack, false}, {properties, #{'Receive-Maximum' => 1}}
    ]),
    {ok, _, [1]} = emqtt:subscribe(Sub, Topic, 1),
    append(Config, Topic, [<<"first">>, <<"second">>, <<"third">>]),
    lists:foreach(
        fun(Payload) ->
            [#{payload := Received, packet_id := PacketId}] = receive_messages(1),
            ?assertEqual(Payload, Received),
            no_messages(),
            emqtt:puback(Sub, PacketId)
        end,
        [<<"first">>, <<"second">>, <<"third">>]
    ),
    ?retry(50, 100, ?assertEqual(3, cursor(Config, <<"inflight">>, Topic))),
    ok = emqtt:disconnect(Sub).

%% Rate-limited messages remain unsettled until ExtSub can send them.
-doc "Check the delivery limiter cannot advance the database cursor past unsent messages.".
t_rate_backpressure(Config) ->
    {ok, _} = emqx:update_config(
        [listeners, tcp, default],
        {update, #{<<"delivery_messages_rate">> => <<"1/1h">>}}
    ),
    emqx_common_test_helpers:on_exit(fun() ->
        emqx:update_config(
            [listeners, tcp, default],
            {update, #{<<"delivery_messages_rate">> => <<"infinity">>}}
        )
    end),
    Topic = <<"offline/rate">>,
    Sub = client(Config, <<"rate">>, []),
    {ok, _, [1]} = emqtt:subscribe(Sub, Topic, 1),
    append(Config, Topic, [<<"first">>, <<"second">>, <<"third">>]),
    ?assertEqual([<<"first">>], payloads(receive_messages(1))),
    no_messages(),
    ?retry(50, 100, ?assertEqual(1, cursor(Config, <<"rate">>, Topic))),
    ok = emqtt:disconnect(Sub),
    {ok, _} = emqx:update_config(
        [listeners, tcp, default],
        {update, #{<<"delivery_messages_rate">> => <<"infinity">>}}
    ),
    Resumed = client(Config, <<"rate">>, []),
    ?assertEqual([<<"second">>, <<"third">>], payloads(receive_messages(2))),
    ok = emqtt:disconnect(Resumed).

%% The shipped defaults round-trip through the plugin configuration schema.
-doc "Check the packaged Avro schema accepts PostgreSQL configuration.".
t_config_schema(_Config) ->
    PrivDir = code:priv_dir(emqx_offline_messages),
    {ok, Schema} = file:read_file(filename:join(PrivDir, "config_schema.avsc")),
    {ok, Defaults} = hocon:load(filename:join(PrivDir, "config.hocon")),
    Store = avro_schema_store:import_schema_json(
        <<"offline_messages">>,
        Schema,
        avro_schema_store:new([map])
    ),
    Encoded = avro_json_encoder:encode(Store, <<"offline_messages">>, Defaults),
    Options = avro:make_decoder_options([{map_type, map}, {record_type, map}, {encoding, avro_json}]),
    Decoded = avro_json_decoder:decode_value(Encoded, <<"offline_messages">>, Store, Options#{
        is_wrapped => false
    }),
    Parsed0 = emqx_omp_pg_config:parse(maps:get(<<"postgresql">>, Defaults)),
    Parsed1 = emqx_omp_pg_config:parse(maps:get(<<"postgresql">>, Decoded)),
    ?assertEqual(maps:remove(instance, Parsed0), maps:remove(instance, Parsed1)),
    #{connection := #{ssl_opts := SSLOpts}} = emqx_omp_pg_config:parse(
        #{<<"host">> => <<"database.example.net">>, <<"ssl">> => #{<<"enable">> => true}}
    ),
    ?assertEqual(verify_peer, proplists:get_value(verify, SSLOpts)),
    ?assertEqual("database.example.net", proplists:get_value(server_name_indication, SSLOpts)).

%% Cleanup applies only to the configured deployment namespace.
-doc "Check one namespace's cleanup cannot remove another namespace's messages or subscriptions.".
t_retention_namespace(Config) ->
    Conn = ?config(db, Config),
    Namespace = ?config(namespace, Config),
    Topic = <<"offline/namespaces">>,
    OtherConfig = [{namespace, <<"another-deployment">>} | Config],
    append(Config, Topic, [<<"one">>, <<"two">>]),
    append(OtherConfig, Topic, [<<"other-one">>, <<"other-two">>]),
    {ok, _} = emqx_omp_pg_store:run(
        Conn,
        {subscribe, <<"another-deployment">>, <<"other">>, Topic, #{qos => 1}, earliest}
    ),
    sql(Conn, "UPDATE mqtt_pg_subscription SET updated_at = now() - interval '1 day'", []),
    {ok, [{Id, 3}]} = emqx_omp_pg_store:run(Conn, {topics, Namespace, 0, 100}),
    {ok, 1} = emqx_omp_pg_store:run(Conn, {trim, Id, 2, 1000}),
    {ok, 0} = emqx_omp_pg_store:run(Conn, {expire, Namespace, 1000, 1000}),
    ?assertEqual([{2, <<"two">>}], messages(Config, Topic)),
    ?assertEqual([{1, <<"other-one">>}, {2, <<"other-two">>}], messages(OtherConfig, Topic)),
    ?assertEqual(0, cursor(OtherConfig, <<"other">>, Topic)).

%% Caught-up subscribers use live copies and recover a commit whose live copy was lost.
-doc "Check the MQTT live path uses no page reads and idle polling recovers a lost final copy.".
t_live_mqtt(Config) ->
    Topic = <<"offline/live-mqtt">>,
    Raw = ?config(raw, Config),
    Read = #{<<"page_max_messages">> => 2, <<"window">> => 4, <<"idle_poll_interval">> => 1000},
    ok = emqx_offline_messages_pg:on_config_changed(Raw, Raw#{<<"read">> => Read}),
    Sub = client(Config, <<"live-mqtt">>, []),
    ?assertWaitEvent(
        emqtt:subscribe(Sub, Topic, 1),
        #{?snk_kind := omp_pg_page, clientid := <<"live-mqtt">>, dirty := false},
        5000
    ),
    %% Record database reads after the initial snapshot finishes.
    ok = meck:new(emqx_omp_pg_store, [passthrough, no_link]),
    Pub = client(Config, <<"live-pub">>, []),
    lists:foreach(
        fun(N) ->
            {ok, _} = emqtt:publish(Pub, Topic, integer_to_binary(N), 1),
            ?assertEqual([integer_to_binary(N)], payloads(receive_messages(1)))
        end,
        lists:seq(1, 10)
    ),
    ?assertEqual(0, page_reads()),
    %% Lose the live copy of a committed message.
    ok = meck:expect(emqx_omp_pg_store, dispatch, fun(_Msg, _Identity, _Offset) -> ok end),
    {ok, _} = emqtt:publish(Pub, Topic, <<"lost-copy">>, 1),
    ok = meck:delete(emqx_omp_pg_store, dispatch, 3),
    ?assertEqual([<<"lost-copy">>], payloads(receive_messages(1))),
    ?retry(20, 100, ?assertEqual(11, cursor(Config, <<"live-mqtt">>, Topic))),
    ok = emqtt:disconnect(Sub),
    ok = emqtt:disconnect(Pub).

%% Unsupported wildcard subscriptions receive no live copy, even beside an exact handler.
-doc "Check no-handler drops and wildcard overlap cannot duplicate a managed live message.".
t_live_without_handler(Config) ->
    Topic = <<"offline/overlap">>,
    Sub = client(Config, <<"overlap">>, []),
    [Channel] = emqx_cm:lookup_channels(<<"overlap">>),
    %% Internal subscription requests bypass authorization, as the REST subscribe API does.
    Channel ! {subscribe, [{<<"offline/#">>, #{qos => 1, nl => 0, rap => 0, rh => 0}}]},
    _ = emqx_connection:info(Channel),
    Pub = client(Config, <<"overlap-pub">>, []),
    {ok, _} = emqtt:publish(Pub, Topic, <<"history">>, 1),
    no_messages(),
    {ok, _, [1]} = emqtt:subscribe(Sub, Topic, 1),
    ?assertEqual([<<"history">>], payloads(receive_messages(1))),
    %% The exact and the wildcard filter each send a live term; the handler accepts one.
    {ok, _} = emqtt:publish(Pub, Topic, <<"live">>, 1),
    ?assertEqual([<<"live">>], payloads(receive_messages(1))),
    no_messages(),
    ok = emqtt:disconnect(Sub),
    ok = emqtt:disconnect(Pub).

%% Publishers on two broker nodes share the SQL counter and send live copies across nodes.
-doc "Check cross-node live dispatch and ordered delivery with concurrent publishers.".
t_cluster_routing(Config) ->
    Apps = [
        {emqx_conf, #{config => #{authorization => #{no_match => allow}}}},
        emqx,
        emqx_extsub,
        emqx_offline_messages
    ],
    Nodes =
        [Node0, Node1] = emqx_cth_cluster:start(
            [{omp_pg_0, #{apps => Apps}}, {omp_pg_1, #{apps => Apps}}],
            #{work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, Config)}
        ),
    emqx_common_test_helpers:on_exit(fun() -> emqx_cth_cluster:stop(Nodes) end),
    Raw = ?config(raw, Config),
    Read = #{<<"window">> => 4, <<"page_max_messages">> => 2, <<"idle_poll_interval">> => 60000},
    lists:foreach(
        fun(Node) ->
            ok = rpc:call(Node, emqx_offline_messages_pg, on_config_changed, [
                #{}, Raw#{<<"read">> => Read}
            ]),
            ok = rpc:call(
                Node,
                emqx_hooks,
                put,
                ['message.delivered', {?MODULE, cluster_delivered, [self()]}, 0]
            )
        end,
        Nodes
    ),
    Config0 = [{port, emqx_cth_cluster:get_tcp_mqtt_port(Node0)} | Config],
    Config1 = [{port, emqx_cth_cluster:get_tcp_mqtt_port(Node1)} | Config],
    Topic = <<"offline/cluster">>,
    Sub = client(Config1, <<"cluster-sub">>, []),
    ?assertWaitEvent(
        emqtt:subscribe(Sub, Topic, 1),
        #{?snk_kind := omp_pg_page, clientid := <<"cluster-sub">>, dirty := false},
        5000
    ),
    ok = emqx_cth_cluster:sync_routes(Nodes),
    ok = rpc:call(Node1, meck, new, [emqx_omp_pg_store, [passthrough, no_link]]),
    Pub0 = client(Config0, <<"cluster-pub0">>, []),
    Pub1 = client(Config1, <<"cluster-pub1">>, []),
    {ok, _} = emqtt:publish(Pub0, Topic, <<"remote">>, 1),
    ?assertEqual([<<"remote">>], payloads(receive_messages(1))),
    receive
        {cluster_delivered, {_, _, 1}} -> ok
    after 5000 -> ct:fail(missing_remote_position)
    end,
    ?assertEqual(
        0,
        rpc:call(
            Node1, meck, num_calls, [emqx_omp_pg_store, exec_async, ['_', {page, '_', '_', '_'}]]
        )
    ),
    ok = rpc:call(Node1, meck, unload, [emqx_omp_pg_store]),
    Parent = self(),
    lists:foreach(
        fun({Pub, Prefix}) ->
            spawn_link(fun() ->
                lists:foreach(
                    fun(N) ->
                        {ok, _} = emqtt:publish(
                            Pub, Topic, <<Prefix/binary, (integer_to_binary(N))/binary>>, 1
                        )
                    end,
                    lists:seq(1, 20)
                ),
                Parent ! {publisher_done, Pub}
            end)
        end,
        [{Pub0, <<"a">>}, {Pub1, <<"b">>}]
    ),
    Received = payloads(receive_messages(40)),
    lists:foreach(
        fun(Pub) ->
            receive
                {publisher_done, Pub} -> ok
            after 5000 -> ct:fail(publisher_timeout)
            end
        end,
        [Pub0, Pub1]
    ),
    ?assertEqual([Payload || {Offset, Payload} <- messages(Config, Topic), Offset > 1], Received),
    Delivered = [
        receive
            {cluster_delivered, {_, _, Offset}} -> Offset
        after 5000 -> ct:fail(missing_cluster_delivery)
        end
     || _ <- lists:seq(1, 40)
    ],
    ?assertEqual(lists:seq(2, 41), Delivered),
    ok = emqtt:disconnect(Sub),
    ok = emqtt:disconnect(Pub0),
    ok = emqtt:disconnect(Pub1).

cluster_delivered(#{clientid := <<"cluster-sub">>}, Msg, Parent) ->
    Parent ! {cluster_delivered, emqx_message:get_header(omp_position, Msg)},
    ok;
cluster_delivered(_ClientInfo, _Msg, _Parent) ->
    ok.

%% The channel overload policy applies to live copies accumulated outside the ExtSub window.
-doc "Check the mailbox limit still shuts down a stalled managed-topic subscriber.".
t_mailbox_overload(Config) ->
    Topic = <<"offline/overload">>,
    Policy = emqx_config:get_zone_conf(default, [force_shutdown]),
    ok = emqx_config:put_zone_conf(default, [force_shutdown], Policy#{max_mailbox_size := 20}),
    emqx_common_test_helpers:on_exit(fun() ->
        emqx_config:put_zone_conf(default, [force_shutdown], Policy)
    end),
    Sub = client(Config, <<"overload">>, []),
    unlink(Sub),
    ?assertWaitEvent(
        emqtt:subscribe(Sub, Topic, 1),
        #{?snk_kind := omp_pg_page, clientid := <<"overload">>, dirty := false},
        5000
    ),
    [Channel] = emqx_cm:lookup_channels(<<"overload">>),
    Monitor = monitor(process, Channel),
    ok = sys:suspend(Channel),
    emqx_common_test_helpers:on_exit(fun() -> catch sys:resume(Channel) end),
    %% The passive-socket boundary checks overload before the queued copies run.
    Channel ! {tcp_passive, undefined},
    lists:foreach(
        fun(_) ->
            {ok, [persisted], _} = emqx_broker:publish2(
                emqx_message:make(<<"p">>, 1, Topic, <<"queued">>)
            )
        end,
        lists:seq(1, 100)
    ),
    {message_queue_len, QueueLength} = process_info(Channel, message_queue_len),
    ?assert(QueueLength >= 100),
    ?assertWaitEvent(
        sys:resume(Channel),
        #{?snk_kind := check_oom_shutdown, shutdown := #{reason := mailbox_overflow}},
        5000
    ),
    receive
        {'DOWN', Monitor, process, Channel, _} -> ok
    after 5000 -> ct:fail(channel_still_running)
    end,
    ?assertEqual(100, length(messages(Config, Topic))).

%% Reordered live copies drain in offset order without querying PostgreSQL.
-doc "Check live delivery, duplicate rejection and the small reorder pocket.".
t_live_pocket(Config) ->
    {Handler0, Context, Identity} = handler(Config, #{}),
    [Live1, Live2, Live3] = live(Identity, [1, 2, 3]),
    {ok, Handler1, []} = live_info(Handler0, Context, [Live3, Live2, Live2]),
    {ok, Handler2, Messages} = live_info(Handler1, Context, [Live1]),
    ?assertEqual([1, 2, 3], offsets(Messages)),
    ?assertEqual([1, 1, 1], [QoS || #message{qos = QoS} <- Messages]),
    {ok, Handler3, []} = live_info(Handler2, Context, [Live1, Live3]),
    no_read(),
    ok = emqx_offline_messages_pg_extsub:handle_terminate(Handler3).

%% Copies from another namespace or topic, and unrelated terms, cannot advance the cursor.
-doc "Check handler identity filtering.".
t_live_identity(Config) ->
    {Handler0, Context, {Namespace, Id} = Identity} = handler(Config, #{}),
    Foreign = live({<<Namespace/binary, "-other">>, Id}, [1]) ++ live({Namespace, Id + 1}, [1]),
    {ok, Handler1, []} = live_info(Handler0, Context, Foreign ++ [unrelated_term]),
    {ok, Handler2, Messages} = live_info(Handler1, Context, live(Identity, [1])),
    ?assertEqual([1], offsets(Messages)),
    no_read(),
    ok = emqx_offline_messages_pg_extsub:handle_terminate(Handler2).

%% A missing offset starts one recovery page after the reorder timer fires.
-doc "Check gap recovery and pocket deduplication.".
t_live_gap(Config) ->
    {Handler0, Context, Identity} = handler(Config, #{}),
    [Live1, Live2, Live3] = live(Identity, [1, 2, 3]),
    {ok, Handler1, []} = live_info(Handler0, Context, [Live3]),
    no_read(),
    {ok, Handler2} = handler_event(Handler1, Context, reorder),
    {Handler3, Ref, {page, _, 0, 2}} = handler_request(Handler2, Context),
    {ok, Handler4, Messages} = handler_result(
        Handler3, Context, Ref, {ok, [db_row(3, Live1), db_row(3, Live2)]}
    ),
    ?assertEqual([1, 2, 3], offsets(Messages)),
    no_read(),
    ok = emqx_offline_messages_pg_extsub:handle_terminate(Handler4).

%% Live copies received during a read remain available when its snapshot reaches the old tip.
-doc "Check the read-to-live transition and page capacity reservation.".
t_read_overlap(Config) ->
    {Handler0, Context, Identity} = handler(Config, #{}),
    %% Force a recovery read through a large gap, independently of wall-clock time.
    {ok, Handler1, []} = live_info(Handler0, Context, live(Identity, [40])),
    {Handler2, Ref, {page, _, 0, 2}} = handler_request(Handler1, Context),
    [Live1, Live2, Live3, Live4] = live(Identity, [1, 2, 3, 4]),
    {ok, Handler3, []} = live_info(Handler2, Context, [Live3]),
    %% The page reservation, the pocket and one delivering message fill the window.
    {ok, Handler4, []} = live_info(Handler3, Context#{delivering_count := 1}, [Live4]),
    {ok, Handler5, Messages} = handler_result(
        Handler4, Context, Ref, {ok, [db_row(2, Live1), db_row(2, Live2)]}
    ),
    ?assertEqual([1, 2, 3], offsets(Messages)),
    %% The dropped copy is recovered from the cursor.
    {Handler6, _Ref1, {page, _, 3, 2}} = handler_request(Handler5, Context),
    ok = emqx_offline_messages_pg_extsub:handle_terminate(Handler6).

%% Failed reads preserve consumed offsets, and late results cannot replace a new request.
-doc "Check read retries and stale request rejection without replaying delivered positions.".
t_read_retry(Config) ->
    {Handler0, Context, Identity} = handler(Config, #{}),
    {ok, Handler1, [_]} = live_info(Handler0, Context, live(Identity, [1])),
    {ok, Handler2, []} = live_info(Handler1, Context, live(Identity, [40])),
    {Handler3, Ref0, {page, _, 1, 2}} = handler_request(Handler2, Context),
    {ok, Handler4} = handler_result(Handler3, Context, Ref0, {error, disconnected}),
    {ok, Handler5} = emqx_offline_messages_pg_extsub:handle_info(Handler4, Context, pg_retry),
    Ref1 =
        receive
            {handler_request, Ref, {page, _, 1, 2}} -> Ref
        after 1000 -> ct:fail(no_retry)
        end,
    [Live2] = live(Identity, [2]),
    {ok, Handler5} = handler_result(Handler5, Context, Ref0, {ok, [db_row(2, Live2)]}),
    {ok, Handler6, Messages} = handler_result(Handler5, Context, Ref1, {ok, [db_row(2, Live2)]}),
    ?assertEqual([2], offsets(Messages)),
    ok = emqx_offline_messages_pg_extsub:handle_terminate(Handler6).

%% Consecutive failures back off up to the idle poll interval.
-doc "Check the retry delay grows with jitter and stays within the idle poll interval.".
t_retry_backoff(Config) ->
    {Handler0, Context, Identity} = handler(Config, #{<<"idle_poll_interval">> => 4000}),
    {ok, Handler1, []} = live_info(Handler0, Context, live(Identity, [40])),
    Delays = retry_delays(Handler1, Context, 4),
    %% Delays are 1000, 2000, 4000 and 4000 milliseconds, each with jitter of +-50%.
    lists:foreach(
        fun({Delay, Base}) -> ?assert(Delay >= Base div 2 andalso Delay =< Base * 3 div 2) end,
        lists:zip(Delays, [1000, 2000, 4000, 4000])
    ).

retry_delays(_Handler, _Context, 0) ->
    [];
retry_delays(Handler0, Context, N) ->
    {Handler1, Ref, _} = handler_request(Handler0, Context),
    {ok, Handler2} = handler_result(Handler1, Context, Ref, {error, disconnected}),
    Delay =
        receive
            {trace_retry, Timeout} -> Timeout
        after 1000 -> ct:fail(no_retry_timer)
        end,
    {ok, Handler3} = emqx_offline_messages_pg_extsub:handle_info(Handler2, Context, pg_retry),
    self() ! #info_to_extsub{info = pg_tick},
    [Delay | retry_delays(Handler3, Context, N - 1)].

%% A full reorder pocket must release capacity so a missing prefix can be read.
-doc "Check full-pocket recovery cannot deadlock the window.".
t_pocket_capacity(Config) ->
    {Handler0, Context, Identity} = handler(Config, #{}),
    {ok, Handler1, []} = live_info(Handler0, Context, live(Identity, [2, 3, 4, 5])),
    {ok, Handler2} = handler_event(Handler1, Context, reorder),
    {Handler3, _Ref, {page, _, 0, 2}} = handler_request(Handler2, Context),
    ok = emqx_offline_messages_pg_extsub:handle_terminate(Handler3).

%% Session drops complete their offsets.
-doc "Check no-local and expiry drops advance the checkpoint.".
t_drop_completion(Config) ->
    {Handler0, Context, Identity} = handler(Config, #{}),
    {ok, Handler1, [Msg1, Msg2]} = live_info(Handler0, Context, live(Identity, [1, 2])),
    {ok, Handler2} = emqx_offline_messages_pg_extsub:handle_delivered(
        Handler1, Context, Msg1, {dropped, no_local}
    ),
    {ok, Handler3} = emqx_offline_messages_pg_extsub:handle_delivered(
        Handler2, Context, Msg2, {dropped, expired}
    ),
    ok = emqx_offline_messages_pg_extsub:handle_terminate(Handler3),
    ?retry(20, 100, ?assertEqual(2, cursor(Config, <<"handler">>, <<"offline/handler">>))).

%% One topic sequence carries every publish QoS and supports both start positions.
-doc "Check mixed-QoS batches, latest and earliest cursors, and count retention.".
t_storage(Config) ->
    Conn = ?config(db, Config),
    Namespace = ?config(namespace, Config),
    Topic = <<"offline/storage">>,
    Encode = fun(QoS) ->
        emqx_omp_pg_store:encode(emqx_message:make(<<"p">>, QoS, Topic, <<"data">>))
    end,
    {ok, {Id, 1}} = emqx_omp_pg_store:run(
        Conn, {append, Namespace, Topic, [Encode(0), Encode(1), Encode(2)]}
    ),
    {ok, {Id, 4}} = emqx_omp_pg_store:run(Conn, {append, Namespace, Topic, [Encode(0)]}),
    ?assertEqual(
        [{1, 0}, {2, 1}, {3, 2}, {4, 0}],
        sql(Conn, "SELECT msg_offset, qos FROM mqtt_pg_message ORDER BY 1", [])
    ),
    ?assertMatch(
        {ok, #{cursor := 4}},
        emqx_omp_pg_store:run(
            Conn, {subscribe, Namespace, <<"latest">>, Topic, #{qos => 1}, latest}
        )
    ),
    {ok, 3} = emqx_omp_pg_store:run(Conn, {trim, Id, 5 - 1, 1000}),
    {ok, Rows} = emqx_omp_pg_store:run(Conn, {page, Id, 0, 10}),
    ?assertEqual([4], [element(2, Row) || Row <- Rows]),
    ?assertMatch(
        {ok, #{cursor := 0}},
        emqx_omp_pg_store:run(
            Conn, {subscribe, Namespace, <<"earliest">>, Topic, #{qos => 1}, earliest}
        )
    ),
    ?assertEqual(
        [{<<"mqtt_pg_subscription_topic">>}],
        sql(
            Conn,
            "SELECT indexname FROM pg_indexes WHERE tablename = 'mqtt_pg_subscription' AND indexname <> 'mqtt_pg_subscription_pkey'",
            []
        )
    ),
    ?assertEqual(
        [{<<"mqtt_pg_message_pkey">>}],
        sql(Conn, "SELECT indexname FROM pg_indexes WHERE tablename = 'mqtt_pg_message'", [])
    ).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

handler(Config, ReadOptions) ->
    Raw = ?config(raw, Config),
    Read = maps:merge(
        #{
            <<"page_max_messages">> => 2,
            <<"window">> => 4,
            <<"reorder_timeout">> => 100,
            <<"idle_poll_interval">> => 60000
        },
        ReadOptions
    ),
    ok = emqx_offline_messages_pg:on_config_changed(Raw, Raw#{<<"read">> => Read}),
    Parent = self(),
    ok = meck:new(emqx_omp_pg_store, [passthrough, no_link]),
    ok = meck:expect(emqx_omp_pg_store, async, fun(_Pool, _Key, Operation, _Target, Ref) ->
        Parent ! {handler_request, Ref, Operation},
        ok
    end),
    ok = meck:new(emqx_extsub_handler, [passthrough, no_link]),
    ok = meck:expect(emqx_extsub_handler, send_after, fun(Timeout, Target, Info) ->
        case Info of
            pg_retry -> Parent ! {trace_retry, Timeout};
            _ -> ok
        end,
        meck:passthrough([Timeout, Target, Info])
    end),
    Context = #{
        can_receive_acks => true,
        message_target => emqx_extsub_handler:message_target(make_ref()),
        clientinfo => #{clientid => <<"handler">>},
        subopts => #{qos => 1},
        delivering_count => 0
    },
    {ok, Handler0} = emqx_offline_messages_pg_extsub:handle_subscribe(
        subscribe, Context, undefined, <<"offline/handler">>
    ),
    {Handler1, Ref0, Init} = handler_request(Handler0, Context),
    {ok, #{topic_id := Id}} = InitResult = emqx_omp_pg_store:run(?config(db, Config), Init),
    {ok, Handler2} = handler_result(Handler1, Context, Ref0, InitResult),
    {Handler3, Ref1, Page} = handler_request(Handler2, Context),
    {ok, Handler4, []} = handler_result(
        Handler3,
        Context,
        Ref1,
        emqx_omp_pg_store:run(?config(db, Config), Page)
    ),
    {Handler4, Context, {?config(namespace, Config), Id}}.

handler_request(Handler0, Context) ->
    receive
        #info_to_extsub{info = pg_tick} -> ok
    after 1000 -> ct:fail(no_tick)
    end,
    {ok, Handler} = emqx_offline_messages_pg_extsub:handle_info(Handler0, Context, pg_tick),
    receive
        {handler_request, Ref, Operation} -> {Handler, Ref, Operation}
    after 1000 -> ct:fail(no_read_request)
    end.

handler_event(Handler, Context, reorder) ->
    receive
        #info_to_extsub{info = {pg_reorder, _} = Event} ->
            emqx_offline_messages_pg_extsub:handle_info(Handler, Context, Event)
    after 1000 -> ct:fail(no_reorder_timer)
    end.

handler_result(Handler, Context, Ref, Result) ->
    emqx_offline_messages_pg_extsub:handle_info(Handler, Context, {pg_result, Ref, Result}).

%% Pass live terms one at a time, as the channel does, and collect returned messages.
live_info(Handler0, Context, Terms) ->
    lists:foldl(
        fun(Term, {ok, Handler, Acc}) ->
            case emqx_offline_messages_pg_extsub:handle_info(Handler, Context, {generic, Term}) of
                {ok, Handler1} -> {ok, Handler1, Acc};
                {ok, Handler1, Messages} -> {ok, Handler1, Acc ++ Messages}
            end
        end,
        {ok, Handler0, []},
        Terms
    ).

live({Namespace, Id}, Offsets) ->
    [
        ?OMP_LIVE(
            Namespace,
            Id,
            Offset,
            emqx_message:make(<<"publisher">>, 0, <<"offline/handler">>, integer_to_binary(Offset))
        )
     || Offset <- Offsets
    ].

offsets(Messages) ->
    [Offset || #message{headers = #{omp_position := {_, _, Offset}}} <- Messages].

db_row(Tip, ?OMP_LIVE(_, _, Offset, #message{id = Id, payload = Payload, timestamp = Published})) ->
    {Tip, Offset, Id, <<"publisher">>, 0, false, Payload,
        emqx_utils_json:encode(#{version => 1, values => #{}}), Published, null}.

no_read() ->
    receive
        {handler_request, _, _} = Request -> ct:fail({unexpected_read, Request});
        #info_to_extsub{info = pg_tick} -> ct:fail(unexpected_read_tick)
    after 20 -> ok
    end.

page_reads() ->
    meck:num_calls(emqx_omp_pg_store, exec_async, ['_', {page, '_', '_', '_'}]).

client(Config, Id, Options) ->
    {ok, Client} = emqtt:start_link(
        Options ++
            [
                {clientid, Id},
                {host, "127.0.0.1"},
                {port, ?config(port, Config)},
                {proto_ver, v5}
            ]
    ),
    {ok, _} = emqtt:connect(Client),
    emqx_common_test_helpers:on_exit(fun() -> catch emqtt:stop(Client) end),
    Client.

append(Config, Topic, Payloads) ->
    {ok, _} = store_append(?config(db, Config), Config, Topic, Payloads),
    ok.

store_append(Conn, Config, Topic, Payloads) ->
    Messages = [
        emqx_omp_pg_store:encode(emqx_message:make(<<"publisher">>, 1, Topic, Payload))
     || Payload <- Payloads
    ],
    emqx_omp_pg_store:run(Conn, {append, ?config(namespace, Config), Topic, Messages}).

messages(Config, Topic) ->
    sql(?config(db, Config), """
    SELECT m.msg_offset, m.payload FROM mqtt_pg_message m JOIN mqtt_pg_topic t USING(topic_id)
    WHERE t.namespace = $1 AND t.topic = $2 ORDER BY m.msg_offset
    """, [?config(namespace, Config), Topic]).

cursor(Config, ClientId, Topic) ->
    case sql(?config(db, Config), """
    SELECT s.ack_offset FROM mqtt_pg_subscription s JOIN mqtt_pg_topic t USING(topic_id)
    WHERE s.namespace = $1 AND s.client_id = $2 AND t.topic = $3
    """, [?config(namespace, Config), ClientId, Topic]) of
        [{Offset}] -> Offset;
        [] -> undefined
    end.

sql(Conn, SQL, Params) ->
    {ok, Result} = emqx_omp_pg_store:query(Conn, SQL, Params),
    Result.

receive_messages(0) ->
    [];
receive_messages(N) ->
    receive
        {publish, Message} -> [Message | receive_messages(N - 1)]
    after 10000 -> ct:fail({missing_messages, N})
    end.

drain_messages() ->
    receive
        {publish, Message} -> [Message | drain_messages()]
    after 500 -> []
    end.

payloads(Messages) -> [Payload || #{payload := Payload} <- Messages].

no_messages() ->
    receive
        {publish, Msg} -> ct:fail({unexpected_publish, Msg})
    after 150 -> ok
    end.
