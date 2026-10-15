%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_persistent_messages_SUITE).

-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-import(emqx_common_test_helpers, [on_exit/1]).

-include("emqx_persistent_message.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

%% Needed for standalone mode:
init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(t_message_gc = TestCase, Config) ->
    DurableSessonsOpts = #{<<"message_retention_period">> => <<"3s">>},
    EMQXOpts = #{<<"durable_storage">> => #{<<"messages">> => #{<<"n_shards">> => 3}}},
    Opts = #{durable_sessions_opts => DurableSessonsOpts, emqx_opts => EMQXOpts},
    common_init_per_testcase(TestCase, [{n_shards, 3} | Config], Opts);
init_per_testcase(t_replication_options = TestCase, Config) ->
    EMQXOpts = #{
        <<"durable_storage">> =>
            #{
                <<"messages">> =>
                    #{
                        <<"replication_options">> =>
                            #{
                                <<"resend_window">> => 60,
                                <<"snapshot_interval">> => 64,
                                <<"wal_compute_checksums">> => false,
                                <<"wal_max_batch_size">> => 1024,
                                <<"wal_max_size_bytes">> => 16000000,
                                <<"wal_sync_method">> => <<"datasync">>,
                                <<"wal_write_strategy">> => <<"o_sync">>
                            }
                    }
            }
    },
    Opts = #{emqx_opts => EMQXOpts},
    common_init_per_testcase(TestCase, Config, Opts);
init_per_testcase(t_message_gc_too_young = TestCase, Config) ->
    common_init_per_testcase(TestCase, Config, _Opts = #{});
init_per_testcase(TestCase, Config) ->
    DurableSessonsOpts = #{
        <<"enable">> => true,
        <<"checkpoint_interval">> => <<"100ms">>
    },
    Opts = #{
        durable_sessions_opts => DurableSessonsOpts
    },
    common_init_per_testcase(TestCase, Config, Opts).

common_init_per_testcase(TestCase, Config, Opts0) ->
    Opts = Opts0#{
        work_dir => emqx_cth_suite:work_dir(TestCase, Config),
        start_emqx_conf => true
    },
    emqx_common_test_helpers:start_apps_ds(Config, _ExtraApps = [], Opts).

end_per_testcase(_TestCase, Config) ->
    emqx_common_test_helpers:run_cleanups(Config, 60_000).

t_messages_persisted(_Config) ->
    C1 = connect(<<?MODULE_STRING "1">>, true, 30),
    C2 = connect(<<?MODULE_STRING "2">>, false, 60),
    C3 = connect(<<?MODULE_STRING "3">>, false, undefined),
    C4 = connect(<<?MODULE_STRING "4">>, false, 0),

    CP = connect(<<?MODULE_STRING "-pub">>, true, undefined),

    {ok, _, [1]} = emqtt:subscribe(C1, <<"client/+/topic">>, qos1),
    {ok, _, [0]} = emqtt:subscribe(C2, <<"client/+/topic">>, qos0),
    {ok, _, [1]} = emqtt:subscribe(C2, <<"random/+">>, qos1),
    {ok, _, [2]} = emqtt:subscribe(C3, <<"client/#">>, qos2),
    {ok, _, [0]} = emqtt:subscribe(C4, <<"random/#">>, qos0),

    Messages = [
        M1 = {<<"client/1/topic">>, <<"1">>},
        M2 = {<<"client/2/topic">>, <<"2">>},
        _M3 = {<<"client/3/topic/sub">>, <<"3">>},
        _M4 = {<<"client/4">>, <<"4">>},
        M5 = {<<"random/5">>, <<"5">>},
        _M6 = {<<"random/6/topic">>, <<"6">>},
        M7 = {<<"client/7/topic">>, <<"7">>},
        _M8 = {<<"client/8/topic/sub">>, <<"8">>},
        M9 = {<<"random/9">>, <<"9">>},
        M10 = {<<"random/10">>, <<"10">>}
    ],

    Results = [emqtt:publish(CP, Topic, Payload, 1) || {Topic, Payload} <- Messages],

    ct:pal("Results = ~p", [Results]),
    timer:sleep(2000),

    Persisted = consume(['#'], 0),

    ct:pal("Persisted = ~p", [Persisted]),

    ?assertEqual(
        lists:sort([M1, M2, M5, M7, M9, M10]),
        lists:sort([{emqx_message:topic(M), emqx_message:payload(M)} || M <- Persisted])
    ),

    ok.

t_messages_persisted_2(_Config) ->
    Prefix = atom_to_binary(?FUNCTION_NAME),
    C1 = connect(<<Prefix/binary, "1">>, _CleanStart0 = true, _EI0 = 30),
    CP = connect(<<Prefix/binary, "-pub">>, _CleanStart1 = true, _EI1 = undefined),
    T = fun(T0) -> <<Prefix/binary, T0/binary>> end,

    %% won't be persisted
    {ok, #{reason_code := ?RC_NO_MATCHING_SUBSCRIBERS}} =
        emqtt:publish(CP, T(<<"random/topic">>), <<"0">>, 1),
    {ok, #{reason_code := ?RC_NO_MATCHING_SUBSCRIBERS}} =
        emqtt:publish(CP, T(<<"client/1/topic">>), <<"1">>, 1),
    {ok, #{reason_code := ?RC_NO_MATCHING_SUBSCRIBERS}} =
        emqtt:publish(CP, T(<<"client/2/topic">>), <<"2">>, 1),

    {ok, _, [?RC_GRANTED_QOS_1]} = emqtt:subscribe(C1, T(<<"client/+/topic">>), qos1),
    {ok, #{reason_code := ?RC_NO_MATCHING_SUBSCRIBERS}} =
        emqtt:publish(CP, T(<<"random/topic">>), <<"3">>, 1),
    %% will be persisted
    {ok, #{reason_code := ?RC_SUCCESS}} =
        emqtt:publish(CP, T(<<"client/1/topic">>), <<"4">>, 1),
    {ok, #{reason_code := ?RC_SUCCESS}} =
        emqtt:publish(CP, T(<<"client/2/topic">>), <<"5">>, 1),

    {ok, _, [?RC_SUCCESS]} = emqtt:unsubscribe(C1, T(<<"client/+/topic">>)),
    %% won't be persisted
    {ok, #{reason_code := ?RC_NO_MATCHING_SUBSCRIBERS}} =
        emqtt:publish(CP, T(<<"random/topic">>), <<"6">>, 1),
    {ok, #{reason_code := ?RC_NO_MATCHING_SUBSCRIBERS}} =
        emqtt:publish(CP, T(<<"client/1/topic">>), <<"7">>, 1),
    {ok, #{reason_code := ?RC_NO_MATCHING_SUBSCRIBERS}} =
        emqtt:publish(CP, T(<<"client/2/topic">>), <<"8">>, 1),

    timer:sleep(2000),

    Persisted = consume(['#'], 0),

    ct:pal("Persisted = ~p", [Persisted]),

    ?assertEqual(
        lists:sort([
            {T(<<"client/1/topic">>), <<"4">>},
            {T(<<"client/2/topic">>), <<"5">>}
        ]),
        lists:sort([{emqx_message:topic(M), emqx_message:payload(M)} || M <- Persisted])
    ),

    ok.

t_qos0(_Config) ->
    Sub = connect(<<?MODULE_STRING "1">>, true, 30),
    Pub = connect(<<?MODULE_STRING "2">>, true, 0),
    try
        {ok, _, [1]} = emqtt:subscribe(Sub, <<"t/#">>, qos1),

        Messages = [
            {<<"t/1">>, <<"1">>, 0},
            {<<"t/1">>, <<"2">>, 1},
            {<<"t/1">>, <<"3">>, 0}
        ],
        [emqtt:publish(Pub, Topic, Payload, Qos) || {Topic, Payload, Qos} <- Messages],
        ?assertMatch(
            [
                #{qos := 0, topic := <<"t/1">>, payload := <<"1">>},
                #{qos := 1, topic := <<"t/1">>, payload := <<"2">>},
                #{qos := 0, topic := <<"t/1">>, payload := <<"3">>}
            ],
            receive_messages(3)
        )
    after
        emqtt:stop(Sub),
        emqtt:stop(Pub)
    end.

t_qos0_only_many_streams(_Config) ->
    ClientId = <<?MODULE_STRING "_sub">>,
    Sub = connect(ClientId, true, 30),
    Pub = connect(<<?MODULE_STRING "_pub">>, true, 0),
    try
        {ok, _, [1]} = emqtt:subscribe(Sub, <<"t/#">>, [{qos, 1}]),
        [
            emqtt:publish(Pub, Topic, Payload, ?QOS_1)
         || {Topic, Payload} <- [
                {<<"t/1">>, <<"foo">>},
                {<<"t/2">>, <<"bar">>},
                {<<"t/3">>, <<"baz">>}
            ]
        ],
        ?assertMatch(
            [_, _, _],
            receive_messages(3)
        ),
        [
            emqtt:publish(Pub, Topic, Payload, ?QOS_1)
         || {Topic, Payload} <- [
                {<<"t/2">>, <<"foo">>},
                {<<"t/2">>, <<"bar">>},
                {<<"t/1">>, <<"baz">>}
            ]
        ],
        ?assertMatch(
            [
                #{topic := <<"t/1">>, payload := <<"baz">>},
                #{topic := <<"t/2">>, payload := <<"foo">>},
                #{topic := <<"t/2">>, payload := <<"bar">>}
            ],
            group_maps_by(topic, receive_messages(3))
        ),
        [
            emqtt:publish(Pub, Topic, Payload, ?QOS_1)
         || {Topic, Payload} <- [
                {<<"t/3">>, <<"foo">>},
                {<<"t/3">>, <<"bar">>},
                {<<"t/2">>, <<"baz">>}
            ]
        ],
        ?assertMatch(
            [
                #{topic := <<"t/2">>, payload := <<"baz">>},
                #{topic := <<"t/3">>, payload := <<"foo">>},
                #{topic := <<"t/3">>, payload := <<"bar">>}
            ],
            group_maps_by(topic, receive_messages(3))
        ),
        ?assertNotReceive(_)
    after
        emqtt:stop(Sub),
        emqtt:stop(Pub)
    end.

t_publish_as_persistent(_Config) ->
    Sub = connect(<<?MODULE_STRING "1">>, true, 30),
    Pub = connect(<<?MODULE_STRING "2">>, true, 30),
    try
        {ok, _, [?RC_GRANTED_QOS_2]} = emqtt:subscribe(Sub, <<"t/#">>, qos2),
        Messages = [
            {<<"t/1">>, <<"1">>, 0},
            {<<"t/1">>, <<"2">>, 1},
            {<<"t/1">>, <<"3">>, 2}
        ],
        [emqtt:publish(Pub, Topic, Payload, Qos) || {Topic, Payload, Qos} <- Messages],
        ?assertMatch(
            [
                #{qos := 0, topic := <<"t/1">>, payload := <<"1">>},
                #{qos := 1, topic := <<"t/1">>, payload := <<"2">>},
                #{qos := 2, topic := <<"t/1">>, payload := <<"3">>}
            ],
            receive_messages(3)
        )
    after
        emqtt:stop(Sub),
        emqtt:stop(Pub)
    end.

t_publish_empty_topic_levels(_Config) ->
    Sub = connect(<<?MODULE_STRING "1">>, true, 30),
    Pub = connect(<<?MODULE_STRING "2">>, true, 30),
    try
        {ok, _, [?RC_GRANTED_QOS_1]} = emqtt:subscribe(Sub, <<"t//+//#">>, qos1),
        Messages = [
            {<<"t//1">>, <<"1">>},
            {<<"t//1/">>, <<"2">>},
            {<<"t//2//">>, <<"3">>},
            {<<"t//2//foo">>, <<"4">>},
            {<<"t//2/foo">>, <<"5">>},
            {<<"t/3/bar">>, <<"6">>}
        ],
        [emqtt:publish(Pub, Topic, Payload, ?QOS_1) || {Topic, Payload} <- Messages],
        Received = receive_messages(length(Messages)),
        ?assertMatch(
            [
                #{topic := <<"t//1/">>, payload := <<"2">>},
                #{topic := <<"t//2//">>, payload := <<"3">>},
                #{topic := <<"t//2//foo">>, payload := <<"4">>}
            ],
            lists:sort(emqx_utils_maps:key_comparer(payload), Received)
        )
    after
        emqtt:stop(Sub),
        emqtt:stop(Pub)
    end.

%% This testcase verifies functions of
%% emqx_persistent_message_ds_gc_worker. To avoid racing with this
%% process, it gets suspended for the duration of the test.
t_rotate_generations(_Config) ->
    Ls = fun(Shard) ->
        {Slabs, []} = emqx_ds:list_slabs(?PERSISTENT_MESSAGE_DB, #{shard => Shard}),
        lists:sort([{Slab, Info} || Slab := Info <- Slabs])
    end,
    ?check_trace(
        try
            ok = sys:suspend(emqx_persistent_message_ds_gc_worker, 5_000),
            Shards = emqx_ds:list_shards(?PERSISTENT_MESSAGE_DB),
            Now1 = erlang:system_time(microsecond),
            %% This operation should leave only two generations for each shard:
            ?assertMatch(
                ok,
                emqx_persistent_message_ds_gc_worker:manage_shards(Now1, 0, 1)
            ),
            Dt1 = erlang:system_time(microsecond) - Now1,
            Data1 = #{Shard => Ls(Shard) || Shard <- Shards},
            maps:foreach(
                fun(Shard, Slabs) ->
                    ?assertMatch(
                        [{_, #{until := U}}, {_, #{created_at := C}}] when
                            C >= Now1 andalso U >= Now1,
                        Slabs,
                        {Shard, Now1}
                    )
                end,
                Data1
            ),
            %% Calling the same function with the same parameters
            %% should not change anything:
            ?assertMatch(
                ok,
                emqx_persistent_message_ds_gc_worker:manage_shards(Now1, 0, 1)
            ),
            ?assertEqual(
                Data1,
                #{Shard => Ls(Shard) || Shard <- Shards}
            ),
            %% Check that retention period gets split into several generations if requested:
            RetentionPeriod = 10_000_000,
            Now2 = Now1 + RetentionPeriod div 2 + Dt1,
            ?assertMatch(
                ok,
                emqx_persistent_message_ds_gc_worker:manage_shards(Now2, RetentionPeriod, 2)
            ),
            Dt2 = erlang:system_time(microsecond) - Now2,
            %% This should add another generation:
            Data2 = #{Shard => Ls(Shard) || Shard <- Shards},
            maps:foreach(
                fun(Shard, Slabs) ->
                    #{Shard := [{S1, _}, {S2, _}]} = Data1,
                    ?assertMatch(
                        [{S1, _}, {S2, _}, _],
                        Slabs,
                        {Shard, Now2}
                    )
                end,
                Data2
            ),
            %% Now "pass" enough time to drop more generations:
            Now3 = Now2 + Dt2,
            ?assertMatch(
                ok,
                emqx_persistent_message_ds_gc_worker:manage_shards(Now3, 0, 2)
            ),
            Data3 = #{Shard => Ls(Shard) || Shard <- Shards},
            maps:foreach(
                fun(Shard, Slabs) ->
                    #{Shard := [_, _, {S3, _}]} = Data2,
                    ?assertMatch(
                        [{S3, _}, _],
                        Slabs,
                        {Shard, Now3}
                    )
                end,
                Data3
            )
        after
            sys:resume(emqx_persistent_message_ds_gc_worker, 5_000)
        end,
        fun(Trace) ->
            ?assertMatch(
                [],
                ?of_kind("Failed to rotate DS generations", Trace)
            )
        end
    ).

t_metrics_not_dropped(_Config) ->
    %% Asserts that, if only persisted sessions are subscribed to a topic being published
    %% to, we don't bump the `message.dropped' metric, nor we run the equivalent hook.
    Sub = connect(<<?MODULE_STRING "1">>, true, 30),
    on_exit(fun() -> emqtt:stop(Sub) end),
    Pub = connect(<<?MODULE_STRING "2">>, true, 30),
    on_exit(fun() -> emqtt:stop(Pub) end),
    Hookpoint = 'message.dropped',
    emqx_hooks:add(Hookpoint, {?MODULE, on_message_dropped, [self()]}, 1_000),
    on_exit(fun() -> emqx_hooks:del(Hookpoint, {?MODULE, on_message_dropped}) end),

    DroppedBefore = emqx_metrics:val_global('messages.dropped'),
    DroppedNoSubBefore = emqx_metrics:val_global('messages.dropped.no_subscribers'),

    {ok, _, [?RC_GRANTED_QOS_1]} = emqtt:subscribe(Sub, <<"t/+">>, ?QOS_1),
    emqtt:publish(Pub, <<"t/ps">>, <<"payload">>, ?QOS_1),
    ?assertMatch([_], receive_messages(1)),
    DroppedAfter = emqx_metrics:val_global('messages.dropped'),
    DroppedNoSubAfter = emqx_metrics:val_global('messages.dropped.no_subscribers'),

    ?assertEqual(DroppedBefore, DroppedAfter),
    ?assertEqual(DroppedNoSubBefore, DroppedNoSubAfter),

    ok.

t_replication_options(_Config) ->
    ?assertMatch(
        #{
            backend := builtin_raft,
            replication_options := #{
                wal_max_size_bytes := 16000000,
                wal_max_batch_size := 1024,
                wal_write_strategy := o_sync,
                wal_sync_method := datasync,
                wal_compute_checksums := false,
                snapshot_interval := 64,
                resend_window := 60
            }
        },
        emqx_ds_builtin_raft_meta:db_config(?PERSISTENT_MESSAGE_DB)
    ),
    ?assertMatch(
        #{
            wal_max_size_bytes := 16000000,
            wal_max_batch_size := 1024,
            wal_write_strategy := o_sync,
            wal_compute_checksums := false,
            wal_sync_method := datasync
        },
        ra_system:fetch(?PERSISTENT_MESSAGE_DB)
    ).

%% This testcase verifies dynamic reconfiguration capability of
%% `emqx_ds_schema' module.
%%
%% Dynamic config update should be triggered for the given DB.
t_runtime_config_update(_Config) ->
    %% List of config roots with the DBs dependent on them
    L = [
        {?DURABLE_TIMERS_DB, ?DURABLE_TIMERS_DB},
        {?PERSISTENT_MESSAGE_DB, ?PERSISTENT_MESSAGE_DB},
        {?DURABLE_SESSION_STATE_DB, ?DURABLE_SESSION_STATE_DB},
        {?MQ_STATE_CONF_ROOT, ?MQ_STATE_DB},
        {?MQ_MESSAGE_CONF_ROOT, ?MQ_MESSAGE_REGULAR_DB},
        {?MQ_MESSAGE_CONF_ROOT, ?MQ_MESSAGE_LASTVALUE_DB},
        {?STREAMS_MESSAGE_CONF_ROOT, ?STREAMS_MESSAGE_REGULAR_DB},
        {?STREAMS_MESSAGE_CONF_ROOT, ?STREAMS_MESSAGE_LASTVALUE_DB}
    ],
    ?check_trace(
        #{timetrap => 5_000},
        lists:foreach(
            fun({ConfRoot, DB}) ->
                ?wait_async_action(
                    emqx_conf:update(
                        [durable_storage, ConfRoot, transaction, idle_flush_interval],
                        10,
                        #{persistent => false}
                    ),
                    #{?snk_kind := "ds_db_runtime_config_update", db := DB}
                )
            end,
            L
        ),
        []
    ).

%%

connect(ClientId, CleanStart, EI) ->
    connect(#{
        clientid => ClientId,
        clean_start => CleanStart,
        properties => maps:from_list(
            [{'Session-Expiry-Interval', EI} || is_integer(EI)]
        )
    }).

connect(Opts0 = #{}) ->
    Defaults = #{proto_ver => v5},
    Opts = maps:to_list(emqx_utils_maps:deep_merge(Defaults, Opts0)),
    {ok, Client} = emqtt:start_link(Opts),
    on_exit(fun() -> catch emqtt:stop(Client) end),
    {ok, _} = emqtt:connect(Client),
    Client.

consume(TopicFilter, StartMS) ->
    Streams = emqx_ds:get_streams(?PERSISTENT_MESSAGE_DB, TopicFilter, StartMS),
    lists:flatmap(
        fun({_Rank, Stream}) ->
            {ok, It} = emqx_ds:make_iterator(?PERSISTENT_MESSAGE_DB, Stream, TopicFilter, StartMS),
            consume(It)
        end,
        Streams
    ).

consume(It) ->
    case emqx_ds:next(?PERSISTENT_MESSAGE_DB, It, 100) of
        {ok, _NIt, _Msgs = []} ->
            [];
        {ok, NIt, Batch} ->
            Batch ++ consume(NIt);
        {ok, end_of_stream} ->
            []
    end.

receive_messages(Count) ->
    receive_messages(Count, 10_000).

receive_messages(Count, Timeout) ->
    lists:reverse(receive_messages(Count, [], Timeout)).

receive_messages(0, Msgs, _Timeout) ->
    Msgs;
receive_messages(Count, Msgs, Timeout) ->
    receive
        {publish, Msg} ->
            receive_messages(Count - 1, [Msg | Msgs], Timeout)
    after Timeout ->
        Msgs
    end.

group_maps_by(K, Maps) ->
    emqx_ds_test_helpers:group_maps_by(K, Maps).

publish(Node, Message) ->
    erpc:call(Node, emqx, publish2, [Message]).

app_specs() ->
    app_specs(_Opts = #{}).

app_specs(Opts) ->
    ExtraEMQXConf = maps:get(extra_emqx_conf, Opts, ""),
    [
        emqx_durable_storage,
        {emqx, "durable_sessions {enable = true}" ++ ExtraEMQXConf}
    ].

cluster() ->
    ExtraConf = "\n durable_storage.messages.n_sites = 2",
    Spec = #{apps => app_specs(#{extra_emqx_conf => ExtraConf})},
    [
        {persistent_messages_SUITE1, Spec},
        {persistent_messages_SUITE2, Spec}
    ].

get_mqtt_port(Node, Type) ->
    {_IP, Port} = erpc:call(Node, emqx_config, get, [[listeners, Type, default, bind]]),
    Port.

message(Topic, Payload, PublishedAt) ->
    #message{
        topic = Topic,
        payload = Payload,
        timestamp = PublishedAt,
        id = emqx_guid:gen()
    }.

on_message_dropped(#message{flags = #{sys := true}}, _Context, _Res, _TestPid) ->
    ok;
on_message_dropped(Msg, Context, Res, TestPid) ->
    ErrCtx = #{msg => Msg, ctx => Context, res => Res},
    ct:pal("this hook should not be called.\n  ~p", [ErrCtx]),
    exit(TestPid, {hookpoint_called, ErrCtx}).
