%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_persistent_messages_SUITE).

-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").
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
-ifndef(EMQX_RELEASE_EDITION).
-define(EMQX_RELEASE_EDITION, ce).
-endif.

-if(?EMQX_RELEASE_EDITION == ee).

init_per_suite(Config) ->
    Config.

-else.

init_per_suite(_Config) ->
    {skip, no_replication}.

-endif.

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
        <<"heartbeat_interval">> => <<"100ms">>,
        <<"session_gc_interval">> => <<"2s">>
    },
    Opts = #{
        durable_sessions_opts => DurableSessonsOpts
    },
    common_init_per_testcase(TestCase, Config, Opts).

common_init_per_testcase(TestCase, Config, Opts0) ->
    Opts = Opts0#{
        work_dir => emqx_cth_suite:work_dir(TestCase, Config),
        start_emqx_conf => false
    },
    emqx_common_test_helpers:start_apps_ds(Config, _ExtraApps = [], Opts).

end_per_testcase(_TestCase, Config) ->
    emqx_common_test_helpers:call_janitor(60_000),
    ok = emqx_common_test_helpers:stop_apps_ds(Config).

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
    [ConnPid] = emqx_cm:lookup_channels(ClientId),
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

        Inflight0 = get_session_inflight(ConnPid),

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
                #{payload := P1},
                #{payload := P2},
                #{payload := P3}
            ] when
                (P1 == <<"foo">> andalso P2 == <<"bar">> andalso P3 == <<"baz">>) orelse
                    (P1 == <<"baz">> andalso P2 == <<"foo">> andalso P3 == <<"bar">>) orelse
                    (P1 == <<"foo">> andalso P2 == <<"baz">> andalso P3 == <<"bar">>),

            receive_messages(3)
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
                #{payload := P1},
                #{payload := P2},
                #{payload := P3}
            ] when
                (P1 == <<"foo">> andalso P2 == <<"bar">> andalso P3 == <<"baz">>) orelse
                    (P1 == <<"baz">> andalso P2 == <<"foo">> andalso P3 == <<"bar">>) orelse
                    (P1 == <<"foo">> andalso P2 == <<"baz">> andalso P3 == <<"bar">>),

            receive_messages(3)
        ),

        Inflight1 = get_session_inflight(ConnPid),

        %% TODO: Kinda stupid way to verify that the runtime state is not growing.
        ?assert(
            erlang:external_size(Inflight1) - erlang:external_size(Inflight0) < 16,
            Inflight1
        )
    after
        emqtt:stop(Sub),
        emqtt:stop(Pub)
    end.

get_session_inflight(ConnPid) ->
    emqx_connection:info({channel, {session, inflight}}, sys:get_state(ConnPid)).

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

t_message_gc_too_young(_Config) ->
    %% Check that GC doesn't attempt to create a new generation if there are fresh enough
    %% generations around.  The stability of this test relies on the default value for
    %% message retention being long enough.  Currently, the default is 1 hour.
    ?check_trace(
        ok = emqx_persistent_message_ds_gc_worker:gc(),
        fun(Trace) ->
            ?assertMatch([_], ?of_kind(ps_message_gc_too_early, Trace)),
            ok
        end
    ),
    ok.

t_message_gc(Config) ->
    %% Check that, after GC runs, a new generation is created, retaining messages, and
    %% older messages no longer are accessible.
    NShards = ?config(n_shards, Config),
    ?check_trace(
        #{timetrap => 10_000},
        begin
            %% ensure some messages are in the first generation
            ?force_ordering(
                #{?snk_kind := inserted_batch},
                #{?snk_kind := ps_message_gc_added_gen}
            ),
            Msgs0 = [
                message(<<"foo/bar">>, <<"1">>, 0),
                message(<<"foo/baz">>, <<"2">>, 1)
            ],
            ok = emqx_ds:store_batch(?PERSISTENT_MESSAGE_DB, Msgs0, #{sync => true}),
            ?tp(inserted_batch, #{}),
            {ok, _} = ?block_until(#{?snk_kind := ps_message_gc_added_gen}),

            Now = emqx_message:timestamp_now(),
            Msgs1 = [
                message(<<"foo/bar">>, <<"3">>, Now + 100),
                message(<<"foo/baz">>, <<"4">>, Now + 101)
            ],
            ok = emqx_ds:store_batch(?PERSISTENT_MESSAGE_DB, Msgs1, #{sync => true}),

            {ok, _} = snabbkaffe:block_until(
                ?match_n_events(NShards, #{?snk_kind := message_gc_generation_dropped}),
                infinity
            ),

            TopicFilter = emqx_topic:words(<<"#">>),
            StartTime = 0,
            Msgs = consume(TopicFilter, StartTime),
            %% "1" and "2" should have been GC'ed
            PresentMessages = sets:from_list(
                [emqx_message:payload(Msg) || Msg <- Msgs],
                [{version, 2}]
            ),
            ?assert(
                sets:is_empty(
                    sets:intersection(
                        PresentMessages,
                        sets:from_list([<<"1">>, <<"2">>], [{version, 2}])
                    )
                ),
                #{present_messages => PresentMessages}
            ),

            ok
        end,
        []
    ),
    ok.

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

    DroppedBefore = emqx_metrics:val('messages.dropped'),
    DroppedNoSubBefore = emqx_metrics:val('messages.dropped.no_subscribers'),

    {ok, _, [?RC_GRANTED_QOS_1]} = emqtt:subscribe(Sub, <<"t/+">>, ?QOS_1),
    emqtt:publish(Pub, <<"t/ps">>, <<"payload">>, ?QOS_1),
    ?assertMatch([_], receive_messages(1)),
    DroppedAfter = emqx_metrics:val('messages.dropped'),
    DroppedNoSubAfter = emqx_metrics:val('messages.dropped.no_subscribers'),

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
        {ok, NIt, MsgsAndKeys} ->
            [Msg || {_DSKey, Msg} <- MsgsAndKeys] ++ consume(NIt);
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

publish(Node, Message) ->
    erpc:call(Node, emqx, publish, [Message]).

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
