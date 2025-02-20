%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-include_lib("emqx/include/asserts.hrl").

all() ->
    [
        {group, declare_explicit},
        {group, declare_implicit}
    ].

groups() ->
    TCs = emqx_common_test_helpers:all(?MODULE),
    Groups = [declare_explicit, declare_implicit],
    GroupTCs = [{Group, TC} || TC <- TCs, Group <- groups_per_testcase(TC, Groups)],
    lists:foldl(
        fun({Group, TC}, Acc) -> orddict:append(Group, TC, Acc) end,
        [],
        GroupTCs
    ).

groups_per_testcase(TC, Groups) ->
    try
        ?MODULE:TC(groups, Groups)
    catch
        error:_ -> Groups
    end.

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx_conf, #{
                config => #{
                    <<"rpc">> => #{
                        <<"port_discovery">> => <<"manual">>
                    },
                    <<"durable_sessions">> => #{
                        <<"enable">> => true,
                        <<"renew_streams_interval">> => "100ms",
                        <<"idle_poll_interval">> => "1s"
                    },
                    <<"durable_storage">> => #{
                        <<"messages">> => #{
                            <<"backend">> => <<"builtin_raft">>
                        },
                        <<"queues">> => #{
                            <<"backend">> => <<"builtin_raft">>,
                            <<"local_write_buffer">> => #{
                                <<"flush_interval">> => <<"10ms">>
                            }
                        }
                    }
                }
            }},
            emqx,
            {emqx_ds_shared_sub, #{
                config => "durable_queues { enable = true }"
            }}
        ],
        #{work_dir => ?config(priv_dir, Config)}
    ),
    [{apps, Apps} | Config].

init_per_group(GroupName, Config) ->
    [{queue_need_declare, GroupName =:= declare_explicit} | Config].

end_per_group(_GroupName, _Config) ->
    ok.

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)),
    ok.

init_per_testcase(TC, Config) ->
    ok = snabbkaffe:start_trace(),
    emqx_common_test_helpers:init_per_testcase(?MODULE, TC, Config).

end_per_testcase(TC, Config) ->
    ok = snabbkaffe:stop(),
    ok = emqx_ds_shared_sub_registry:purge(),
    emqx_common_test_helpers:end_per_testcase(?MODULE, TC, Config).

declare_queue_if_needed(Group, Topic, Config) ->
    case proplists:get_value(queue_need_declare, Config) of
        true -> declare_queue(Group, Topic, Config);
        false -> Config
    end.

declare_queue(Group, Topic, Config) ->
    Now = emqx_message:timestamp_now(),
    {ok, Queue} = emqx_ds_shared_sub_queue:declare(Group, Topic, Now, _StartTime = 0),
    [
        {queue_group, Group},
        {queue_topic, Topic},
        {queue, Queue}
        | Config
    ].

destroy_queue(Config) ->
    Group = proplists:get_value(queue_group, Config),
    Topic = proplists:get_value(queue_topic, Config),
    case Group of
        undefined -> ok;
        _ -> emqx_ds_shared_sub_queue:destroy(Group, Topic)
    end.

t_lease_initial('init', Config) ->
    declare_queue_if_needed(<<"gr1">>, <<"topic1/#">>, Config);
t_lease_initial('end', Config) ->
    destroy_queue(Config).

t_lease_initial(_Config) ->
    ConnShared = emqtt_connect_sub(<<"client_shared">>),
    {ok, _, [1]} = emqtt:subscribe(ConnShared, <<"$share/gr1/topic1/#">>, 1),

    ConnPub = emqtt_connect_pub(<<"client_pub">>),

    {ok, _} = emqtt:publish(ConnPub, <<"topic1/1">>, <<"hello1">>, 1),
    ct:sleep(2_000),
    {ok, _} = emqtt:publish(ConnPub, <<"topic1/2">>, <<"hello2">>, 1),

    ?assertReceive({publish, #{payload := <<"hello1">>}}, 10_000),
    ?assertReceive({publish, #{payload := <<"hello2">>}}, 10_000),

    ok = emqtt:disconnect(ConnShared),
    ok = emqtt:disconnect(ConnPub).

t_declare_triggers_persistence(groups, _Groups) ->
    [declare_explicit];
t_declare_triggers_persistence('init', Config) ->
    declare_queue(<<"dtp">>, <<"topic1/#">>, Config);
t_declare_triggers_persistence('end', Config) ->
    destroy_queue(Config).

t_declare_triggers_persistence(_Config) ->
    ConnPub = emqtt_connect_pub(<<"client_pub">>),

    {ok, _} = emqtt:publish(ConnPub, <<"topic1/1">>, <<"hello1">>, 1),
    {ok, _} = emqtt:publish(ConnPub, <<"topic1/2">>, <<"hello2">>, 1),
    {ok, _} = emqtt:publish(ConnPub, <<"topic2/1">>, <<"oops1">>, 1),
    {ok, _} = emqtt:publish(ConnPub, <<"topic1/42">>, <<"42">>, 1),
    {ok, _} = emqtt:publish(ConnPub, <<"topic3/1">>, <<"oops2">>, 1),

    ConnShared = emqtt_connect_sub(<<"client_shared">>),
    {ok, _, [1]} = emqtt:subscribe(ConnShared, <<"$share/dtp/topic1/#">>, 1),

    {ok, _} = emqtt:publish(ConnPub, <<"topic1/3">>, <<"hello3">>, 1),

    %% Messages published before `ConnShared` has subscribed should be sent.
    ?assertReceive({publish, #{payload := <<"hello1">>}}, 5_000),
    ?assertReceive({publish, #{payload := <<"hello2">>}}, 5_000),
    ?assertReceive({publish, #{payload := <<"42">>}}, 5_000),
    ?assertReceive({publish, #{payload := <<"hello3">>}}, 5_000),

    ok = emqtt:disconnect(ConnShared),
    ok = emqtt:disconnect(ConnPub).

t_destroy_queue_live_clients('init', Config) ->
    declare_queue_if_needed(<<"dqlc">>, <<"t1337/#">>, Config);
t_destroy_queue_live_clients('end', Config) ->
    destroy_queue(Config).

t_destroy_queue_live_clients(_Config) ->
    ConnPub = emqtt_connect_pub(<<"client_pub">>),

    ConnShared1 = emqtt_connect_sub(<<"client_shared1">>),
    ConnShared2 = emqtt_connect_sub(<<"client_shared2">>),
    {ok, _, [1]} = emqtt:subscribe(ConnShared1, <<"$share/dqlc/t1337/#">>, 1),
    {ok, _, [1]} = emqtt:subscribe(ConnShared2, <<"$share/dqlc/t1337/#">>, 1),

    {ok, _} = emqtt:publish(ConnPub, <<"t1337/1">>, <<"hello1">>, 1),
    {ok, _} = emqtt:publish(ConnPub, <<"t1337/2">>, <<"hello2">>, 1),
    {ok, _} = emqtt:publish(ConnPub, <<"t1337/3/4">>, <<"hello3">>, 1),
    {ok, _} = emqtt:publish(ConnPub, <<"t1337/5/6">>, <<"hello4">>, 1),

    ?assertReceive({publish, #{payload := <<"hello1">>}}, 2_000),
    ?assertReceive({publish, #{payload := <<"hello2">>}}, 2_000),
    ?assertReceive({publish, #{payload := <<"hello3">>}}, 2_000),
    ?assertReceive({publish, #{payload := <<"hello4">>}}, 2_000),

    ok = emqx_ds_shared_sub_queue:destroy(<<"dqlc">>, <<"t1337/#">>),

    %% No more publishes after the queue was destroyed.
    {ok, _} = emqtt:publish(ConnPub, <<"t1337/1">>, <<"hello5">>, 1),
    {ok, _} = emqtt:publish(ConnPub, <<"t1337/2">>, <<"hello6">>, 1),
    ?assertNotReceive({publish, #{payload := _}}, 2_000),

    ok = emqtt:disconnect(ConnShared1),
    ok = emqtt:disconnect(ConnShared2),
    ok = emqtt:disconnect(ConnPub).

t_two_clients('init', Config) ->
    declare_queue_if_needed(<<"gr4">>, <<"topic4/#">>, Config);
t_two_clients('end', Config) ->
    destroy_queue(Config).

t_two_clients(_Config) ->
    ConnShared1 = emqtt_connect_sub(<<"client_shared1">>),
    {ok, _, [1]} = emqtt:subscribe(ConnShared1, <<"$share/gr4/topic4/#">>, 1),

    ConnShared2 = emqtt_connect_sub(<<"client_shared2">>),
    {ok, _, [1]} = emqtt:subscribe(ConnShared2, <<"$share/gr4/topic4/#">>, 1),

    ConnPub = emqtt_connect_pub(<<"client_pub">>),

    {ok, _} = emqtt:publish(ConnPub, <<"topic4/1">>, <<"hello1">>, 1),
    {ok, _} = emqtt:publish(ConnPub, <<"topic4/2">>, <<"hello2">>, 1),
    ct:sleep(2_000),
    {ok, _} = emqtt:publish(ConnPub, <<"topic4/1">>, <<"hello3">>, 1),
    {ok, _} = emqtt:publish(ConnPub, <<"topic4/2">>, <<"hello4">>, 1),

    ?assertReceive({publish, #{payload := <<"hello1">>}}, 10_000),
    ?assertReceive({publish, #{payload := <<"hello2">>}}, 10_000),
    ?assertReceive({publish, #{payload := <<"hello3">>}}, 10_000),
    ?assertReceive({publish, #{payload := <<"hello4">>}}, 10_000),

    ok = emqtt:disconnect(ConnShared1),
    ok = emqtt:disconnect(ConnShared2),
    ok = emqtt:disconnect(ConnPub).

t_client_loss('init', Config) ->
    declare_queue_if_needed(<<"gr5">>, <<"topic5/#">>, Config);
t_client_loss('end', Config) ->
    destroy_queue(Config).

t_client_loss(_Config) ->
    ConnShared1 = emqtt_connect_sub(<<"client_shared1">>),
    {ok, _, [1]} = emqtt:subscribe(ConnShared1, <<"$share/gr5/topic5/#">>, 1),

    ConnShared2 = emqtt_connect_sub(<<"client_shared2">>),
    {ok, _, [1]} = emqtt:subscribe(ConnShared2, <<"$share/gr5/topic5/#">>, 1),

    ConnPub = emqtt_connect_pub(<<"client_pub">>),

    {ok, _} = emqtt:publish(ConnPub, <<"topic5/1">>, <<"hello1">>, 1),
    {ok, _} = emqtt:publish(ConnPub, <<"topic5/2">>, <<"hello2">>, 1),

    true = unlink(ConnShared1),
    exit(ConnShared1, kill),

    {ok, _} = emqtt:publish(ConnPub, <<"topic5/1">>, <<"hello3">>, 1),
    {ok, _} = emqtt:publish(ConnPub, <<"topic5/2">>, <<"hello4">>, 1),

    ?assertReceive({publish, #{payload := <<"hello3">>}}, 10_000),
    ?assertReceive({publish, #{payload := <<"hello4">>}}, 10_000),

    ok = emqtt:disconnect(ConnShared2),
    ok = emqtt:disconnect(ConnPub).

t_stream_revoke('init', Config) ->
    declare_queue_if_needed(<<"gr6">>, <<"topic6/#">>, Config);
t_stream_revoke('end', Config) ->
    destroy_queue(Config).

t_stream_revoke(_Config) ->
    ConnShared1 = emqtt_connect_sub(<<"client_shared1">>),
    {ok, _, [1]} = emqtt:subscribe(ConnShared1, <<"$share/gr6/topic6/#">>, 1),

    ConnPub = emqtt_connect_pub(<<"client_pub">>),

    {ok, _} = emqtt:publish(ConnPub, <<"topic6/1">>, <<"hello1">>, 1),
    {ok, _} = emqtt:publish(ConnPub, <<"topic6/2">>, <<"hello2">>, 1),

    ?assertReceive({publish, #{payload := <<"hello1">>}}, 10_000),
    ?assertReceive({publish, #{payload := <<"hello2">>}}, 10_000),

    ConnShared2 = emqtt_connect_sub(<<"client_shared2">>),

    ?assertWaitEvent(
        {ok, _, [1]} = emqtt:subscribe(ConnShared2, <<"$share/gr6/topic6/#">>, 1),
        #{
            ?snk_kind := ds_shared_sub_borrower_leader_grant,
            session_id := <<"client_shared2">>
        },
        5_000
    ),

    {ok, _} = emqtt:publish(ConnPub, <<"topic6/1">>, <<"hello3">>, 1),
    {ok, _} = emqtt:publish(ConnPub, <<"topic6/2">>, <<"hello4">>, 1),

    ?assertReceive({publish, #{payload := <<"hello3">>}}, 10_000),
    ?assertReceive({publish, #{payload := <<"hello4">>}}, 10_000),

    ok = emqtt:disconnect(ConnShared1),
    ok = emqtt:disconnect(ConnShared2),
    ok = emqtt:disconnect(ConnPub).

t_graceful_disconnect('init', Config) ->
    declare_queue_if_needed(<<"gr4">>, <<"topic7/#">>, Config);
t_graceful_disconnect('end', Config) ->
    destroy_queue(Config).

t_graceful_disconnect(_Config) ->
    ConnShared1 = emqtt_connect_sub(<<"client_shared1">>),
    {ok, _, [1]} = emqtt:subscribe(ConnShared1, <<"$share/gr4/topic7/#">>, 1),

    ConnShared2 = emqtt_connect_sub(<<"client_shared2">>),
    {ok, _, [1]} = emqtt:subscribe(ConnShared2, <<"$share/gr4/topic7/#">>, 1),

    ConnPub = emqtt_connect_pub(<<"client_pub">>),

    {ok, _} = emqtt:publish(ConnPub, <<"topic7/1">>, <<"hello1">>, 1),
    {ok, _} = emqtt:publish(ConnPub, <<"topic7/2">>, <<"hello2">>, 1),

    ?assertReceive({publish, #{payload := <<"hello1">>}}, 2_000),
    ?assertReceive({publish, #{payload := <<"hello2">>}}, 2_000),

    ?assertWaitEvent(
        ok = emqtt:disconnect(ConnShared1),
        #{?snk_kind := ds_shared_sub_leader_disconnect_borrower},
        1_000
    ),

    {ok, _} = emqtt:publish(ConnPub, <<"topic7/1">>, <<"hello3">>, 1),
    {ok, _} = emqtt:publish(ConnPub, <<"topic7/2">>, <<"hello4">>, 1),

    %% Since the disconnect is graceful, the streams should rebalance quickly,
    %% before the timeout.
    ?assertReceive({publish, #{payload := <<"hello3">>}}, 2_000),
    ?assertReceive({publish, #{payload := <<"hello4">>}}, 2_000),

    ok = emqtt:disconnect(ConnShared2),
    ok = emqtt:disconnect(ConnPub).

t_leader_state_preserved('init', Config) ->
    declare_queue_if_needed(<<"lsp">>, <<"topic42/#">>, Config);
t_leader_state_preserved('end', Config) ->
    destroy_queue(Config).

t_leader_state_preserved(_Config) ->
    ?check_trace(
        begin
            ConnShared1 = emqtt_connect_sub(<<"client1">>),
            {ok, _, [1]} = emqtt:subscribe(ConnShared1, <<"$share/lsp/topic42/#">>, 1),

            ConnShared2 = emqtt_connect_sub(<<"client2">>),
            {ok, _, [1]} = emqtt:subscribe(ConnShared2, <<"$share/lsp/topic42/#">>, 1),

            ConnPub = emqtt_connect_pub(<<"client_pub">>),

            {ok, _} = emqtt:publish(ConnPub, <<"topic42/1/2">>, <<"hello1">>, 1),
            {ok, _} = emqtt:publish(ConnPub, <<"topic42/3/4">>, <<"hello2">>, 1),
            ?assertReceive({publish, #{payload := <<"hello1">>}}, 2_000),
            ?assertReceive({publish, #{payload := <<"hello2">>}}, 2_000),

            ok = emqtt:disconnect(ConnShared1),
            ok = emqtt:disconnect(ConnShared2),

            %% Equivalent to node restart.
            ok = emqx_ds_shared_sub_registry:purge(),
            ok = timer:sleep(1_000),

            {ok, _} = emqtt:publish(ConnPub, <<"topic42/1/2">>, <<"hello3">>, 1),
            {ok, _} = emqtt:publish(ConnPub, <<"topic42/3/4">>, <<"hello4">>, 1),

            ConnShared3 = emqtt_connect_sub(<<"client3">>),
            {ok, _, [1]} = emqtt:subscribe(ConnShared3, <<"$share/lsp/topic42/#">>, 1),

            ?assertReceive({publish, #{payload := <<"hello3">>}}, 2_000),
            ?assertReceive({publish, #{payload := <<"hello4">>}}, 2_000),

            ok = emqtt:disconnect(ConnShared3),
            ok = emqtt:disconnect(ConnPub)
        end,
        []
    ).

t_intensive_reassign('init', Config) ->
    declare_queue_if_needed(<<"gr8">>, <<"topic8/#">>, Config);
t_intensive_reassign('end', Config) ->
    destroy_queue(Config).

t_intensive_reassign(_Config) ->
    ConnPub = emqtt_connect_pub(<<"client_pub">>),

    ConnShared1 = emqtt_connect_sub(<<"client_shared1">>),
    {ok, _, [1]} = emqtt:subscribe(ConnShared1, <<"$share/gr8/topic8/#">>, 1),

    ct:sleep(1000),

    NPubs = 10_000,

    Topics = [<<"topic8/1">>, <<"topic8/2">>, <<"topic8/3">>],
    ok = publish_n(ConnPub, Topics, 1, NPubs),

    Self = self(),
    _ = spawn_link(fun() ->
        ok = publish_n(ConnPub, Topics, NPubs + 1, 2 * NPubs),
        Self ! publish_done
    end),

    ConnShared2 = emqtt_connect_sub(<<"client_shared2">>),
    ConnShared3 = emqtt_connect_sub(<<"client_shared3">>),
    {ok, _, [1]} = emqtt:subscribe(ConnShared2, <<"$share/gr8/topic8/#">>, 1),
    {ok, _, [1]} = emqtt:subscribe(ConnShared3, <<"$share/gr8/topic8/#">>, 1),

    receive
        publish_done -> ok
    end,

    Pubs = drain_publishes(),

    ClientByBid = fun(Pid) ->
        case Pid of
            ConnShared1 -> <<"client_shared1">>;
            ConnShared2 -> <<"client_shared2">>;
            ConnShared3 -> <<"client_shared3">>
        end
    end,

    {Missing, Duplicate} = verify_received_pubs(Pubs, 2 * NPubs, ClientByBid),

    ?assertEqual([], Missing),
    ?assertEqual([], Duplicate),

    ok = emqtt:disconnect(ConnShared1),
    ok = emqtt:disconnect(ConnShared2),
    ok = emqtt:disconnect(ConnShared3),
    ok = emqtt:disconnect(ConnPub).

t_multiple_groups(groups, _Groups) ->
    [declare_explicit];
t_multiple_groups('init', Config) ->
    Now = emqx_message:timestamp_now(),
    NQueues = 50,
    Group = <<"multi">>,
    Topics = [emqx_utils:format("t/mg/~p", [I]) || I <- lists:seq(1, NQueues)],
    Queues = lists:map(
        fun(Topic) ->
            {ok, Queue} = emqx_ds_shared_sub_queue:declare(Group, wildcard(Topic), Now, 0),
            Queue
        end,
        Topics
    ),
    [
        {queue_group, Group},
        {queue_topics, Topics},
        {queues, Queues}
        | Config
    ];
t_multiple_groups('end', Config) ->
    Topics = proplists:get_value(queue_topics, Config),
    lists:foreach(
        fun(Topic) -> emqx_ds_shared_sub_queue:destroy(<<"multi">>, Topic) end,
        Topics
    ).

t_multiple_groups(Config) ->
    Topics = proplists:get_value(queue_topics, Config),
    NSubs = 20,
    NPubs = 1000,
    NQueues = length(Topics),
    ConnPub = emqtt_connect_pub(<<"t_multiple_groups:pub">>),
    ConnSubs = lists:map(
        fun(I) ->
            ClientId = emqx_utils:format("t_multiple_groups:sub:~p", [I]),
            ConnSub = emqtt_connect_sub(ClientId),
            ok = lists:foreach(
                fun(Ti) ->
                    Topic = lists:nth(Ti, Topics),
                    TopicSub = emqx_topic:join([<<"$share/multi">>, wildcard(Topic)]),
                    {ok, _, [1]} = emqtt:subscribe(ConnSub, TopicSub, 1)
                end,
                lists:seq(I, NQueues, NSubs)
            ),
            ConnSub
        end,
        lists:seq(1, NSubs)
    ),

    Payloads = lists:map(
        fun(Pi) ->
            Qi = pick_queue(Pi, NQueues),
            Payload = integer_to_binary(Pi),
            TopicPub = emqx_topic:join([lists:nth(Qi, Topics), integer_to_binary(Pi)]),
            {ok, _} = emqtt:publish(ConnPub, TopicPub, Payload, 1),
            Payload
        end,
        lists:seq(1, NPubs)
    ),

    Pubs = drain_publishes(),
    ?assertMatch(
        {[_ | _], []},
        lists:partition(fun(#{payload := P}) -> lists:member(P, Payloads) end, Pubs)
    ),

    lists:foreach(fun emqtt:disconnect/1, [ConnPub | ConnSubs]).

pick_queue(I, NQueues) ->
    %% NOTE: Allocate publishes to queues unevenly, but every queue is utilized.
    round(math:sqrt(NQueues) * math:log2(I)) rem NQueues + 1.

wildcard(Topic) ->
    emqx_topic:join([Topic, '#']).

t_unsubscribe('init', Config) ->
    declare_queue_if_needed(<<"gr9">>, <<"topic9/#">>, Config);
t_unsubscribe('end', Config) ->
    destroy_queue(Config).

t_unsubscribe(_Config) ->
    ConnPub = emqtt_connect_pub(<<"client_pub">>),

    ConnShared1 = emqtt_connect_sub(<<"client_shared1">>),
    {ok, _, [1]} = emqtt:subscribe(ConnShared1, <<"$share/gr9/topic9/#">>, 1),

    ct:sleep(1000),

    NPubs = 10_000,

    Topics = [<<"topic9/1">>, <<"topic9/2">>, <<"topic9/3">>],
    ok = publish_n(ConnPub, Topics, 1, NPubs),

    Self = self(),
    _ = spawn_link(fun() ->
        ok = publish_n(ConnPub, Topics, NPubs + 1, 2 * NPubs),
        Self ! publish_done
    end),

    ConnShared2 = emqtt_connect_sub(<<"client_shared2">>),
    {ok, _, [1]} = emqtt:subscribe(ConnShared2, <<"$share/gr9/topic9/#">>, 1),
    {ok, _, _} = emqtt:unsubscribe(ConnShared1, <<"$share/gr9/topic9/#">>),

    receive
        publish_done -> ok
    end,

    Pubs = drain_publishes(),

    ClientByBid = fun(Pid) ->
        case Pid of
            ConnShared1 -> <<"client_shared1">>;
            ConnShared2 -> <<"client_shared2">>
        end
    end,

    {Missing, Duplicate} = verify_received_pubs(Pubs, 2 * NPubs, ClientByBid),

    ?assertEqual([], Missing),
    ?assertEqual([], Duplicate),

    ok = emqtt:disconnect(ConnShared1),
    ok = emqtt:disconnect(ConnShared2),
    ok = emqtt:disconnect(ConnPub).

t_quick_resubscribe('init', Config) ->
    declare_queue_if_needed(<<"gr10">>, <<"topic10/#">>, Config);
t_quick_resubscribe('end', Config) ->
    destroy_queue(Config).

t_quick_resubscribe(_Config) ->
    ConnPub = emqtt_connect_pub(<<"client_pub">>),

    ConnShared1 = emqtt_connect_sub(<<"client_shared1">>),
    {ok, _, [1]} = emqtt:subscribe(ConnShared1, <<"$share/gr10/topic10/#">>, 1),

    ct:sleep(1000),

    NPubs = 10_000,

    Topics = [<<"topic10/1">>, <<"topic10/2">>, <<"topic10/3">>],
    ok = publish_n(ConnPub, Topics, 1, NPubs),

    Self = self(),
    _ = spawn_link(fun() ->
        ok = publish_n(ConnPub, Topics, NPubs + 1, 2 * NPubs),
        Self ! publish_done
    end),

    ConnShared2 = emqtt_connect_sub(<<"client_shared2">>),
    {ok, _, [1]} = emqtt:subscribe(ConnShared2, <<"$share/gr10/topic10/#">>, 1),
    ok = lists:foreach(
        fun(_) ->
            {ok, _, _} = emqtt:unsubscribe(ConnShared1, <<"$share/gr10/topic10/#">>),
            {ok, _, [1]} = emqtt:subscribe(ConnShared1, <<"$share/gr10/topic10/#">>, 1),
            ct:sleep(5)
        end,
        lists:seq(1, 10)
    ),

    receive
        publish_done -> ok
    end,

    Pubs = drain_publishes(),

    ClientByBid = fun(Pid) ->
        case Pid of
            ConnShared1 -> <<"client_shared1">>;
            ConnShared2 -> <<"client_shared2">>
        end
    end,

    {Missing, Duplicate} = verify_received_pubs(Pubs, 2 * NPubs, ClientByBid),

    ?assertEqual([], Missing),
    ?assertEqual([], Duplicate),

    ok = emqtt:disconnect(ConnShared1),
    ok = emqtt:disconnect(ConnShared2),
    ok = emqtt:disconnect(ConnPub).

t_disconnect_no_double_replay1('init', Config) ->
    declare_queue_if_needed(<<"gr11">>, <<"topic11/#">>, Config);
t_disconnect_no_double_replay1('end', Config) ->
    destroy_queue(Config).

t_disconnect_no_double_replay1(_Config) ->
    ConnPub = emqtt_connect_pub(<<"client_pub">>),

    ConnShared1 = emqtt_connect_sub(<<"client_shared1">>),
    {ok, _, [1]} = emqtt:subscribe(ConnShared1, <<"$share/gr11/topic11/#">>, 1),

    ConnShared2 = emqtt_connect_sub(<<"client_shared2">>),
    {ok, _, [1]} = emqtt:subscribe(ConnShared2, <<"$share/gr11/topic11/#">>, 1),

    ct:sleep(1000),

    NPubs = 10_000,

    Topics = [<<"topic11/1">>, <<"topic11/2">>, <<"topic11/3">>],
    ok = publish_n(ConnPub, Topics, 1, NPubs),

    Self = self(),
    _ = spawn_link(fun() ->
        ok = publish_n(ConnPub, Topics, NPubs + 1, 2 * NPubs),
        Self ! publish_done
    end),

    ok = emqtt:disconnect(ConnShared2),

    receive
        publish_done -> ok
    end,

    Pubs = drain_publishes(),

    ClientByBid = fun(Pid) ->
        case Pid of
            ConnShared1 -> <<"client_shared1">>;
            ConnShared2 -> <<"client_shared2">>
        end
    end,

    {Missing, _Duplicate} = verify_received_pubs(Pubs, 2 * NPubs, ClientByBid),

    ?assertEqual([], Missing),

    %% We cannnot garantee that the message are not duplicated until we are able
    %% to send progress of a partially replayed stream range to the leader.
    % ?assertEqual([], Duplicate),

    ok = emqtt:disconnect(ConnShared1),
    ok = emqtt:disconnect(ConnPub).

t_disconnect_no_double_replay2('init', Config) ->
    declare_queue_if_needed(<<"gr12">>, <<"topic12/#">>, Config);
t_disconnect_no_double_replay2('end', Config) ->
    destroy_queue(Config).

t_disconnect_no_double_replay2(_Config) ->
    ConnPub = emqtt_connect_pub(<<"client_pub">>),

    ConnShared1 = emqtt_connect_sub(<<"client_shared1">>, [{auto_ack, false}]),
    {ok, _, [1]} = emqtt:subscribe(ConnShared1, <<"$share/gr12/topic12/#">>, 1),

    ct:sleep(1000),

    ok = publish_n(ConnPub, [<<"topic12/1">>], 1, 20),

    receive
        {publish, #{payload := <<"1">>, packet_id := PacketId1}} ->
            ok = emqtt:puback(ConnShared1, PacketId1)
    after 5000 ->
        ct:fail("No publish received")
    end,

    ok = emqtt:disconnect(ConnShared1),

    ConnShared12 = emqtt_connect_sub(<<"client_shared12">>),
    {ok, _, [1]} = emqtt:subscribe(ConnShared12, <<"$share/gr12/topic12/#">>, 1),

    %% We cannnot garantee that the message is not duplicated until we are able
    %% to send progress of a partially replayed stream range to the leader.
    % ?assertNotReceive(
    %     {publish, #{payload := <<"1">>}},
    %     3000
    % ),

    ok = emqtt:disconnect(ConnShared12),
    ok = emqtt:disconnect(ConnPub).

t_lease_reconnect('init', Config) ->
    declare_queue_if_needed(<<"gr2">>, <<"topic2/#">>, Config);
t_lease_reconnect('end', Config) ->
    meck:unload(),
    destroy_queue(Config).

t_lease_reconnect(_Config) ->
    ConnPub = emqtt_connect_pub(<<"client_pub">>),

    ConnShared = emqtt_connect_sub(<<"client_shared">>),

    %% Simulate unability to find leader.
    ok = meck:expect(
        emqx_ds_shared_sub_store,
        claim_leadership,
        fun(_, _, _) -> {error, recoverable, {mocked, ?MODULE}} end
    ),

    ?assertWaitEvent(
        {ok, _, [1]} = emqtt:subscribe(ConnShared, <<"$share/gr2/topic2/#">>, 1),
        #{?snk_kind := ds_shared_sub_borrower_find_leader_timeout},
        5_000
    ),

    %% Agent should retry after some time and find the leader.
    ?assertWaitEvent(
        ok = meck:unload(emqx_ds_shared_sub_store),
        #{?snk_kind := ds_shared_sub_leader_borrower_connect},
        5_000
    ),

    {ok, _} = emqtt:publish(ConnPub, <<"topic2/2">>, <<"hello2">>, 1),

    ?assertReceive({publish, #{payload := <<"hello2">>}}, 10_000),

    ok = emqtt:disconnect(ConnShared),
    ok = emqtt:disconnect(ConnPub).

t_renew_lease_timeout('init', Config) ->
    declare_queue_if_needed(<<"gr3">>, <<"topic3/#">>, Config);
t_renew_lease_timeout('end', Config) ->
    destroy_queue(Config).

t_renew_lease_timeout(_Config) ->
    ConnShared = emqtt_connect_sub(<<"client_shared">>),

    ?assertWaitEvent(
        {ok, _, [1]} = emqtt:subscribe(ConnShared, <<"$share/gr3/topic3/#">>, 1),
        #{?snk_kind := ds_shared_sub_leader_borrower_connect},
        5_000
    ),

    ?check_trace(
        ?wait_async_action(
            ok = emqx_ds_shared_sub_registry:purge(),
            #{?snk_kind := ds_shared_sub_leader_borrower_connect},
            10_000
        ),
        fun(Trace) ->
            ?strict_causality(
                #{?snk_kind := ds_shared_sub_borrower_ping_leader_timeout},
                #{?snk_kind := ds_shared_sub_leader_borrower_connect},
                Trace
            )
        end
    ),

    ok = emqtt:disconnect(ConnShared).

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

emqtt_connect_sub(ClientId) ->
    emqtt_connect_sub(ClientId, []).

emqtt_connect_sub(ClientId, Options) ->
    {ok, C} = emqtt:start_link(
        [
            {clientid, ClientId},
            {clean_start, true},
            {proto_ver, v5},
            {properties, #{'Session-Expiry-Interval' => 7_200}}
        ] ++ Options
    ),
    {ok, _} = emqtt:connect(C),
    C.

emqtt_connect_pub(ClientId) ->
    {ok, C} = emqtt:start_link([
        {clientid, ClientId},
        {clean_start, true},
        {proto_ver, v5}
    ]),
    {ok, _} = emqtt:connect(C),
    C.

publish_n(_Conn, _Topics, From, To) when From > To ->
    ok;
publish_n(Conn, [Topic | RestTopics], From, To) ->
    {ok, _} = emqtt:publish(Conn, Topic, integer_to_binary(From), 1),
    publish_n(Conn, RestTopics ++ [Topic], From + 1, To).

drain_publishes() ->
    drain_publishes([]).

drain_publishes(Acc) ->
    receive
        {publish, Msg} ->
            drain_publishes([Msg | Acc])
    after 5_000 ->
        lists:reverse(Acc)
    end.

verify_received_pubs(Pubs, NPubs, ClientByBid) ->
    Messages = lists:foldl(
        fun(#{payload := Payload, client_pid := Pid}, Acc) ->
            maps:update_with(
                Payload,
                fun(Clients) ->
                    [ClientByBid(Pid) | Clients]
                end,
                [ClientByBid(Pid)],
                Acc
            )
        end,
        #{},
        Pubs
    ),
    Expected = [integer_to_binary(N) || N <- lists:seq(1, NPubs)],

    Missing = lists:filter(
        fun(NBin) -> not maps:is_key(NBin, Messages) end,
        Expected
    ),
    Duplicate = lists:filtermap(
        fun(NBin) ->
            case Messages of
                #{NBin := [_]} -> false;
                #{NBin := [_ | _] = Clients} -> {true, {NBin, Clients}};
                _ -> false
            end
        end,
        Expected
    ),

    {Missing, Duplicate}.
