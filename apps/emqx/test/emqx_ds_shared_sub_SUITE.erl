%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/asserts.hrl").
-include("../src/emqx_ds_shared_sub/emqx_ds_shared_sub_format.hrl").

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

init_per_group(GroupName, Config) ->
    [{queue_need_declare, GroupName =:= declare_explicit} | Config].

end_per_group(_GroupName, _Config) ->
    ok.

init_per_testcase(TC, Config) ->
    ok = snabbkaffe:start_trace(),
    emqx_common_test_helpers:init_per_testcase(?MODULE, TC, Config).

end_per_testcase(TC, Config) ->
    ok = snabbkaffe:stop(),
    emqx_common_test_helpers:run_cleanups(
        emqx_common_test_helpers:end_per_testcase(?MODULE, TC, Config)
    ).

declare_group_if_needed(Group, Topic, Config) ->
    case proplists:get_value(queue_need_declare, Config) of
        true -> declare_group(Group, Topic, Config);
        false -> Config
    end.

declare_group(Group, Topic, Config) ->
    {ok, Queue} = emqx_ds_shared_sub:declare(Group, Topic, #{start_time => 0}),
    [
        {queue_group, Group},
        {queue_topic, Topic},
        {queue, Queue}
        | Config
    ].

destroy_group(Config) ->
    Group = proplists:get_value(queue_group, Config),
    Topic = proplists:get_value(queue_topic, Config),
    case Group of
        undefined -> ok;
        _ -> emqx_ds_shared_sub:destroy(Group, Topic)
    end.

t_lease_initial('init', Config) ->
    declare_group_if_needed(<<"gr1">>, <<"topic1/#">>, start_local(Config));
t_lease_initial('end', Config) ->
    destroy_group(Config).
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
    declare_group(<<"dtp">>, <<"topic1/#">>, start_local(Config));
t_declare_triggers_persistence('end', Config) ->
    destroy_group(Config).

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
    declare_group_if_needed(<<"dqlc">>, <<"t/#">>, start_local(Config));
t_destroy_queue_live_clients('end', Config) ->
    destroy_group(Config).

t_destroy_queue_live_clients(_Config) ->
    ConnPub = emqtt_connect_pub(<<"client_pub">>),

    ConnShared1 = emqtt_connect_sub(<<"client_shared1">>),
    ConnShared2 = emqtt_connect_sub(<<"client_shared2">>),
    {ok, _, [1]} = emqtt:subscribe(ConnShared1, <<"$share/dqlc/t/#">>, 1),
    {ok, _, [1]} = emqtt:subscribe(ConnShared2, <<"$share/dqlc/t/#">>, 1),

    {ok, _} = emqtt:publish(ConnPub, <<"t/1">>, <<"hello1">>, 1),
    {ok, _} = emqtt:publish(ConnPub, <<"t/2">>, <<"hello2">>, 1),
    {ok, _} = emqtt:publish(ConnPub, <<"t/3/4">>, <<"hello3">>, 1),
    {ok, _} = emqtt:publish(ConnPub, <<"t/5/6">>, <<"hello4">>, 1),

    ?assertReceive({publish, #{payload := <<"hello1">>}}, 2_000),
    ?assertReceive({publish, #{payload := <<"hello2">>}}, 2_000),
    ?assertReceive({publish, #{payload := <<"hello3">>}}, 2_000),
    ?assertReceive({publish, #{payload := <<"hello4">>}}, 2_000),

    ?assertMatch(true, emqx_ds_shared_sub:destroy(<<"dqlc">>, <<"t/#">>)),

    %% No more publishes after the queue was destroyed.
    {ok, _} = emqtt:publish(ConnPub, <<"t/1">>, <<"hello5">>, 1),
    {ok, _} = emqtt:publish(ConnPub, <<"t/2">>, <<"hello6">>, 1),
    ?assertNotReceive({publish, #{payload := _}}, 2_000),

    ok = emqtt:disconnect(ConnShared1),
    ok = emqtt:disconnect(ConnShared2),
    ok = emqtt:disconnect(ConnPub).

t_two_clients('init', Config) ->
    declare_group_if_needed(<<"gr4">>, <<"topic4/#">>, start_local(Config));
t_two_clients('end', Config) ->
    destroy_group(Config).

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
    declare_group_if_needed(<<"gr5">>, <<"topic5/#">>, start_local(Config));
t_client_loss('end', Config) ->
    destroy_group(Config).

t_client_loss(_Config) ->
    ?check_trace(
        #{timetrap => 30_000},
        begin
            ConnShared1 = emqtt_connect_sub(<<"client_shared1">>),
            {ok, _, [1]} = emqtt:subscribe(ConnShared1, <<"$share/gr5/topic5/#">>, 1),

            ConnShared2 = emqtt_connect_sub(<<"client_shared2">>),
            {ok, _, [1]} = emqtt:subscribe(ConnShared2, <<"$share/gr5/topic5/#">>, 1),

            ConnPub = emqtt_connect_pub(<<"client_pub">>),

            {ok, _} = emqtt:publish(ConnPub, <<"topic5/1">>, <<"hello1">>, 1),
            {ok, _} = emqtt:publish(ConnPub, <<"topic5/2">>, <<"hello2">>, 1),

            ?tp(test_kill_shared1, #{}),
            true = unlink(ConnShared1),
            exit(ConnShared1, kill),

            {ok, _} = emqtt:publish(ConnPub, <<"topic5/1">>, <<"hello3">>, 1),
            {ok, _} = emqtt:publish(ConnPub, <<"topic5/2">>, <<"hello4">>, 1),

            ?assertReceive({publish, #{payload := <<"hello3">>}}, 10_000),
            ?assertReceive({publish, #{payload := <<"hello4">>}}, 10_000),

            ok = emqtt:disconnect(ConnShared2),
            ok = emqtt:disconnect(ConnPub)
        end,
        []
    ).

t_stream_revoke('init', Config) ->
    declare_group_if_needed(<<"gr6">>, <<"topic6/#">>, start_local(Config));
t_stream_revoke('end', Config) ->
    destroy_group(Config).

%% This testcase verifies stream revokation during rebalancing action.
t_stream_revoke(_Config) ->
    ?check_trace(
        #{timetrap => 30_000},
        begin
            CIDSub1 = <<"shared1">>,
            CIDSub2 = <<"shared2">>,
            CIDPub1 = <<"client_pub1">>,
            CIDPub2 = <<"client_pub2">>,
            %% Precondition:
            ?assert(
                emqx_ds:shard_of(messages, CIDSub1) =/= emqx_ds:shard_of(messages, CIDSub2) andalso
                    emqx_ds:shard_of(messages, CIDPub1) =/= emqx_ds:shard_of(messages, CIDPub2),
                """
                This test uses `shard' strategy of stream allocation.
                So it's an important precondition that shards of subscribers and publishers are different.
                """
            ),
            %% Connect the first subscriber:
            ConnShared1 = emqtt_connect_sub(CIDSub1),
            {ok, _, [1]} = emqtt:subscribe(ConnShared1, <<"$share/gr6/topic6/#">>, 1),

            %% Prepare the system by publishing messages to 2
            %% different shards, it should create two distinct
            %% streams:
            ConnPub1 = emqtt_connect_pub(CIDPub1),
            ConnPub2 = emqtt_connect_pub(CIDPub2),
            {ok, _} = emqtt:publish(ConnPub1, <<"topic6/1">>, <<"hello1">>, 1),
            {ok, _} = emqtt:publish(ConnPub2, <<"topic6/2">>, <<"hello2">>, 1),

            %% The first client, that is currently the sole group
            %% member, receives both messages:
            ?assertReceive({publish, #{payload := <<"hello1">>}}, 10_000),
            ?assertReceive({publish, #{payload := <<"hello2">>}}, 10_000),

            %% Now connect the second client, it should steal one of
            %% the streams:
            ConnShared2 = emqtt_connect_sub(CIDSub2),

            ?wait_async_action(
                {ok, _, [1]} = emqtt:subscribe(ConnShared2, <<"$share/gr6/topic6/#">>, 1),
                #{
                    ?snk_kind := ds_shared_sub_borrower_leader_grant,
                    session_id := CIDSub2
                }
            ),

            %% Publish more messages to both streams, messages should
            %% be still received, once:
            {ok, _} = emqtt:publish(ConnPub1, <<"topic6/1">>, <<"hello3">>, 1),
            {ok, _} = emqtt:publish(ConnPub2, <<"topic6/2">>, <<"hello4">>, 1),

            ?assertReceive({publish, #{payload := <<"hello3">>}}, 10_000),
            ?assertReceive({publish, #{payload := <<"hello4">>}}, 10_000),

            ok = emqtt:disconnect(ConnShared1),
            ok = emqtt:disconnect(ConnShared2),
            ok = emqtt:disconnect(ConnPub1),
            ok = emqtt:disconnect(ConnPub2)
        end,
        []
    ).

t_graceful_disconnect('init', Config) ->
    declare_group_if_needed(<<"gr4">>, <<"topic7/#">>, start_local(Config));
t_graceful_disconnect('end', Config) ->
    destroy_group(Config).

t_graceful_disconnect(_Config) ->
    ?check_trace(
        #{timetrap => 30_000},
        begin
            ConnShared1 = emqtt_connect_sub(<<"client_shared1">>),
            {ok, _, [1]} = emqtt:subscribe(ConnShared1, <<"$share/gr4/topic7/#">>, 1),

            ConnShared2 = emqtt_connect_sub(<<"client_shared2">>),
            {ok, _, [1]} = emqtt:subscribe(ConnShared2, <<"$share/gr4/topic7/#">>, 1),

            ConnPub = emqtt_connect_pub(<<"client_pub">>),

            {ok, _} = emqtt:publish(ConnPub, <<"topic7/1">>, <<"hello1">>, 1),
            {ok, _} = emqtt:publish(ConnPub, <<"topic7/2">>, <<"hello2">>, 1),

            ?assertReceive({publish, #{payload := <<"hello1">>}}, 2_000),
            ?assertReceive({publish, #{payload := <<"hello2">>}}, 2_000),

            ?tp(test_disconnect, #{}),
            ?assertWaitEvent(
                ok = emqtt:disconnect(ConnShared1),
                #{?snk_kind := ?tp_leader_disconnect_borrower},
                1_000
            ),

            ?tp(test_publish2, #{}),
            {ok, _} = emqtt:publish(ConnPub, <<"topic7/1">>, <<"hello3">>, 1),
            {ok, _} = emqtt:publish(ConnPub, <<"topic7/2">>, <<"hello4">>, 1),

            %% Since the disconnect is graceful, the streams should rebalance quickly,
            %% before the timeout.
            ?assertReceive({publish, #{payload := <<"hello3">>}}, 2_000),
            ?assertReceive({publish, #{payload := <<"hello4">>}}, 2_000),

            ok = emqtt:disconnect(ConnShared2),
            ok = emqtt:disconnect(ConnPub)
        end,
        []
    ).

t_intensive_reassign('init', Config) ->
    declare_group_if_needed(<<"gr8">>, <<"topic8/#">>, start_local(Config));
t_intensive_reassign('end', Config) ->
    destroy_group(Config).

t_intensive_reassign(_Config) ->
    ?check_trace(
        begin
            ConnPub = emqtt_connect_pub(<<"client_pub">>),

            ConnShared1 = emqtt_connect_sub(<<"client_shared1">>),
            {ok, _, [1]} = emqtt:subscribe(ConnShared1, <<"$share/gr8/topic8/#">>, 1),

            ct:sleep(1000),

            NPubs = 100,

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

            snabbkaffe_diff:assert_lists_eq(
                [],
                Missing,
                #{comment => "Missing"}
            ),
            snabbkaffe_diff:assert_lists_eq(
                [],
                Duplicate,
                #{comment => "Duplicates"}
            ),

            ok = emqtt:disconnect(ConnShared1),
            ok = emqtt:disconnect(ConnShared2),
            ok = emqtt:disconnect(ConnShared3),
            ok = emqtt:disconnect(ConnPub)
        end,
        []
    ).

t_multiple_groups(groups, _Groups) ->
    [declare_explicit];
t_multiple_groups('init', Config0) ->
    Config = start_local(Config0),
    NQueues = 50,
    Group = <<"multi">>,
    Topics = [emqx_utils:format("t/mg/~p", [I]) || I <- lists:seq(1, NQueues)],
    Queues = lists:map(
        fun(Topic) ->
            {ok, Queue} = emqx_ds_shared_sub:declare(Group, wildcard(Topic), #{start_time => 0}),
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
        fun(Topic) -> emqx_ds_shared_sub:destroy(<<"multi">>, Topic) end,
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
    declare_group_if_needed(<<"gr9">>, <<"topic9/#">>, start_local(Config));
t_unsubscribe('end', Config) ->
    destroy_group(Config).

t_unsubscribe(_Config) ->
    ConnPub = emqtt_connect_pub(<<"client_pub">>),

    ConnShared1 = emqtt_connect_sub(<<"client_shared1">>),
    {ok, _, [1]} = emqtt:subscribe(ConnShared1, <<"$share/gr9/topic9/#">>, 1),

    ct:sleep(1000),

    NPubs = 100,

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

    snabbkaffe_diff:assert_lists_eq(
        [],
        Missing,
        #{comment => "Missing"}
    ),
    snabbkaffe_diff:assert_lists_eq(
        [],
        Duplicate,
        #{comment => "Duplicates"}
    ),

    ok = emqtt:disconnect(ConnShared1),
    ok = emqtt:disconnect(ConnShared2),
    ok = emqtt:disconnect(ConnPub).

t_quick_resubscribe('init', Config) ->
    declare_group_if_needed(<<"gr10">>, <<"topic10/#">>, start_local(Config));
t_quick_resubscribe('end', Config) ->
    destroy_group(Config).

t_quick_resubscribe(_Config) ->
    ConnPub = emqtt_connect_pub(<<"client_pub">>),

    ConnShared1 = emqtt_connect_sub(<<"client_shared1">>),
    {ok, _, [1]} = emqtt:subscribe(ConnShared1, <<"$share/gr10/topic10/#">>, 1),

    ct:sleep(1000),

    NPubs = 100,

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

    snabbkaffe_diff:assert_lists_eq(
        [],
        Missing,
        #{comment => "Missing"}
    ),
    snabbkaffe_diff:assert_lists_eq(
        [],
        Duplicate,
        #{comment => "Duplicate"}
    ),

    ok = emqtt:disconnect(ConnShared1),
    ok = emqtt:disconnect(ConnShared2),
    ok = emqtt:disconnect(ConnPub).

t_disconnect_no_double_replay1('init', Config) ->
    declare_group_if_needed(<<"gr11">>, <<"topic11/#">>, start_local(Config));
t_disconnect_no_double_replay1('end', Config) ->
    destroy_group(Config).

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
    declare_group_if_needed(<<"gr12">>, <<"topic12/#">>, start_local(Config));
t_disconnect_no_double_replay2('end', Config) ->
    destroy_group(Config).

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
    declare_group_if_needed(<<"gr2">>, <<"topic2/#">>, start_local(Config));
t_lease_reconnect('end', Config) ->
    meck:unload(),
    destroy_group(Config).

t_lease_reconnect(_Config) ->
    ?check_trace(
        #{timetrap => 30_000},
        begin
            ConnPub = emqtt_connect_pub(<<"client_pub">>),

            ConnShared = emqtt_connect_sub(<<"client_shared">>),

            %% Simulate inability to find leader.
            ok = meck:expect(
                emqx_ds_shared_sub_registry,
                leader_wanted,
                fun(_, _) -> ok end
            ),

            ?assertWaitEvent(
                {ok, _, [1]} = emqtt:subscribe(ConnShared, <<"$share/gr2/topic2/#">>, 1),
                #{?snk_kind := ds_shared_sub_borrower_find_leader_timeout},
                5_000
            ),

            %% Agent should retry after some time and find the leader.
            ?assertWaitEvent(
                ok = meck:unload(emqx_ds_shared_sub_registry),
                #{?snk_kind := ?tp_leader_borrower_connect},
                5_000
            ),

            {ok, _} = emqtt:publish(ConnPub, <<"topic2/2">>, <<"hello2">>, 1),

            ?assertReceive({publish, #{payload := <<"hello2">>}}, 10_000),

            ok = emqtt:disconnect(ConnShared),
            ok = emqtt:disconnect(ConnPub)
        end,
        []
    ).

t_renew_lease_timeout('init', Config) ->
    declare_group_if_needed(<<"gr3">>, <<"topic3/#">>, start_local(Config));
t_renew_lease_timeout('end', Config) ->
    destroy_group(Config).

%% Verify that borrower reconnects if the leader doesn't respond to
%% pings for a long time.
t_renew_lease_timeout(_Config) ->
    ?check_trace(
        #{timetrap => 20_000},
        begin
            ConnShared = emqtt_connect_sub(<<"client_shared">>),

            ?wait_async_action(
                {ok, _, [1]} = emqtt:subscribe(ConnShared, <<"$share/gr3/topic3/#">>, 1),
                #{?snk_kind := ?tp_leader_borrower_connect}
            ),

            ?tp(info, test_leader_shutdown, #{}),
            ?wait_async_action(
                begin
                    Share = #share{group = <<"gr3">>, topic = <<"topic3/#">>},
                    {ok, Leader} = emqx_ds_shared_sub_registry:get_leader_sync(Share, #{}),
                    erlang:exit(Leader, shutdown)
                end,
                #{?snk_kind := ?tp_leader_borrower_connect}
            ),

            ok = emqtt:disconnect(ConnShared)
        end,
        []
    ).

t_leader_election(groups, _Groups) ->
    [declare_implicit];
t_leader_election('init', Config) ->
    start_cluster(Config);
t_leader_election('end', Config) ->
    Config.

t_leader_election(Config) ->
    [N1, N2, N3] = Nodes = proplists:get_value(cluster_nodes, Config),
    ?check_trace(
        #{timetrap => 60_000},
        begin
            ?tp(notice, test_start_clients, #{}),
            C1 = emqtt_connect_sub(<<"client_shared1">>, [{port, get_mqtt_port(N1)}]),
            C2 = emqtt_connect_sub(<<"client_shared2">>, [{port, get_mqtt_port(N2)}]),
            C3 = emqtt_connect_sub(<<"client_shared3">>, [{port, get_mqtt_port(N3)}]),
            %% 1. Subscribe clients sequentially. The first node
            %% becomes the leader:
            ?tp(notice, test_sub_1, #{}),
            ?assertMatch(
                {_, {ok, #{?snk_meta := #{node := N1}}}},
                ?wait_async_action(
                    emqtt:subscribe(C1, <<"$share/g1/t1">>, qos1),
                    #{
                        ?snk_kind := ds_shared_sub_become_leader,
                        group := <<"g1">>,
                        topic := <<"t1">>
                    }
                )
            ),
            ?assertMatch(
                {_, {ok, #{?snk_meta := #{node := N2}}}},
                ?wait_async_action(
                    emqtt:subscribe(C2, <<"$share/g1/t1">>, qos1),
                    #{
                        ?snk_kind := ds_shared_sub_become_standby,
                        group := <<"g1">>,
                        topic := <<"t1">>
                    }
                )
            ),
            ?assertMatch(
                {_, {ok, #{?snk_meta := #{node := N3}}}},
                ?wait_async_action(
                    emqtt:subscribe(C3, <<"$share/g1/t1">>, qos1),
                    #{
                        ?snk_kind := ds_shared_sub_become_standby,
                        group := <<"g1">>,
                        topic := <<"t1">>
                    }
                )
            ),
            %% 2. Subscribe simultaneously, creating a contention:
            ?tp(notice, test_sub_2, #{}),
            ?force_ordering(
                #{?snk_kind := test_trigger_sub2},
                #{?snk_kind := ds_shared_sub_become_candidate}
            ),
            %% Create candidates on all nodes except N1, which will be
            %% killed later. We ignore it here to simplify trace specs:
            _ = emqtt:subscribe(C2, <<"$share/g2/t2">>, qos1),
            _ = emqtt:subscribe(C3, <<"$share/g2/t2">>, qos1),
            %% Let candidates compete for the leadership:
            ?tp(notice, test_trigger_sub2, #{}),
            ?block_until(
                #{
                    ?snk_kind := ds_shared_sub_become_leader,
                    group := <<"g2">>,
                    topic := <<"t2">>
                }
            ),
            %% 3. Trigger re-election for "$share/g1/t1" by stopping
            %% N1 (where we know the leader is located):
            ?tp(notice, test_trigger_reelection, #{}),
            unlink(C1),
            ?wait_async_action(
                emqx_cth_peer:stop(N1),
                #{
                    ?snk_kind := ds_shared_sub_become_leader,
                    group := <<"g1">>,
                    topic := <<"t1">>
                }
            ),
            %% Sleep some more to catch unexpected events in the trace:
            ct:sleep(5000),
            Nodes
        end,
        [
            {"leader election for g1 happens only when we expect", fun(Trace) ->
                ?assert(
                    ?strict_causality(
                        #{?snk_kind := K} when K =:= test_sub_1; K =:= test_trigger_reelection,
                        #{?snk_kind := ds_shared_sub_become_leader, group := <<"g1">>},
                        Trace
                    )
                )
            end},
            {"leader election for g2 happens only when we expect", fun(Trace) ->
                ?assert(
                    ?strict_causality(
                        #{?snk_kind := test_trigger_sub2},
                        #{?snk_kind := ds_shared_sub_become_leader, group := <<"g2">>},
                        Trace
                    )
                )
            end},
            {"g2 leader election happens on all nodes", fun([_, N2, N3], Trace) ->
                ?assertEqual(
                    [N2, N3],
                    lists:usort([
                        Node
                     || #{
                            ?snk_kind := ds_shared_sub_become_candidate,
                            group := <<"g2">>,
                            ?snk_meta := #{node := Node}
                        } <- Trace
                    ])
                )
            end},
            {"state transitions during g2 election", fun(Trace) ->
                %% Note: here we ignore N1, since we kill it during the test
                ?assert(
                    ?strict_causality(
                        #{
                            ?snk_kind := ds_shared_sub_become_candidate,
                            group := <<"g2">>,
                            ?snk_meta := #{node := N}
                        },
                        #{
                            ?snk_kind := K,
                            group := <<"g2">>,
                            ?snk_meta := #{node := N}
                        } when
                            K =:= ds_shared_sub_become_leader orelse
                                K =:= ds_shared_sub_become_standby,
                        Trace
                    )
                )
            end}
        ]
    ).

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

emqtt_connect_sub(ClientId) ->
    emqtt_connect_sub(ClientId, []).

emqtt_connect_sub(ClientId, Options) ->
    ?tp(test_connect_client, #{clientid => ClientId}),
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

get_mqtt_port(Node) ->
    {_IP, Port} = erpc:call(Node, emqx_config, get, [[listeners, tcp, default, bind]]),
    Port.

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

start_local(Config) ->
    SessionOpts = #{
        <<"shared_subs">> =>
            #{
                <<"heartbeat_interval">> => 100,
                <<"realloc_interval">> => 100,
                <<"leader_timeout">> => 100,
                <<"checkpoint_interval">> => 10,
                <<"revocation_timeout">> => 1000
            }
    },
    clean_local(
        emqx_common_test_helpers:start_apps_ds(Config, [], #{durable_sessions_opts => SessionOpts})
    ).

clean_local(Config) ->
    [
        {cleanup, fun() ->
            ok = emqx_ds_shared_sub_registry:purge(),
            emqx_common_test_helpers:drop_all_ds_messages()
        end}
        | Config
    ].

start_cluster(Config) ->
    Conf = #{
        <<"durable_sessions">> => #{
            <<"shared_subs">> =>
                #{
                    <<"heartbeat_interval">> => 100,
                    <<"realloc_interval">> => 100,
                    <<"leader_timeout">> => 100,
                    <<"checkpoint_interval">> => 10,
                    <<"revocation_timeout">> => 1000,
                    <<"commit_retries">> => 10,
                    <<"commit_retry_interval">> => 100,
                    <<"commit_timeout">> => 1000
                }
        },
        <<"log">> => #{
            <<"file">> => #{
                <<"default">> => #{
                    <<"enable">> => true,
                    <<"level">> => info
                }
            }
        }
    },
    NodeSpec = #{role => core, apps => []},
    NodeSpecs = [{n1, NodeSpec}, {n2, NodeSpec}, {n3, NodeSpec}],
    emqx_common_test_helpers:start_cluster_ds(Config, NodeSpecs, #{
        emqx_conf => Conf,
        keep_work_dir => false
    }).
