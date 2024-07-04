%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-include_lib("emqx/include/asserts.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx, #{
                config => #{
                    <<"durable_sessions">> => #{
                        <<"enable">> => true,
                        <<"renew_streams_interval">> => "100ms"
                    },
                    <<"durable_storage">> => #{
                        <<"messages">> => #{
                            <<"backend">> => <<"builtin_raft">>
                        }
                    }
                }
            }},
            emqx_ds_shared_sub
        ],
        #{work_dir => ?config(priv_dir, Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)),
    ok.

init_per_testcase(_TC, Config) ->
    ok = snabbkaffe:start_trace(),
    Config.

end_per_testcase(_TC, _Config) ->
    ok = snabbkaffe:stop(),
    ok = terminate_leaders(),
    ok.

t_lease_initial(_Config) ->
    ConnShared = emqtt_connect_sub(<<"client_shared">>),
    {ok, _, _} = emqtt:subscribe(ConnShared, <<"$share/gr1/topic1/#">>, 1),

    ConnPub = emqtt_connect_pub(<<"client_pub">>),

    {ok, _} = emqtt:publish(ConnPub, <<"topic1/1">>, <<"hello1">>, 1),
    ct:sleep(2_000),
    {ok, _} = emqtt:publish(ConnPub, <<"topic1/2">>, <<"hello2">>, 1),

    ?assertReceive({publish, #{payload := <<"hello1">>}}, 10_000),
    ?assertReceive({publish, #{payload := <<"hello2">>}}, 10_000),

    ok = emqtt:disconnect(ConnShared),
    ok = emqtt:disconnect(ConnPub).

t_two_clients(_Config) ->
    ConnShared1 = emqtt_connect_sub(<<"client_shared1">>),
    {ok, _, _} = emqtt:subscribe(ConnShared1, <<"$share/gr4/topic4/#">>, 1),

    ConnShared2 = emqtt_connect_sub(<<"client_shared2">>),
    {ok, _, _} = emqtt:subscribe(ConnShared2, <<"$share/gr4/topic4/#">>, 1),

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

t_client_loss(_Config) ->
    process_flag(trap_exit, true),

    ConnShared1 = emqtt_connect_sub(<<"client_shared1">>),
    {ok, _, _} = emqtt:subscribe(ConnShared1, <<"$share/gr5/topic5/#">>, 1),

    ConnShared2 = emqtt_connect_sub(<<"client_shared2">>),
    {ok, _, _} = emqtt:subscribe(ConnShared2, <<"$share/gr5/topic5/#">>, 1),

    ConnPub = emqtt_connect_pub(<<"client_pub">>),

    {ok, _} = emqtt:publish(ConnPub, <<"topic5/1">>, <<"hello1">>, 1),
    {ok, _} = emqtt:publish(ConnPub, <<"topic5/2">>, <<"hello2">>, 1),

    exit(ConnShared1, kill),

    {ok, _} = emqtt:publish(ConnPub, <<"topic5/1">>, <<"hello3">>, 1),
    {ok, _} = emqtt:publish(ConnPub, <<"topic5/2">>, <<"hello4">>, 1),

    ?assertReceive({publish, #{payload := <<"hello3">>}}, 10_000),
    ?assertReceive({publish, #{payload := <<"hello4">>}}, 10_000),

    ok = emqtt:disconnect(ConnShared2),
    ok = emqtt:disconnect(ConnPub).

t_stream_revoke(_Config) ->
    process_flag(trap_exit, true),

    ConnShared1 = emqtt_connect_sub(<<"client_shared1">>),
    {ok, _, _} = emqtt:subscribe(ConnShared1, <<"$share/gr6/topic6/#">>, 1),

    ConnPub = emqtt_connect_pub(<<"client_pub">>),

    {ok, _} = emqtt:publish(ConnPub, <<"topic6/1">>, <<"hello1">>, 1),
    {ok, _} = emqtt:publish(ConnPub, <<"topic6/2">>, <<"hello2">>, 1),

    ?assertReceive({publish, #{payload := <<"hello1">>}}, 10_000),
    ?assertReceive({publish, #{payload := <<"hello2">>}}, 10_000),

    ConnShared2 = emqtt_connect_sub(<<"client_shared2">>),

    ?assertWaitEvent(
        {ok, _, _} = emqtt:subscribe(ConnShared2, <<"$share/gr6/topic6/#">>, 1),
        #{
            ?snk_kind := shared_sub_group_sm_leader_update_streams,
            stream_progresses := [_ | _],
            id := <<"client_shared2">>
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

t_graceful_disconnect(_Config) ->
    ConnShared1 = emqtt_connect_sub(<<"client_shared1">>),
    {ok, _, _} = emqtt:subscribe(ConnShared1, <<"$share/gr4/topic7/#">>, 1),

    ConnShared2 = emqtt_connect_sub(<<"client_shared2">>),
    {ok, _, _} = emqtt:subscribe(ConnShared2, <<"$share/gr4/topic7/#">>, 1),

    ConnPub = emqtt_connect_pub(<<"client_pub">>),

    {ok, _} = emqtt:publish(ConnPub, <<"topic7/1">>, <<"hello1">>, 1),
    {ok, _} = emqtt:publish(ConnPub, <<"topic7/2">>, <<"hello2">>, 1),

    ?assertReceive({publish, #{payload := <<"hello1">>}}, 2_000),
    ?assertReceive({publish, #{payload := <<"hello2">>}}, 2_000),

    ?assertWaitEvent(
        ok = emqtt:disconnect(ConnShared1),
        #{?snk_kind := shared_sub_leader_disconnect_agent},
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

t_intensive_reassign(_Config) ->
    ConnPub = emqtt_connect_pub(<<"client_pub">>),

    ConnShared1 = emqtt_connect_sub(<<"client_shared1">>),
    {ok, _, _} = emqtt:subscribe(ConnShared1, <<"$share/gr8/topic8/#">>, 1),

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
    {ok, _, _} = emqtt:subscribe(ConnShared2, <<"$share/gr8/topic8/#">>, 1),
    {ok, _, _} = emqtt:subscribe(ConnShared3, <<"$share/gr8/topic8/#">>, 1),

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

t_unsubscribe(_Config) ->
    ConnPub = emqtt_connect_pub(<<"client_pub">>),

    ConnShared1 = emqtt_connect_sub(<<"client_shared1">>),
    {ok, _, _} = emqtt:subscribe(ConnShared1, <<"$share/gr9/topic9/#">>, 1),

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
    {ok, _, _} = emqtt:subscribe(ConnShared2, <<"$share/gr9/topic9/#">>, 1),
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

t_quick_resubscribe(_Config) ->
    ConnPub = emqtt_connect_pub(<<"client_pub">>),

    ConnShared1 = emqtt_connect_sub(<<"client_shared1">>),
    {ok, _, _} = emqtt:subscribe(ConnShared1, <<"$share/gr10/topic10/#">>, 1),

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
    {ok, _, _} = emqtt:subscribe(ConnShared2, <<"$share/gr10/topic10/#">>, 1),
    ok = lists:foreach(
        fun(_) ->
            {ok, _, _} = emqtt:unsubscribe(ConnShared1, <<"$share/gr10/topic10/#">>),
            {ok, _, _} = emqtt:subscribe(ConnShared1, <<"$share/gr10/topic10/#">>, 1),
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

t_lease_reconnect(_Config) ->
    ConnPub = emqtt_connect_pub(<<"client_pub">>),

    ConnShared = emqtt_connect_sub(<<"client_shared">>),

    %% Stop registry to simulate unability to find leader.
    ok = supervisor:terminate_child(emqx_ds_shared_sub_sup, emqx_ds_shared_sub_registry),

    ?assertWaitEvent(
        {ok, _, _} = emqtt:subscribe(ConnShared, <<"$share/gr2/topic2/#">>, 1),
        #{?snk_kind := find_leader_timeout},
        5_000
    ),

    %% Start registry, agent should retry after some time and find the leader.
    ?assertWaitEvent(
        {ok, _} = supervisor:restart_child(emqx_ds_shared_sub_sup, emqx_ds_shared_sub_registry),
        #{?snk_kind := leader_lease_streams},
        5_000
    ),

    {ok, _} = emqtt:publish(ConnPub, <<"topic2/2">>, <<"hello2">>, 1),

    ?assertReceive({publish, #{payload := <<"hello2">>}}, 10_000),

    ok = emqtt:disconnect(ConnShared),
    ok = emqtt:disconnect(ConnPub).

t_renew_lease_timeout(_Config) ->
    ConnShared = emqtt_connect_sub(<<"client_shared">>),

    ?assertWaitEvent(
        {ok, _, _} = emqtt:subscribe(ConnShared, <<"$share/gr3/topic3/#">>, 1),
        #{?snk_kind := leader_lease_streams},
        5_000
    ),

    ?check_trace(
        ?wait_async_action(
            ok = terminate_leaders(),
            #{?snk_kind := leader_lease_streams},
            10_000
        ),
        fun(Trace) ->
            ?strict_causality(
                #{?snk_kind := renew_lease_timeout},
                #{?snk_kind := leader_lease_streams},
                Trace
            )
        end
    ),

    ok = emqtt:disconnect(ConnShared).

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

emqtt_connect_sub(ClientId) ->
    {ok, C} = emqtt:start_link([
        {clientid, ClientId},
        {clean_start, true},
        {proto_ver, v5},
        {properties, #{'Session-Expiry-Interval' => 7_200}}
    ]),
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

terminate_leaders() ->
    ok = supervisor:terminate_child(emqx_ds_shared_sub_sup, emqx_ds_shared_sub_leader_sup),
    {ok, _} = supervisor:restart_child(emqx_ds_shared_sub_sup, emqx_ds_shared_sub_leader_sup),
    ok.

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
                binary_to_integer(Payload),
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

    Missing = lists:filter(
        fun(N) -> not maps:is_key(N, Messages) end,
        lists:seq(1, NPubs)
    ),
    Duplicate = lists:filtermap(
        fun(N) ->
            case Messages of
                #{N := [_]} -> false;
                #{N := [_ | _] = Clients} -> {true, {N, Clients}};
                _ -> false
            end
        end,
        lists:seq(1, NPubs)
    ),

    {Missing, Duplicate}.
