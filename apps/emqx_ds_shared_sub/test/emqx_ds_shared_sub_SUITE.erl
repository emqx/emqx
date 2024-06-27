%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-include_lib("emqx/include/emqx_mqtt.hrl").
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
