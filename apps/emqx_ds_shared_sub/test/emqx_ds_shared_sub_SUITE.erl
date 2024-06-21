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

all() -> emqx_common_test_helpers:all(?MODULE).

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
    ConnPub = emqtt_connect_pub(<<"client_pub">>),

    %% Need to pre-create some streams in "topic/#".
    %% Leader is dummy by far and won't update streams after the first lease to the agent.
    %% So there should be some streams already when the agent connects.
    ok = init_streams(ConnPub, <<"topic1/1">>),

    ConnShared = emqtt_connect_sub(<<"client_shared">>),
    {ok, _, _} = emqtt:subscribe(ConnShared, <<"$share/gr1/topic1/#">>, 1),

    {ok, _} = emqtt:publish(ConnPub, <<"topic1/1">>, <<"hello2">>, 1),
    ?assertReceive({publish, #{payload := <<"hello2">>}}, 10_000),

    ok = emqtt:disconnect(ConnShared),
    ok = emqtt:disconnect(ConnPub).

t_lease_reconnect(_Config) ->
    ConnPub = emqtt_connect_pub(<<"client_pub">>),

    %% Need to pre-create some streams in "topic/#".
    %% Leader is dummy by far and won't update streams after the first lease to the agent.
    %% So there should be some streams already when the agent connects.
    ok = init_streams(ConnPub, <<"topic2/2">>),

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

    ct:sleep(1_000),
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
            5_000
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

init_streams(ConnPub, Topic) ->
    ConnRegular = emqtt_connect_sub(<<"client_regular">>),
    {ok, _, _} = emqtt:subscribe(ConnRegular, Topic, 1),
    {ok, _} = emqtt:publish(ConnPub, Topic, <<"hello1">>, 1),

    ?assertReceive({publish, #{payload := <<"hello1">>}}, 5_000),

    ok = emqtt:disconnect(ConnRegular).

emqtt_connect_sub(ClientId) ->
    {ok, C} = emqtt:start_link([
        {client_id, ClientId},
        {clean_start, true},
        {proto_ver, v5},
        {properties, #{'Session-Expiry-Interval' => 7_200}}
    ]),
    {ok, _} = emqtt:connect(C),
    C.

emqtt_connect_pub(ClientId) ->
    {ok, C} = emqtt:start_link([
        {client_id, ClientId},
        {clean_start, true},
        {proto_ver, v5}
    ]),
    {ok, _} = emqtt:connect(C),
    C.

terminate_leaders() ->
    ok = supervisor:terminate_child(emqx_ds_shared_sub_sup, emqx_ds_shared_sub_leader_sup),
    {ok, _} = supervisor:restart_child(emqx_ds_shared_sub_sup, emqx_ds_shared_sub_leader_sup),
    ok.
