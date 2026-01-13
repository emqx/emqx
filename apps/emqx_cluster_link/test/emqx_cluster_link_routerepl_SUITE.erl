%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster_link_routerepl_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/asserts.hrl").

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").
-include_lib("emqx_cluster_link/include/emqx_cluster_link.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-import(emqx_common_test_helpers, [on_exit/1]).

%%

suite() ->
    [{timetrap, {seconds, 30}}].

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_cluster_link
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    ok = emqx_config:put([cluster, name], rrtest),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(TCName, Config) ->
    emqx_common_test_helpers:init_per_testcase(?MODULE, TCName, Config).

end_per_testcase(TCName, Config) ->
    emqx_common_test_helpers:end_per_testcase(?MODULE, TCName, Config).

%%

%% If an exception occurs while handling a route op message, we disconnect the upstream
%% actor client so it restarts.
t_disconnect_on_errors('init', Config) ->
    ok = snabbkaffe:start_trace(),
    Config;
t_disconnect_on_errors('end', _Config) ->
    ok = snabbkaffe:stop().

t_disconnect_on_errors(_Config) ->
    Actor = ?FUNCTION_NAME,
    ActorIn = atom_to_binary(Actor),
    TargetCluster = <<"disconnerr">>,
    ActorMF = fun
        (incarnation) -> erlang:system_time(millisecond);
        (marker) -> TargetCluster
    end,
    Link = mk_self_link(TargetCluster, [<<"#">>]),
    ?check_trace(
        #{timetrap => 5_000},
        begin
            %% Make the remote connection side crash on receiving actor init:
            ?inject_crash(
                #{?snk_kind := "cluster_link_routerepl_in_actor_init", actor := ActorIn},
                snabbkaffe_nemesis:recover_after(1)
            ),
            %% Start route replication and wait until it is online:
            %% Remote side should see a random crash, interpret it as a protocol error,
            %% and disconnect.
            {ok, RouteRepl} = emqx_cluster_link_routerepl:start_link(Actor, ActorMF, Link),
            ?block_until(#{?snk_kind := "cluster_link_routerepl_connected", actor := Actor}),
            ?block_until(#{?snk_kind := "cluster_link_routerepl_connection_down", actor := Actor}),
            %% Stop the route replication:
            true = erlang:unlink(RouteRepl),
            ok = gen:stop(RouteRepl, shutdown, infinity)
        end,
        fun(Trace) ->
            ?strict_causality(
                #{?snk_kind := snabbkaffe_crash},
                #{?snk_kind := "cluster_link_routerepl_protocol_error"},
                Trace
            )
        end
    ).

%% If a timeout occurs during actor state initialization, we close the (potentially
%% unhealthy) connection and start anew.
t_restart_connection_on_actor_init_timeout('init', Config) ->
    ok = snabbkaffe:start_trace(),
    HSt = mk_hookst(),
    ok = emqx_hooks:add(
        'message.publish',
        {?MODULE, h_restart_connection_on_actor_init_timeout, [HSt]},
        ?HP_HIGHEST
    ),
    [{hook_state, HSt} | Config];
t_restart_connection_on_actor_init_timeout('end', _Config) ->
    ok = emqx_hooks:del(
        'message.publish',
        {?MODULE, h_restart_connection_on_actor_init_timeout}
    ),
    snabbkaffe:stop().

t_restart_connection_on_actor_init_timeout(Config) ->
    Actor = ?FUNCTION_NAME,
    TargetCluster = <<"actorinit">>,
    ActorMF = fun
        (incarnation) -> erlang:system_time(millisecond);
        (marker) -> TargetCluster
    end,
    Link = mk_self_link(TargetCluster, [<<"#">>]),
    %% Make hook initially drop actor init handshakes messages:
    HSt = ?config(hook_state, Config),
    hookst_put(ignore_init, true, HSt),
    ?check_trace(
        #{timetrap => 10_000},
        begin
            %% Start route replication and wait until it is connected:
            {ok, RouteRepl} = emqx_cluster_link_routerepl:start_link(Actor, ActorMF, Link),
            ?block_until(#{?snk_kind := "cluster_link_routerepl_connected", actor := Actor}),
            %% Since `h_restart_connection_on_actor_init_timeout` stops cluster link from
            %% seeing handshakes, routerepl should eventually timeout:
            ?block_until(#{?snk_kind := "cluster_link_routerepl_handshake_timeout", actor := Actor}),
            %% Fix the hook:
            %% Route replication should eventually reconnect and rebootstrap.
            hookst_put(ignore_init, false, HSt),
            ?block_until(#{?snk_kind := "cluster_link_routerepl_bootstrap_complete", actor := Actor}),
            %% Stop the route replication:
            true = erlang:unlink(RouteRepl),
            ok = gen:stop(RouteRepl, shutdown, infinity)
        end,
        [
            {"There were 2 separate connection attempts", fun(Trace) ->
                ?assertMatch(
                    [_Conn1, _Conn2],
                    ?of_kind("cluster_link_routerepl_connected", Trace)
                )
            end},
            {"Handshake timeout caused route replication to abandon existing connection", fun(
                Trace
            ) ->
                ?strict_causality(
                    #{?snk_kind := "cluster_link_routerepl_handshake_timeout"},
                    #{?snk_kind := "cluster_link_routerepl_stop_client"},
                    Trace
                )
            end}
        ]
    ).

h_restart_connection_on_actor_init_timeout(
    Msg = #message{
        topic = Topic = <<?ROUTE_TOPIC_PREFIX, Cluster/binary>>,
        payload = Payload
    },
    HSt
) ->
    ?assertEqual(<<"rrtest">>, Cluster),
    RouteOp = emqx_cluster_link_mqtt:decode_route_op(Payload),
    ct:pal("~p: ~s <- ~p", [?FUNCTION_NAME, Topic, RouteOp]),
    case RouteOp of
        {actor_init, #{actor := Actor}, _Info} ->
            case hookst_get(ignore_init, HSt) of
                true ->
                    %% Ignore the handshake:
                    {stop, Msg};
                _ ->
                    %% Accept the handshake:
                    MsgResp = emqx_cluster_link_mqtt:mk_actor_init_ack(Actor, true, Msg),
                    emqx_broker:publish(MsgResp),
                    {stop, Msg}
            end;
        {route_updates, _ActorInfo, _Ops} ->
            {stop, Msg};
        {heartbeat, _ActorInfo} ->
            {stop, Msg}
    end;
h_restart_connection_on_actor_init_timeout(_Msg, _HSt) ->
    ok.

%% Connect / subscribe errors during routerepl actor initialization are handled gracefully.
t_graceful_retry_on_actor_error('init', Config) ->
    ok = snabbkaffe:start_trace(),
    ok = emqx_hooks:add(
        'message.publish',
        {?MODULE, h_graceful_retry_on_actor_error, []},
        ?HP_HIGHEST
    ),
    ok = emqx_listeners:stop_listener('tcp:default'),
    Config;
t_graceful_retry_on_actor_error('end', _Config) ->
    _ = emqx_listeners:start_listener('tcp:default'),
    ok = emqx_hooks:del(
        'message.publish',
        {?MODULE, h_graceful_retry_on_actor_error}
    ),
    snabbkaffe:stop().

t_graceful_retry_on_actor_error(_Config) ->
    Actor = ?FUNCTION_NAME,
    TargetCluster = <<"gracefulretry">>,
    ActorMF = fun
        (incarnation) -> erlang:system_time(millisecond);
        (marker) -> TargetCluster
    end,
    Link = mk_self_link(TargetCluster, [<<"#">>]),
    ?check_trace(
        #{timetrap => 15_000},
        begin
            %% Start route replication:
            %% Connection failure should be tolerated.
            {ok, RouteRepl} = emqx_cluster_link_routerepl:start_link(Actor, ActorMF, Link),
            ?block_until(#{
                ?snk_kind := "cluster_link_routerepl_connection_failed",
                actor := Actor
            }),
            %% Make sure that connection is forcefully disconnected on authz failures:
            ok = emqx_authz_test_lib:reset_authorizers(deny, disconnect, false, []),
            %% Start listener back:
            ok = emqx_listeners:start_listener('tcp:default'),
            %% Disconnect during SUBSCRIBE should be tolerated:
            ?block_until(#{
                ?snk_kind := "cluster_link_routerepl_connection_failed",
                actor := Actor,
                reason := {{shutdown, {disconnected, ?RC_NOT_AUTHORIZED, _}}, _}
            }),
            %% Restore lax authorization rules:
            %% Route replication should eventually be restored.
            ok = emqx_authz_test_lib:restore_authorizers(),
            ?block_until(#{
                ?snk_kind := "cluster_link_routerepl_bootstrap_complete",
                actor := Actor
            }),
            %% Stop the route replication:
            true = erlang:unlink(RouteRepl),
            ok = gen:stop(RouteRepl, shutdown, infinity)
        end,
        [
            {"No actor should have restarted", fun(Trace) ->
                ?assertMatch(
                    [_],
                    ?of_kind("cluster_link_routerepl_init", Trace)
                )
            end}
        ]
    ).

h_graceful_retry_on_actor_error(
    Msg = #message{
        topic = Topic = <<?ROUTE_TOPIC_PREFIX, Cluster/binary>>,
        payload = Payload
    }
) ->
    ?assertEqual(<<"rrtest">>, Cluster),
    RouteOp = emqx_cluster_link_mqtt:decode_route_op(Payload),
    ct:pal("~p: ~s <- ~p", [?FUNCTION_NAME, Topic, RouteOp]),
    case RouteOp of
        {actor_init, #{actor := Actor}, _Info} ->
            %% Accept the handshake:
            MsgResp = emqx_cluster_link_mqtt:mk_actor_init_ack(Actor, true, Msg),
            emqx_broker:publish(MsgResp),
            {stop, Msg};
        {route_updates, _ActorInfo, _Ops} ->
            {stop, Msg};
        {heartbeat, _ActorInfo} ->
            {stop, Msg}
    end;
h_graceful_retry_on_actor_error(_Msg) ->
    ok.

%%

mk_self_link(TargetCluster, Topics) ->
    #{
        name => TargetCluster,
        server => "127.0.0.1",
        ssl => #{enable => false},
        topics => Topics
    }.

mk_hookst() ->
    ets:new(?MODULE, [set, public]).

hookst_get(K, HSt) ->
    ets:lookup_element(HSt, K, 2, undefined).

hookst_put(K, V, HSt) ->
    ets:insert(HSt, {K, V}).

hookst_all(HSt) ->
    ets:tab2list(HSt).

