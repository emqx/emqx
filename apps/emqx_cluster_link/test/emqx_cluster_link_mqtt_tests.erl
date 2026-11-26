%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_cluster_link_mqtt_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-define(MOD, emqx_cluster_link_mqtt).

%% Reasons that mean the connection to the remote broker is gone (or was never
%% established).  The message may still be delivered once the broker is reachable
%% again, so these must be classified as recoverable: the buffer worker retries
%% recoverable results, but acknowledges (and counts as `failed') any other
%% `{error, _}' result, dropping the message.
connection_errors() ->
    [
        closed,
        einval,
        enotconn,
        epipe,
        tcp_closed,
        tcp_error,
        ssl_closed,
        ssl_error,
        quic_closed,
        quic_error,
        econnaborted,
        econnrefused,
        econnreset,
        ehostdown,
        ehostunreach,
        enetdown,
        enetreset,
        enetunreach,
        etimedout,
        timeout,
        nxdomain
    ].

%% Tuple forms of the connection error reasons.  emqtt hands the *inner* detail of
%% a socket event (`tcp_closed', a posix atom, ...) to the publish callbacks, so
%% these are defensive: they pin the classifier's contract for shapes a transport
%% may report, not shapes observed on the current code path.
connection_error_tuples() ->
    [
        {Kind, Sock}
     || Kind <- [tcp_closed, tcp_error, ssl_closed, ssl_error, quic_closed, quic_error],
        Sock <- [make_ref(), {sslsocket, gen_tcp, tls_connection}]
    ].

%% `disconnected' is the `ecpool' answer for a pool worker without a connection
%% (normally intercepted by `handle_ecpool_result/1') and `{disconnected, RC,
%% Props}' is the peer's MQTT DISCONNECT packet.  Like the tuple forms above,
%% these are defensive: neither is produced by the publish callbacks on the
%% current code path, and neither is permanent.
disconnected_errors() ->
    [
        disconnected,
        {disconnected, ?RC_SERVER_UNAVAILABLE, #{}},
        {disconnected, ?RC_SESSION_TAKEN_OVER, #{}}
    ].

shutdown_errors() ->
    [
        shutdown,
        {shutdown, tcp_closed}
    ].

%% Reasons on the fail-fast side: the query is acknowledged and counted `failed'
%% instead of being retried until `request_ttl'.  This change moves
%% `eaddrnotavail' here: it is a bind/connect-time error rather than a result of
%% the send path, and a local address or address family that the host does not
%% have is not something another attempt against the same peer fixes.  `dropped'
%% is emqtt giving up on in-flight publishes (in-flight window overflow, or a
%% clean-start reconnect) without resending them; it stays fail-fast because it is
%% outside this change's scope.
%%
%% Not pinned here, because they are outside this change's scope: broker replies
%% (`classify_reply/1') and the client-owner exit shape `{owner, _, _}' that a
%% pool restart can produce.
unrecoverable_errors() ->
    [
        dropped,
        eaddrnotavail,
        {tls_alert, {handshake_failure, <<"certificate expired">>}},
        {frame_parse_error, <<"not mqtt">>},
        eaddrinuse,
        emsgsize,
        eproto
    ].

%% The emqtt callback feeding async results into the buffer worker.
async_result(Result) ->
    ?MOD:on_async_result({fun(Res) -> Res end, []}, Result).

async_query_result(Reason) ->
    async_result({error, Reason}).

%%--------------------------------------------------------------------
%% Recoverable errors
%%--------------------------------------------------------------------

retriable_errors_are_recoverable_test() ->
    lists:foreach(
        fun(Reason) ->
            ?assertEqual(
                {error, {recoverable_error, Reason}},
                async_query_result(Reason),
                #{reason => Reason}
            )
        end,
        connection_errors() ++
            connection_error_tuples() ++
            disconnected_errors() ++
            shutdown_errors()
    ).

permanent_errors_are_unrecoverable_test() ->
    lists:foreach(
        fun(Reason) ->
            ?assertEqual(
                {error, {unrecoverable_error, Reason}},
                async_query_result(Reason),
                #{reason => Reason}
            )
        end,
        unrecoverable_errors()
    ).

successful_async_results_test() ->
    ?assertEqual(ok, async_result(ok)),
    ?assertEqual(ok, async_result({ok, #{reason_code => ?RC_SUCCESS}})),
    ?assertEqual(ok, async_result({ok, #{reason_code => ?RC_NO_MATCHING_SUBSCRIBERS}})).

%%--------------------------------------------------------------------
%% Pool errors
%%--------------------------------------------------------------------

%% A query sent while the pool does not exist (e.g. the resource is being
%% restarted) must be retried, not acknowledged and dropped.
no_such_pool_is_recoverable_test() ->
    Pool = <<"emqx_cluster_link_mqtt:msg:nonexistent">>,
    Result = emqx_cluster_link_mqtt:on_query_async(
        Pool,
        forwarded_message(),
        {fun(_Res) -> ok end, []},
        #{pool_name => Pool, topic => <<"t/link">>}
    ),
    ?assertEqual({error, {recoverable_error, no_such_pool}}, Result).

forwarded_message() ->
    (emqx_message:make(<<"t/link">>, <<"payload">>))#message{extra = <<"pick-key">>}.

%% Resource id of the message forwarding pool of the link named `remote'.
resource_id() ->
    emqx_cluster_link_mqtt:resource_id(<<"remote">>).

%% The PINGREQ round trip decides the resource status: an answer means the link is
%% usable, and every other outcome means it is not.
health_check_probe_test_() ->
    {foreach, fun setup/0, fun teardown/1, [
        fun answered_probe_is_connected/0,
        fun unanswered_probe_is_disconnected/0,
        fun unsent_probe_is_disconnected/0,
        fun probe_outliving_the_budget_is_disconnected/0,
        fun infinity_timeout_is_probed/0,
        fun client_down_is_disconnected/0,
        fun not_connected_client_is_connecting/0
    ]}.

setup() ->
    ok = meck:new(emqx_cluster_link_config, [passthrough]),
    ok = meck:new(emqx_resource, [passthrough]),
    ok = meck:new(ecpool, [passthrough]),
    ok = meck:new(ecpool_worker, [passthrough]),
    ok = meck:new(emqtt, [passthrough]),
    expect_health_check_timeout(2_000),
    meck:expect(emqx_resource, get_allocated_resources_list, fun(_ResId) -> [] end),
    meck:expect(ecpool, workers, fun(_Pool) -> [{worker, self()}] end),
    meck:expect(ecpool_worker, client, fun(_Worker) -> {ok, self()} end),
    ok.

teardown(_) ->
    lists:foreach(
        fun meck:unload/1,
        [emqtt, ecpool_worker, ecpool, emqx_resource, emqx_cluster_link_config]
    ).

expect_health_check_timeout(Timeout) ->
    meck:expect(emqx_cluster_link_config, get_link, fun(_Name) ->
        #{resource_opts => #{health_check_timeout => Timeout}}
    end).

answered_probe_is_connected() ->
    expect_connected_client(pong),
    ?assertEqual(connected, health_check()).

unanswered_probe_is_disconnected() ->
    %% `emqtt' gave up on the PINGRESP while the socket was still there.
    expect_connected_client({error, ack_timeout}),
    ?assertEqual(disconnected, health_check()).

unsent_probe_is_disconnected() ->
    expect_connected_client({error, closed}),
    ?assertEqual(disconnected, health_check()).

probe_outliving_the_budget_is_disconnected() ->
    %% The health check's own budget (1 s here, half of the 2 s configured above)
    %% gives up on a remote that does not answer the PINGREQ.
    meck:expect(emqtt, status, fun(_Pid) -> connected end),
    meck:expect(emqtt, ping, fun(_Pid) -> timer:sleep(5000) end),
    ?assertEqual({disconnected, pingresp_timeout}, health_check()).

infinity_timeout_is_probed() ->
    %% `health_check_timeout' also accepts `infinity'.
    expect_health_check_timeout(infinity),
    expect_connected_client(pong),
    ?assertEqual(connected, health_check()).

client_down_is_disconnected() ->
    meck:expect(emqtt, status, fun(_Pid) -> exit(noproc) end),
    ?assertEqual(disconnected, health_check()).

not_connected_client_is_connecting() ->
    meck:expect(emqtt, status, fun(_Pid) -> reconnect end),
    ?assertEqual(connecting, health_check()).

expect_connected_client(PingReply) ->
    meck:expect(emqtt, status, fun(_Pid) -> connected end),
    meck:expect(emqtt, ping, fun(_Pid) -> PingReply end).

health_check() ->
    emqx_cluster_link_mqtt:on_get_status(resource_id(), #{pool_name => test_pool}).

-define(ACTOR, 10).
-define(INCARNATION, 11).
-define(ROUTES, 12).
-define(TARGET_CLUSTER, 13).
-define(PROTO_VER, 14).
-define(RESULT, 15).
-define(NEED_BOOTSTRAP, 16).

route_op_types_test_() ->
    Actor = <<"ps-routes-v1">>,
    ActorInfo = #{actor => Actor, incarnation => 0},
    Base = #{?ACTOR => Actor, ?INCARNATION => 0},
    Init = Base#{
        '$op' => <<"actor_init">>, ?TARGET_CLUSTER => <<"east">>, ?PROTO_VER => 1
    },
    Heartbeat = Base#{'$op' => <<"heartbeat">>},
    Topic = <<"sensor/+">>,
    ID = <<"$s/group/sensor/#">>,
    Add = {Topic, ID},
    Delete = {100, Topic, ID},
    Route = Base#{'$op' => <<"route">>, ?ROUTES => [Add, Delete]},
    Unknown = Base#{'$op' => <<"other">>},
    Invalid = {error, invalid_route_op},
    [
        ?_assertEqual(
            {actor_init, ActorInfo, #{target_cluster => <<"east">>, proto_ver => 1}},
            decode_op(Init)
        ),
        ?_assertEqual({heartbeat, ActorInfo}, decode_op(Heartbeat#{99 => <<"extension">>})),
        ?_assertEqual({route_updates, ActorInfo, [{add, Add}, {delete, Add}]}, decode_op(Route)),
        ?_assertEqual({route_updates, ActorInfo, []}, decode_op(Route#{?ROUTES := []})),
        ?_assertEqual({error, {unknown_operation, <<"other">>}}, decode_op(Unknown)),
        ?_assertEqual(
            {error, {unknown_operation, binary:copy(<<"x">>, 128)}},
            decode_op(Unknown#{'$op' := binary:copy(<<"x">>, 128)})
        ),
        ?_assertEqual({error, {unknown_payload, redacted}}, decode_op(maps:remove('$op', Unknown))),
        ?_assertEqual({error, {unknown_payload, redacted}}, decode_op(Unknown#{'$op' := 42})),
        ?_assertEqual(
            {error, {unknown_payload, redacted}},
            decode_op(Unknown#{'$op' := binary:copy(<<"x">>, 129)})
        ),
        [
            ?_assertEqual(Invalid, decode_op(Op#{?INCARNATION := BadIncarnation}))
         || Op <- [Init, Heartbeat, Route], BadIncarnation <- [<<"1">>, 1.0]
        ],
        [?_assertEqual(Invalid, decode_op(Op#{?ACTOR := 42})) || Op <- [Init, Heartbeat, Route]],
        ?_assertEqual(Invalid, decode_op(maps:remove(?INCARNATION, Route))),
        ?_assertEqual(Invalid, decode_op(Init#{?TARGET_CLUSTER := east})),
        ?_assertEqual(Invalid, decode_op(Init#{?PROTO_VER := <<"1">>})),
        [
            ?_assertEqual(Invalid, decode_op(Route#{?ROUTES := BadRoutes}))
         || BadRoutes <- [
                Add,
                [Add | invalid],
                [Add, {42, ID}],
                [{Topic, 42}],
                [{100, 42, ID}],
                [{100, Topic, 42}],
                [{101, Topic, ID}]
            ]
        ]
    ].

actor_init_ack_types_test_() ->
    Actor = <<"node@east">>,
    Ack = #{
        '$op' => <<"actor_init_ack">>,
        ?ACTOR => Actor,
        ?PROTO_VER => 1,
        ?RESULT => ok,
        ?NEED_BOOTSTRAP => true
    },
    Error = {error, <<"unknown_cluster">>},
    [
        [
            ?_assertEqual(
                {actor_init_ack, #{
                    actor => Actor, proto_ver => 1, result => Result, need_bootstrap => Bootstrap
                }},
                decode_ack(Ack#{?RESULT := Result, ?NEED_BOOTSTRAP := Bootstrap})
            )
         || {Result, Bootstrap} <- [{ok, true}, {ok, false}, {Error, undefined}, {Error, false}]
        ],
        ?_assertEqual({error, invalid_actor_init_ack}, emqx_cluster_link_mqtt:decode_resp(<<>>)),
        ?_assertEqual({error, invalid_actor_init_ack}, decode_ack(maps:remove(?PROTO_VER, Ack))),
        [
            ?_assertEqual({error, invalid_actor_init_ack}, decode_ack(maps:merge(Ack, Fields)))
         || Fields <- [
                #{?ACTOR => 42},
                #{?PROTO_VER => <<"1">>},
                #{?NEED_BOOTSTRAP => undefined},
                #{?NEED_BOOTSTRAP => 1},
                #{?RESULT => Error},
                #{?RESULT => error, ?NEED_BOOTSTRAP => undefined},
                #{'$op' => <<"heartbeat">>}
            ]
        ]
    ].

decode_op(Map) ->
    emqx_cluster_link_mqtt:decode_route_op(term_to_binary(Map)).

decode_ack(Map) ->
    emqx_cluster_link_mqtt:decode_resp(term_to_binary(Map)).
