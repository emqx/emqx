%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-define(RES_ID, <<"emqx_cluster_link_mqtt:msg:", "remote">>).

%% A connected client is reported as `connected' only when the remote answers the
%% probe's PINGREQ.  Every way of not getting an answer -- `emqtt''s own timeout
%% reply, no answer at all within the round's budget, a dead client -- has to come
%% back as `connecting' (or `disconnected' when the client itself is gone), so that
%% the resource manager keeps a silent link retriable instead of turning an
%% unanswered probe into an unexpected result, which it treats as a disconnected
%% resource.
health_check_probe_test_() ->
    {foreach, fun setup/0, fun teardown/1, [
        fun answered_probe_is_connected/0,
        fun unanswered_probe_is_connecting/0,
        fun unsent_probe_is_disconnected/0,
        fun probe_outliving_the_budget_is_connecting/0,
        fun client_down_is_disconnected/0,
        fun not_connected_client_is_connecting/0
    ]}.

setup() ->
    ok = meck:new(emqx_cluster_link_config, [passthrough]),
    ok = meck:new(emqx_resource, [passthrough]),
    ok = meck:new(ecpool, [passthrough]),
    ok = meck:new(ecpool_worker, [passthrough]),
    ok = meck:new(emqtt, [passthrough]),
    meck:expect(emqx_cluster_link_config, get_link, fun(_Name) ->
        #{resource_opts => #{health_check_interval => 1000, health_check_timeout => 60_000}}
    end),
    meck:expect(emqx_resource, get_allocated_resources_list, fun(_ResId) -> [] end),
    meck:expect(ecpool, workers, fun(_Pool) -> [{worker, self()}] end),
    meck:expect(ecpool_worker, client, fun(_Worker) -> {ok, self()} end),
    ok.

teardown(_) ->
    lists:foreach(
        fun meck:unload/1,
        [emqtt, ecpool_worker, ecpool, emqx_resource, emqx_cluster_link_config]
    ).

answered_probe_is_connected() ->
    expect_connected_client(pong),
    ?assertEqual(connected, health_check()).

unanswered_probe_is_connecting() ->
    %% `emqtt' gave up on the PINGRESP while the socket was still there.
    expect_connected_client({error, ack_timeout}),
    ?assertEqual(connecting, health_check()).

unsent_probe_is_disconnected() ->
    expect_connected_client({error, closed}),
    ?assertEqual(disconnected, health_check()).

probe_outliving_the_budget_is_connecting() ->
    %% `emqtt:ping/1' waits forever, so a remote that never answers is given up on by
    %% the health check's own budget (1 s here, see `resource_opts' above).
    meck:expect(emqtt, status, fun(_Pid) -> connected end),
    meck:expect(emqtt, ping, fun(_Pid) -> timer:sleep(5000) end),
    ?assertEqual(connecting, health_check()).

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
    emqx_cluster_link_mqtt:on_get_status(?RES_ID, #{pool_name => test_pool}).
