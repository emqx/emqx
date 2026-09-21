%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_mqtt_connector_tests).

-moduledoc """
Classification contract of the `mqtt' connector's asynchronous publish results.

Only the classification of a result the connector receives from its client is covered
here: `on_async_result/2' is a plain function of the result, so no connector state has
to be fabricated.  The pool answers and the channel health status are asserted on a
real resource in `emqx_bridge_mqtt_action_SUITE'.
""".

-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-define(CONNECTOR, emqx_bridge_mqtt_connector).

%% Errors that are reported by a connection that is gone (or was never
%% established).  Retrying may still deliver the message, so they must not be
%% classified as unrecoverable: the buffer worker drops in-flight messages whose
%% error is unrecoverable.  This is the set `emqtt' passes to the async publish
%% callback when its socket is closed (see `emqtt:terminate/3').
connection_errors() ->
    [
        disconnected,
        shutdown,
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
        nxdomain,
        eaddrnotavail
    ].

async_result(Result) ->
    ?CONNECTOR:on_async_result(fun(R) -> R end, Result).

%%--------------------------------------------------------------------
%% In-flight (async) query results
%%--------------------------------------------------------------------

inflight_connection_errors_are_recoverable_test_() ->
    [
        ?_assertMatch({error, {recoverable_error, _}}, async_result({error, Reason}))
     || Reason <- connection_errors()
    ] ++
        [
            ?_assertMatch(
                {error, {recoverable_error, _}},
                async_result({error, {shutdown, Reason}})
            )
         || Reason <- [tcp_closed, closed, ssl_closed]
        ].

%% A DISCONNECT received from the remote broker.
inflight_disconnect_errors_are_recoverable_test_() ->
    ?_assertMatch(
        {error, {recoverable_error, _}},
        async_result({error, {disconnected, 16#8B, #{}}})
    ).

inflight_non_mqtt_data_is_unrecoverable_test_() ->
    [
        ?_assertMatch({error, {unrecoverable_error, _}}, async_result({error, Reason}))
     || Reason <- [
            {frame_parse_error, <<"not mqtt">>},
            {shutdown, {frame_parse_error, <<"not mqtt">>}}
        ]
    ].

inflight_unknown_errors_are_unrecoverable_test_() ->
    ?_assertMatch(
        {error, {unrecoverable_error, _}},
        async_result({error, {some, unknown, error}})
    ).

publish_replies_are_classified_test_() ->
    [
        ?_assertEqual(ok, async_result({ok, #{reason_code => ?RC_SUCCESS}})),
        ?_assertEqual(ok, async_result({ok, #{reason_code => ?RC_NO_MATCHING_SUBSCRIBERS}})),
        ?_assertMatch(
            {error, {recoverable_error, _}},
            async_result({ok, #{reason_code => ?RC_PACKET_IDENTIFIER_IN_USE}})
        ),
        ?_assertMatch(
            {error, {unrecoverable_error, _}},
            async_result({ok, #{reason_code => ?RC_NOT_AUTHORIZED}})
        )
    ].
