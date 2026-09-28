%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_gateway_conn_ws_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(SOCK_PEER, {{127, 0, 0, 1}, 3456}).

all() ->
    emqx_common_test_helpers:all(?MODULE).

req() ->
    #{
        peer => ?SOCK_PEER,
        headers => #{
            <<"x-forwarded-for">> => <<"100.100.100.100, 99.99.99.99">>,
            <<"x-forwarded-port">> => <<"1000">>
        }
    }.

opts(AddrHeaderName, PortHeaderName) ->
    #{
        websocket => #{
            proxy_address_header => AddrHeaderName,
            proxy_port_header => PortHeaderName
        }
    }.

-doc """
Empty `proxy_address_header` and `proxy_port_header` (the default) mean
forwarded headers are not consulted; the socket peer address and port are used.
""".
t_get_peer_empty_header_names(_Config) ->
    ?assertEqual(?SOCK_PEER, emqx_gateway_conn_ws:get_peer(req(), opts("", ""))).

-doc """
Configured header names select the client address and port from the forwarded
headers; the name match is case-insensitive.
""".
t_get_peer_configured_header_names(_Config) ->
    ?assertEqual(
        {{100, 100, 100, 100}, 1000},
        emqx_gateway_conn_ws:get_peer(req(), opts("x-forwarded-for", "x-forwarded-port"))
    ),
    ?assertEqual(
        {{100, 100, 100, 100}, 1000},
        emqx_gateway_conn_ws:get_peer(req(), opts("X-Forwarded-For", "X-Forwarded-Port"))
    ).

-doc """
A configured header name that is absent from the request falls back to the
socket peer address and port.
""".
t_get_peer_header_absent(_Config) ->
    ?assertEqual(
        ?SOCK_PEER,
        emqx_gateway_conn_ws:get_peer(req(), opts("x-real-ip", "x-real-port"))
    ).

-doc """
A `Sec-WebSocket-Protocol' value Cowboy cannot parse is treated as absent,
so the request gets the normal no-subprotocol handling instead of crashing
the connection process.
""".
t_parse_sec_websocket_protocol_unparseable(_Config) ->
    ok = meck:new(cowboy_req, [passthrough, no_history, no_link]),
    ok = meck:expect(cowboy_req, parse_header, fun(<<"sec-websocket-protocol">>, _Req) ->
        exit({request_error, {header, <<"sec-websocket-protocol">>}, 'Malformed header.'})
    end),
    Opts = #{
        websocket => #{
            fail_if_no_subprotocol => true,
            supported_subprotocols => ["mqtt"]
        }
    },
    ?assertEqual(
        {error, no_subprotocol},
        emqx_gateway_conn_ws:parse_sec_websocket_protocol([req(), Opts, ws_opts()], st)
    ),
    ok = meck:expect(cowboy_req, set_resp_header, fun(_Name, _Value, Req) -> Req end),
    Opts1 = #{
        websocket => #{
            fail_if_no_subprotocol => false,
            supported_subprotocols => ["mqtt"]
        }
    },
    ?assertEqual(
        {ok, [req(), Opts1, ws_opts()], st},
        emqx_gateway_conn_ws:parse_sec_websocket_protocol([req(), Opts1, ws_opts()], st)
    ),
    meck:unload(cowboy_req).

ws_opts() ->
    #{}.

st() ->
    st.
