%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_gateway_conn_ws_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

-doc "A request without an optional subprotocol accepts an empty supported list.".
t_optional_subprotocol_empty(_Config) ->
    check_optional_subprotocol([]).

-doc "A request without an optional subprotocol does not select an unrequested protocol.".
t_optional_subprotocol_absent(_Config) ->
    lists:foreach(fun check_optional_subprotocol/1, [["NATS"], ["ocpp1.6", "ocpp2.0"]]).

check_optional_subprotocol(Supported) ->
    Req = #{headers => #{}},
    Opts = opts(false, Supported),
    ?assertEqual(
        {ok, [Req, Opts, #{}], state},
        emqx_gateway_conn_ws:parse_sec_websocket_protocol([Req, Opts, #{}], state)
    ).

-doc "A request without a required subprotocol returns a controlled handshake error.".
t_required_subprotocol_absent(_Config) ->
    lists:foreach(
        fun(Supported) ->
            Req = #{headers => #{}},
            ?assertEqual(
                {error, no_subprotocol},
                emqx_gateway_conn_ws:parse_sec_websocket_protocol(
                    [Req, opts(true, Supported), #{}], state
                )
            )
        end,
        [[], ["NATS"]]
    ).

-doc "A requested supported subprotocol is selected in client preference order.".
t_subprotocol_selected(_Config) ->
    Req = #{headers => #{<<"sec-websocket-protocol">> => <<"other, NATS, ocpp1.6">>}},
    Opts = opts(false, ["ocpp1.6", "NATS"]),
    {ok, [Resp, Opts, #{}], state} =
        emqx_gateway_conn_ws:parse_sec_websocket_protocol([Req, Opts, #{}], state),
    ?assertEqual(<<"NATS">>, cowboy_req:resp_header(<<"sec-websocket-protocol">>, Resp)).

-doc "A request with no supported subprotocol returns a controlled handshake error.".
t_subprotocol_not_supported(_Config) ->
    lists:foreach(
        fun(Supported) ->
            Req = #{headers => #{<<"sec-websocket-protocol">> => <<"other">>}},
            ?assertEqual(
                {error, no_supported_subprotocol},
                emqx_gateway_conn_ws:parse_sec_websocket_protocol(
                    [Req, opts(false, Supported), #{}], state
                )
            )
        end,
        [[], ["NATS"]]
    ).

opts(FailIfNoSubprotocol, SupportedSubprotocols) ->
    #{
        websocket => #{
            fail_if_no_subprotocol => FailIfNoSubprotocol,
            supported_subprotocols => SupportedSubprotocols
        }
    }.
