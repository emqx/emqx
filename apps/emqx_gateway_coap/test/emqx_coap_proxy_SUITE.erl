%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_coap_proxy_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_coap.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

t_proxy_conn_format_error_con(_) ->
    State = emqx_coap_proxy_conn:initialize([]),
    Packet = <<1:2, 0:2, 9:4, 0:3, 1:5, 400:16>>,
    {ok, {peer, _}, [{coap_format_error, con, 400, _}], _} =
        emqx_coap_proxy_conn:get_connection_id(undefined, {{127, 0, 0, 1}, 5683}, State, Packet),
    ok.

t_proxy_conn_format_error_non(_) ->
    State = emqx_coap_proxy_conn:initialize([]),
    Packet = <<1:2, 1:2, 9:4, 0:3, 1:5, 401:16>>,
    {ok, {peer, _}, [{coap_format_error, non, 401, _}], _} =
        emqx_coap_proxy_conn:get_connection_id(undefined, {{127, 0, 0, 1}, 5683}, State, Packet),
    ok.

t_proxy_conn_request_error(_) ->
    State = emqx_coap_proxy_conn:initialize([]),
    Msg = #coap_message{
        type = con,
        method = get,
        id = 402,
        options = #{9 => <<1>>}
    },
    Packet = emqx_coap_frame:serialize_pkt(Msg, emqx_coap_frame:serialize_opts()),
    {ok, {peer, _}, [{coap_request_error, #coap_message{id = 402}, {error, bad_option}}], _} =
        emqx_coap_proxy_conn:get_connection_id(undefined, {{127, 0, 0, 1}, 5683}, State, Packet),
    ok.

t_proxy_conn_ignore(_) ->
    State = emqx_coap_proxy_conn:initialize([]),
    Packet = <<2:2, 0:2, 0:4, 0:3, 1:5, 403:16>>,
    {ok, {peer, _}, [{coap_ignore, _}], _} =
        emqx_coap_proxy_conn:get_connection_id(undefined, {{127, 0, 0, 1}, 5683}, State, Packet),
    ok.

t_proxy_conn_paths(_) ->
    State = emqx_coap_frame:initial_parse_state(#{}),
    ?assertEqual(invalid, emqx_coap_proxy_conn:get_connection_id(dummy, dummy, State, <<>>)),
    Msg = #coap_message{
        type = con,
        method = post,
        id = 1,
        options = #{uri_path => [<<"mqtt">>, <<"connection">>]}
    },
    Bin = emqx_coap_frame:serialize_pkt(Msg, emqx_coap_frame:serialize_opts()),
    {ok, {peer, _}, [#coap_message{type = con, method = post, id = 1}], _} =
        emqx_coap_proxy_conn:get_connection_id(dummy, dummy, State, Bin),
    ok.
