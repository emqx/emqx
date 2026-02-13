%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_coap_message_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_coap.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

t_message_helpers(_) ->
    Msg1 = emqx_coap_message:request(con, get),
    Msg2 = emqx_coap_message:request(con, post, <<"p">>),
    Resp1 = emqx_coap_message:response(Msg1),
    Resp2 = emqx_coap_message:response({ok, content}, Msg1),
    Resp3 = emqx_coap_message:response({ok, content}, <<"p">>, Msg1),
    ?assertMatch(#coap_message{}, Resp1),
    ?assertMatch(#coap_message{}, Resp2),
    ?assertMatch(#coap_message{}, Resp3),
    ?assertEqual(Msg1, emqx_coap_message:set(max_age, ?DEFAULT_MAX_AGE, Msg1)),
    QueryMsg = #coap_message{options = #{uri_query => #{<<"c">> => <<"client1">>}}},
    ?assertMatch(
        #{<<"clientid">> := <<"client1">>},
        emqx_coap_message:extract_uri_query(QueryMsg)
    ),
    _ = emqx_coap_message:set_payload("list", Msg2),
    BlockMsg1 =
        emqx_coap_message:set_payload_block(<<"abcdefghij">>, {0, true, 4}, Msg2),
    BlockMsg2 =
        emqx_coap_message:set_payload_block(<<"abcdefghij">>, {1, false, 5}, Resp2),
    ?assertMatch(#coap_message{}, BlockMsg1),
    ?assertMatch(#coap_message{}, BlockMsg2),
    ok.

t_medium_helpers(_) ->
    Msg = #coap_message{type = con, method = get, id = 1},
    Result0 = emqx_coap_medium:out(Msg),
    Result1 = emqx_coap_medium:out(Msg, Result0),
    Result2 = emqx_coap_medium:reset(Msg, #{}),
    Result3 = emqx_coap_medium:proto_out({request, Msg}, #{}),
    ReplyMsg = #coap_message{type = ack, method = {ok, content}, id = 2},
    Result4 = emqx_coap_medium:reply(ReplyMsg, #{}),
    ?assertMatch(#{out := _}, Result1),
    ?assertMatch(#{out := _}, Result2),
    ?assertMatch(#{proto := _}, Result3),
    ?assertMatch(#{reply := _}, Result4),
    ok.

t_short_name_uri_query(_) ->
    UriQueryMap = #{
        <<"c">> => <<"clientid_value">>,
        <<"t">> => <<"token_value">>,
        <<"u">> => <<"username_value">>,
        <<"p">> => <<"password_value">>,
        <<"q">> => 1,
        <<"r">> => false
    },
    ParsedQuery = #{
        <<"clientid">> => <<"clientid_value">>,
        <<"token">> => <<"token_value">>,
        <<"username">> => <<"username_value">>,
        <<"password">> => <<"password_value">>,
        <<"qos">> => 1,
        <<"retain">> => false
    },
    Msg = emqx_coap_message:request(
        con, put, <<"payload contents...">>, #{uri_query => UriQueryMap}
    ),
    ?assertEqual(ParsedQuery, emqx_coap_message:extract_uri_query(Msg)).
