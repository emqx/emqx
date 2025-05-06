%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_coap_message_tests).

-include_lib("eunit/include/eunit.hrl").

short_name_uri_query_test() ->
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
