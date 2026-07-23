%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_coap_frame_tests).

-include_lib("eunit/include/eunit.hrl").

format_redacts_sensitive_uri_query_test() ->
    Query = #{
        <<"password">> => <<"password-value">>,
        <<"secret">> => <<"secret-value">>,
        <<"private_key">> => <<"private-key-value">>,
        <<"access_token">> => <<"access-token-value">>
    },
    Msg = emqx_coap_message:request(
        con, post, <<>>, #{uri_path => [<<"rd">>], uri_query => Query}
    ),
    Formatted = iolist_to_binary(emqx_coap_frame:format(Msg)),
    lists:foreach(
        fun(Value) ->
            ?assertEqual(nomatch, binary:match(Formatted, Value))
        end,
        maps:values(Query)
    ),
    ?assertNotEqual(nomatch, binary:match(Formatted, <<"******">>)).
