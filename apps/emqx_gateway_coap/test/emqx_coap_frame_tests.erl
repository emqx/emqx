%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_coap_frame_tests).

-include("emqx_coap.hrl").
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

format_redacts_short_uri_query_credentials_test() ->
    Query = #{
        <<"c">> => <<"client1">>,
        <<"u">> => <<"admin">>,
        <<"p">> => <<"password-value">>,
        <<"t">> => <<"session-token-value">>
    },
    Msg = emqx_coap_message:request(
        con, post, <<>>, #{uri_path => [<<"mqtt">>, <<"connection">>], uri_query => Query}
    ),
    Formatted = iolist_to_binary(emqx_coap_frame:format(Msg)),
    ?assertEqual(nomatch, binary:match(Formatted, <<"password-value">>)),
    ?assertEqual(nomatch, binary:match(Formatted, <<"session-token-value">>)),
    %% Only the values are redacted: the keys stay as the client sent them and
    %% non-sensitive parameters remain readable.
    ?assertNotEqual(nomatch, binary:match(Formatted, <<"<<\"p\">>">>)),
    ?assertNotEqual(nomatch, binary:match(Formatted, <<"<<\"t\">>">>)),
    ?assertEqual(nomatch, binary:match(Formatted, <<"password">>)),
    ?assertEqual(nomatch, binary:match(Formatted, <<"token">>)),
    ?assertNotEqual(nomatch, binary:match(Formatted, <<"client1">>)),
    ?assertNotEqual(nomatch, binary:match(Formatted, <<"admin">>)).

format_redacts_wrapped_secret_payload_test() ->
    Token = <<"3606183915">>,
    Request = emqx_coap_message:request(con, post, <<>>, #{}),
    Msg = emqx_coap_message:piggyback({ok, created}, emqx_secret:wrap(Token), Request),
    Formatted = iolist_to_binary(emqx_coap_frame:format(Msg)),
    ?assertEqual(nomatch, binary:match(Formatted, Token)),
    %% The non-sensitive metadata is preserved for troubleshooting.
    ?assertNotEqual(nomatch, binary:match(Formatted, <<"created">>)),
    ?assertNotEqual(nomatch, binary:match(Formatted, <<"******">>)).

serialize_unwraps_wrapped_secret_payload_test() ->
    Token = <<"3606183915">>,
    Request = emqx_coap_message:request(con, post, <<>>, #{}),
    Msg0 = emqx_coap_message:piggyback({ok, created}, emqx_secret:wrap(Token), Request),
    Msg = Msg0#coap_message{id = 1, token = <<>>},
    Bin = emqx_coap_frame:serialize_pkt(Msg, emqx_coap_frame:serialize_opts()),
    {ok, Decoded, <<>>, _} = emqx_coap_frame:parse(Bin, #{}),
    %% The token is redacted in logs but still delivered on the wire.
    ?assertEqual(Token, Decoded#coap_message.payload).

%% `redact/1' is the alias-aware redaction reused outside of `format/1', e.g. by
%% the channel when it logs a rejected request.
redact_masks_short_uri_query_credentials_test() ->
    Query = #{
        <<"c">> => <<"client1">>,
        <<"p">> => <<"password-value">>,
        <<"t">> => <<"session-token-value">>
    },
    Msg = emqx_coap_message:request(
        con, post, <<>>, #{uri_path => [<<"ps">>, <<"topic">>], uri_query => Query}
    ),
    #coap_message{options = #{uri_query := Redacted}} = emqx_coap_frame:redact(Msg),
    ?assertEqual(<<"******">>, maps:get(<<"p">>, Redacted)),
    ?assertEqual(<<"******">>, maps:get(<<"t">>, Redacted)),
    %% Keys are kept as sent and non-sensitive values stay readable.
    ?assertEqual(<<"client1">>, maps:get(<<"c">>, Redacted)).
