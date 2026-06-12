%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_sso_saml_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("esaml/include/esaml.hrl").

-define(SAML_HTTP_REDIRECT_BINDING,
    <<"urn:oasis:names:tc:SAML:2.0:bindings:URL-Encoding:DEFLATE">>
).

all() ->
    [
        t_callback_rejects_xxe_response,
        t_callback_rejects_deflate_xxe_response
    ].

t_callback_rejects_xxe_response(Config) ->
    assert_callback_rejects_xxe_response(Config, undefined).

t_callback_rejects_deflate_xxe_response(Config) ->
    assert_callback_rejects_xxe_response(Config, ?SAML_HTTP_REDIRECT_BINDING).

assert_callback_rejects_xxe_response(Config, SAMLEncoding) ->
    SecretPath = filename:join(?config(priv_dir, Config), "xxe-secret.txt"),
    Secret = <<"emqx_xxe_secret">>,
    ok = file:write_file(SecretPath, Secret),

    Body = saml_callback_body(SecretPath, SAMLEncoding),
    SP = #esaml_sp{
        consume_uri = "https://127.0.0.1:18083/api/v5/sso/saml/acs",
        metadata_uri = "https://127.0.0.1:18083/api/v5/sso/saml/metadata",
        idp_signs_envelopes = false,
        idp_signs_assertions = false
    },
    State = #{sp => SP, dashboard_addr => <<"https://127.0.0.1:18083">>},

    {error, Reason} = emqx_dashboard_sso_saml:callback(#{body => Body}, State),
    ?assertMatch(<<"Access denied, assertion failed validation:\n{bad_decode,", _/binary>>, Reason),
    ?assertEqual(nomatch, binary:match(Reason, Secret)).

saml_callback_body(SecretPath, SAMLEncoding) ->
    Xml = iolist_to_binary([
        <<"<?xml version=\"1.0\"?>">>,
        <<"<!DOCTYPE foo [<!ENTITY xxe SYSTEM \"file://">>,
        SecretPath,
        <<"\">]>\n">>,
        <<"<samlp:Response ">>,
        <<"xmlns:samlp=\"urn:oasis:names:tc:SAML:2.0:protocol\" ">>,
        <<"xmlns:saml=\"urn:oasis:names:tc:SAML:2.0:assertion\" ">>,
        <<"Version=\"2.0\" IssueInstant=\"2026-01-01T00:00:00Z\">">>,
        <<"<saml:Issuer>&xxe;</saml:Issuer>">>,
        <<"</samlp:Response>">>
    ]),
    Payload =
        case SAMLEncoding of
            undefined -> base64:encode(Xml);
            ?SAML_HTTP_REDIRECT_BINDING -> base64:encode(zlib:zip(Xml))
        end,
    Query =
        case SAMLEncoding of
            undefined -> [{<<"SAMLResponse">>, Payload}];
            _ -> [{<<"SAMLEncoding">>, SAMLEncoding}, {<<"SAMLResponse">>, Payload}]
        end,
    iolist_to_binary(uri_string:compose_query(Query)).
