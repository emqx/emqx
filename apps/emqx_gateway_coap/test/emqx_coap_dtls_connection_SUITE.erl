%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_coap_dtls_connection_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("er_coap_client/include/coap.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(CONF_DEFAULT, """
    gateway.coap {
        idle_timeout = 30s
        enable_stats = false
        mountpoint = ""
        notify_type = qos
        connection_required = true
        subscribe_qos = qos1
        publish_qos = qos1

        listeners.dtls.default {
            bind = 5684
            dtls_options {
                verify = verify_none
            }
        }
    }
""").

-define(MQTT_PREFIX, "coaps://127.0.0.1/mqtt").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    application:load(emqx_gateway_coap),
    Apps = emqx_cth_suite:start(
        [
            {emqx_conf, ?CONF_DEFAULT},
            emqx_gateway,
            emqx_auth
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)),
    emqx_config:delete_override_conf_files(),
    ok.

init_per_testcase(_CaseName, Config) ->
    ok = meck:new(emqx_access_control, [passthrough]),
    Config.

end_per_testcase(_CaseName, _Config) ->
    ok = meck:unload(emqx_access_control).

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------

t_connection(_Config) ->
    emqx_gateway_test_utils:meck_emqx_hook_calls(),

    {ok, Sock, Channel} = er_coap_dtls_socket:connect({127, 0, 0, 1}, 5684, [
        {verify, verify_none}
    ]),

    Prefix = ?MQTT_PREFIX ++ "/connection",
    Queries0 = #{
        "clientid" => <<"client1">>,
        "username" => <<"admin">>,
        "password" => <<"public">>
    },
    URI0 = emqx_coap_SUITE:compose_uri(Prefix, Queries0, false),
    Req0 = emqx_coap_SUITE:make_req(post),
    {ok, created, Data} = emqx_coap_SUITE:do_request(Channel, URI0, Req0),
    #coap_content{payload = BinToken} = Data,
    Token = binary_to_list(BinToken),

    timer:sleep(100),
    ?assertNotEqual(
        [],
        emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>)
    ),

    ?assertMatch(
        ['client.connect' | _],
        emqx_gateway_test_utils:collect_emqx_hooks_calls()
    ),

    %% heartbeat
    Queries1 = #{
        "clientid" => <<"client1">>,
        "token" => Token
    },
    URI1 = emqx_coap_SUITE:compose_uri(Prefix, Queries1, false),
    Req1 = emqx_coap_SUITE:make_req(put),
    {ok, changed, _} = emqx_coap_SUITE:do_request(Channel, URI1, Req1),

    er_coap_channel:close(Channel),
    er_coap_dtls_socket:close(Sock).

t_same_dtls_session_token_request(_Config) ->
    {ok, Sock, Channel} = er_coap_dtls_socket:connect({127, 0, 0, 1}, 5684, [
        {verify, verify_none}
    ]),

    Prefix = ?MQTT_PREFIX ++ "/connection",
    Queries0 = #{
        "clientid" => <<"client1">>,
        "username" => <<"admin">>,
        "password" => <<"public">>
    },
    URI0 = emqx_coap_SUITE:compose_uri(Prefix, Queries0, false),
    Req0 = emqx_coap_SUITE:make_req(post),
    {ok, created, Data} = emqx_coap_SUITE:do_request(Channel, URI0, Req0),
    #coap_content{payload = BinToken} = Data,
    Token = binary_to_list(BinToken),

    URI1 = emqx_coap_SUITE:compose_uri(
        "coaps://127.0.0.1/ps/coap/same_session",
        #{
            "clientid" => <<"client1">>,
            "token" => Token
        },
        false
    ),
    Req1 = emqx_coap_SUITE:make_req(post, <<"x">>),
    {ok, changed, _} = emqx_coap_SUITE:do_request(Channel, URI1, Req1),

    er_coap_channel:close(Channel),
    er_coap_dtls_socket:close(Sock).

t_wrong_clientid_with_valid_token_rejected(_Config) ->
    {ok, Sock1, Channel1} = er_coap_dtls_socket:connect({127, 0, 0, 1}, 5684, [
        {verify, verify_none}
    ]),

    Prefix = ?MQTT_PREFIX ++ "/connection",
    Queries0 = #{
        "clientid" => <<"client1">>,
        "username" => <<"admin">>,
        "password" => <<"public">>
    },
    URI0 = emqx_coap_SUITE:compose_uri(Prefix, Queries0, false),
    Req0 = emqx_coap_SUITE:make_req(post),
    {ok, created, Data} = emqx_coap_SUITE:do_request(Channel1, URI0, Req0),
    #coap_content{payload = BinToken} = Data,
    Token = binary_to_list(BinToken),

    er_coap_channel:close(Channel1),
    er_coap_dtls_socket:close(Sock1),
    timer:sleep(100),
    ?assertNotEqual([], emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>)),

    {ok, Sock2, Channel2} = er_coap_dtls_socket:connect({127, 0, 0, 1}, 5684, [
        {verify, verify_none}
    ]),
    URI1 = emqx_coap_SUITE:compose_uri(
        "coaps://127.0.0.1/ps/coap/test",
        #{
            "clientid" => <<"client2">>,
            "token" => Token
        },
        false
    ),
    Req1 = emqx_coap_SUITE:make_req(post, <<"x">>),
    ?assertMatch({error, unauthorized, _}, emqx_coap_SUITE:do_request(Channel2, URI1, Req1)),

    er_coap_channel:close(Channel2),
    er_coap_dtls_socket:close(Sock2).

t_partial_token_params_rejected(_Config) ->
    {ok, Sock, Channel} = er_coap_dtls_socket:connect({127, 0, 0, 1}, 5684, [
        {verify, verify_none}
    ]),

    Prefix = ?MQTT_PREFIX ++ "/connection",
    Queries0 = #{
        "clientid" => <<"client1">>,
        "username" => <<"admin">>,
        "password" => <<"public">>
    },
    URI0 = emqx_coap_SUITE:compose_uri(Prefix, Queries0, false),
    Req0 = emqx_coap_SUITE:make_req(post),
    {ok, created, Data} = emqx_coap_SUITE:do_request(Channel, URI0, Req0),
    #coap_content{payload = BinToken} = Data,
    Token = binary_to_list(BinToken),

    URI1 = emqx_coap_SUITE:compose_uri(
        "coaps://127.0.0.1/ps/coap/missing_token",
        #{"clientid" => <<"client1">>},
        false
    ),
    Req1 = emqx_coap_SUITE:make_req(post, <<"x">>),
    ?assertMatch({error, bad_request, _}, emqx_coap_SUITE:do_request(Channel, URI1, Req1)),

    URI2 = emqx_coap_SUITE:compose_uri(
        "coaps://127.0.0.1/ps/coap/missing_clientid",
        #{"token" => Token},
        false
    ),
    Req2 = emqx_coap_SUITE:make_req(post, <<"x">>),
    ?assertMatch({error, bad_request, _}, emqx_coap_SUITE:do_request(Channel, URI2, Req2)),

    er_coap_channel:close(Channel),
    er_coap_dtls_socket:close(Sock).

-doc """
A channel that already serves a client refuses a request that names another
client ID, also when the request carries the valid token of that client.  Both
clients keep their channels.
""".
t_connected_channel_refuses_request_for_other_client(_Config) ->
    {ok, Sock1, Channel1} = dtls_connect(),
    {ok, Sock2, Channel2} = dtls_connect(),
    try
        Token1 = connection(Channel1, <<"client1">>),
        Token2 = connection(Channel2, <<"client2">>),
        [Pid1] = emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>),
        [Pid2] = emqx_gateway_cm_registry:lookup_channels(coap, <<"client2">>),
        URI1 = publish_uri(<<"client1">>, Token1),
        URI2 = publish_uri(<<"client2">>, Token2),
        Req = emqx_coap_SUITE:make_req(post, <<"x">>),

        ?assertMatch({error, unauthorized, _}, emqx_coap_SUITE:do_request(Channel2, URI1, Req)),

        ?assertEqual([Pid1], emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>)),
        ?assertEqual([Pid2], emqx_gateway_cm_registry:lookup_channels(coap, <<"client2">>)),
        ?assertMatch(
            #{clientinfo := #{clientid := <<"client2">>}}, emqx_gateway_conn:info(Pid2)
        ),
        ?assertMatch({ok, changed, _}, emqx_coap_SUITE:do_request(Channel1, URI1, Req)),
        ?assertMatch({ok, changed, _}, emqx_coap_SUITE:do_request(Channel2, URI2, Req))
    after
        _ = catch emqx_gateway_cm:kick_session(coap, <<"client1">>),
        _ = catch emqx_gateway_cm:kick_session(coap, <<"client2">>),
        er_coap_channel:close(Channel2),
        er_coap_dtls_socket:close(Sock2),
        er_coap_channel:close(Channel1),
        er_coap_dtls_socket:close(Sock1)
    end.

-doc """
A client that opens a new DTLS session resumes with its client ID and token:
the new channel has no client of its own yet, so it takes the session over.
""".
t_token_resumes_on_new_dtls_session(_Config) ->
    {ok, Sock1, Channel1} = dtls_connect(),
    Token = connection(Channel1, <<"client1">>),
    [Pid1] = emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>),
    er_coap_channel:close(Channel1),
    er_coap_dtls_socket:close(Sock1),

    {ok, Sock2, Channel2} = dtls_connect(),
    try
        Req = emqx_coap_SUITE:make_req(post, <<"x">>),
        ?assertMatch(
            {ok, changed, _},
            emqx_coap_SUITE:do_request(Channel2, publish_uri(<<"client1">>, Token), Req)
        ),
        [Pid2] = emqx_gateway_cm_registry:lookup_channels(coap, <<"client1">>),
        ?assertNotEqual(Pid1, Pid2),
        ?assertMatch(
            #{clientinfo := #{clientid := <<"client1">>}}, emqx_gateway_conn:info(Pid2)
        )
    after
        _ = catch emqx_gateway_cm:kick_session(coap, <<"client1">>),
        er_coap_channel:close(Channel2),
        er_coap_dtls_socket:close(Sock2)
    end.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

dtls_connect() ->
    er_coap_dtls_socket:connect({127, 0, 0, 1}, 5684, [{verify, verify_none}]).

connection(Channel, ClientId) ->
    URI = emqx_coap_SUITE:compose_uri(
        ?MQTT_PREFIX ++ "/connection",
        #{"clientid" => ClientId, "username" => <<"admin">>, "password" => <<"public">>},
        false
    ),
    {ok, created, #coap_content{payload = Token}} =
        emqx_coap_SUITE:do_request(Channel, URI, emqx_coap_SUITE:make_req(post)),
    binary_to_list(Token).

publish_uri(ClientId, Token) ->
    emqx_coap_SUITE:compose_uri(
        "coaps://127.0.0.1/ps/coap/dtls_takeover",
        #{"clientid" => ClientId, "token" => Token},
        false
    ).
