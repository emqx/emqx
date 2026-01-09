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

    {ok, Sock, Channel} = emqx_coap_dtls_client_socket:connect({127, 0, 0, 1}, 5684, [
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
    emqx_coap_dtls_client_socket:close(Sock).
