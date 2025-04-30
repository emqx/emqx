%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_nats_protocol_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_nats.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(DEFAULT_CLIENT_OPTS, #{
    host => "localhost",
    port => 4222,
    verbose => false
}).

-define(CONF_DEFAULT, <<
    "\n"
    "gateway.nats {\n"
    "  protocol {\n"
    "    max_payload_size = 1024\n"
    "  }\n"
    "  listeners.tcp.default {\n"
    "    bind = 4222\n"
    "  }\n"
    "}\n"
>>).

%%--------------------------------------------------------------------
%% CT Callbacks
%%--------------------------------------------------------------------

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    application:load(emqx_gateway_nats),
    Apps = emqx_cth_suite:start(
        [
            {emqx_conf, ?CONF_DEFAULT},
            emqx_gateway,
            emqx_auth,
            emqx_management,
            {emqx_dashboard, "dashboard.listeners.http { enable = true, bind = 18083 }"}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    emqx_common_test_http:create_default_app(),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_common_test_http:delete_default_app(),
    emqx_cth_suite:stop(?config(suite_apps, Config)),
    ok.

init_per_testcase(_TestCase, Config) ->
    snabbkaffe:start_trace(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    snabbkaffe:stop(),
    ok.

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------

t_connect(_Config) ->
    ClientOpts = ?DEFAULT_CLIENT_OPTS,
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    {ok, Msgs} = emqx_nats_client:receive_message(Client),
    ?assertMatch([#nats_frame{operation = ?OP_INFO}], Msgs),
    ok = emqx_nats_client:connect(Client),
    ok = emqx_nats_client:ping(Client),
    {ok, Msgs2} = emqx_nats_client:receive_message(Client),
    ?assertMatch([#nats_frame{operation = ?OP_PONG}], Msgs2),
    emqx_nats_client:stop(Client).

t_ping_pong(_Config) ->
    ClientOpts = ?DEFAULT_CLIENT_OPTS,
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    {ok, Msgs} = emqx_nats_client:receive_message(Client),
    ?assertMatch([#nats_frame{operation = ?OP_INFO}], Msgs),
    ok = emqx_nats_client:connect(Client),
    ok = emqx_nats_client:ping(Client),
    {ok, Msgs2} = emqx_nats_client:receive_message(Client),
    ?assertMatch([#nats_frame{operation = ?OP_PONG}], Msgs2),
    emqx_nats_client:stop(Client).

t_subscribe(_Config) ->
    ClientOpts = ?DEFAULT_CLIENT_OPTS,
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    {ok, Msgs} = emqx_nats_client:receive_message(Client),
    ?assertMatch([#nats_frame{operation = ?OP_INFO}], Msgs),
    ok = emqx_nats_client:connect(Client),
    ok = emqx_nats_client:subscribe(Client, <<"foo">>, <<"sid-1">>),
    emqx_nats_client:stop(Client).

t_publish(_Config) ->
    ClientOpts = ?DEFAULT_CLIENT_OPTS,
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    {ok, Msgs} = emqx_nats_client:receive_message(Client),
    ?assertMatch([#nats_frame{operation = ?OP_INFO}], Msgs),
    ok = emqx_nats_client:connect(Client),
    ok = emqx_nats_client:publish(Client, <<"foo">>, <<"hello">>),
    emqx_nats_client:stop(Client).

t_receive_message(_Config) ->
    ClientOpts = ?DEFAULT_CLIENT_OPTS,
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    {ok, Msgs} = emqx_nats_client:receive_message(Client),
    ?assertMatch([#nats_frame{operation = ?OP_INFO}], Msgs),
    ok = emqx_nats_client:connect(Client),
    ok = emqx_nats_client:subscribe(Client, <<"foo">>, <<"sid-1">>),
    ok = emqx_nats_client:publish(Client, <<"foo">>, <<"hello">>),
    {ok, [Msg]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_MSG,
            message = #{
                subject := <<"foo">>,
                sid := <<"sid-1">>,
                payload := <<"hello">>
            }
        },
        Msg
    ),
    emqx_nats_client:stop(Client).

t_unsubscribe(_Config) ->
    ClientOpts = ?DEFAULT_CLIENT_OPTS,
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    {ok, Msgs} = emqx_nats_client:receive_message(Client),
    ?assertMatch([#nats_frame{operation = ?OP_INFO}], Msgs),
    ok = emqx_nats_client:connect(Client),
    ok = emqx_nats_client:subscribe(Client, <<"foo">>, <<"sid-1">>),
    ok = emqx_nats_client:unsubscribe(Client, <<"sid-1">>),
    emqx_nats_client:stop(Client).
