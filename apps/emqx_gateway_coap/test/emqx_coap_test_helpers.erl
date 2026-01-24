%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_coap_test_helpers).

-export([
    default_conf/0,
    start_gateway/1,
    start_gateway/2,
    stop_gateway/1,
    with_udp_channel/1,
    send_raw/2,
    add_test_hook/2,
    del_test_hook/2,
    hook_capture/4,
    hook_return_error/3
]).

default_conf() ->
    <<
        "\n"
        "gateway.coap\n"
        "{\n"
        "    idle_timeout = 30s\n"
        "    enable_stats = false\n"
        "    mountpoint = \"\"\n"
        "    notify_type = qos\n"
        "    connection_required = true\n"
        "    subscribe_qos = qos1\n"
        "    publish_qos = qos1\n"
        "\n"
        "    listeners.udp.default\n"
        "    {bind = 5683}\n"
        "}\n"
    >>.

start_gateway(Config) ->
    start_gateway(Config, default_conf()).

start_gateway(Config, Conf) ->
    application:load(emqx_gateway_coap),
    GenRpcPort = emqx_common_test_helpers:select_free_port(tcp),
    Apps = emqx_cth_suite:start(
        [
            {gen_rpc, #{
                override_env => [
                    {port_discovery, manual},
                    {tcp_server_port, GenRpcPort},
                    {tcp_client_port, GenRpcPort},
                    {default_client_driver, tcp}
                ]
            }},
            {emqx_conf, Conf},
            emqx_gateway,
            emqx_auth,
            emqx_management,
            {emqx_dashboard, "dashboard.listeners.http { enable = true, bind = 18083 }"}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

stop_gateway(Config) ->
    case proplists:get_value(suite_apps, Config) of
        undefined -> ok;
        Apps -> emqx_cth_suite:stop(Apps)
    end,
    emqx_config:delete_override_conf_files(),
    ok.

with_udp_channel(Fun) ->
    ChId = {{127, 0, 0, 1}, 5683},
    {ok, Sock} = er_coap_udp_socket:start_link(),
    {ok, Channel} = er_coap_udp_socket:get_channel(Sock, ChId),
    Res = Fun(Channel),
    er_coap_channel:close(Channel),
    er_coap_udp_socket:close(Sock),
    Res.

send_raw(Packet, Timeout) ->
    {ok, Sock} = gen_udp:open(0, [binary, {active, false}]),
    ok = gen_udp:send(Sock, {127, 0, 0, 1}, 5683, Packet),
    Res = gen_udp:recv(Sock, 0, Timeout),
    ok = gen_udp:close(Sock),
    case Res of
        {ok, {_Addr, _Port, Data}} ->
            {ok, Data};
        {error, Reason} ->
            {error, Reason}
    end.

add_test_hook(HookPoint, Action) ->
    emqx_hooks:add(HookPoint, Action, 1000).

del_test_hook(HookPoint, Action) ->
    emqx_hooks:del(HookPoint, Action).

hook_capture(_ConnInfo, ConnProps, Pid, HookPoint) ->
    Pid ! {hook_call, HookPoint},
    {ok, ConnProps}.

hook_return_error(_ConnInfo, _ConnProps, Reason) ->
    {ok, {error, Reason}}.
