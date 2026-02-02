%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_nats_channel_SUITE).

-include("emqx_nats.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-define(CONF_FMT, <<
    "gateway.nats {\n"
    "  default_heartbeat_interval = 2s\n"
    "  heartbeat_wait_timeout = 1s\n"
    "  protocol {\n"
    "    max_payload_size = 1024\n"
    "  }\n"
    "  listeners.tcp.default {\n"
    "    bind = ~p\n"
    "  }\n"
    "  listeners.ssl.default {\n"
    "    bind = ~p\n"
    "    ssl_options {\n"
    "      cacertfile = \"~s\"\n"
    "      certfile = \"~s\"\n"
    "      keyfile = \"~s\"\n"
    "      verify = verify_peer\n"
    "      fail_if_no_peer_cert = true\n"
    "    }\n"
    "  }\n"
    "}\n"
>>).

%%--------------------------------------------------------------------
%% CT Callbacks
%%--------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    application:load(emqx_gateway_nats),
    TcpPort = emqx_common_test_helpers:select_free_port(tcp),
    SslPort = emqx_common_test_helpers:select_free_port(ssl),
    Conf = nats_conf(TcpPort, SslPort),
    Apps = emqx_cth_suite:start(
        [
            {emqx_conf, Conf},
            emqx_gateway,
            emqx_auth,
            emqx_management,
            {emqx_dashboard, "dashboard.listeners.http { enable = true, bind = 18083 }"}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    emqx_common_test_http:create_default_app(),
    RawConf = emqx:get_raw_config([gateway, nats]),
    [
        {suite_apps, Apps},
        {tcp_port, TcpPort},
        {ssl_port, SslPort},
        {raw_conf, RawConf}
        | Config
    ].

end_per_suite(Config) ->
    emqx_common_test_http:delete_default_app(),
    emqx_cth_suite:stop(?config(suite_apps, Config)),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, Config) ->
    cleanup_hooks(),
    allow_pubsub_all(),
    disable_auth(),
    update_nats_with_clientinfo_override(#{}),
    restore_nats_conf(?config(raw_conf, Config)),
    ok.

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------

t_connect_hook_error(Config) ->
    ok = emqx_hooks:put('client.connect', {?MODULE, hook_connect_error, []}, 0),
    ClientOpts = maps:merge(tcp_client_opts(Config), #{verbose => true}),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    recv_info_frame(Client),
    ok = emqx_nats_client:connect(Client),
    {ok, [Err]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(#nats_frame{operation = ?OP_ERR}, Err),
    emqx_nats_client:stop(Client).

t_subscribe_hook_blocked(Config) ->
    ok = emqx_hooks:put('client.subscribe', {?MODULE, hook_subscribe_block, []}, 0),
    ClientOpts = maps:merge(tcp_client_opts(Config), #{verbose => true}),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    recv_info_frame(Client),
    ok = emqx_nats_client:connect(Client),
    recv_ok_frame(Client),
    ok = emqx_nats_client:subscribe(Client, <<"foo">>, <<"sid-1">>),
    {ok, [Err]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(#nats_frame{operation = ?OP_ERR}, Err),
    emqx_nats_client:stop(Client).

t_subscribe_duplicate_sid(Config) ->
    ClientOpts = maps:merge(tcp_client_opts(Config), #{verbose => true}),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    recv_info_frame(Client),
    ok = emqx_nats_client:connect(Client),
    recv_ok_frame(Client),
    ok = emqx_nats_client:subscribe(Client, <<"foo">>, <<"sid-1">>),
    recv_ok_frame(Client),
    ok = emqx_nats_client:subscribe(Client, <<"bar">>, <<"sid-1">>),
    {ok, [Err]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(#nats_frame{operation = ?OP_ERR}, Err),
    emqx_nats_client:stop(Client).

t_subscribe_duplicate_topic(Config) ->
    ClientOpts = maps:merge(tcp_client_opts(Config), #{verbose => true}),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    recv_info_frame(Client),
    ok = emqx_nats_client:connect(Client),
    recv_ok_frame(Client),
    ok = emqx_nats_client:subscribe(Client, <<"foo">>, <<"sid-1">>),
    recv_ok_frame(Client),
    ok = emqx_nats_client:subscribe(Client, <<"foo">>, <<"sid-2">>),
    {ok, [Err]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(#nats_frame{operation = ?OP_ERR}, Err),
    emqx_nats_client:stop(Client).

t_subscribe_before_connect_denied(Config) ->
    ok = enable_auth(),
    ClientOpts = maps:merge(tcp_client_opts(Config), #{verbose => true}),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    recv_info_frame(Client),
    ok = emqx_nats_client:subscribe(Client, <<"foo">>, <<"sid-1">>),
    {ok, [Err]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(#nats_frame{operation = ?OP_ERR}, Err),
    emqx_nats_client:stop(Client).

t_double_connect_no_responders_paths(Config) ->
    ClientOpts = maps:merge(tcp_client_opts(Config), #{verbose => true}),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    recv_info_frame(Client),
    ok = emqx_nats_client:connect(Client),
    recv_ok_frame(Client),

    ok = emqx_nats_client:connect(Client, #{no_responders => true, headers => false}),
    {ok, [Err]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(#nats_frame{operation = ?OP_ERR}, Err),

    ok = emqx_nats_client:connect(Client, #{no_responders => true, headers => true}),
    {ok, [Ok]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(#nats_frame{operation = ?OP_OK}, Ok),
    emqx_nats_client:stop(Client).

t_auth_expire_disconnect(Config) ->
    ok = enable_auth(),
    ExpireAt = erlang:system_time(millisecond) + 200,
    ok = emqx_hooks:put(
        'client.authenticate',
        {?MODULE, hook_auth_expire, [ExpireAt]},
        ?HP_AUTHN
    ),
    ClientOpts = maps:merge(tcp_client_opts(Config), #{verbose => true}),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    recv_info_frame(Client),
    ok = emqx_nats_client:connect(Client),
    recv_ok_frame(Client),
    Err = recv_err_frame(Client, 2000),
    ?assertMatch(#nats_frame{operation = ?OP_ERR}, Err),
    emqx_nats_client:stop(Client).

t_clean_authz_cache(Config) ->
    ClientOpts = maps:merge(tcp_client_opts(Config), #{verbose => true, user => <<"cache_user">>}),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    recv_info_frame(Client),
    ok = emqx_nats_client:connect(Client),
    recv_ok_frame(Client),
    [ClientInfo] = wait_for_client_by_username(<<"cache_user">>),
    ClientId = maps:get(clientid, ClientInfo),
    Pids = emqx_gateway_cm:lookup_by_clientid(nats, ClientId),
    lists:foreach(fun(Pid) -> Pid ! clean_authz_cache end, Pids),
    emqx_nats_client:stop(Client).

t_cast_unexpected(Config) ->
    ClientOpts = maps:merge(tcp_client_opts(Config), #{verbose => true, user => <<"cast_user">>}),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    recv_info_frame(Client),
    ok = emqx_nats_client:connect(Client),
    recv_ok_frame(Client),
    [ClientInfo] = wait_for_client_by_username(<<"cast_user">>),
    ClientId = maps:get(clientid, ClientInfo),
    ok = emqx_gateway_cm:cast(nats, ClientId, unexpected_cast),
    emqx_nats_client:stop(Client).

t_discard_on_duplicate_clientid(Config) ->
    update_nats_with_clientinfo_override(#{<<"clientid">> => <<"fixed-client">>}),
    ClientOpts = maps:merge(tcp_client_opts(Config), #{verbose => true}),
    {ok, Client1} = emqx_nats_client:start_link(ClientOpts),
    recv_info_frame(Client1),
    ok = emqx_nats_client:connect(Client1),
    recv_ok_frame(Client1),

    {ok, Client2} = emqx_nats_client:start_link(ClientOpts),
    recv_info_frame(Client2),
    ok = emqx_nats_client:connect(Client2),
    recv_ok_frame(Client2),

    Err = recv_err_frame(Client1, 2000),
    ?assertMatch(#nats_frame{operation = ?OP_ERR, message = <<"Discarded">>}, Err),
    emqx_nats_client:stop(Client1),
    emqx_nats_client:stop(Client2).

t_tls_info_and_peercert(Config) ->
    update_nats_with_clientinfo_override(#{}),
    ClientOpts = maps:merge(ssl_client_opts(Config), #{verbose => true, user => <<"ssl_user">>}),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    {ok, [InfoMsg]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_INFO,
            message = #{
                <<"tls_required">> := true,
                <<"tls_handshake_first">> := true,
                <<"tls_verify">> := true
            }
        },
        InfoMsg
    ),
    ok = emqx_nats_client:connect(Client),
    recv_ok_frame(Client),
    [ClientInfo] = wait_for_client_by_username(<<"ssl_user">>),
    ClientId = maps:get(clientid, ClientInfo),
    ChanInfo = wait_for_chan_info(ClientId),
    ?assertMatch(#{clientinfo := #{dn := _, cn := _}}, ChanInfo),
    emqx_nats_client:stop(Client).

%%--------------------------------------------------------------------
%% Hook callbacks
%%--------------------------------------------------------------------

hook_connect_error(_ConnInfo, _Acc) ->
    {stop, {error, <<"hook_failed">>}}.

hook_subscribe_block(_ClientInfo, _Props, _Acc) ->
    {stop, []}.

hook_auth_expire(_Credential, _Acc, ExpireAt) ->
    {stop, {ok, #{is_superuser => false, expire_at => ExpireAt}}}.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

nats_conf(TcpPort, SslPort) ->
    Ca = cert_path("cacert.pem"),
    Cert = cert_path("cert.pem"),
    Key = cert_path("key.pem"),
    lists:flatten(io_lib:format(?CONF_FMT, [TcpPort, SslPort, Ca, Cert, Key])).

cert_path(Name) ->
    filename:join([code:lib_dir(emqx), "etc", "certs", Name]).

tcp_client_opts(Config) ->
    #{host => "tcp://localhost", port => ?config(tcp_port, Config)}.

ssl_client_opts(Config) ->
    #{
        host => "ssl://localhost",
        port => ?config(ssl_port, Config),
        ssl_opts => #{
            cacertfile => cert_path("cacert.pem"),
            certfile => cert_path("client-cert.pem"),
            keyfile => cert_path("client-key.pem"),
            verify => verify_none
        }
    }.

recv_ok_frame(Client) ->
    {ok, [Frame]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(#nats_frame{operation = ?OP_OK}, Frame).

recv_info_frame(Client) ->
    {ok, [Frame]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(#nats_frame{operation = ?OP_INFO}, Frame).

find_client_by_username(Username) ->
    ClientInfos = emqx_gateway_test_utils:list_gateway_clients(<<"nats">>),
    lists:filter(fun(ClientInfo) -> maps:get(username, ClientInfo) =:= Username end, ClientInfos).

wait_for_client_by_username(Username) ->
    wait_for_client_by_username(Username, 10).

wait_for_client_by_username(Username, 0) ->
    find_client_by_username(Username);
wait_for_client_by_username(Username, Attempts) ->
    case find_client_by_username(Username) of
        [] ->
            timer:sleep(100),
            wait_for_client_by_username(Username, Attempts - 1);
        Clients ->
            Clients
    end.

wait_for_chan_info(ClientId) ->
    wait_for_chan_info(ClientId, 10).

wait_for_chan_info(_ClientId, 0) ->
    undefined;
wait_for_chan_info(ClientId, Attempts) ->
    case emqx_gateway_cm:lookup_by_clientid(nats, ClientId) of
        [Pid | _] ->
            case emqx_gateway_cm:get_chan_info(nats, ClientId, Pid) of
                undefined ->
                    timer:sleep(100),
                    wait_for_chan_info(ClientId, Attempts - 1);
                Info ->
                    Info
            end;
        [] ->
            timer:sleep(100),
            wait_for_chan_info(ClientId, Attempts - 1)
    end.

recv_err_frame(Client, Timeout) ->
    recv_err_frame(Client, Timeout, erlang:monotonic_time(millisecond)).

recv_err_frame(Client, Timeout, StartMs) ->
    Now = erlang:monotonic_time(millisecond),
    case Now - StartMs > Timeout of
        true ->
            exit(timeout);
        false ->
            case emqx_nats_client:receive_message(Client, 1, 500) of
                {ok, [#nats_frame{operation = ?OP_ERR} = Err]} ->
                    Err;
                {ok, [_Other]} ->
                    recv_err_frame(Client, Timeout, StartMs);
                {ok, []} ->
                    recv_err_frame(Client, Timeout, StartMs)
            end
    end.

enable_auth() ->
    emqx_gateway_test_utils:enable_gateway_auth(<<"nats">>).

disable_auth() ->
    emqx_gateway_test_utils:disable_gateway_auth(<<"nats">>).

allow_pubsub_all() ->
    emqx_gateway_test_utils:update_authz_file_rule(
        <<
            "\n"
            "        {allow,all}.\n"
            "    "
        >>
    ).

update_nats_with_clientinfo_override(ClientInfoOverride) ->
    DefaultOverride = #{
        <<"username">> => <<"${Packet.user}">>,
        <<"password">> => <<"${Packet.pass}">>
    },
    ClientInfoOverride1 = maps:merge(DefaultOverride, ClientInfoOverride),
    Conf = emqx:get_raw_config([gateway, nats]),
    emqx_gateway_conf:update_gateway(
        nats,
        Conf#{<<"clientinfo_override">> => ClientInfoOverride1}
    ).

restore_nats_conf(Conf) ->
    _ = emqx_gateway_conf:update_gateway(nats, Conf),
    ok.

cleanup_hooks() ->
    _ = emqx_hooks:del('client.connect', {?MODULE, hook_connect_error}),
    _ = emqx_hooks:del('client.subscribe', {?MODULE, hook_subscribe_block}),
    _ = emqx_hooks:del('client.authenticate', {?MODULE, hook_auth_expire}),
    ok.
