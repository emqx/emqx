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

-define(NKEY_ACCOUNT_PREFIX, 16#00).
-define(NKEY_OPERATOR_PREFIX, 16#70).

-define(CONF_FMT,
    ~b"""
gateway.nats {
  default_heartbeat_interval = 2s
  heartbeat_wait_timeout = 1s
  protocol {
    max_payload_size = 1024
  }
  listeners.tcp.default {
    bind = ~p
  }
  listeners.ssl.default {
    bind = ~p
    ssl_options {
      cacertfile = "~s"
      certfile = "~s"
      keyfile = "~s"
      verify = verify_peer
      fail_if_no_peer_cert = true
    }
  }
}
"""
).

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

t_listener_authn_disabled_keeps_mountpoint_and_authn_hooks(Config) ->
    ok = emqx_hooks:put(
        'client.check_authn_complete',
        {?MODULE, hook_authn_complete, [self()]},
        0
    ),
    Username = <<"listener-auth-disabled-user">>,
    update_nats_tcp_listener_authn_and_mountpoint(false, <<"${username}/">>),
    ClientOpts = maps:merge(
        tcp_client_opts(Config),
        #{
            verbose => true,
            user => Username
        }
    ),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    recv_info_frame(Client),
    ok = emqx_nats_client:connect(Client),
    recv_ok_frame(Client),
    [ClientInfo] = wait_for_client_by_username(Username),
    ?assertEqual(<<Username/binary, "/">>, maps:get(mountpoint, ClientInfo)),
    ?assertMatch(
        #{is_anonymous := true, reason_code := success},
        wait_for_authn_complete(Username)
    ),
    emqx_nats_client:stop(Client).

t_internal_token_auth_recomputes_mountpoint(Config) ->
    AuthToken = <<"internal-token">>,
    update_nats_with_internal_authn_and_mountpoint(
        [
            #{
                <<"type">> => <<"token">>,
                <<"token">> => AuthToken
            }
        ],
        <<"${username}/">>
    ),
    ClientOpts = maps:merge(
        tcp_client_opts(Config),
        #{
            verbose => true,
            user => <<"spoofed-user">>,
            auth_token => AuthToken
        }
    ),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    recv_info_frame(Client),
    ok = emqx_nats_client:connect(Client),
    recv_ok_frame(Client),
    [ClientInfo] = wait_for_client_by_username(<<"token">>),
    ?assertEqual(<<"token">>, maps:get(username, ClientInfo)),
    ?assertEqual(<<"token/">>, maps:get(mountpoint, ClientInfo)),
    emqx_nats_client:stop(Client).

t_internal_auth_continue_without_gateway_auth(Config) ->
    disable_auth(),
    update_nats_with_internal_authn_and_mountpoint([], <<"${username}/">>),
    Username = <<"internal-auth-continue-user">>,
    ClientOpts = maps:merge(
        tcp_client_opts(Config),
        #{
            verbose => true,
            user => Username
        }
    ),
    {ok, Client} = emqx_nats_client:start_link(ClientOpts),
    {ok, [InfoMsg]} = emqx_nats_client:receive_message(Client),
    ?assertMatch(
        #nats_frame{
            operation = ?OP_INFO,
            message = #{<<"auth_required">> := false}
        },
        InfoMsg
    ),
    ok = emqx_nats_client:connect(Client),
    recv_ok_frame(Client),
    [ClientInfo] = wait_for_client_by_username(Username),
    ?assertEqual(<<Username/binary, "/">>, maps:get(mountpoint, ClientInfo)),
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

hook_authn_complete(Credential, Result, Parent) ->
    Parent ! {client_check_authn_complete, maps:get(username, Credential, undefined), Result},
    ok.

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

wait_for_authn_complete(Username) ->
    wait_for_authn_complete(Username, 10).

wait_for_authn_complete(_Username, 0) ->
    error(timeout_waiting_for_authn_complete);
wait_for_authn_complete(Username, Attempts) ->
    receive
        {client_check_authn_complete, Username, Result} ->
            Result;
        {client_check_authn_complete, _OtherUsername, _Result} ->
            wait_for_authn_complete(Username, Attempts)
    after 1000 ->
        wait_for_authn_complete(Username, Attempts - 1)
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

update_nats_tcp_listener_authn_and_mountpoint(EnableAuthn, Mountpoint) ->
    Conf = emqx:get_config([gateway, nats]),
    Conf1 = emqx_utils_maps:deep_put([mountpoint], Conf, Mountpoint),
    Conf2 = emqx_utils_maps:deep_put(
        [listeners, tcp, default, enable_authn],
        Conf1,
        EnableAuthn
    ),
    ok =
        case emqx_gateway:update(nats, Conf2) of
            ok -> ok;
            {ok, _} -> ok
        end,
    ok.

update_nats_with_internal_authn_and_mountpoint(InternalAuthn, Mountpoint) ->
    Conf = emqx:get_raw_config([gateway, nats]),
    _ = emqx_gateway_conf:update_gateway(
        nats,
        Conf#{
            <<"internal_authn">> => InternalAuthn,
            <<"mountpoint">> => Mountpoint
        }
    ),
    ok.

cleanup_hooks() ->
    _ = emqx_hooks:del('client.connect', {?MODULE, hook_connect_error}),
    _ = emqx_hooks:del('client.subscribe', {?MODULE, hook_subscribe_block}),
    _ = emqx_hooks:del('client.authenticate', {?MODULE, hook_auth_expire}),
    _ = emqx_hooks:del('client.check_authn_complete', {?MODULE, hook_authn_complete}),
    ok.

%%--------------------------------------------------------------------
%% NKEY Unit Tests
%%--------------------------------------------------------------------

t_nkey_normalize(_Config) ->
    ?assertEqual(<<"UABC">>, emqx_nats_nkey:normalize("uabc")).

t_nkey_decode_public_ok(_Config) ->
    {ok, PubKey} = emqx_nats_nkey:decode_public(nkey_pub()),
    ?assertEqual(32, byte_size(PubKey)).

t_nkey_decode_public_any_ok(_Config) ->
    {ok, AccountPub} = emqx_nats_nkey:decode_public_any(account_nkey()),
    ?assertEqual(32, byte_size(AccountPub)),
    {ok, OperatorPub} = emqx_nats_nkey:decode_public_any(operator_nkey()),
    ?assertEqual(32, byte_size(OperatorPub)).

t_nkey_encode_public_roundtrip(_Config) ->
    {ok, PubKey} = emqx_nats_nkey:decode_public(nkey_pub()),
    Encoded = emqx_nats_nkey:encode_public(PubKey),
    ?assertEqual(emqx_nats_nkey:normalize(nkey_pub()), Encoded).

t_nkey_decode_public_invalid_prefix(_Config) ->
    Bad = replace_first_char(nkey_pub(), $A),
    ?assertEqual({error, invalid_nkey_prefix}, emqx_nats_nkey:decode_public(Bad)).

t_nkey_decode_public_empty_nkey(_Config) ->
    ?assertEqual({error, invalid_nkey_prefix}, emqx_nats_nkey:decode_public(<<>>)),
    ?assertEqual({error, invalid_nkey_prefix}, emqx_nats_nkey:decode_public_any(<<>>)).

t_nkey_decode_public_invalid_base32(_Config) ->
    ?assertEqual(
        {error, invalid_base32_char},
        emqx_nats_nkey:decode_public(<<"U0">>)
    ).

t_nkey_decode_public_invalid_size(_Config) ->
    Short = binary:part(nkey_pub(), 0, byte_size(nkey_pub()) - 2),
    ?assertEqual({error, invalid_nkey_size}, emqx_nats_nkey:decode_public(Short)).

t_nkey_decode_public_invalid_crc(_Config) ->
    Bad = toggle_last_base32_char(nkey_pub()),
    ?assertEqual({error, invalid_nkey_crc}, emqx_nats_nkey:decode_public(Bad)).

t_nkey_decode_public_non_canonical_length(_Config) ->
    %% Reject trailing base32 chars even when decoded payload bytes still match.
    NonCanonical = <<(nkey_pub())/binary, "A">>,
    ?assertEqual({error, invalid_nkey_size}, emqx_nats_nkey:decode_public(NonCanonical)).

t_nkey_decode_public_internal_prefix_mismatch(_Config) ->
    %% Keep first 5 bits as "U", but use an invalid internal prefix byte.
    Payload = <<16#A1, 0:256>>,
    Crc = emqx_nats_nkey:crc16_xmodem(Payload),
    BadNKey = emqx_nats_nkey:base32_encode(<<Payload/binary, Crc:16/little-unsigned>>),
    ?assertMatch(<<$U, _/binary>>, BadNKey),
    ?assertEqual({error, invalid_nkey_prefix}, emqx_nats_nkey:decode_public(BadNKey)).

t_nkey_base32_encode_padding_branch(_Config) ->
    %% Exercise the partial-bit branch in base32 encoding helper.
    ?assertEqual(<<"A">>, emqx_nats_nkey:base32_encode(<<0:3>>)).

t_nkey_verify_signature_ok(_Config) ->
    Nonce = <<"nonce">>,
    Sig = nkey_sig(Nonce),
    {ok, _} = emqx_nats_nkey:verify_signature(nkey_pub(), Sig, Nonce).

t_nkey_verify_signature_bad_sig(_Config) ->
    Nonce = <<"nonce">>,
    Sig = nkey_sig(Nonce),
    BadSig = mutate_sig(Sig),
    ?assertEqual(
        {error, invalid_nkey_sig},
        emqx_nats_nkey:verify_signature(nkey_pub(), BadSig, Nonce)
    ).

t_nkey_verify_signature_bad_sig_format(_Config) ->
    ?assertEqual(
        {error, invalid_nkey_sig_format},
        emqx_nats_nkey:verify_signature(nkey_pub(), <<"***">>, <<"nonce">>)
    ).

t_nkey_verify_signature_standard_base64(_Config) ->
    Nonce = <<"nonce">>,
    %% "////" is valid standard base64 but invalid urlsafe.
    StdSig = <<"////">>,
    ?assertEqual(
        {error, invalid_nkey_sig},
        emqx_nats_nkey:verify_signature(nkey_pub(), StdSig, Nonce)
    ).

t_nkey_verify_signature_bad_nkey(_Config) ->
    Nonce = <<"nonce">>,
    Sig = nkey_sig(Nonce),
    BadNKey = replace_first_char(nkey_pub(), $A),
    ?assertEqual(
        {error, invalid_nkey_prefix},
        emqx_nats_nkey:verify_signature(BadNKey, Sig, Nonce)
    ).

%%--------------------------------------------------------------------
%% NKEY Helpers
%%--------------------------------------------------------------------

nkey_pub() ->
    <<"UB4G32YJ2GVZG3KTC3Z7BLIU3PXPJC2Y4QF6SNJUN2XIF3M3E3NDEUCZ">>.

nkey_priv() ->
    <<205, 42, 56, 73, 83, 88, 159, 152, 35, 244, 15, 34, 196, 39, 226, 60, 111, 109, 0, 79, 72,
        148, 60, 239, 181, 139, 118, 231, 215, 12, 158, 116>>.

nkey_sig(Nonce) ->
    Sig = crypto:sign(eddsa, none, Nonce, [nkey_priv(), ed25519]),
    base64:encode(Sig, #{mode => urlsafe, padding => false}).

account_nkey() ->
    public_nkey(?NKEY_ACCOUNT_PREFIX, jwt_account_priv()).

operator_nkey() ->
    public_nkey(?NKEY_OPERATOR_PREFIX, jwt_operator_priv()).

public_nkey(Prefix, PrivateKey) ->
    {PubKey, _} = crypto:generate_key(eddsa, ed25519, PrivateKey),
    Payload = <<Prefix:8, PubKey/binary>>,
    Crc = emqx_nats_nkey:crc16_xmodem(Payload),
    emqx_nats_nkey:base32_encode(<<Payload/binary, Crc:16/little-unsigned>>).

jwt_operator_priv() ->
    <<1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
        27, 28, 29, 30, 31, 32>>.

jwt_account_priv() ->
    <<33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55,
        56, 57, 58, 59, 60, 61, 62, 63, 64>>.

replace_first_char(<<_Old, Rest/binary>>, New) ->
    <<New, Rest/binary>>.

toggle_last_base32_char(Bin) ->
    <<Head:(byte_size(Bin) - 1)/binary, Last>> = Bin,
    NewLast =
        case Last of
            $A -> $B;
            _ -> $A
        end,
    <<Head/binary, NewLast>>.

mutate_sig(Sig) ->
    SigBin = base64:decode(Sig, #{mode => urlsafe, padding => false}),
    <<First:8, Rest/binary>> = SigBin,
    BadSigBin = <<(First bxor 1), Rest/binary>>,
    base64:encode(BadSigBin, #{mode => urlsafe, padding => false}).
