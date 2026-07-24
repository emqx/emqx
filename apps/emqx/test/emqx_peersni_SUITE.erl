%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_peersni_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/emqx_schema.hrl").
-include_lib("common_test/include/ct.hrl").

%%--------------------------------------------------------------------
%% setups
%%--------------------------------------------------------------------

all() ->
    [
        {group, tcp_ppv2},
        {group, tcp_socket_ppv2},
        {group, ws_ppv2},
        {group, ssl},
        {group, wss}
    ].

groups() ->
    TCs = emqx_common_test_helpers:all(?MODULE),
    [
        {tcp_ppv2, [], TCs},
        {tcp_socket_ppv2, [], TCs},
        {ws_ppv2, [], TCs},
        {ssl, [], TCs},
        {wss, [], TCs}
    ].

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [{emqx, #{}}],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(proplists:get_value(apps, Config)).

init_per_group(tcp_ppv2, Config) ->
    Port = start_ppv2_listener(tcp, ppv2, #{<<"tcp_backend">> => <<"gen_tcp">>}),
    [
        {listener, {tcp, ppv2}},
        {listener_port, Port}
        | Config
    ];
init_per_group(tcp_socket_ppv2, Config) ->
    Port = start_ppv2_listener(tcp, ppv2, #{<<"tcp_backend">> => <<"socket">>}),
    [
        {listener, {tcp, ppv2}},
        {listener_port, Port}
        | Config
    ];
init_per_group(ws_ppv2, Config) ->
    Port = start_ppv2_listener(ws, ppv2),
    ok = meck_recv_proxy_header(),
    [
        {listener, {ws, ppv2}},
        {listener_port, Port}
        | Config
    ];
init_per_group(ssl, Config) ->
    [
        {listener, {ssl, default}},
        {listener_port, 8883}
        | Config
    ];
init_per_group(wss, Config) ->
    [
        {listener, {wss, default}},
        {listener_port, 8084}
        | Config
    ];
init_per_group(_, Config) ->
    Config.

end_per_group(tcp_ppv2, Config) ->
    stop_ppv2_listener(?config(listener, Config));
end_per_group(ws_ppv2, Config) ->
    stop_ppv2_listener(?config(listener, Config)),
    unmeck_recv_proxy_header();
end_per_group(_, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% cases
%%--------------------------------------------------------------------

t_peersni_saved_into_conninfo(Config) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    ServerName = <<"peersni.saved.into.conninfo">>,
    {ConnMod, Fun, ClientOpts} = mk_client(ClientId, ServerName, Config),

    persistent_term:put(current_client_sni, ServerName),

    {ok, Client} = emqtt:start_link(ClientOpts),
    {ok, _Props} = ConnMod:Fun(Client),
    ?assertMatch(#{clientinfo := #{peersni := ServerName}}, get_chan_info(ClientId)),

    persistent_term:erase(current_client_sni),
    emqtt:disconnect(Client).

t_parse_peersni_to_client_attr(Config) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    ServerName = <<"clientattr.peersni">>,
    {ConnMod, Fun, ClientOpts} = mk_client(ClientId, ServerName, Config),

    persistent_term:put(current_client_sni, ServerName),

    %% set the peersni to the client attribute
    {ok, Variform} = emqx_variform:compile("nth(1, tokens(peersni, '.'))"),
    emqx_config:put([mqtt, client_attrs_init], [
        #{expression => Variform, set_as_attr => tns}
    ]),

    {ok, Client} = emqtt:start_link(ClientOpts),
    {ok, _Props} = ConnMod:Fun(Client),
    ?assertMatch(
        #{clientinfo := #{client_attrs := #{tns := <<"clientattr">>}}},
        get_chan_info(ClientId)
    ),

    persistent_term:erase(current_client_sni),
    emqx_config:put([mqtt, client_attrs_init], []),
    emqtt:disconnect(Client).

mk_client(ClientId, SNI, Config) ->
    Port = ?config(listener_port, Config),
    Listener = ?config(listener, Config),
    Opts = maps:merge(
        #{
            host => "127.0.0.1",
            port => Port,
            proto_ver => v5,
            connect_timeout => 5,
            clientid => ClientId
        },
        listener_client_opts(Listener, SNI)
    ),
    {ConnMod, Fun} = listener_client_mf(Listener),
    {ConnMod, Fun, Opts}.

listener_client_opts({tcp, ppv2}, SNI) ->
    #{tcp_opts => [{pp2_authority, SNI}]};
listener_client_opts({ws, ppv2}, _SNI) ->
    #{};
listener_client_opts({ssl, _}, SNI) ->
    #{
        ssl => true,
        ssl_opts => [
            {verify, verify_none},
            {server_name_indication, binary_to_list(SNI)}
        ]
    };
listener_client_opts({wss, _}, SNI) ->
    #{
        %% NOTE: Supplying SNI in the "Host" header is crucial here.
        ws_headers => [{<<"host">>, SNI}],
        ws_transport_options => [
            {transport, tls},
            {protocols, [http]},
            {tls_opts, [
                {verify, verify_none},
                {server_name_indication, binary_to_list(SNI)},
                {customize_hostname_check, []}
            ]}
        ]
    }.

listener_client_mf({tcp, ppv2}) ->
    {?MODULE, connect};
listener_client_mf({ws, _}) ->
    {emqtt, ws_connect};
listener_client_mf({ssl, _}) ->
    {emqtt, connect};
listener_client_mf({wss, _}) ->
    {emqtt, ws_connect}.

get_chan_info(ClientId) ->
    ?retry(
        3_000,
        100,
        #{} = emqx_cm:get_chan_info(ClientId)
    ).

%%--------------------------------------------------------------------
%% helpers
%%--------------------------------------------------------------------

start_ppv2_listener(Type, Name) ->
    start_ppv2_listener(Type, Name, #{}).

start_ppv2_listener(Type, Name, RawConfOverride) ->
    Port = emqx_common_test_helpers:select_free_port(tcp),
    RawConf = maps:merge(
        emqx:get_raw_config([listeners, Type, default]),
        maps:merge(
            #{
                <<"bind">> => format_bind({"127.0.0.1", Port}),
                <<"proxy_protocol">> => true
            },
            RawConfOverride
        )
    ),
    ?assertMatch({ok, _}, emqx:update_config([listeners, Type, Name], {create, RawConf})),
    Port.

stop_ppv2_listener({Type, Name}) ->
    {ok, _} = emqx:update_config([listeners, Type, Name], ?TOMBSTONE_CONFIG_CHANGE_REQ).

%%

meck_recv_proxy_header() ->
    ok = meck:new(ranch_tcp, [passthrough, no_history, no_link]),
    ok = meck:expect(
        ranch_tcp,
        recv_proxy_header,
        fun(Socket, _Timeout) ->
            SNI = persistent_term:get(current_client_sni, undefined),
            {ok, {SrcAddr, SrcPort}} = esockd_transport:peername(Socket),
            {ok, {DstAddr, DstPort}} = esockd_transport:sockname(Socket),
            {ok, #{
                authority => SNI,
                command => proxy,
                dest_address => DstAddr,
                dest_port => DstPort,
                src_address => SrcAddr,
                src_port => SrcPort,
                transport_family => ipv4,
                transport_protocol => stream,
                version => 2
            }}
        end
    ).

unmeck_recv_proxy_header() ->
    ok = meck:unload(ranch_tcp).

%% emqtt connect module

connect(Client) ->
    gen_statem:call(Client, {connect, ?MODULE}).

connect(Host, Port, SockOpts0, Timeout) ->
    {SNI, SockOpts} = take_pp2_authority(SockOpts0),
    case emqtt_sock:connect(Host, Port, SockOpts, Timeout) of
        {ok, Sock} ->
            ok = emqtt_sock:send(Sock, pp2_header(Sock, SNI)),
            {ok, Sock};
        Error ->
            Error
    end.

send(Sock, Data) ->
    emqtt_sock:send(Sock, Data).

recv(Sock, Length) ->
    emqtt_sock:recv(Sock, Length).

close(Sock) ->
    emqtt_sock:close(Sock).

sockname(Sock) ->
    emqtt_sock:sockname(Sock).

setopts(Sock, Opts) ->
    emqtt_sock:setopts(Sock, Opts).

getstat(Sock, Options) ->
    emqtt_sock:getstat(Sock, Options).

take_pp2_authority(SockOpts) ->
    case lists:keytake(pp2_authority, 1, SockOpts) of
        {value, {pp2_authority, SNI}, Rest} ->
            {SNI, Rest};
        false ->
            {undefined, SockOpts}
    end.

pp2_header(Sock, SNI) ->
    {ok, {SrcAddr, SrcPort}} = inet:sockname(Sock),
    {ok, {DstAddr, DstPort}} = inet:peername(Sock),
    Addr = pp2_ipv4_addrs(SrcAddr, DstAddr, SrcPort, DstPort),
    TLVs = pp2_authority_tlv(SNI),
    Len = byte_size(Addr) + byte_size(TLVs),
    %% esockd first consumes the leading CRLF marker, then parses the full v2 header.
    <<"\r\n", "\r\n\0\r\nQUIT\n", 16#21, 16#11, Len:16, Addr/binary, TLVs/binary>>.

pp2_ipv4_addrs({A, B, C, D}, {W, X, Y, Z}, SrcPort, DstPort) ->
    <<A:8, B:8, C:8, D:8, W:8, X:8, Y:8, Z:8, SrcPort:16, DstPort:16>>.

pp2_authority_tlv(undefined) ->
    <<>>;
pp2_authority_tlv(SNI) when is_binary(SNI) ->
    Len = byte_size(SNI),
    <<16#02, Len:16, SNI/binary>>.

format_bind(Bind) ->
    iolist_to_binary(emqx_listeners:format_bind(Bind)).
