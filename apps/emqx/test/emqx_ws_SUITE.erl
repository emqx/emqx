%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ws_SUITE).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-define(WS_PORT, 8083).

-define(OP_CONTINUATION, 0).
-define(OP_BINARY, 2).
-define(OP_PING, 9).
-define(OP_PONG, 10).

all() -> emqx_common_test_helpers:all(?MODULE).

%%--------------------------------------------------------------------
%% CT callbacks
%%--------------------------------------------------------------------

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [{emqx, #{config => "listeners.ws.default.websocket.compress = true"}}],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

-doc """
A compressed CONNECT split into fragments completes with a CONNACK, and the pong
for a ping carries the ping payload.
""".
t_pong_payload_echo(_Config) ->
    {Sock, Head} = raw_ws_open("Sec-WebSocket-Extensions: permessage-deflate\r\n"),
    ?assertMatch(
        <<"permessage-deflate", _/binary>>,
        response_header(Head, <<"sec-websocket-extensions">>)
    ),
    Connect = connect_packet(),
    [FirstFragment, Continuation] = compress_segments(Connect),
    ok = gen_tcp:send(Sock, masked_frame(0, 1, ?OP_BINARY, FirstFragment)),
    PingPayload = <<"ping">>,
    ok = gen_tcp:send(Sock, masked_frame(1, 0, ?OP_PING, PingPayload)),
    ?assertMatch({1, 0, ?OP_PONG, PingPayload}, recv_frame(Sock)),
    ok = gen_tcp:send(Sock, masked_frame(1, 0, ?OP_CONTINUATION, Continuation)),
    {1, 4, ?OP_BINARY, CompressedConnack} = recv_frame(Sock),
    ?assertMatch(
        #mqtt_packet{
            header = #mqtt_packet_header{type = ?CONNACK},
            variable = #mqtt_packet_connack{ack_flags = 0, reason_code = 0}
        },
        parse_packet(inflate(CompressedConnack))
    ),
    %% No more frames arrive on the idle connection.
    ?assertEqual({error, timeout}, gen_tcp:recv(Sock, 0, 500)),
    gen_tcp:close(Sock).

connect_packet() ->
    Connect = ?CONNECT_PACKET(#mqtt_packet_connect{
        proto_name = <<"MQTT">>,
        proto_ver = ?MQTT_PROTO_V4,
        clientid = <<"ws_pong_payload_echo">>
    }),
    iolist_to_binary(emqx_frame:serialize(Connect)).

parse_packet(Frame) ->
    {Packet, <<>>, _ParseState} = emqx_frame:parse(Frame),
    Packet.

%% Build permessage-deflate wire data for two chunks, using one sync flush per
%% chunk and dropping the trailing sync marker as RFC 7692 requires.
compress_segments(Connect) ->
    SplitAt = 8,
    Chunks = [
        binary:part(Connect, 0, SplitAt),
        binary:part(Connect, SplitAt, byte_size(Connect) - SplitAt)
    ],
    Z = zlib:open(),
    try
        zlib:deflateInit(Z, default, deflated, -15, 8, default),
        Encoded = [iolist_to_binary(zlib:deflate(Z, Chunk, sync)) || Chunk <- Chunks],
        Last = lists:last(Encoded),
        lists:droplast(Encoded) ++ [binary:part(Last, 0, byte_size(Last) - 4)]
    after
        zlib:close(Z)
    end.

%%--------------------------------------------------------------------
%% Minimal WebSocket client: gen_tcp handshake and frame codec.
%%--------------------------------------------------------------------

raw_ws_open(ExtraHeaders) ->
    {ok, Sock} = gen_tcp:connect("127.0.0.1", ?WS_PORT, [binary, {active, false}], 5000),
    ok = gen_tcp:send(Sock, [
        "GET /mqtt HTTP/1.1\r\n",
        "Host: 127.0.0.1:",
        integer_to_list(?WS_PORT),
        "\r\n",
        "Upgrade: websocket\r\n",
        "Connection: Upgrade\r\n",
        "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n",
        "Sec-WebSocket-Version: 13\r\n",
        "Sec-WebSocket-Protocol: mqtt\r\n",
        ExtraHeaders,
        "\r\n"
    ]),
    Head = recv_http_head(Sock, <<>>),
    ?assertMatch(<<"HTTP/1.1 101 ", _/binary>>, Head),
    {Sock, Head}.

recv_http_head(Sock, Acc0) ->
    case binary:match(Acc0, <<"\r\n\r\n">>) of
        nomatch ->
            Data = recv_any(Sock),
            recv_http_head(Sock, <<Acc0/binary, Data/binary>>);
        _ ->
            Acc0
    end.

response_header(Head, Name) ->
    Pattern = <<Name/binary, ": *([^\\r\\n]+)">>,
    case re:run(Head, Pattern, [caseless, {capture, [1], binary}]) of
        {match, [Value]} -> Value;
        nomatch -> ct:fail({header_not_found, Name, Head})
    end.

%% A client frame with a zero masking key, so the payload is sent unchanged.
masked_frame(Fin, Rsv1, Opcode, Payload) when byte_size(Payload) =< 125 ->
    Len = byte_size(Payload),
    <<Fin:1, Rsv1:1, 0:2, Opcode:4, 1:1, Len:7, 0:32, Payload/binary>>.

%% Server frames are never masked, and only short frames are expected here.
recv_frame(Sock) ->
    <<Fin:1, Rsv:3, Opcode:4, 0:1, Len:7>> = recv_exact(Sock, 2),
    true = Len =< 125,
    {Fin, Rsv, Opcode, recv_payload(Sock, Len)}.

recv_payload(_Sock, 0) ->
    <<>>;
recv_payload(Sock, Len) ->
    recv_exact(Sock, Len).

recv_exact(Sock, Len) ->
    expect_ok(gen_tcp:recv(Sock, Len, 5000)).

recv_any(Sock) ->
    expect_ok(gen_tcp:recv(Sock, 0, 5000)).

expect_ok({ok, Data}) ->
    Data;
expect_ok({error, Reason}) ->
    ct:fail({websocket_recv_failed, Reason}).

inflate(Data) ->
    Z = zlib:open(),
    try
        zlib:inflateInit(Z, -15),
        iolist_to_binary([zlib:inflate(Z, Data), zlib:inflate(Z, <<0, 0, 255, 255>>)])
    after
        zlib:close(Z)
    end.
