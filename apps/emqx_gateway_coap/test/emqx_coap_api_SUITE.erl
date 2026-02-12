%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_coap_api_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_coap.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").

-define(HOST, "127.0.0.1").
-define(PORT, 5683).
-define(CONN_URI,
    "coap://127.0.0.1/mqtt/connection?clientid=client1&"
    "username=admin&password=public"
).

-define(LOGT(Format, Args), ct:pal("TEST_SUITE: " ++ Format, Args)).

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Conf = <<
        (emqx_coap_test_helpers:default_conf())/binary,
        "\n",
        "gateway.coap.blockwise { max_body_size = \"1KB\" }\n"
    >>,
    Config1 = emqx_coap_test_helpers:start_gateway(
        Config,
        Conf
    ),
    _ = emqx_common_test_http:create_default_app(),
    Config1.

end_per_suite(Config) ->
    emqx_coap_test_helpers:stop_gateway(Config).

%%--------------------------------------------------------------------
%% Cases
%%--------------------------------------------------------------------
t_send_request_api(_) ->
    ClientId = start_client(),
    timer:sleep(200),
    Test = fun(API) ->
        Path = emqx_mgmt_api_test_util:api_path([API]),
        Token = <<"atoken">>,
        Payload = <<"simple echo this">>,
        Req = #{
            token => Token,
            payload => Payload,
            timeout => <<"10s">>,
            content_type => <<"text/plain">>,
            method => <<"get">>
        },
        Auth = emqx_mgmt_api_test_util:auth_header_(),
        {ok, Response} = emqx_mgmt_api_test_util:request_api(
            post,
            Path,
            "method=get",
            Auth,
            Req
        ),
        #{<<"token">> := RToken, <<"payload">> := RPayload} =
            emqx_utils_json:decode(Response),
        ?assertEqual(Token, RToken),
        ?assertEqual(Payload, RPayload)
    end,
    Test("gateways/coap/clients/client1/request"),
    erlang:exit(ClientId, kill),
    ok.

t_send_request_api_block2(_) ->
    ClientId = start_block2_client(),
    timer:sleep(200),
    Path = emqx_mgmt_api_test_util:api_path(
        ["gateways/coap/clients/client1/request"]
    ),
    Req = #{
        token => <<"btoken">>,
        payload => <<"hello">>,
        timeout => <<"10s">>,
        content_type => <<"text/plain">>,
        method => <<"get">>
    },
    Auth = emqx_mgmt_api_test_util:auth_header_(),
    {ok, Response} = emqx_mgmt_api_test_util:request_api(
        post,
        Path,
        "method=get",
        Auth,
        Req
    ),
    #{<<"payload">> := Payload} = emqx_utils_json:decode(Response),
    ?assertEqual(block2_payload(), Payload),
    erlang:exit(ClientId, kill),
    ok.

t_send_request_api_block2_too_large(_) ->
    ClientId = start_block2_client(block2_large_payload()),
    timer:sleep(200),
    Path = emqx_mgmt_api_test_util:api_path(
        ["gateways/coap/clients/client1/request"]
    ),
    Req = #{
        token => <<"btlrg">>,
        payload => <<"hello">>,
        timeout => <<"10s">>,
        content_type => <<"text/plain">>,
        method => <<"get">>
    },
    Auth = emqx_mgmt_api_test_util:auth_header_(),
    {ok, Response} = emqx_mgmt_api_test_util:request_api(
        post,
        Path,
        "method=get",
        Auth,
        Req
    ),
    #{<<"method">> := Method} = emqx_utils_json:decode(Response),
    ?assertEqual(<<"{error,request_entity_too_large}">>, Method),
    erlang:exit(ClientId, kill),
    ok.

t_send_request_api_block2_timeout_followup_fake_channel(_) ->
    ClientId = <<"fake_timeout">>,
    Token = <<"ftok">>,
    Resp0 = #coap_message{
        type = ack,
        method = {ok, content},
        token = Token,
        payload = <<"part">>,
        options = #{block2 => {0, true, 16}}
    },
    {Pid, Cleanup} = start_fake_channel(ClientId, [Resp0]),
    try
        Body = #{
            <<"method">> => get,
            <<"token">> => Token,
            <<"payload">> => <<>>,
            <<"timeout">> => 50,
            <<"content_type">> => 'text/plain'
        },
        {504, #{code := 'CLIENT_NOT_RESPONSE'}} =
            emqx_coap_api:request(post, #{bindings => #{clientid => ClientId}, body => Body}),
        ok
    after
        Cleanup(),
        erlang:exit(Pid, kill)
    end.

t_send_request_api_block2_other_followup_fake_channel(_) ->
    ClientId = <<"fake_other">>,
    Token = <<"fotok">>,
    Resp0 = #coap_message{
        type = ack,
        method = {ok, content},
        token = Token,
        payload = <<"part">>,
        options = #{block2 => {0, true, 16}}
    },
    {Pid, Cleanup} = start_fake_channel(ClientId, [Resp0, {error, fake_reply}]),
    try
        Body = #{
            <<"method">> => get,
            <<"token">> => Token,
            <<"payload">> => <<>>,
            <<"timeout">> => 50,
            <<"content_type">> => 'text/plain'
        },
        {502, #{code := 'CLIENT_BAD_RESPONSE'}} =
            emqx_coap_api:request(post, #{bindings => #{clientid => ClientId}, body => Body})
    after
        Cleanup(),
        erlang:exit(Pid, kill)
    end.

t_send_request_api_block2_invalid_followup_option_fake_channel(_) ->
    ClientId = <<"fake_invalid_block2_option">>,
    Token = <<"fivopt">>,
    Resp0 = #coap_message{
        type = ack,
        method = {ok, content},
        token = Token,
        payload = <<"part1">>,
        options = #{block2 => {0, true, 16}}
    },
    Resp1 = #coap_message{
        type = ack,
        method = {ok, content},
        token = Token,
        payload = <<"part2">>,
        options = #{}
    },
    {Pid, Cleanup} = start_fake_channel(ClientId, [Resp0, Resp1]),
    try
        Body = #{
            <<"method">> => get,
            <<"token">> => Token,
            <<"payload">> => <<>>,
            <<"timeout">> => 50,
            <<"content_type">> => 'text/plain'
        },
        {502, #{code := 'CLIENT_BAD_RESPONSE'}} =
            emqx_coap_api:request(post, #{bindings => #{clientid => ClientId}, body => Body})
    after
        Cleanup(),
        erlang:exit(Pid, kill)
    end.

t_send_request_api_block2_invalid_followup_sequence_fake_channel(_) ->
    ClientId = <<"fake_invalid_block2_sequence">>,
    Token = <<"fivseq">>,
    Resp0 = #coap_message{
        type = ack,
        method = {ok, content},
        token = Token,
        payload = <<"part1">>,
        options = #{block2 => {0, true, 16}}
    },
    Resp1 = #coap_message{
        type = ack,
        method = {ok, content},
        token = Token,
        payload = <<"part2">>,
        options = #{block2 => {2, false, 16}}
    },
    {Pid, Cleanup} = start_fake_channel(ClientId, [Resp0, Resp1]),
    try
        Body = #{
            <<"method">> => get,
            <<"token">> => Token,
            <<"payload">> => <<>>,
            <<"timeout">> => 50,
            <<"content_type">> => 'text/plain'
        },
        {502, #{code := 'CLIENT_BAD_RESPONSE'}} =
            emqx_coap_api:request(post, #{bindings => #{clientid => ClientId}, body => Body})
    after
        Cleanup(),
        erlang:exit(Pid, kill)
    end.

t_send_request_api_block2_respects_disable_config(_) ->
    KeyPath = [gateway, coap, blockwise, enable],
    OldValue = emqx_config:find(KeyPath),
    ok = emqx_config:force_put(KeyPath, false),
    try
        ClientId = <<"fake_block2_disabled">>,
        Token = <<"fbdtok">>,
        Resp0 = #coap_message{
            type = ack,
            method = {ok, content},
            token = Token,
            payload = <<"part">>,
            options = #{block2 => {0, true, 16}}
        },
        {Pid, Cleanup} = start_fake_channel(ClientId, [Resp0]),
        try
            Body = #{
                <<"method">> => get,
                <<"token">> => Token,
                <<"payload">> => <<>>,
                <<"timeout">> => 50,
                <<"content_type">> => 'text/plain'
            },
            {200, #{token := Token, payload := <<"part">>}} =
                emqx_coap_api:request(post, #{bindings => #{clientid => ClientId}, body => Body})
        after
            Cleanup(),
            erlang:exit(Pid, kill)
        end
    after
        case OldValue of
            {ok, Val} ->
                ok = emqx_config:force_put(KeyPath, Val);
            _ ->
                ok = emqx_config:force_put(KeyPath, true)
        end
    end.

t_send_request_api_non_coap_reply(_) ->
    ClientId = <<"fake_non_coap">>,
    Token = <<"nctok">>,
    {Pid, Cleanup} = start_fake_channel(ClientId, [{error, fake_reply}]),
    try
        Body = #{
            <<"method">> => get,
            <<"token">> => Token,
            <<"payload">> => <<>>,
            <<"timeout">> => 50,
            <<"content_type">> => 'text/plain'
        },
        {502, #{code := 'CLIENT_BAD_RESPONSE'}} =
            emqx_coap_api:request(post, #{bindings => #{clientid => ClientId}, body => Body})
    after
        Cleanup(),
        erlang:exit(Pid, kill)
    end.

t_send_request_api_missing_content_type(_) ->
    ClientId = <<"fake_missing_ct">>,
    Body = #{
        <<"method">> => get,
        <<"token">> => <<"mctok">>,
        <<"payload">> => <<>>,
        <<"timeout">> => 50
    },
    {400, #{code := 'BAD_REQUEST', message := <<"missing content_type">>}} =
        emqx_coap_api:request(post, #{bindings => #{clientid => ClientId}, body => Body}).

t_send_request_api_client_not_found(_) ->
    Path = emqx_mgmt_api_test_util:api_path(
        ["gateways/coap/clients/not_found/request"]
    ),
    Req = #{
        token => <<"atoken">>,
        payload => <<"hello">>,
        timeout => <<"50ms">>,
        content_type => <<"text/plain">>,
        method => <<"get">>
    },
    Auth = emqx_mgmt_api_test_util:auth_header_(),
    {error, {{"HTTP/1.1", 404, _}, _Headers, Body}} =
        emqx_mgmt_api_test_util:request_api(
            post,
            Path,
            "method=get",
            Auth,
            Req,
            #{return_all => true}
        ),
    #{<<"code">> := <<"CLIENT_NOT_FOUND">>} = emqx_utils_json:decode(Body),
    ok.

t_send_request_api_timeout(_) ->
    ClientId = start_silent_client(),
    timer:sleep(200),
    Path = emqx_mgmt_api_test_util:api_path(
        ["gateways/coap/clients/client1/request"]
    ),
    Req = #{
        token => <<"atoken">>,
        payload => <<"hello">>,
        timeout => <<"50ms">>,
        content_type => <<"text/plain">>,
        method => <<"get">>
    },
    Auth = emqx_mgmt_api_test_util:auth_header_(),
    {error, {{"HTTP/1.1", 504, _}, _Headers, Body}} =
        emqx_mgmt_api_test_util:request_api(
            post,
            Path,
            "method=get",
            Auth,
            Req,
            #{return_all => true}
        ),
    #{<<"code">> := <<"CLIENT_NOT_RESPONSE">>} = emqx_utils_json:decode(Body),
    erlang:exit(ClientId, kill),
    ok.

t_send_request_api_octet_stream(_) ->
    ClientId = start_client(),
    timer:sleep(200),
    Path = emqx_mgmt_api_test_util:api_path(
        ["gateways/coap/clients/client1/request"]
    ),
    Payload = <<0, 1, 2, 3, 4>>,
    Encoded = base64:encode(Payload),
    Req = #{
        token => <<"atoken">>,
        payload => Encoded,
        timeout => <<"10s">>,
        content_type => <<"application/octet-stream">>,
        method => <<"get">>
    },
    Auth = emqx_mgmt_api_test_util:auth_header_(),
    {ok, Response} = emqx_mgmt_api_test_util:request_api(
        post,
        Path,
        "method=get",
        Auth,
        Req
    ),
    #{<<"payload">> := Encoded} = emqx_utils_json:decode(Response),
    erlang:exit(ClientId, kill),
    ok.

t_send_request_api_exception(_) ->
    Tab = emqx_gateway_cm_registry:tabname(coap),
    {atomic, ok} = mnesia:delete_table(Tab),
    try
        Path = emqx_mgmt_api_test_util:api_path(
            ["gateways/coap/clients/client1/request"]
        ),
        Req = #{
            token => <<"atoken">>,
            payload => <<"hello">>,
            timeout => <<"50ms">>,
            content_type => <<"text/plain">>,
            method => <<"get">>
        },
        Auth = emqx_mgmt_api_test_util:auth_header_(),
        {error, {{"HTTP/1.1", 502, _}, _Headers, Body}} =
            emqx_mgmt_api_test_util:request_api(
                post,
                Path,
                "method=get",
                Auth,
                Req,
                #{return_all => true}
            ),
        #{<<"code">> := <<"CLIENT_BAD_RESPONSE">>} = emqx_utils_json:decode(Body)
    after
        ok = mria:create_table(Tab, [
            {type, bag},
            {rlog_shard, emqx_gateway_cm_shard},
            {storage, ram_copies},
            {record_name, channel},
            {attributes, [chid, pid]},
            {storage_properties, [{ets, [{read_concurrency, true}, {write_concurrency, true}]}]}
        ]),
        ok = mria:wait_for_tables([Tab])
    end,
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Internal Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start_client() ->
    spawn(fun coap_client/0).

start_silent_client() ->
    spawn(fun coap_silent_client/0).

start_block2_client() ->
    start_block2_client(block2_payload()).

start_block2_client(Payload) ->
    spawn(fun() -> coap_block2_client(Payload) end).

start_fake_channel(ClientId, Responses) ->
    Pid = spawn(fun() -> fake_channel_loop(Responses) end),
    ok = emqx_gateway_cm_registry:register_channel(coap, {ClientId, Pid}),
    Cleanup = fun() -> emqx_gateway_cm_registry:unregister_channel(coap, {ClientId, Pid}) end,
    {Pid, Cleanup}.

fake_channel_loop(Responses) ->
    receive
        {_Tag, From, {send_request, _Msg}} ->
            case Responses of
                [Resp | Rest] ->
                    gen_server:reply(From, Resp),
                    fake_channel_loop(Rest);
                [] ->
                    fake_channel_loop([])
            end;
        _Other ->
            fake_channel_loop(Responses)
    end.

coap_client() ->
    {ok, CSock} = gen_udp:open(0, [binary, {active, false}]),
    test_send_coap_request(CSock, post, <<>>, [], 1),
    Response = test_recv_coap_response(CSock),
    ?assertEqual({ok, created}, Response#coap_message.method),
    echo_loop(CSock).

echo_loop(CSock) ->
    #coap_message{payload = Payload} = Req = test_recv_coap_request(CSock),
    test_send_coap_response(CSock, ?HOST, ?PORT, {ok, content}, Payload, Req),
    echo_loop(CSock).

coap_silent_client() ->
    {ok, CSock} = gen_udp:open(0, [binary, {active, false}]),
    test_send_coap_request(CSock, post, <<>>, [], 1),
    Response = test_recv_coap_response(CSock),
    ?assertEqual({ok, created}, Response#coap_message.method),
    silent_loop(CSock).

coap_block2_client(FullPayload) ->
    {ok, CSock} = gen_udp:open(0, [binary, {active, false}]),
    test_send_coap_request(CSock, post, <<>>, [], 1),
    Response = test_recv_coap_response(CSock),
    ?assertEqual({ok, created}, Response#coap_message.method),
    block2_loop(CSock, FullPayload).

block2_loop(CSock, FullPayload) ->
    Req0 = test_recv_coap_request(CSock),
    Num =
        case emqx_coap_message:get_option(block2, Req0, undefined) of
            undefined -> 0;
            {N, _M, _Size} -> N
        end,
    test_send_coap_response_block2(CSock, ?HOST, ?PORT, {ok, content}, FullPayload, Req0, Num),
    block2_loop(CSock, FullPayload).

silent_loop(CSock) ->
    _ = gen_udp:recv(CSock, 0, 500),
    silent_loop(CSock).

test_send_coap_request(UdpSock, Method, Content, Options, MsgId) ->
    is_list(Options) orelse error("Options must be a list"),
    case resolve_uri(?CONN_URI) of
        {coap, {IpAddr, Port}, Path, Query} ->
            Request0 = emqx_coap_message:request(
                con,
                Method,
                Content,
                [
                    {uri_path, Path},
                    {uri_query, Query}
                    | Options
                ]
            ),
            Request = Request0#coap_message{id = MsgId},
            ?LOGT("send_coap_request Request=~p", [Request]),
            RequestBinary = emqx_coap_frame:serialize_pkt(Request, undefined),
            ?LOGT(
                "test udp socket send to ~p:~p, data=~p",
                [IpAddr, Port, RequestBinary]
            ),
            ok = gen_udp:send(UdpSock, IpAddr, Port, RequestBinary);
        {SchemeDiff, ChIdDiff, _, _} ->
            error(
                lists:flatten(
                    io_lib:format(
                        "scheme ~ts or ChId ~ts does not match with socket",
                        [SchemeDiff, ChIdDiff]
                    )
                )
            )
    end.

test_recv_coap_response(UdpSock) ->
    {ok, {Address, Port, Packet}} = gen_udp:recv(UdpSock, 0, 2000),
    {ok, Response, _, _} = emqx_coap_frame:parse(Packet, undefined),
    ?LOGT(
        "test udp receive from ~p:~p, data1=~p, Response=~p",
        [Address, Port, Packet, Response]
    ),
    #coap_message{
        type = ack,
        method = Method,
        id = Id,
        token = Token,
        options = Options,
        payload = Payload
    } = Response,
    ?LOGT(
        "receive coap response Method=~p, Id=~p, Token=~p, "
        "Options=~p, Payload=~p",
        [Method, Id, Token, Options, Payload]
    ),
    Response.

test_recv_coap_request(UdpSock) ->
    case gen_udp:recv(UdpSock, 0) of
        {ok, {_Address, _Port, Packet}} ->
            {ok, Request, _, _} = emqx_coap_frame:parse(Packet, undefined),
            #coap_message{
                type = con,
                method = Method,
                id = Id,
                token = Token,
                payload = Payload,
                options = Options
            } = Request,
            ?LOGT(
                "receive coap request Method=~p, Id=~p, "
                "Token=~p, Options=~p, Payload=~p",
                [Method, Id, Token, Options, Payload]
            ),
            Request;
        {error, Reason} ->
            ?LOGT("test_recv_coap_request failed, Reason=~p", [Reason]),
            timeout_test_recv_coap_request
    end.

test_send_coap_response(UdpSock, Host, Port, Code, Content, Request) ->
    is_list(Host) orelse error("Host is not a string"),
    {ok, IpAddr} = inet:getaddr(Host, inet),
    Response0 = emqx_coap_message:piggyback(Code, Content, Request),
    Response = Response0#coap_message{options = #{uri_query => [<<"clientid=client1">>]}},
    ?LOGT("test_send_coap_response Response=~p", [Response]),
    Binary = emqx_coap_frame:serialize_pkt(Response, undefined),
    ok = gen_udp:send(UdpSock, IpAddr, Port, Binary).

block2_payload() ->
    <<"This is a block2 payload from the device.">>.

block2_large_payload() ->
    binary:copy(<<"L">>, 2048).

test_send_coap_response_block2(UdpSock, Host, Port, Code, FullContent, Request, Num) ->
    is_list(Host) orelse error("Host is not a string"),
    {ok, IpAddr} = inet:getaddr(Host, inet),
    Response0 = emqx_coap_message:piggyback(Code, Request),
    Response1 = emqx_coap_message:set_payload_block(
        FullContent, block2, {Num, true, 16}, Response0
    ),
    Block = emqx_coap_message:get_option(block2, Response1, {Num, false, 16}),
    Response2 = emqx_coap_message:set(block2, Block, Response1),
    Response = Response2#coap_message{
        options = maps:put(uri_query, [<<"clientid=client1">>], Response2#coap_message.options)
    },
    ?LOGT("test_send_coap_response_block2 Response=~p", [Response]),
    Binary = emqx_coap_frame:serialize_pkt(Response, undefined),
    ok = gen_udp:send(UdpSock, IpAddr, Port, Binary).

resolve_uri(Uri) ->
    {ok,
        #{
            scheme := Scheme,
            host := Host,
            port := PortNo,
            path := Path
        } = URIMap} = emqx_http_lib:uri_parse(Uri),
    Query = maps:get(query, URIMap, undefined),
    {ok, PeerIP} = inet:getaddr(Host, inet),
    {Scheme, {PeerIP, PortNo}, split_path(Path), split_query(Query)}.

split_path([]) -> [];
split_path([$/]) -> [];
split_path([$/ | Path]) -> split_segments(Path, $/, []).

split_query(undefined) -> #{};
split_query(Path) -> split_segments(Path, $&, []).

split_segments(Path, Char, Acc) ->
    case string:rchr(Path, Char) of
        0 ->
            [make_segment(Path) | Acc];
        N when N > 0 ->
            split_segments(
                string:substr(Path, 1, N - 1),
                Char,
                [make_segment(string:substr(Path, N + 1)) | Acc]
            )
    end.

make_segment(Seg) ->
    list_to_binary(emqx_http_lib:uri_decode(Seg)).

get_path([], Acc) ->
    %?LOGT("get_path Acc=~p", [Acc]),
    Acc;
get_path([{uri_path, Path1} | T], Acc) ->
    %?LOGT("Path=~p, Acc=~p", [Path1, Acc]),
    get_path(T, join_path(Path1, Acc));
get_path([{_, _} | T], Acc) ->
    get_path(T, Acc).

join_path([], Acc) -> Acc;
join_path([<<"/">> | T], Acc) -> join_path(T, Acc);
join_path([H | T], Acc) -> join_path(T, <<Acc/binary, $/, H/binary>>).

sprintf(Format, Args) ->
    lists:flatten(io_lib:format(Format, Args)).
