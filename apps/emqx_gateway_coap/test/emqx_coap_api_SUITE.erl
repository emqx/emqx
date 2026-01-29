%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_coap_api_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_coap.hrl").
-include_lib("eunit/include/eunit.hrl").
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
    Config1 = emqx_coap_test_helpers:start_gateway(
        Config,
        emqx_coap_test_helpers:default_conf()
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
    ok = meck:new(emqx_gateway_cm_registry, [passthrough, no_history, no_link]),
    ok = meck:expect(
        emqx_gateway_cm_registry,
        lookup_channels,
        fun(_, _) -> erlang:error(bad_registry) end
    ),
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
    meck:unload(emqx_gateway_cm_registry),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Internal Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start_client() ->
    spawn(fun coap_client/0).

start_silent_client() ->
    spawn(fun coap_silent_client/0).

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
