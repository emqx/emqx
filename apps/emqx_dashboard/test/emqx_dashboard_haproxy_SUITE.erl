%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_haproxy_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx_dashboard, """
                dashboard.listeners.http {
                    enable = true
                    bind = 18083
                    proxy_header = true          
                }
            """}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)).

t_status(_Config) ->
    ProxyInfo = #{
        version => 1,
        command => proxy,
        transport_family => ipv4,
        transport_protocol => stream,
        src_address => {127, 0, 0, 1},
        src_port => 444,
        dest_address => {192, 168, 0, 1},
        dest_port => 443
    },
    {ok, Socket} = gen_tcp:connect(
        "localhost",
        18083,
        [binary, {active, false}, {packet, raw}]
    ),
    ok = gen_tcp:send(Socket, ranch_proxy_header:header(ProxyInfo)),
    {ok, #{token := Token}} = emqx_dashboard_admin:sign_token(<<"admin">>, <<"public">>),
    ok = gen_tcp:send(
        Socket,
        "GET /status HTTP/1.1\r\n"
        "Host: localhost\r\n"
        "Authorization: Bearer " ++ binary_to_list(Token) ++
            "\r\n"
            "\r\n"
    ),
    {_, 200, _, Rest0} = cow_http:parse_status_line(raw_recv_head(Socket)),
    {Headers, Body0} = cow_http:parse_headers(Rest0),
    {_, LenBin} = lists:keyfind(<<"content-length">>, 1, Headers),
    Len = binary_to_integer(LenBin),
    Body =
        if
            byte_size(Body0) =:= Len ->
                Body0;
            true ->
                {ok, Body1} = gen_tcp:recv(Socket, Len - byte_size(Body0), 5000),
                <<Body0/bits, Body1/bits>>
        end,
    ?assertMatch({match, _}, re:run(Body, "Node .+ is started\nemqx is running")),
    ok.

raw_recv_head(Socket) ->
    {ok, Data} = gen_tcp:recv(Socket, 0, 10000),
    raw_recv_head(Socket, Data).

raw_recv_head(Socket, Buffer) ->
    case binary:match(Buffer, <<"\r\n\r\n">>) of
        nomatch ->
            {ok, Data} = gen_tcp:recv(Socket, 0, 10000),
            raw_recv_head(Socket, <<Buffer/binary, Data/binary>>);
        {_, _} ->
            Buffer
    end.
