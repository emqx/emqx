%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_connector_lib_tests).

-include_lib("eunit/include/eunit.hrl").

http_connectivity_ok_test() ->
    {ok, Socket} = gen_tcp:listen(0, []),
    {ok, Port} = inet:port(Socket),
    ?assertEqual(
        ok,
        emqx_connector_lib:http_connectivity("http://127.0.0.1:" ++ integer_to_list(Port), 1000)
    ),
    gen_tcp:close(Socket).

http_connectivity_error_test() ->
    {ok, Socket} = gen_tcp:listen(0, []),
    {ok, Port} = inet:port(Socket),
    ok = gen_tcp:close(Socket),
    ?assertEqual(
        {error, econnrefused},
        emqx_connector_lib:http_connectivity("http://127.0.0.1:" ++ integer_to_list(Port), 1000)
    ).

tcp_connectivity_ok_test() ->
    {ok, Socket} = gen_tcp:listen(0, []),
    {ok, Port} = inet:port(Socket),
    ?assertEqual(
        ok,
        emqx_connector_lib:tcp_connectivity("127.0.0.1", Port, 1000)
    ),
    ok = gen_tcp:close(Socket).

tcp_connectivity_error_test() ->
    {ok, Socket} = gen_tcp:listen(0, []),
    {ok, Port} = inet:port(Socket),
    ok = gen_tcp:close(Socket),
    ?assertEqual(
        {error, econnrefused},
        emqx_connector_lib:tcp_connectivity("127.0.0.1", Port, 1000)
    ).
