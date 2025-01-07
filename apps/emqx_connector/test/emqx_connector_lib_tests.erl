%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
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
