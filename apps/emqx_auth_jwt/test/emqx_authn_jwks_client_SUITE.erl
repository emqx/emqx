%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authn_jwks_client_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(inets),
    Config.

end_per_suite(_Config) ->
    ok.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

%% Asserts that the JWKS HTTP request does not emit an empty `te:' header on
%% the wire. Erlang/OTP `inets' versions before 9.4.2 (OTP 28.1) violate
%% RFC 2616 by sending an empty `TE' header value, which strict gateways such
%% as PingFederate reject with 503 or TCP RST. The client must inject an
%% explicit `te: trailers' to suppress the broken default.
t_no_empty_te_header(_Config) ->
    Port = emqx_common_test_helpers:select_free_port(tcp),
    Parent = self(),
    {Pid, Ref} = spawn_monitor(fun() -> raw_http_capture(Port, Parent) end),
    receive
        {listening, Pid} -> ok
    after 2000 ->
        ct:fail(listener_not_ready)
    end,
    Endpoint = "http://127.0.0.1:" ++ integer_to_list(Port) ++ "/jwks.json",
    Opts = #{
        endpoint => Endpoint,
        headers => #{<<"accept">> => "application/json"},
        refresh_interval => 1000,
        ssl => #{enable => false}
    },
    {ok, ClientPid} = emqx_authn_jwks_client:start_link(Opts),
    Bytes =
        receive
            {captured, Pid, B} -> B
        after 5000 ->
            ct:fail(no_capture)
        end,
    receive
        {'DOWN', Ref, process, Pid, _} -> ok
    after 1000 -> ok
    end,
    ok = emqx_authn_jwks_client:stop(ClientPid),
    ct:pal("captured request bytes:~n~s", [Bytes]),
    ?assertEqual(
        nomatch,
        binary:match(Bytes, <<"\r\nte: \r\n">>),
        {empty_te_header_on_wire, Bytes}
    ),
    {Pos, _Len} = binary:match(Bytes, <<"\r\nte: trailers\r\n">>),
    ?assert(is_integer(Pos) andalso Pos > 0).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

raw_http_capture(Port, Parent) ->
    {ok, LSock} = gen_tcp:listen(Port, [
        binary, {active, false}, {reuseaddr, true}, {packet, raw}
    ]),
    Parent ! {listening, self()},
    {ok, Sock} = gen_tcp:accept(LSock, 5000),
    Bytes = read_until_header_end(Sock, <<>>),
    Body = <<"{\"keys\":[]}">>,
    BodyLen = integer_to_binary(byte_size(Body)),
    Resp = iolist_to_binary([
        <<"HTTP/1.1 200 OK\r\n">>,
        <<"content-type: application/json\r\n">>,
        <<"content-length: ">>,
        BodyLen,
        <<"\r\n">>,
        <<"connection: close\r\n\r\n">>,
        Body
    ]),
    ok = gen_tcp:send(Sock, Resp),
    ok = gen_tcp:close(Sock),
    ok = gen_tcp:close(LSock),
    Parent ! {captured, self(), Bytes},
    ok.

read_until_header_end(Sock, Acc) ->
    case binary:match(Acc, <<"\r\n\r\n">>) of
        nomatch ->
            case gen_tcp:recv(Sock, 0, 5000) of
                {ok, Data} -> read_until_header_end(Sock, <<Acc/binary, Data/binary>>);
                {error, _} = E -> error({recv_failed, E, Acc})
            end;
        _ ->
            Acc
    end.
