%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_client_sock).

-export([ connect/4
        , send/2
        , close/1
        ]).

-export([ sockname/1
        , setopts/2
        , getstat/2
        ]).

-record(ssl_socket, {tcp, ssl}).

-type(socket() :: inet:socket() | #ssl_socket{}).

-type(sockname() :: {inet:ip_address(), inet:port_number()}).

-type(option() :: gen_tcp:connect_option() | {ssl_opts, [ssl:ssl_option()]}).

-export_type([socket/0, option/0]).

-define(DEFAULT_TCP_OPTIONS, [binary, {packet, raw}, {active, false},
                              {nodelay, true}, {reuseaddr, true}]).

-spec(connect(inet:ip_address() | inet:hostname(),
              inet:port_number(), [option()], timeout())
      -> {ok, socket()} | {error, term()}).
connect(Host, Port, SockOpts, Timeout) ->
    TcpOpts = emqx_misc:merge_opts(?DEFAULT_TCP_OPTIONS,
                                   lists:keydelete(ssl_opts, 1, SockOpts)),
    case gen_tcp:connect(Host, Port, TcpOpts, Timeout) of
        {ok, Sock} ->
            case lists:keyfind(ssl_opts, 1, SockOpts) of
                {ssl_opts, SslOpts} ->
                    ssl_upgrade(Sock, SslOpts, Timeout);
                false -> {ok, Sock}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

ssl_upgrade(Sock, SslOpts, Timeout) ->
    TlsVersions = proplists:get_value(versions, SslOpts, []),
    Ciphers = proplists:get_value(ciphers, SslOpts, default_ciphers(TlsVersions)),
    SslOpts2 = emqx_misc:merge_opts(SslOpts, [{ciphers, Ciphers}]),
    case ssl:connect(Sock, SslOpts2, Timeout) of
        {ok, SslSock} ->
            ok = ssl:controlling_process(SslSock, self()),
            {ok, #ssl_socket{tcp = Sock, ssl = SslSock}};
        {error, Reason} -> {error, Reason}
    end.

-spec(send(socket(), iodata()) -> ok | {error, einval | closed}).
send(Sock, Data) when is_port(Sock) ->
    try erlang:port_command(Sock, Data) of
        true -> ok
    catch
        error:badarg -> {error, einval}
    end;
send(#ssl_socket{ssl = SslSock}, Data) ->
    ssl:send(SslSock, Data).

-spec(close(socket()) -> ok).
close(Sock) when is_port(Sock) ->
    gen_tcp:close(Sock);
close(#ssl_socket{ssl = SslSock}) ->
    ssl:close(SslSock).

-spec(setopts(socket(), [gen_tcp:option() | ssl:socketoption()]) -> ok).
setopts(Sock, Opts) when is_port(Sock) ->
    inet:setopts(Sock, Opts);
setopts(#ssl_socket{ssl = SslSock}, Opts) ->
    ssl:setopts(SslSock, Opts).

-spec(getstat(socket(), [atom()])
      -> {ok, [{atom(), integer()}]} | {error, term()}).
getstat(Sock, Options) when is_port(Sock) ->
    inet:getstat(Sock, Options);
getstat(#ssl_socket{tcp = Sock}, Options) ->
    inet:getstat(Sock, Options).

-spec(sockname(socket()) -> {ok, sockname()} | {error, term()}).
sockname(Sock) when is_port(Sock) ->
    inet:sockname(Sock);
sockname(#ssl_socket{ssl = SslSock}) ->
    ssl:sockname(SslSock).

default_ciphers(TlsVersions) ->
    lists:foldl(
        fun(TlsVer, Ciphers) ->
            Ciphers ++ ssl:cipher_suites(all, TlsVer)
        end, [], TlsVersions).
