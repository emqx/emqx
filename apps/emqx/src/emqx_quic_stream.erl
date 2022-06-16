%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% MQTT/QUIC Stream
-module(emqx_quic_stream).

%% emqx transport Callbacks
-export([
    type/1,
    wait/1,
    getstat/2,
    fast_close/1,
    ensure_ok_or_exit/2,
    async_send/3,
    setopts/2,
    getopts/2,
    peername/1,
    sockname/1,
    peercert/1
]).

wait({ConnOwner, Conn}) ->
    {ok, Conn} = quicer:async_accept_stream(Conn, []),
    ConnOwner ! {self(), stream_acceptor_ready},
    receive
        %% from msquic
        {quic, new_stream, Stream} ->
            {ok, {quic, Conn, Stream}};
        {'EXIT', ConnOwner, _Reason} ->
            {error, enotconn}
    end.

type(_) ->
    quic.

peername({quic, Conn, _Stream}) ->
    quicer:peername(Conn).

sockname({quic, Conn, _Stream}) ->
    quicer:sockname(Conn).

peercert(_S) ->
    %% @todo but unsupported by msquic
    nossl.

getstat({quic, Conn, _Stream}, Stats) ->
    case quicer:getstat(Conn, Stats) of
        {error, _} -> {error, closed};
        Res -> Res
    end.

setopts(Socket, Opts) ->
    lists:foreach(
        fun
            ({Opt, V}) when is_atom(Opt) ->
                quicer:setopt(Socket, Opt, V);
            (Opt) when is_atom(Opt) ->
                quicer:setopt(Socket, Opt, true)
        end,
        Opts
    ),
    ok.

getopts(_Socket, _Opts) ->
    %% @todo
    {ok, [
        {high_watermark, 0},
        {high_msgq_watermark, 0},
        {sndbuf, 0},
        {recbuf, 0},
        {buffer, 80000}
    ]}.

fast_close({quic, _Conn, Stream}) ->
    %% Flush send buffer, gracefully shutdown
    quicer:async_shutdown_stream(Stream),
    ok.

-spec ensure_ok_or_exit(atom(), list(term())) -> term().
ensure_ok_or_exit(Fun, Args = [Sock | _]) when is_atom(Fun), is_list(Args) ->
    case erlang:apply(?MODULE, Fun, Args) of
        {error, Reason} when Reason =:= enotconn; Reason =:= closed ->
            fast_close(Sock),
            exit(normal);
        {error, Reason} ->
            fast_close(Sock),
            exit({shutdown, Reason});
        Result ->
            Result
    end.

async_send({quic, _Conn, Stream}, Data, _Options) ->
    case quicer:send(Stream, Data) of
        {ok, _Len} -> ok;
        Other -> Other
    end.
