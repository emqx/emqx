%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-export([ type/1
        , wait/1
        , getstat/2
        , fast_close/1
        , ensure_ok_or_exit/2
        , async_send/3
        , setopts/2
        , getopts/2
        , peername/1
        , sockname/1
        , peercert/1
        ]).

wait(Conn) ->
    quicer:accept_stream(Conn, []).

type(_) ->
    quic.

peername(S) ->
    quicer:peername(S).

sockname(S) ->
    quicer:sockname(S).

peercert(_S) ->
    nossl.

getstat(Socket, Stats) ->
    case quicer:getstat(Socket, Stats) of
        {error, _} -> {error, closed};
        Res -> Res
    end.

setopts(Socket, Opts) ->
    lists:foreach(fun({Opt, V}) when is_atom(Opt) ->
                          quicer:setopt(Socket, Opt, V);
                     (Opt) when is_atom(Opt) ->
                          quicer:setopt(Socket, Opt, true)
                  end, Opts),
    ok.

getopts(_Socket, _Opts) ->
    %% @todo
    {ok, [{high_watermark, 0},
          {high_msgq_watermark, 0},
          {sndbuf, 0},
          {recbuf, 0},
          {buffer,80000}]}.

fast_close(Stream) ->
    %% Stream might be closed already.
    _ = quicer:async_close_stream(Stream),
    ok.

-spec(ensure_ok_or_exit(atom(), list(term())) -> term()).
ensure_ok_or_exit(Fun, Args = [Sock|_]) when is_atom(Fun), is_list(Args) ->
    case erlang:apply(?MODULE, Fun, Args) of
        {error, Reason} when Reason =:= enotconn; Reason =:= closed ->
            fast_close(Sock),
            exit(normal);
        {error, Reason} ->
            fast_close(Sock),
            exit({shutdown, Reason});
        Result -> Result
    end.

async_send(Stream, Data, Options) when is_list(Data) ->
    async_send(Stream, iolist_to_binary(Data), Options);
async_send(Stream, Data, _Options) when is_binary(Data) ->
    {ok, _Len} = quicer:send(Stream, Data),
    ok.
