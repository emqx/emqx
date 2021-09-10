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

-module(emqx_quic_connection).

%% Callbacks
-export([ init/1
        , new_conn/2
        , connected/2
        , shutdown/2
        ]).

-type cb_state() :: map() | proplists:proplist().


-spec init(cb_state()) -> cb_state().
init(ConnOpts) when is_list(ConnOpts) ->
    init(maps:from_list(ConnOpts));
init(ConnOpts) when is_map(ConnOpts) ->
    ConnOpts.

-spec new_conn(quicer:connection_handler(), cb_state()) -> {ok, cb_state()} | {error, any()}.
new_conn(Conn, S) ->
    process_flag(trap_exit, true),
    {ok, Pid} = emqx_connection:start_link(emqx_quic_stream, {self(), Conn}, S),
    receive
        {Pid, stream_acceptor_ready} ->
            ok = quicer:async_handshake(Conn),
            {ok, S};
        {'EXIT', Pid, _Reason} ->
            {error, stream_accept_error}
    end.

-spec connected(quicer:connection_handler(), cb_state()) -> {ok, cb_state()} | {error, any()}.
connected(Conn, #{slow_start := false} = S) ->
    {ok, _Pid} = emqx_connection:start_link(emqx_quic_stream, Conn, S),
    {ok, S};
connected(_Conn, S) ->
    {ok, S}.

-spec shutdown(quicer:connection_handler(), cb_state()) -> {ok, cb_state()} | {error, any()}.
shutdown(Conn, S) ->
    quicer:async_close_connection(Conn),
    {ok, S}.
