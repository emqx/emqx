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

-module(emqx_quic_connection).

-ifndef(BUILD_WITHOUT_QUIC).
-include_lib("quicer/include/quicer.hrl").
-else.
-define(QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, 0).
-endif.

%% Callbacks
-export([
    init/1,
    new_conn/2,
    connected/2,
    shutdown/2
]).

-type cb_state() :: map() | proplists:proplist().

-spec init(cb_state()) -> cb_state().
init(ConnOpts) when is_list(ConnOpts) ->
    init(maps:from_list(ConnOpts));
init(ConnOpts) when is_map(ConnOpts) ->
    ConnOpts.

-spec new_conn(quicer:connection_handler(), cb_state()) -> {ok, cb_state()} | {error, any()}.
new_conn(Conn, #{zone := Zone} = S) ->
    process_flag(trap_exit, true),
    case emqx_olp:is_overloaded() andalso is_zone_olp_enabled(Zone) of
        false ->
            {ok, Pid} = emqx_connection:start_link(emqx_quic_stream, {self(), Conn}, S),
            receive
                {Pid, stream_acceptor_ready} ->
                    ok = quicer:async_handshake(Conn),
                    {ok, S};
                {'EXIT', Pid, _Reason} ->
                    {error, stream_accept_error}
            end;
        true ->
            emqx_metrics:inc('olp.new_conn'),
            {error, overloaded}
    end.

-spec connected(quicer:connection_handler(), cb_state()) -> {ok, cb_state()} | {error, any()}.
connected(Conn, #{slow_start := false} = S) ->
    {ok, _Pid} = emqx_connection:start_link(emqx_quic_stream, Conn, S),
    {ok, S};
connected(_Conn, S) ->
    {ok, S}.

-spec shutdown(quicer:connection_handler(), cb_state()) -> {ok, cb_state()} | {error, any()}.
shutdown(Conn, S) ->
    quicer:async_shutdown_connection(Conn, ?QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, 0),
    {ok, S}.

-spec is_zone_olp_enabled(emqx_types:zone()) -> boolean().
is_zone_olp_enabled(Zone) ->
    case emqx_config:get_zone_conf(Zone, [overload_protection]) of
        #{enable := true} ->
            true;
        _ ->
            false
    end.
