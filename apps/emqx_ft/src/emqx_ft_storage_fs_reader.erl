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

-module(emqx_ft_storage_fs_reader).

-behaviour(gen_server).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/types.hrl").

%% API
-export([
    start_link/2,
    start_supervised/2,
    table/1,
    table/2,
    read/2
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(DEFAULT_CHUNK_SIZE, 1024).
-define(IS_FILENAME(Filename), (is_list(Filename) or is_binary(Filename))).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec table(pid()) -> qlc:query_handle().
table(ReaderPid) when is_pid(ReaderPid) ->
    table(ReaderPid, ?DEFAULT_CHUNK_SIZE).

-spec table(pid(), pos_integer()) -> qlc:query_handle().
table(ReaderPid, Bytes) when is_pid(ReaderPid) andalso is_integer(Bytes) andalso Bytes > 0 ->
    NextFun = fun NextFun(Pid) ->
        case emqx_ft_storage_fs_reader_proto_v1:read(node(Pid), Pid, Bytes) of
            eof ->
                [];
            {ok, Data} ->
                [Data] ++ fun() -> NextFun(Pid) end;
            {ErrorKind, Reason} when ErrorKind =:= badrpc; ErrorKind =:= error ->
                ?SLOG(warning, #{msg => "file_read_error", kind => ErrorKind, reason => Reason}),
                []
        end
    end,
    qlc:table(fun() -> NextFun(ReaderPid) end, []).

-spec start_link(pid(), file:name()) -> startlink_ret().
start_link(CallerPid, Filename) when
    is_pid(CallerPid) andalso
        ?IS_FILENAME(Filename)
->
    gen_server:start_link(?MODULE, [CallerPid, Filename], []).

-spec start_supervised(pid(), file:name()) -> startlink_ret().
start_supervised(CallerPid, Filename) when
    is_pid(CallerPid) andalso
        ?IS_FILENAME(Filename)
->
    emqx_ft_storage_fs_reader_sup:start_child(CallerPid, Filename).

-spec read(pid(), pos_integer()) -> {ok, binary()} | eof | {error, term()}.
read(Pid, Bytes) when
    is_pid(Pid) andalso
        is_integer(Bytes) andalso
        Bytes > 0
->
    gen_server:call(Pid, {read, Bytes}).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([CallerPid, Filename]) ->
    MRef = erlang:monitor(process, CallerPid),
    case file:open(Filename, [read, raw, binary]) of
        {ok, File} ->
            {ok, #{
                filename => Filename,
                file => File,
                caller_pid => CallerPid,
                mref => MRef
            }};
        {error, Reason} ->
            {stop, Reason}
    end.

handle_call({read, Bytes}, _From, #{file := File} = State) ->
    case file:read(File, Bytes) of
        {ok, Data} ->
            ?SLOG(debug, #{msg => "read", bytes => byte_size(Data)}),
            {reply, {ok, Data}, State};
        eof ->
            ?SLOG(debug, #{msg => "read", eof => true}),
            {stop, normal, eof, State};
        {error, Reason} = Error ->
            {stop, Reason, Error, State}
    end;
handle_call(Msg, _From, State) ->
    {reply, {error, {bad_call, Msg}}, State}.

handle_info(
    {'DOWN', MRef, process, CallerPid, _Reason}, #{mref := MRef, caller_pid := CallerPid} = State
) ->
    {stop, {caller_down, CallerPid}, State};
handle_info(Msg, State) ->
    ?SLOG(warning, #{msg => "unexpected_message", info_msg => Msg}),
    {noreply, State}.

handle_cast(Msg, State) ->
    ?SLOG(warning, #{msg => "unexpected_message", case_msg => Msg}),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
