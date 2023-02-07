%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-export([
    start_link/2,
    start_link/3,
    start_supervised/2,
    start_supervised/3,
    read/1
]).

-export([
    table/1
]).

-define(DEFAULT_CHUNK_SIZE, 1024).

table(ReaderPid) ->
    NextFun = fun NextFun(Pid) ->
        try
            case emqx_ft_storage_fs_reader_proto_v1:read(node(Pid), Pid) of
                eof ->
                    [];
                {ok, Data} ->
                    [Data | fun() -> NextFun(Pid) end];
                {error, Reason} ->
                    ?SLOG(warning, #{msg => "file_read_error", reason => Reason}),
                    [];
                {BadRPC, Reason} when BadRPC =:= badrpc orelse BadRPC =:= badtcp ->
                    ?SLOG(warning, #{msg => "file_read_rpc_error", kind => BadRPC, reason => Reason}),
                    []
            end
        catch
            Class:Error:Stacktrace ->
                ?SLOG(warning, #{
                    msg => "file_read_error",
                    class => Class,
                    reason => Error,
                    stacktrace => Stacktrace
                }),
                []
        end
    end,
    qlc:table(fun() -> NextFun(ReaderPid) end, []).

start_link(CallerPid, Filename) ->
    start_link(CallerPid, Filename, ?DEFAULT_CHUNK_SIZE).

start_link(CallerPid, Filename, ChunkSize) ->
    gen_server:start_link(?MODULE, [CallerPid, Filename, ChunkSize], []).

start_supervised(CallerPid, Filename) ->
    start_supervised(CallerPid, Filename, ?DEFAULT_CHUNK_SIZE).

start_supervised(CallerPid, Filename, ChunkSize) ->
    emqx_ft_storage_fs_reader_sup:start_child(CallerPid, Filename, ChunkSize).

read(Pid) ->
    gen_server:call(Pid, read).

init([CallerPid, Filename, ChunkSize]) ->
    MRef = erlang:monitor(process, CallerPid),
    case file:open(Filename, [read, raw, binary]) of
        {ok, File} ->
            {ok, #{
                filename => Filename,
                file => File,
                chunk_size => ChunkSize,
                caller_pid => CallerPid,
                mref => MRef
            }};
        {error, Reason} ->
            {stop, Reason}
    end.

handle_call(read, _From, #{file := File, chunk_size := ChunkSize} = State) ->
    case file:read(File, ChunkSize) of
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
    {stop, {bad_call, Msg}, {bad_call, Msg}, State}.

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
