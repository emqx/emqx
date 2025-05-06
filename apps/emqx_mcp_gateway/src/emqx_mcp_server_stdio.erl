%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mcp_server_stdio).
-include_lib("emqx/include/logger.hrl").

-export([
    connect_server/1,
    unpack/2,
    handle_close/1
]).

-export([
    send_msg/2
]).

-define(LINE_BYTES, 4096).

connect_server(#{command := Cmd, args := Args, env := Env0}) ->
    Env = [{emqx_utils_conv:str(K), emqx_utils_conv:str(V)} || {K, V} <- maps:to_list(Env0)],
    ?SLOG(debug, #{msg => "connect_server", cmd => Cmd, args => Args, env => Env}),
    try
        Port = open_port(Cmd, Args, Env),
        MonRef = erlang:monitor(port, Port),
        {ok, #{port => Port, port_mon => MonRef}}
    catch
        error:Reason ->
            {error, Reason}
    end.

unpack({_Port, {data, Data}}, State) ->
    case Data of
        {eol, Data1} ->
            PartialData = maps:get(partial_data, State, <<>>),
            Data2 = <<PartialData/binary, Data1/binary>>,
            {ok, Data2, State#{partial_data => <<>>}};
        {noeol, Data1} ->
            PartialData = maps:get(partial_data, State, <<>>),
            Data2 = <<PartialData/binary, Data1/binary>>,
            {more, State#{partial_data => Data2}};
        _ ->
            {error, {invalid_port_data, Data}}
    end;
unpack({'DOWN', MonRef, port, _Port, _Reason}, #{port_mon := MonRef} = State) ->
    handle_close(State),
    {stop, port_closed};
unpack(Data, _State) ->
    {error, {unexpected_stdio_data, Data}}.

send_msg(#{port := Port} = State, Msg) ->
    try
        true = erlang:port_command(Port, [Msg, io_lib:nl()]),
        {ok, State}
    catch
        error:badarg ->
            {error, badarg}
    end.

handle_close(#{port := Port, port_mon := MonRef}) ->
    erlang:demonitor(MonRef, [flush]),
    erlang:port_close(Port),
    ok;
handle_close(_) ->
    ok.

open_port(Cmd, Args, Env) ->
    PortSetting = [{args, Args}, {env, Env}, binary, {line, ?LINE_BYTES}, hide, stderr_to_stdout],
    erlang:open_port({spawn_executable, Cmd}, PortSetting).
