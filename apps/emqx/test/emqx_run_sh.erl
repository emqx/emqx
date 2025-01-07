%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_run_sh).
-export([do/2]).

do(Command, Options0) ->
    Options =
        Options0 ++
            [
                use_stdio,
                stderr_to_stdout,
                exit_status,
                {line, 906},
                hide,
                eof
            ],
    Port = erlang:open_port({spawn, Command}, Options),
    try
        collect_output(Port, [])
    after
        erlang:port_close(Port)
    end.

collect_output(Port, Lines) ->
    receive
        {Port, {data, {eol, Line}}} ->
            collect_output(Port, [Line ++ "\n" | Lines]);
        {Port, {data, {noeol, Line}}} ->
            collect_output(Port, [Line | Lines]);
        {Port, eof} ->
            Result = lists:flatten(lists:reverse(Lines)),
            receive
                {Port, {exit_status, 0}} ->
                    {ok, Result};
                {Port, {exit_status, ExitCode}} ->
                    {error, {ExitCode, Result}}
            end
    end.
