%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
