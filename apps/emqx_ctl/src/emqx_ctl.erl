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

-module(emqx_ctl).

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-export([start_link/0, stop/0]).

-export([
    register_command/2,
    register_command/3,
    unregister_command/1
]).

-export([
    run_command/1,
    run_command/2,
    lookup_command/1,
    get_commands/0
]).

-export([
    print/1,
    print/2,
    warning/1,
    warning/2,
    usage/1,
    usage/2
]).

-export([
    eval_erl/1
]).

%% Exports mainly for test cases
-export([
    format/2,
    format_usage/1,
    format_usage/2
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

-record(state, {seq = 0}).

-type cmd() :: atom().
-type cmd_params() :: string().
-type cmd_descr() :: string().
-type cmd_usage() :: {cmd_params(), cmd_descr()}.

-define(SERVER, ?MODULE).
-define(CMD_TAB, emqx_command).

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec stop() -> ok.
stop() ->
    try
        gen_server:stop(?SERVER)
    catch
        exit:R when R =:= noproc orelse R =:= timeout ->
            ok
    end.

-spec register_command(cmd(), {module(), atom()}) -> ok.
register_command(Cmd, MF) when is_atom(Cmd) ->
    register_command(Cmd, MF, []).

-spec register_command(cmd(), {module(), atom()}, list()) -> ok.
register_command(Cmd, MF, Opts) when is_atom(Cmd) ->
    call({register_command, Cmd, MF, Opts}).

-spec unregister_command(cmd()) -> ok.
unregister_command(Cmd) when is_atom(Cmd) ->
    cast({unregister_command, Cmd}).

call(Req) -> gen_server:call(?SERVER, Req).

cast(Msg) -> gen_server:cast(?SERVER, Msg).

-spec run_command(list(string())) -> ok | {error, term()}.
run_command([]) ->
    run_command(help, []);
run_command([Cmd | Args]) ->
    case safe_to_existing_atom(Cmd) of
        {ok, Cmd1} ->
            run_command(Cmd1, Args);
        _ ->
            help(),
            {error, cmd_not_found}
    end.

-spec run_command(cmd(), list(string())) -> ok | {error, term()}.
run_command(help, []) ->
    help();
run_command(Cmd, Args) when is_atom(Cmd) ->
    Start = erlang:monotonic_time(),
    Result =
        case lookup_command(Cmd) of
            {ok, {Mod, Fun}} ->
                try
                    apply(Mod, Fun, [Args])
                catch
                    _:Reason:Stacktrace ->
                        ?LOG_ERROR(#{
                            msg => "ctl_command_crashed",
                            stacktrace => Stacktrace,
                            reason => Reason,
                            module => Mod,
                            function => Fun
                        }),
                        {error, Reason}
                end;
            {error, Reason} ->
                help(),
                {error, Reason}
        end,
    Duration = erlang:convert_time_unit(erlang:monotonic_time() - Start, native, millisecond),

    audit_log(
        audit_level(Result, Duration),
        cli,
        #{duration_ms => Duration, cmd => Cmd, args => Args, node => node()}
    ),
    Result.

-spec lookup_command(cmd()) -> {module(), atom()} | {error, any()}.
lookup_command(eval_erl) ->
    %% So far 'emqx ctl eval_erl Expr' is a undocumented hidden command.
    %% For backward compatibility,
    %% the documented command 'emqx eval Expr' has the expression parsed
    %% in the remsh node (nodetool).
    %%
    %% 'eval_erl' is added for two purposes
    %% 1. 'emqx eval Expr' can be audited
    %% 2. 'emqx ctl eval_erl Expr' simplifies the scripting part
    {ok, {?MODULE, eval_erl}};
lookup_command(Cmd) when is_atom(Cmd) ->
    case is_initialized() of
        true ->
            case ets:match(?CMD_TAB, {{'_', Cmd}, '$1', '_'}) of
                [[{M, F}]] -> {ok, {M, F}};
                [] -> {error, cmd_not_found}
            end;
        false ->
            {error, cmd_is_initializing}
    end.

-spec get_commands() -> list({cmd(), module(), atom()}).
get_commands() ->
    [{Cmd, M, F} || {{_Seq, Cmd}, {M, F}, _Opts} <- ets:tab2list(?CMD_TAB)].

help() ->
    case is_initialized() of
        true ->
            case ets:tab2list(?CMD_TAB) of
                [] ->
                    print("No commands available.~n");
                Cmds ->
                    print("Usage: ~ts~n", ["emqx ctl"]),
                    lists:foreach(fun print_usage/1, Cmds)
            end;
        false ->
            print("Command table is initializing.~n")
    end.

print_usage({_, {Mod, Cmd}, Opts}) ->
    case proplists:get_bool(hidden, Opts) of
        true ->
            ok;
        false ->
            print("~110..-s~n", [""]),
            apply(Mod, Cmd, [usage])
    end.

-spec print(io:format()) -> ok.
print(Msg) ->
    io:format("~ts", [format(Msg, [])]).

-spec print(io:format(), [term()]) -> ok.
print(Format, Args) ->
    io:format("~ts", [format(Format, Args)]).

-spec warning(io:format()) -> ok.
warning(Format) ->
    warning(Format, []).

-spec warning(io:format(), [term()]) -> ok.
warning(Format, Args) ->
    io:format("\e[31m~ts\e[0m", [format(Format, Args)]).

-spec usage([cmd_usage()]) -> ok.
usage(UsageList) ->
    io:format(format_usage(UsageList)).

-spec usage(cmd_params(), cmd_descr()) -> ok.
usage(CmdParams, Desc) ->
    io:format(format_usage(CmdParams, Desc)).

-spec format(io:format(), [term()]) -> string().
format(Format, Args) ->
    lists:flatten(io_lib:format(Format, Args)).

-spec format_usage([cmd_usage()]) -> [string()].
format_usage(UsageList) ->
    Width = lists:foldl(
        fun({CmdStr, _}, W) ->
            max(iolist_size(CmdStr), W)
        end,
        0,
        UsageList
    ),
    lists:map(
        fun({CmdParams, Desc}) ->
            format_usage(CmdParams, Desc, Width)
        end,
        UsageList
    ).

-spec format_usage(cmd_params(), cmd_descr()) -> string().
format_usage(CmdParams, Desc) ->
    format_usage(CmdParams, Desc, 0).

format_usage(CmdParams, Desc, 0) ->
    format_usage(CmdParams, Desc, iolist_size(CmdParams));
format_usage(CmdParams, Desc, Width) ->
    CmdLines = split_cmd(CmdParams),
    DescLines = split_cmd(Desc),
    Zipped = zip_cmd(CmdLines, DescLines),
    Fmt = "~-" ++ integer_to_list(Width + 1) ++ "s# ~ts~n",
    lists:foldl(
        fun({CmdStr, DescStr}, Usage) ->
            Usage ++ format(Fmt, [CmdStr, DescStr])
        end,
        "",
        Zipped
    ).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    _ = ets:new(?CMD_TAB, [named_table, protected, ordered_set]),
    {ok, #state{seq = 0}}.

handle_call({register_command, Cmd, MF, Opts}, _From, State = #state{seq = Seq}) ->
    case ets:match(?CMD_TAB, {{'$1', Cmd}, '_', '_'}) of
        [] ->
            ets:insert(?CMD_TAB, {{Seq, Cmd}, MF, Opts}),
            {reply, ok, next_seq(State)};
        [[OriginSeq] | _] ->
            ?LOG_INFO(#{msg => "CMD_overridden", cmd => Cmd, mf => MF}),
            true = ets:insert(?CMD_TAB, {{OriginSeq, Cmd}, MF, Opts}),
            {reply, ok, State}
    end;
handle_call(Req, _From, State) ->
    ?LOG_ERROR(#{msg => "unexpected_call", call => Req}),
    {reply, ignored, State}.

handle_cast({unregister_command, Cmd}, State) ->
    ets:match_delete(?CMD_TAB, {{'_', Cmd}, '_', '_'}),
    noreply(State);
handle_cast(Msg, State) ->
    ?LOG_ERROR(#{msg => "unexpected_cast", cast => Msg}),
    noreply(State).

handle_info(Info, State) ->
    ?LOG_ERROR(#{msg => "unexpected_info", info => Info}),
    noreply(State).

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%------------------------------------------------------------------------------
%% Internal Function
%%------------------------------------------------------------------------------

noreply(State) ->
    {noreply, State, hibernate}.

next_seq(State = #state{seq = Seq}) ->
    State#state{seq = Seq + 1}.

split_cmd(CmdStr) ->
    Lines = string:split(CmdStr, "\n", all),
    [L || L <- Lines, L =/= []].

zip_cmd([X | Xs], [Y | Ys]) -> [{X, Y} | zip_cmd(Xs, Ys)];
zip_cmd([X | Xs], []) -> [{X, ""} | zip_cmd(Xs, [])];
zip_cmd([], [Y | Ys]) -> [{"", Y} | zip_cmd([], Ys)];
zip_cmd([], []) -> [].

safe_to_existing_atom(Str) ->
    try
        {ok, list_to_existing_atom(Str)}
    catch
        _:badarg ->
            undefined
    end.

is_initialized() ->
    ets:info(?CMD_TAB) =/= undefined.

audit_log(Level, From, Log) ->
    case lookup_command(audit) of
        {error, _} ->
            ignore;
        {ok, {Mod, Fun}} ->
            case prune_unnecessary_log(Log) of
                false -> ok;
                {ok, Log1} -> apply_audit_command(Log1, Mod, Fun, Level, From)
            end
    end.

apply_audit_command(Log, Mod, Fun, Level, From) ->
    try
        apply(Mod, Fun, [Level, From, Log])
    catch
        _:{aborted, {no_exists, emqx_audit}} ->
            case Log of
                #{cmd := cluster, args := [<<"leave">>]} ->
                    ok;
                _ ->
                    ?LOG_ERROR(#{
                        msg => "ctl_command_crashed",
                        reason => "emqx_audit table not found",
                        log => Log,
                        from => From
                    })
            end;
        _:Reason:Stacktrace ->
            ?LOG_ERROR(#{
                msg => "ctl_command_crashed",
                stacktrace => Stacktrace,
                reason => Reason,
                log => Log,
                from => From
            })
    end.

prune_unnecessary_log(Log) ->
    case normalize_audit_log_args(Log) of
        #{args := [<<"emqx:is_running()">>]} -> false;
        Log1 -> {ok, Log1}
    end.

audit_level(ok, _Duration) -> info;
audit_level({ok, _}, _Duration) -> info;
audit_level(_, _) -> error.

normalize_audit_log_args(Log = #{args := [Parsed | _] = Exprs, cmd := eval_erl}) when
    is_tuple(Parsed)
->
    String = erl_pp:exprs(Exprs, [{linewidth, 10000}]),
    Log#{args => [unicode:characters_to_binary(String)]};
normalize_audit_log_args(Log = #{args := Args}) ->
    Log#{args => [unicode:characters_to_binary(A) || A <- Args]}.

eval_erl([Parsed | _] = Expr) when is_tuple(Parsed) ->
    eval_expr(Expr);
eval_erl([String]) ->
    % convenience to users, if they forgot a trailing
    % '.' add it for them.
    Normalized =
        case lists:reverse(String) of
            [$. | _] -> String;
            R -> lists:reverse([$. | R])
        end,
    % then scan and parse the string
    {ok, Scanned, _} = erl_scan:string(Normalized),
    {ok, Parsed} = erl_parse:parse_exprs(Scanned),
    {ok, Value} = eval_expr(Parsed),
    print("~p~n", [Value]).

eval_expr(Parsed) ->
    {value, Value, _} = erl_eval:exprs(Parsed, []),
    {ok, Value}.
