%%--------------------------------------------------------------------
%% Copyright (c) 2017-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include("types.hrl").
-include("logger.hrl").

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
    usage/1,
    usage/2
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

-spec start_link() -> startlink_ret().
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
    case emqx_misc:safe_to_existing_atom(Cmd) of
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
    case lookup_command(Cmd) of
        [{Mod, Fun}] ->
            try
                _ = apply(Mod, Fun, [Args]),
                ok
            catch
                _:Reason:Stacktrace ->
                    ?SLOG(error, #{
                        msg => "ctl_command_crashed",
                        stacktrace => Stacktrace,
                        reason => Reason
                    }),
                    {error, Reason}
            end;
        [] ->
            help(),
            {error, cmd_not_found}
    end.

-spec lookup_command(cmd()) -> [{module(), atom()}].
lookup_command(Cmd) when is_atom(Cmd) ->
    case ets:match(?CMD_TAB, {{'_', Cmd}, '$1', '_'}) of
        [El] -> El;
        [] -> []
    end.

-spec get_commands() -> list({cmd(), module(), atom()}).
get_commands() ->
    [{Cmd, M, F} || {{_Seq, Cmd}, {M, F}, _Opts} <- ets:tab2list(?CMD_TAB)].

help() ->
    case ets:tab2list(?CMD_TAB) of
        [] ->
            print("No commands available.~n");
        Cmds ->
            print("Usage: ~ts~n", [?MODULE]),
            lists:foreach(
                fun({_, {Mod, Cmd}, _}) ->
                    print("~110..-s~n", [""]),
                    apply(Mod, Cmd, [usage])
                end,
                Cmds
            )
    end.

-spec print(io:format()) -> ok.
print(Msg) ->
    io:format("~ts", [format(Msg, [])]).

-spec print(io:format(), [term()]) -> ok.
print(Format, Args) ->
    io:format("~ts", [format(Format, Args)]).

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
    ok = emqx_tables:new(?CMD_TAB, [protected, ordered_set]),
    {ok, #state{seq = 0}}.

handle_call({register_command, Cmd, MF, Opts}, _From, State = #state{seq = Seq}) ->
    case ets:match(?CMD_TAB, {{'$1', Cmd}, '_', '_'}) of
        [] ->
            ets:insert(?CMD_TAB, {{Seq, Cmd}, MF, Opts}),
            {reply, ok, next_seq(State)};
        [[OriginSeq] | _] ->
            ?SLOG(warning, #{msg => "CMD_overidden", cmd => Cmd, mf => MF}),
            true = ets:insert(?CMD_TAB, {{OriginSeq, Cmd}, MF, Opts}),
            {reply, ok, State}
    end;
handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignored, State}.

handle_cast({unregister_command, Cmd}, State) ->
    ets:match_delete(?CMD_TAB, {{'_', Cmd}, '_', '_'}),
    noreply(State);
handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Msg}),
    noreply(State).

handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
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
