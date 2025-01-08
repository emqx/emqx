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
-module(user_default).

%% INCLUDE BEGIN
%% Import all the record definitions from the header file into the erlang shell.
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx_conf/include/emqx_conf.hrl").
-include_lib("emqx_dashboard/include/emqx_dashboard.hrl").
%% INCLUDE END

-define(TIME, 3 * 60).
-define(MESSAGE, 512).
-define(GREEN, <<"\e[32;1m">>).
-define(RED, <<"\e[31m">>).
-define(RESET, <<"\e[0m">>).

%% print to default group-leader but not user
%% so it should work in remote shell
-define(PRINT(FMT, ARGS), io:format(FMT, ARGS)).

%% API
-export([lock/0, unlock/0]).
-export([trace/0, t/0, t/1, t/2, t_msg/0, t_msg/1, t_stop/0]).

-dialyzer({nowarn_function, start_trace/3}).
-dialyzer({no_return, [t/0, t/1, t/2]}).

lock() -> emqx_restricted_shell:lock().
unlock() -> emqx_restricted_shell:unlock().

trace() ->
    ?PRINT("Trace Usage:~n", []),
    ?PRINT("  --------------------------------------------------~n", []),
    ?PRINT("  t(Mod, Func) -> trace a specify function.~n", []),
    ?PRINT("  t(RTPs) -> trace in Redbug Trace Patterns.~n", []),
    ?PRINT("       eg1: t(\"emqx_hooks:run\").~n", []),
    ?PRINT("       eg2: t(\"emqx_hooks:run/2\").~n", []),
    ?PRINT("       eg3: t(\"emqx_hooks:run/2 -> return\").~n", []),
    ?PRINT(
        "       eg4: t(\"emqx_hooks:run('message.dropped',[_, #{node := N}, _])"
        "when N =:= 'emqx@127.0.0.1' -> stack,return\"~n",
        []
    ),
    ?PRINT("  t() ->   when you forget the RTPs.~n", []),
    ?PRINT("  --------------------------------------------------~n", []),
    ?PRINT("  t_msg(PidorRegName) -> trace a pid/registed name's messages.~n", []),
    ?PRINT("  t_msg([Pid,RegName]) -> trace a list pids's messages.~n", []),
    ?PRINT("  t_msg() ->  when you forget the pids.~n", []),
    ?PRINT("  --------------------------------------------------~n", []),
    ?PRINT("  t_stop() -> stop running trace.~n", []),
    ?PRINT("  --------------------------------------------------~n", []),
    ok.

t_stop() ->
    ensure_redbug_stop().

t() ->
    {M, F} = get_rtp_fun(),
    t(M, F).

t(M) ->
    t(M, "").

t(M, F) ->
    ensure_redbug_stop(),
    RTP = format_rtp(emqx_utils_conv:str(M), emqx_utils_conv:str(F)),
    Pids = get_procs(erlang:system_info(process_count)),
    Options = [{time, ?TIME * 1000}, {msgs, ?MESSAGE}, debug, {procs, Pids}],
    start_trace(RTP, Options, Pids).

t_msg() ->
    ?PRINT("Tracing on specific pids's send/receive message: ~n", []),
    Pids = get_pids(),
    t_msg(Pids).

t_msg([]) ->
    exit("procs can't be empty");
t_msg(Pids) when is_list(Pids) ->
    ensure_redbug_stop(),
    Options = [{time, ?TIME * 1000}, {msgs, ?MESSAGE}, {procs, Pids}],
    start_trace(['send', 'receive'], Options, Pids);
t_msg(Pid) ->
    t_msg([Pid]).

start_trace(RTP, Options, Pids) ->
    info("~nredbug:start(~0p, ~0p)", [RTP, Options]),
    case redbug:start(RTP, Options) of
        {argument_error, no_matching_functions} ->
            warning("~p no matching function", [RTP]);
        {argument_error, no_matching_processes} ->
            case Pids of
                [Pid] -> warning("~p is dead", [Pid]);
                _ -> warning("~p are dead", [Pids])
            end;
        {argument_error, Reason} ->
            warning("argument_error:~p~n", [Reason]);
        normal ->
            warning("bad RTPs: ~p", [RTP]);
        {_Name, ProcessCount, 0} ->
            info(
                "Tracing (~w) processes matching ~p within ~w seconds",
                [ProcessCount, RTP, ?TIME]
            );
        {_Name, ProcessCount, FunCount} ->
            info(
                "Tracing (~w) processes matching ~ts within ~w seconds and ~w function",
                [ProcessCount, RTP, ?TIME, FunCount]
            )
    end.

get_rtp_fun() ->
    RTP0 = io:get_line("Module:Function | Module | RTPs:\n"),
    RTP1 = string:trim(RTP0, both, " \n"),
    case string:split(RTP1, ":") of
        [M] -> {M, get_function()};
        [M, ""] -> {M, get_function()};
        [M, F] -> {M, F}
    end.

get_function() ->
    ?PRINT("Function(func|func/3|func('_', atom, X) when is_integer(X)) :~n", []),
    F0 = io:get_line(""),
    string:trim(F0, both, " \n").

format_rtp("", _) ->
    exit("Module can't be empty");
format_rtp(M, "") ->
    add_return(M);
format_rtp(M, F) ->
    M ++ ":" ++ add_return(F).

add_return(M) ->
    case string:find(M, "->") of
        nomatch -> M ++ "-> return";
        _ -> M
    end.

get_procs(ProcCount) when ProcCount > 2500 ->
    warning("Tracing include all(~w) processes can be very risky", [ProcCount]),
    get_pids();
get_procs(_ProcCount) ->
    all.

get_pids() ->
    Str = io:get_line("<0.1.0>|<0.1.0>,<0.2.0>|all|new|running|RegName:"),
    try
        lists:map(fun parse_pid/1, string:tokens(Str, ", \n"))
    catch
        throw:{not_registered, Name} ->
            warning("~ts not registered~n", [Name]),
            get_pids();
        throw:new ->
            new;
        throw:running ->
            running;
        throw:quit ->
            throw(quit);
        throw:all ->
            all;
        _:_ ->
            warning("Invalid pid: ~ts~n:", [Str]),
            get_pids()
    end.

parse_pid("<0." ++ _ = L) ->
    list_to_pid(L);
parse_pid("all") ->
    throw(all);
parse_pid("new") ->
    throw(new);
parse_pid("running") ->
    throw(running);
parse_pid("q") ->
    throw(quit);
parse_pid(NameStr) ->
    case emqx_utils:safe_to_existing_atom(NameStr, utf8) of
        {ok, Name} ->
            case whereis(Name) of
                undefined -> throw({not_registered, NameStr});
                Pid -> Pid
            end;
        {error, _} ->
            throw({not_registered, NameStr})
    end.

warning(Fmt, Args) -> ?PRINT("~s" ++ Fmt ++ ".~s~n", [?RED] ++ Args ++ [?RESET]).
info(Fmt, Args) -> ?PRINT("~s" ++ Fmt ++ ".~s~n", [?GREEN] ++ Args ++ [?RESET]).

ensure_redbug_stop() ->
    case redbug:stop() of
        not_started ->
            ok;
        stopped ->
            timer:sleep(80),
            ok
    end.
