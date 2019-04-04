%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_session_sup).

-behaviour(gen_server).

-include("logger.hrl").
-include("types.hrl").

-export([start_link/1]).

-export([ start_session/1
        , count_sessions/0
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-type(shutdown() :: brutal_kill | infinity | pos_integer()).

-record(state,
        { sessions :: #{pid() => emqx_types:client_id()}
        , mfargs :: mfa()
        , shutdown :: shutdown()
        , clean_down :: fun()
        }).

-define(SUP, ?MODULE).
-define(BATCH_EXIT, 100000).

%% @doc Start session supervisor.
-spec(start_link(map()) -> startlink_ret()).
start_link(SessSpec) when is_map(SessSpec) ->
    gen_server:start_link({local, ?SUP}, ?MODULE, [SessSpec], []).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

%% @doc Start a session.
-spec(start_session(map()) -> startlink_ret()).
start_session(SessAttrs) ->
    gen_server:call(?SUP, {start_session, SessAttrs}, infinity).

%% @doc Count sessions.
-spec(count_sessions() -> non_neg_integer()).
count_sessions() ->
    gen_server:call(?SUP, count_sessions, infinity).

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

init([Spec]) ->
    process_flag(trap_exit, true),
    MFA = maps:get(start, Spec),
    Shutdown = maps:get(shutdown, Spec, brutal_kill),
    CleanDown = maps:get(clean_down, Spec, undefined),
    State = #state{sessions = #{},
                   mfargs = MFA,
                   shutdown = Shutdown,
                   clean_down = CleanDown
                  },
    {ok, State}.

handle_call({start_session, SessAttrs = #{client_id := ClientId}}, _From,
            State = #state{sessions = SessMap, mfargs = {M, F, Args}}) ->
    try erlang:apply(M, F, [SessAttrs | Args]) of
        {ok, Pid} ->
            reply({ok, Pid}, State#state{sessions = maps:put(Pid, ClientId, SessMap)});
        ignore ->
            reply(ignore, State);
        {error, Reason} ->
            reply({error, Reason}, State)
    catch
        _:Error:Stk ->
            ?LOG(error, "[Session Supervisor] Failed to start session ~p: ~p, stacktrace:~n~p",
                   [ClientId, Error, Stk]),
            reply({error, Error}, State)
    end;

handle_call(count_sessions, _From, State = #state{sessions = SessMap}) ->
    {reply, maps:size(SessMap), State};

handle_call(Req, _From, State) ->
    ?LOG(error, "[Session Supervisor] Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?LOG(error, "[Session Supervisor] Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info({'EXIT', Pid, _Reason}, State = #state{sessions = SessMap, clean_down = CleanDown}) ->
    SessPids = [Pid | drain_exit(?BATCH_EXIT, [])],
    {SessItems, SessMap1} = erase_all(SessPids, SessMap),
    (CleanDown =:= undefined)
        orelse emqx_pool:async_submit(
                 fun lists:foreach/2, [CleanDown, SessItems]),
    {noreply, State#state{sessions = SessMap1}};

handle_info(Info, State) ->
    ?LOG(notice, "[Session Supervisor] Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, State) ->
    terminate_children(State).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

drain_exit(0, Acc) ->
    lists:reverse(Acc);
drain_exit(Cnt, Acc) ->
    receive
        {'EXIT', Pid, _Reason} ->
            drain_exit(Cnt - 1, [Pid|Acc])
    after 0 ->
          lists:reverse(Acc)
    end.

erase_all(Pids, Map) ->
    lists:foldl(
      fun(Pid, {Acc, M}) ->
            case maps:take(Pid, M) of
                {Val, M1} ->
                    {[{Val, Pid}|Acc], M1};
                error ->
                    {Acc, M}
            end
      end, {[], Map}, Pids).

terminate_children(State = #state{sessions = SessMap, shutdown = Shutdown}) ->
    {Pids, EStack0} = monitor_children(SessMap),
    Sz = sets:size(Pids),
    EStack =
    case Shutdown of
        brutal_kill ->
            sets:fold(fun(P, _) -> exit(P, kill) end, ok, Pids),
            wait_children(Shutdown, Pids, Sz, undefined, EStack0);
        infinity ->
            sets:fold(fun(P, _) -> exit(P, shutdown) end, ok, Pids),
            wait_children(Shutdown, Pids, Sz, undefined, EStack0);
        Time when is_integer(Time) ->
            sets:fold(fun(P, _) -> exit(P, shutdown) end, ok, Pids),
            TRef = erlang:start_timer(Time, self(), kill),
            wait_children(Shutdown, Pids, Sz, TRef, EStack0)
    end,
    %% Unroll stacked errors and report them
    dict:fold(fun(Reason, Pid, _) ->
                  report_error(connection_shutdown_error, Reason, Pid, State)
              end, ok, EStack).

monitor_children(SessMap) ->
    lists:foldl(
      fun(Pid, {Pids, EStack}) ->
          case monitor_child(Pid) of
              ok ->
                  {sets:add_element(Pid, Pids), EStack};
              {error, normal} ->
                  {Pids, EStack};
              {error, Reason} ->
                  {Pids, dict:append(Reason, Pid, EStack)}
          end
      end, {sets:new(), dict:new()}, maps:keys(SessMap)).

%% Help function to shutdown/2 switches from link to monitor approach
monitor_child(Pid) ->
    %% Do the monitor operation first so that if the child dies
    %% before the monitoring is done causing a 'DOWN'-message with
    %% reason noproc, we will get the real reason in the 'EXIT'-message
    %% unless a naughty child has already done unlink...
    erlang:monitor(process, Pid),
    unlink(Pid),

    receive
	%% If the child dies before the unlik we must empty
	%% the mail-box of the 'EXIT'-message and the 'DOWN'-message.
	{'EXIT', Pid, Reason} ->
	    receive
		{'DOWN', _, process, Pid, _} ->
		    {error, Reason}
	    end
    after 0 ->
	    %% If a naughty child did unlink and the child dies before
	    %% monitor the result will be that shutdown/2 receives a
	    %% 'DOWN'-message with reason noproc.
	    %% If the child should die after the unlink there
	    %% will be a 'DOWN'-message with a correct reason
	    %% that will be handled in shutdown/2.
	    ok
    end.

wait_children(_Shutdown, _Pids, 0, undefined, EStack) ->
    EStack;
wait_children(_Shutdown, _Pids, 0, TRef, EStack) ->
	%% If the timer has expired before its cancellation, we must empty the
	%% mail-box of the 'timeout'-message.
    erlang:cancel_timer(TRef),
    receive
        {timeout, TRef, kill} ->
            EStack
    after 0 ->
            EStack
    end;

%%TODO: Copied from supervisor.erl, rewrite it later.
wait_children(brutal_kill, Pids, Sz, TRef, EStack) ->
    receive
        {'DOWN', _MRef, process, Pid, killed} ->
            wait_children(brutal_kill, sets:del_element(Pid, Pids), Sz-1, TRef, EStack);

        {'DOWN', _MRef, process, Pid, Reason} ->
            wait_children(brutal_kill, sets:del_element(Pid, Pids),
                          Sz-1, TRef, dict:append(Reason, Pid, EStack))
    end;

wait_children(Shutdown, Pids, Sz, TRef, EStack) ->
    receive
        {'DOWN', _MRef, process, Pid, shutdown} ->
            wait_children(Shutdown, sets:del_element(Pid, Pids), Sz-1, TRef, EStack);
        {'DOWN', _MRef, process, Pid, normal} ->
            wait_children(Shutdown, sets:del_element(Pid, Pids), Sz-1, TRef, EStack);
        {'DOWN', _MRef, process, Pid, Reason} ->
            wait_children(Shutdown, sets:del_element(Pid, Pids), Sz-1,
                          TRef, dict:append(Reason, Pid, EStack));
        {timeout, TRef, kill} ->
            sets:fold(fun(P, _) -> exit(P, kill) end, ok, Pids),
            wait_children(Shutdown, Pids, Sz-1, undefined, EStack)
    end.

report_error(Error, Reason, Pid, #state{mfargs = MFA}) ->
    SupName  = list_to_atom("esockd_connection_sup - " ++ pid_to_list(self())),
    ErrorMsg = [{supervisor, SupName},
                {errorContext, Error},
                {reason, Reason},
                {offender, [{pid, Pid},
                            {name, connection},
                            {mfargs, MFA}]}],
    error_logger:error_report(supervisor_report, ErrorMsg).

reply(Repy, State) ->
    {reply, Repy, State}.

