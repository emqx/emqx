%%--------------------------------------------------------------------
%% Copyright © 2013-2018 EMQ Inc. All rights reserved.
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

-module(emqx_sm).

-behaviour(gen_server).

-include("emqx.hrl").

-export([start_link/1]).

-export([open_session/1, lookup_session/1, close_session/1]).
-export([resume_session/1, discard_session/1]).
-export([register_session/1, unregister_session/2]).

%% lock_session/1, create_session/1, unlock_session/1,

-export([dispatch/3]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {stats_fun, stats_timer, monitors = #{}}).

-spec(start_link(StatsFun :: fun()) -> {ok, pid()} | ignore | {error, term()}).
start_link(StatsFun) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [StatsFun], []).

open_session(Session = #{client_id := ClientId, clean_start := true}) ->
    with_lock(ClientId,
              fun() ->
                  case rpc:multicall(ekka:nodelist(), ?MODULE, discard_session, [ClientId]) of
                      {_Res, []} -> ok;
                      {_Res, BadNodes} -> emqx_log:error("[SM] Bad nodes found when lock a session: ~p", [BadNodes])
                  end,
                  {ok, emqx_session_sup:start_session(Session)}
              end);

open_session(Session = #{client_id := ClientId, clean_start := false}) ->
    with_lock(ClientId,
              fun() ->
                  {ResL, _BadNodes} = emqx_rpc:multicall(ekka:nodelist(), ?MODULE, lookup_session, [ClientId]),
                  case lists:flatten([Pid || Pid <- ResL, Pid =/= undefined]) of
                      [] ->
                          {ok, emqx_session_sup:start_session(Session)};
                      [SessPid|_] ->
                          case resume_session(SessPid) of
                              ok -> {ok, SessPid};
                              {error, Reason} ->
                                  emqx_log:error("[SM] Failed to resume session: ~p, ~p", [Session, Reason]),
                                  {ok, emqx_session_sup:start_session(Session)}
                          end
                  end
              end).

resume_session(SessPid) when node(SessPid) == node() ->
    case is_process_alive(SessPid) of
        true ->
            emqx_session:resume(SessPid, self());
        false ->
            emqx_log:error("Cannot resume ~p which seems already dead!", [SessPid]),
            {error, session_died}
    end;
    
resume_session(SessPid) ->
    case rpc:call(node(SessPid), emqx_session, resume, [SessPid]) of
        ok -> {ok, SessPid};
        {badrpc, Reason} ->
            {error, Reason};
        {error, Reason} ->
            {error, Reason}
    end.

discard_session(ClientId) ->
    case lookup_session(ClientId) of
        undefined -> ok;
        Pid -> emqx_session:discard(Pid)
    end.

lookup_session(ClientId) ->
    try ets:lookup_element(session, ClientId, 2) catch error:badarg -> undefined end.

close_session(SessPid) ->
    emqx_session:close(SessPid).    

with_lock(ClientId, Fun) ->
    case emqx_sm_locker:lock(ClientId) of
        true -> Result = Fun(),
                emqx_sm_locker:unlock(ClientId),
                Result;
        false -> {error, client_id_unavailable};
        {error, Reason} -> {error, Reason}
    end.

-spec(register_session(client_id()) -> true).
register_session(ClientId) ->
    ets:insert(session, {ClientId, self()}).

unregister_session(ClientId, Pid) ->
    case ets:lookup(session, ClientId) of
        [{_, Pid}] ->
            ets:delete_object(session, {ClientId, Pid});
        _ ->
            false
    end.

dispatch(ClientId, Topic, Msg) ->
    case lookup_session(ClientId) of
        Pid when is_pid(Pid) ->
            Pid ! {dispatch, Topic, Msg};
        undefined ->
            emqx_hooks:run('message.dropped', [ClientId, Msg])
    end.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([StatsFun]) ->
    {ok, TRef} = timer:send_interval(timer:seconds(1), stats),
    {ok, #state{stats_fun = StatsFun, stats_timer = TRef}}.

handle_call(Req, _From, State) ->
    emqx_log:error("[SM] Unexpected request: ~p", [Req]),
    {reply, ignore, State}.

handle_cast({monitor_session, SessionPid, ClientId},
            State = #state{monitors = Monitors}) ->
    MRef = erlang:monitor(process, SessionPid),
    {noreply, State#state{monitors = maps:put(MRef, ClientId, Monitors)}};

handle_cast(Msg, State) ->
    emqx_log:error("[SM] Unexpected msg: ~p", [Msg]),
    {noreply, State}.

handle_info(stats, State) ->
    {noreply, setstats(State), hibernate};

handle_info({'DOWN', MRef, process, DownPid, _Reason},
            State = #state{monitors = Monitors}) ->
    case maps:find(MRef, Monitors) of
        {ok, {ClientId, Pid}} ->
            ets:delete_object(session, {ClientId, Pid}),
            {noreply, setstats(State#state{monitors = maps:remove(MRef, Monitors)})};
        error ->
            emqx_log:error("session ~p not found", [DownPid]),
            {noreply, State}
    end;

handle_info(Info, State) ->
    emqx_log:error("[SM] Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State = #state{stats_timer = TRef}) ->
    timer:cancel(TRef).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

setstats(State = #state{stats_fun = StatsFun}) ->
    StatsFun(ets:info(session, size)), State.

