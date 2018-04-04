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
-export([register_session/1, register_session/2]).
-export([unregister_session/1, unregister_session/2]).

%% Internal functions for rpc
-export([lookup/1, dispatch/3]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {stats, pids = #{}}).

-spec(start_link(fun()) -> {ok, pid()} | ignore | {error, term()}).
start_link(StatsFun) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [StatsFun], []).

open_session(Attrs = #{clean_start := true,
                       client_id := ClientId, client_pid := ClientPid}) ->
    CleanStart = fun(_) ->
                     discard_session(ClientId, ClientPid),
                     emqx_session_sup:start_session(Attrs)
                 end,
    emqx_sm_locker:trans(ClientId, CleanStart);

open_session(Attrs = #{clean_start := false,
                       client_id := ClientId, client_pid := ClientPid}) ->
    ResumeStart = fun(_) ->
                      case resume_session(ClientId, ClientPid) of
                          {ok, SessionPid} ->
                              {ok, SessionPid};
                          {error, not_found} ->
                              emqx_session_sup:start_session(Attrs);
                          {error, Reason} ->
                              {error, Reason}
                      end
                  end,
    emqx_sm_locker:trans(ClientId, ResumeStart).

discard_session(ClientId) ->
    discard_session(ClientId, self()).

discard_session(ClientId, ClientPid) ->
    lists:foreach(fun({_, SessionPid}) ->
                      catch emqx_session:discard(SessionPid, ClientPid)
                  end, lookup_session(ClientId)).

resume_session(ClientId) ->
    resume_session(ClientId, self()).

resume_session(ClientId, ClientPid) ->
    case lookup_session(ClientId) of
        [] -> {error, not_found};
        [{_, SessionPid}] ->
            ok = emqx_session:resume(SessionPid, ClientPid),
            {ok, SessionPid};
        [{_, SessionPid}|_More] = Sessions ->
            emqx_log:error("[SM] More than one session found: ~p", [Sessions]),
            ok = emqx_session:resume(SessionPid, ClientPid),
            {ok, SessionPid}
    end.

lookup_session(ClientId) ->
    {ResL, _} = multicall(?MODULE, lookup, [ClientId]),
    lists:append(ResL).

close_session(ClientId) ->
    lists:foreach(fun(#session{pid = SessionPid}) ->
                      emqx_session:close(SessionPid)
                  end, lookup_session(ClientId)).

register_session(ClientId) ->
    register_session(ClientId, self()).

register_session(ClientId, SessionPid) ->
    ets:insert(session, {ClientId, SessionPid}).

unregister_session(ClientId) ->
    unregister_session(ClientId, self()).

unregister_session(ClientId, SessionPid) ->
    case ets:lookup(session, ClientId) of
        [Session = {ClientId, SessionPid}] ->
            ets:delete(session_attrs, Session),
            ets:delete(session_stats, Session),
            ets:delete_object(session, Session);
        _ ->
            false
    end.

dispatch(ClientId, Topic, Msg) ->
    case lookup(ClientId) of
        [{_, Pid}] ->
            Pid ! {dispatch, Topic, Msg};
        [] ->
            emqx_hooks:run('message.dropped', [ClientId, Msg])
    end.

lookup(ClientId) ->
    ets:lookup(session, ClientId).

multicall(Mod, Fun, Args) ->
    multicall(ekka:nodelist(up), Mod, Fun, Args).

multicall([Node], Mod, Fun, Args) when Node == node() ->
    Res = erlang:apply(Mod, Fun, Args), [Res];

multicall(Nodes, Mod, Fun, Args) ->
    {ResL, _} = emqx_rpc:multicall(Nodes, Mod, Fun, Args),
    ResL.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([StatsFun]) ->
    {ok, sched_stats(StatsFun, #state{pids = #{}})}.

sched_stats(Fun, State) ->
    {ok, TRef} = timer:send_interval(timer:seconds(1), stats),
    State#state{stats = #{func => Fun, timer => TRef}}.

handle_call(Req, _From, State) ->
    emqx_log:error("[SM] Unexpected request: ~p", [Req]),
    {reply, ignore, State}.

handle_cast({registered, ClientId, SessionPid},
            State = #state{pids = Pids}) ->
    _ = erlang:monitor(process, SessionPid),
    {noreply, State#state{pids = maps:put(SessionPid, ClientId, Pids)}};

handle_cast(Msg, State) ->
    emqx_log:error("[SM] Unexpected msg: ~p", [Msg]),
    {noreply, State}.

handle_info(stats, State) ->
    {noreply, setstats(State), hibernate};

handle_info({'DOWN', _MRef, process, DownPid, _Reason},
            State = #state{pids = Pids}) ->
    case maps:find(DownPid, Pids) of
        {ok, ClientId} ->
            unregister_session(ClientId, DownPid),
            {noreply, State#state{pids = maps:remove(DownPid, Pids)}};
        error ->
            emqx_log:error("[SM] Session ~p not found", [DownPid]),
            {noreply, State}
    end;

handle_info(Info, State) ->
    emqx_log:error("[SM] Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State = #state{stats = #{timer := TRef}}) ->
    timer:cancel(TRef).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

setstats(State = #state{stats = #{func := Fun}}) ->
    Fun(ets:info(session, size)), State.

