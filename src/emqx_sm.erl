%%--------------------------------------------------------------------
%% Copyright (c) 2013-2018 EMQ Inc. All rights reserved.
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

-export([start_link/0]).

-export([open_session/1, lookup_session/1, close_session/1]).
-export([resume_session/1, resume_session/2, discard_session/1, discard_session/2]).
-export([register_session/2, unregister_session/1]).

%% Internal functions for rpc
-export([dispatch/3]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {pmon}).

-define(SM, ?MODULE).

-spec(start_link() -> {ok, pid()} | ignore | {error, term()}).
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% Open Session
%%--------------------------------------------------------------------

open_session(Attrs = #{clean_start := true,
                       client_id := ClientId, client_pid := ClientPid}) ->
    CleanStart = fun(_) ->
                     ok = discard_session(ClientId, ClientPid),
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

%%--------------------------------------------------------------------
%% Discard Session
%%--------------------------------------------------------------------

discard_session(ClientId) when is_binary(ClientId) ->
    discard_session(ClientId, self()).

discard_session(ClientId, ClientPid) when is_binary(ClientId) ->
    lists:foreach(
      fun(#session{pid = SessionPid}) ->
          case catch emqx_session:discard(SessionPid, ClientPid) of
              {'EXIT', Error} ->
                  emqx_log:error("[SM] Failed to discard ~p: ~p", [SessionPid, Error]);
              ok -> ok
          end
      end, lookup_session(ClientId)).

%%--------------------------------------------------------------------
%% Resume Session
%%--------------------------------------------------------------------

resume_session(ClientId) ->
    resume_session(ClientId, self()).

resume_session(ClientId, ClientPid) ->
    case lookup_session(ClientId) of
        [] -> {error, not_found};
        [#session{pid = SessionPid}] ->
            ok = emqx_session:resume(SessionPid, ClientPid),
            {ok, SessionPid};
        Sessions ->
            [#session{pid = SessionPid}|StaleSessions] = lists:reverse(Sessions),
            emqx_log:error("[SM] More than one session found: ~p", [Sessions]),
            lists:foreach(fun(#session{pid = Pid}) ->
                              catch emqx_session:discard(Pid, ClientPid)
                          end, StaleSessions),
            ok = emqx_session:resume(SessionPid, ClientPid),
            {ok, SessionPid}
    end.

%%--------------------------------------------------------------------
%% Close a session
%%--------------------------------------------------------------------

close_session(#session{pid = SessionPid}) ->
    emqx_session:close(SessionPid).

%%--------------------------------------------------------------------
%% Create/Delete a session
%%--------------------------------------------------------------------

register_session(Session, Attrs) when is_record(Session, session) ->
    ets:insert(session, Session),
    ets:insert(session_attrs, {Session, Attrs}),
    emqx_sm_registry:register_session(Session),
    gen_server:cast(?MODULE, {registered, Session}).

unregister_session(Session) when is_record(Session, session) ->
    emqx_sm_registry:unregister_session(Session),
    emqx_sm_stats:del_session_stats(Session),
    ets:delete(session_attrs, Session),
    ets:delete_object(session, Session),
    gen_server:cast(?MODULE, {unregistered, Session}).

%%--------------------------------------------------------------------
%% Lookup a session from registry
%%--------------------------------------------------------------------
    
lookup_session(ClientId) ->
    emqx_sm_registry:lookup_session(ClientId).

%%--------------------------------------------------------------------
%% Dispatch by client Id
%%--------------------------------------------------------------------

dispatch(ClientId, Topic, Msg) ->
    case lookup_session_pid(ClientId) of
        Pid when is_pid(Pid) ->
            Pid ! {dispatch, Topic, Msg};
        undefined ->
            emqx_hooks:run('message.dropped', [ClientId, Msg])
    end.

lookup_session_pid(ClientId) ->
    try ets:lookup_element(session, ClientId, #session.pid)
    catch error:badarg ->
        undefined
    end.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    _ = emqx_tables:create(session, [public, set, {keypos, 2},
                                     {read_concurrency, true},
                                     {write_concurrency, true}]),
    _ = emqx_tables:create(session_attrs, [public, set,
                                           {write_concurrency, true}]),
    {ok, #state{pmon = emqx_pmon:new()}}.

handle_call(Req, _From, State) ->
    emqx_log:error("[SM] Unexpected request: ~p", [Req]),
    {reply, ignore, State}.

handle_cast({registered, #session{sid = ClientId, pid = SessionPid}},
            State = #state{pmon = PMon}) ->
    {noreply, State#state{pmon = PMon:monitor(SessionPid, ClientId)}};

handle_cast({unregistered, #session{sid = _ClientId, pid = SessionPid}},
            State = #state{pmon = PMon}) ->
    {noreply, State#state{pmon = PMon:erase(SessionPid)}};

handle_cast(Msg, State) ->
    emqx_log:error("[SM] Unexpected msg: ~p", [Msg]),
    {noreply, State}.

handle_info({'DOWN', _MRef, process, DownPid, _Reason},
            State = #state{pmon = PMon}) ->
    case PMon:find(DownPid) of
        {ok, ClientId} ->
            case ets:lookup(session, ClientId) of
                [] -> ok;
                _  -> unregister_session(#session{sid = ClientId, pid = DownPid})
            end,
            {noreply, State};
        undefined ->
            {noreply, State}
    end;

handle_info(Info, State) ->
    emqx_log:error("[SM] Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

