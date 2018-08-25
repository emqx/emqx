%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_sm).

-behaviour(gen_server).

-include("emqx.hrl").

-export([start_link/0]).

-export([open_session/1, close_session/1]).
-export([lookup_session/1, lookup_session_pid/1]).
-export([resume_session/1, resume_session/2]).
-export([discard_session/1, discard_session/2]).
-export([register_session/2, get_session_attrs/1, unregister_session/1]).
-export([get_session_stats/1, set_session_stats/2]).

%% Internal functions for rpc
-export([dispatch/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-record(state, {session_pmon}).

-define(SM, ?MODULE).

%% ETS Tables
-define(SESSION,       emqx_session).
-define(SESSION_P,     emqx_persistent_session).
-define(SESSION_ATTRS, emqx_session_attrs).
-define(SESSION_STATS, emqx_session_stats).

-spec(start_link() -> {ok, pid()} | ignore | {error, term()}).
start_link() ->
    gen_server:start_link({local, ?SM}, ?MODULE, [], []).

%% @doc Open a session.
-spec(open_session(map()) -> {ok, pid()} | {ok, pid(), boolean()} | {error, term()}).
open_session(Attrs = #{clean_start := true,
                       client_id   := ClientId,
                       client_pid  := ClientPid}) ->
    CleanStart = fun(_) ->
                     ok = discard_session(ClientId, ClientPid),
                     emqx_session_sup:start_session(Attrs)
                 end,
    emqx_sm_locker:trans(ClientId, CleanStart);

open_session(Attrs = #{clean_start := false,
                       client_id   := ClientId,
                       client_pid  := ClientPid}) ->
    ResumeStart = fun(_) ->
                      case resume_session(ClientId, ClientPid) of
                          {ok, SPid} ->
                              {ok, SPid, true};
                          {error, not_found} ->
                              emqx_session_sup:start_session(Attrs);
                          {error, Reason} ->
                              {error, Reason}
                      end
                  end,
    emqx_sm_locker:trans(ClientId, ResumeStart).

%% @doc Discard all the sessions identified by the ClientId.
-spec(discard_session(client_id()) -> ok).
discard_session(ClientId) when is_binary(ClientId) ->
    discard_session(ClientId, self()).

discard_session(ClientId, ClientPid) when is_binary(ClientId) ->
    lists:foreach(
      fun({_ClientId, SPid}) ->
          case catch emqx_session:discard(SPid, ClientPid) of
              {Err, Reason} when Err =:= 'EXIT'; Err =:= error ->
                  emqx_logger:error("[SM] Failed to discard ~p: ~p", [SPid, Reason]);
              ok -> ok
          end
      end, lookup_session(ClientId)).

%% @doc Try to resume a session.
-spec(resume_session(client_id()) -> {ok, pid()} | {error, term()}).
resume_session(ClientId) ->
    resume_session(ClientId, self()).

resume_session(ClientId, ClientPid) ->
    case lookup_session(ClientId) of
        [] -> {error, not_found};
        [{_ClientId, SPid}] ->
            ok = emqx_session:resume(SPid, ClientPid),
            {ok, SPid};
        Sessions ->
            [{_, SPid}|StaleSessions] = lists:reverse(Sessions),
            emqx_logger:error("[SM] More than one session found: ~p", [Sessions]),
            lists:foreach(fun({_, StalePid}) ->
                              catch emqx_session:discard(StalePid, ClientPid)
                          end, StaleSessions),
            ok = emqx_session:resume(SPid, ClientPid),
            {ok, SPid}
    end.

%% @doc Close a session.
-spec(close_session({client_id(), pid()} | pid()) -> ok).
close_session({_ClientId, SPid}) ->
    emqx_session:close(SPid);
close_session(SPid) when is_pid(SPid) ->
    emqx_session:close(SPid).

%% @doc Register a session with attributes.
-spec(register_session(client_id() | {client_id(), pid()},
                       list(emqx_session:attribute())) -> ok).
register_session(ClientId, Attrs) when is_binary(ClientId) ->
    register_session({ClientId, self()}, Attrs);

register_session(Session = {ClientId, SPid}, Attrs)
    when is_binary(ClientId), is_pid(SPid) ->
    ets:insert(?SESSION, Session),
    ets:insert(?SESSION_ATTRS, {Session, Attrs}),
    case proplists:get_value(clean_start, Attrs, true) of
        true  -> ok;
        false  -> ets:insert(?SESSION_P, Session)
    end,
    emqx_sm_registry:register_session(Session),
    notify({registered, ClientId, SPid}).

%% @doc Get session attrs
-spec(get_session_attrs({client_id(), pid()})
      -> list(emqx_session:attribute())).
get_session_attrs(Session = {ClientId, SPid})
    when is_binary(ClientId), is_pid(SPid) ->
    safe_lookup_element(?SESSION_ATTRS, Session, []).

%% @doc Unregister a session
-spec(unregister_session(client_id() | {client_id(), pid()}) -> ok).
unregister_session(ClientId) when is_binary(ClientId) ->
    unregister_session({ClientId, self()});

unregister_session(Session = {ClientId, SPid})
    when is_binary(ClientId), is_pid(SPid) ->
    emqx_sm_registry:unregister_session(Session),
    ets:delete(?SESSION_STATS, Session),
    ets:delete(?SESSION_ATTRS, Session),
    ets:delete_object(?SESSION_P, Session),
    ets:delete_object(?SESSION, Session),
    notify({unregistered, ClientId, SPid}).

%% @doc Get session stats
-spec(get_session_stats({client_id(), pid()}) -> list(emqx_stats:stats())).
get_session_stats(Session = {ClientId, SPid})
    when is_binary(ClientId), is_pid(SPid) ->
    safe_lookup_element(?SESSION_STATS, Session, []).

%% @doc Set session stats
-spec(set_session_stats(client_id() | {client_id(), pid()},
                        emqx_stats:stats()) -> ok).
set_session_stats(ClientId, Stats) when is_binary(ClientId) ->
    set_session_stats({ClientId, self()}, Stats);

set_session_stats(Session = {ClientId, SPid}, Stats)
    when is_binary(ClientId), is_pid(SPid) ->
    ets:insert(?SESSION_STATS, {Session, Stats}).

%% @doc Lookup a session from registry
-spec(lookup_session(client_id()) -> list({client_id(), pid()})).
lookup_session(ClientId) ->
    case emqx_sm_registry:is_enabled() of
        true  -> emqx_sm_registry:lookup_session(ClientId);
        false -> ets:lookup(?SESSION, ClientId)
    end.

%% @doc Dispatch a message to the session.
-spec(dispatch(client_id(), topic(), message()) -> any()).
dispatch(ClientId, Topic, Msg) ->
    case lookup_session_pid(ClientId) of
        Pid when is_pid(Pid) ->
            Pid ! {dispatch, Topic, Msg};
        undefined ->
            emqx_hooks:run('message.dropped', [ClientId, Msg])
    end.

%% @doc Lookup session pid.
-spec(lookup_session_pid(client_id()) -> pid() | undefined).
lookup_session_pid(ClientId) ->
    safe_lookup_element(?SESSION, ClientId, undefined).

safe_lookup_element(Tab, Key, Default) ->
    try ets:lookup_element(Tab, Key, 2)
    catch
        error:badarg -> Default
    end.

notify(Event) ->
    gen_server:cast(?SM, {notify, Event}).

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

init([]) ->
    TabOpts = [public, set, {write_concurrency, true}],
    _ = emqx_tables:new(?SESSION, [{read_concurrency, true} | TabOpts]),
    _ = emqx_tables:new(?SESSION_P, TabOpts),
    _ = emqx_tables:new(?SESSION_ATTRS, TabOpts),
    _ = emqx_tables:new(?SESSION_STATS, TabOpts),
    emqx_stats:update_interval(sm_stats, stats_fun()),
    {ok, #state{session_pmon = emqx_pmon:new()}}.

handle_call(Req, _From, State) ->
    emqx_logger:error("[SM] unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast({notify, {registered, ClientId, SPid}}, State = #state{session_pmon = PMon}) ->
    {noreply, State#state{session_pmon = emqx_pmon:monitor(SPid, ClientId, PMon)}};

handle_cast({notify, {unregistered, _ClientId, SPid}}, State = #state{session_pmon = PMon}) ->
    {noreply, State#state{session_pmon = emqx_pmon:demonitor(SPid, PMon)}};

handle_cast(Msg, State) ->
    emqx_logger:error("[SM] unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info({'DOWN', _MRef, process, DownPid, _Reason}, State = #state{session_pmon = PMon}) ->
    case emqx_pmon:find(DownPid, PMon) of
        undefined -> {noreply, State};
        ClientId  ->
            unregister_session({ClientId, DownPid}),
            {noreply, State#state{session_pmon = emqx_pmon:erase(DownPid, PMon)}}
    end;

handle_info(Info, State) ->
    emqx_logger:error("[SM] unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    emqx_stats:cancel_update(sm_stats).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

stats_fun() ->
    fun() ->
        safe_update_stats(?SESSION, 'sessions/count', 'sessions/max'),
        safe_update_stats(?SESSION_P, 'sessions/persistent/count', 'sessions/persistent/max')
    end.

safe_update_stats(Tab, Stat, MaxStat) ->
    case ets:info(Tab, size) of
        undefined -> ok;
        Size -> emqx_stats:setstat(Stat, MaxStat, Size)
    end.

