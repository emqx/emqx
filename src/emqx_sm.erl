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
-export([register_session/2, unregister_session/1]).
-export([get_session_attrs/1, set_session_attrs/2]).
-export([get_session_stats/1, set_session_stats/2]).

%% Internal functions for rpc
-export([dispatch/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-define(SM, ?MODULE).

%% ETS Tables
-define(SESSION_TAB,       emqx_session).
-define(SESSION_P_TAB,     emqx_persistent_session).
-define(SESSION_ATTRS_TAB, emqx_session_attrs).
-define(SESSION_STATS_TAB, emqx_session_stats).

-spec(start_link() -> emqx_types:startlink_ret()).
start_link() ->
    gen_server:start_link({local, ?SM}, ?MODULE, [], []).

%% @doc Open a session.
-spec(open_session(map()) -> {ok, pid()} | {ok, pid(), boolean()} | {error, term()}).
open_session(SessAttrs = #{clean_start := true, client_id := ClientId, conn_pid := ConnPid}) ->
    CleanStart = fun(_) ->
                     ok = discard_session(ClientId, ConnPid),
                     emqx_session_sup:start_session(SessAttrs)
                 end,
    emqx_sm_locker:trans(ClientId, CleanStart);

open_session(SessAttrs = #{clean_start := false, 
                           client_id := ClientId, 
                           conn_pid := ConnPid, 
                           max_inflight := MaxInflight,
                           topic_alias_maximum := TopicAliasMaximum}) ->
    ResumeStart = fun(_) ->
                      case resume_session(ClientId, ConnPid) of
                          {ok, SPid} ->
                              emqx_session:update_misc(SPid, #{max_inflight => MaxInflight, topic_alias_maximum => TopicAliasMaximum}),
                              {ok, SPid, true};
                          {error, not_found} ->
                              emqx_session_sup:start_session(SessAttrs)
                      end
                  end,
    emqx_sm_locker:trans(ClientId, ResumeStart).

%% @doc Discard all the sessions identified by the ClientId.
-spec(discard_session(emqx_types:client_id()) -> ok).
discard_session(ClientId) when is_binary(ClientId) ->
    discard_session(ClientId, self()).

discard_session(ClientId, ConnPid) when is_binary(ClientId) ->
    lists:foreach(fun({_ClientId, SPid}) ->
                      case catch emqx_session:discard(SPid, ConnPid) of
                          {Err, Reason} when Err =:= 'EXIT'; Err =:= error ->
                              emqx_logger:error("[SM] Failed to discard ~p: ~p", [SPid, Reason]);
                          ok -> ok
                      end
                  end, lookup_session(ClientId)).

%% @doc Try to resume a session.
-spec(resume_session(emqx_types:client_id()) -> {ok, pid()} | {error, term()}).
resume_session(ClientId) ->
    resume_session(ClientId, self()).

resume_session(ClientId, ConnPid) ->
    case lookup_session(ClientId) of
        [] -> {error, not_found};
        [{_ClientId, SPid}] ->
            ok = emqx_session:resume(SPid, ConnPid),
            {ok, SPid};
        Sessions ->
            [{_, SPid}|StaleSessions] = lists:reverse(Sessions),
            emqx_logger:error("[SM] More than one session found: ~p", [Sessions]),
            lists:foreach(fun({_, StalePid}) ->
                              catch emqx_session:discard(StalePid, ConnPid)
                          end, StaleSessions),
            ok = emqx_session:resume(SPid, ConnPid),
            {ok, SPid}
    end.

%% @doc Close a session.
-spec(close_session({emqx_types:client_id(), pid()} | pid()) -> ok).
close_session({_ClientId, SPid}) ->
    emqx_session:close(SPid);
close_session(SPid) when is_pid(SPid) ->
    emqx_session:close(SPid).

%% @doc Register a session with attributes.
-spec(register_session(emqx_types:client_id() | {emqx_types:client_id(), pid()},
                       list(emqx_session:attr())) -> ok).
register_session(ClientId, SessAttrs) when is_binary(ClientId) ->
    register_session({ClientId, self()}, SessAttrs);

register_session(Session = {ClientId, SPid}, SessAttrs)
    when is_binary(ClientId), is_pid(SPid) ->
    ets:insert(?SESSION_TAB, Session),
    ets:insert(?SESSION_ATTRS_TAB, {Session, SessAttrs}),
    proplists:get_value(clean_start, SessAttrs, true)
        andalso ets:insert(?SESSION_P_TAB, Session),
    emqx_sm_registry:register_session(Session),
    notify({registered, ClientId, SPid}).

%% @doc Get session attrs
-spec(get_session_attrs({emqx_types:client_id(), pid()}) -> list(emqx_session:attr())).
get_session_attrs(Session = {ClientId, SPid}) when is_binary(ClientId), is_pid(SPid) ->
    safe_lookup_element(?SESSION_ATTRS_TAB, Session, []).

%% @doc Set session attrs
-spec(set_session_attrs(emqx_types:client_id() | {emqx_types:client_id(), pid()},
                        list(emqx_session:attr())) -> true).
set_session_attrs(ClientId, SessAttrs) when is_binary(ClientId) ->
    set_session_attrs({ClientId, self()}, SessAttrs);
set_session_attrs(Session = {ClientId, SPid}, SessAttrs) when is_binary(ClientId), is_pid(SPid) ->
    ets:insert(?SESSION_ATTRS_TAB, {Session, SessAttrs}).

%% @doc Unregister a session
-spec(unregister_session(emqx_types:client_id() | {emqx_types:client_id(), pid()}) -> ok).
unregister_session(ClientId) when is_binary(ClientId) ->
    unregister_session({ClientId, self()});

unregister_session(Session = {ClientId, SPid}) when is_binary(ClientId), is_pid(SPid) ->
    emqx_sm_registry:unregister_session(Session),
    ets:delete(?SESSION_STATS_TAB, Session),
    ets:delete(?SESSION_ATTRS_TAB, Session),
    ets:delete_object(?SESSION_P_TAB, Session),
    ets:delete_object(?SESSION_TAB, Session),
    notify({unregistered, ClientId, SPid}).

%% @doc Get session stats
-spec(get_session_stats({emqx_types:client_id(), pid()}) -> list(emqx_stats:stats())).
get_session_stats(Session = {ClientId, SPid}) when is_binary(ClientId), is_pid(SPid) ->
    safe_lookup_element(?SESSION_STATS_TAB, Session, []).

%% @doc Set session stats
-spec(set_session_stats(emqx_types:client_id() | {emqx_types:client_id(), pid()},
                        emqx_stats:stats()) -> true).
set_session_stats(ClientId, Stats) when is_binary(ClientId) ->
    set_session_stats({ClientId, self()}, Stats);
set_session_stats(Session = {ClientId, SPid}, Stats) when is_binary(ClientId), is_pid(SPid) ->
    ets:insert(?SESSION_STATS_TAB, {Session, Stats}).

%% @doc Lookup a session from registry
-spec(lookup_session(emqx_types:client_id()) -> list({emqx_types:client_id(), pid()})).
lookup_session(ClientId) ->
    case emqx_sm_registry:is_enabled() of
        true  -> emqx_sm_registry:lookup_session(ClientId);
        false -> ets:lookup(?SESSION_TAB, ClientId)
    end.

%% @doc Dispatch a message to the session.
-spec(dispatch(emqx_types:client_id(), emqx_topic:topic(), emqx_types:message()) -> any()).
dispatch(ClientId, Topic, Msg) ->
    case lookup_session_pid(ClientId) of
        Pid when is_pid(Pid) ->
            Pid ! {dispatch, Topic, Msg};
        undefined ->
            emqx_hooks:run('message.dropped', [#{client_id => ClientId}, Msg])
    end.

%% @doc Lookup session pid.
-spec(lookup_session_pid(emqx_types:client_id()) -> pid() | undefined).
lookup_session_pid(ClientId) ->
    safe_lookup_element(?SESSION_TAB, ClientId, undefined).

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
    _ = emqx_tables:new(?SESSION_TAB, [{read_concurrency, true} | TabOpts]),
    _ = emqx_tables:new(?SESSION_P_TAB, TabOpts),
    _ = emqx_tables:new(?SESSION_ATTRS_TAB, TabOpts),
    _ = emqx_tables:new(?SESSION_STATS_TAB, TabOpts),
    emqx_stats:update_interval(sm_stats, stats_fun()),
    {ok, #{session_pmon => emqx_pmon:new()}}.

handle_call(Req, _From, State) ->
    emqx_logger:error("[SM] unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast({notify, {registered, ClientId, SPid}}, State = #{session_pmon := PMon}) ->
    {noreply, State#{session_pmon := emqx_pmon:monitor(SPid, ClientId, PMon)}};

handle_cast({notify, {unregistered, _ClientId, SPid}}, State = #{session_pmon := PMon}) ->
    {noreply, State#{session_pmon := emqx_pmon:demonitor(SPid, PMon)}};

handle_cast(Msg, State) ->
    emqx_logger:error("[SM] unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info({'DOWN', _MRef, process, DownPid, _Reason}, State = #{session_pmon := PMon}) ->
    case emqx_pmon:find(DownPid, PMon) of
        undefined ->
            {noreply, State};
        ClientId  ->
            unregister_session({ClientId, DownPid}),
            {noreply, State#{session_pmon := emqx_pmon:erase(DownPid, PMon)}}
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
        safe_update_stats(?SESSION_TAB, 'sessions/count', 'sessions/max'),
        safe_update_stats(?SESSION_P_TAB, 'sessions/persistent/count', 'sessions/persistent/max')
    end.

safe_update_stats(Tab, Stat, MaxStat) ->
    case ets:info(Tab, size) of
        undefined -> ok;
        Size -> emqx_stats:setstat(Stat, MaxStat, Size)
    end.

