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

-module(emqx_sm).

-behaviour(gen_server).

-include("emqx.hrl").
-include("logger.hrl").
-include("types.hrl").

%% APIs
-export([start_link/0]).

-export([ open_session/1
        , close_session/1
        , resume_session/2
        , discard_session/1
        , discard_session/2
        , register_session/1
        , register_session/2
        , unregister_session/1
        , unregister_session/2
        ]).

-export([ get_session_attrs/1
        , get_session_attrs/2
        , set_session_attrs/2
        , set_session_attrs/3
        , get_session_stats/1
        , get_session_stats/2
        , set_session_stats/2
        , set_session_stats/3
        ]).

-export([lookup_session_pids/1]).

%% Internal functions for rpc
-export([dispatch/3]).

%% Internal function for stats
-export([stats_fun/0]).

%% Internal function for emqx_session_sup
-export([clean_down/1]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-define(SM, ?MODULE).

%% ETS Tables for session management.
-define(SESSION_TAB, emqx_session).
-define(SESSION_P_TAB, emqx_session_p).
-define(SESSION_ATTRS_TAB, emqx_session_attrs).
-define(SESSION_STATS_TAB, emqx_session_stats).

-define(BATCH_SIZE, 100000).

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

-spec(start_link() -> startlink_ret()).
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

open_session(SessAttrs = #{clean_start := false, client_id := ClientId}) ->
    ResumeStart = fun(_) ->
                      case resume_session(ClientId, SessAttrs) of
                          {ok, SessPid} ->
                              {ok, SessPid, true};
                          {error, not_found} ->
                              emqx_session_sup:start_session(SessAttrs)
                      end
                  end,
    emqx_sm_locker:trans(ClientId, ResumeStart).

%% @doc Discard all the sessions identified by the ClientId.
-spec(discard_session(emqx_types:client_id()) -> ok).
discard_session(ClientId) when is_binary(ClientId) ->
    discard_session(ClientId, self()).

-spec(discard_session(emqx_types:client_id(), pid()) -> ok).
discard_session(ClientId, ConnPid) when is_binary(ClientId) ->
    lists:foreach(
      fun(SessPid) ->
          try emqx_session:discard(SessPid, ConnPid)
          catch
              _:Error:_Stk ->
                  ?LOG(warning, "[SM] Failed to discard ~p: ~p", [SessPid, Error])
          end
      end, lookup_session_pids(ClientId)).

%% @doc Try to resume a session.
-spec(resume_session(emqx_types:client_id(), map()) -> {ok, pid()} | {error, term()}).
resume_session(ClientId, SessAttrs = #{conn_pid := ConnPid}) ->
    case lookup_session_pids(ClientId) of
        [] -> {error, not_found};
        [SessPid] ->
            ok = emqx_session:resume(SessPid, SessAttrs),
            {ok, SessPid};
        SessPids ->
            [SessPid|StalePids] = lists:reverse(SessPids),
            ?LOG(error, "[SM] More than one session found: ~p", [SessPids]),
            lists:foreach(fun(StalePid) ->
                              catch emqx_session:discard(StalePid, ConnPid)
                          end, StalePids),
            ok = emqx_session:resume(SessPid, SessAttrs),
            {ok, SessPid}
    end.

%% @doc Close a session.
-spec(close_session(emqx_types:client_id() | pid()) -> ok).
close_session(ClientId) when is_binary(ClientId) ->
    case lookup_session_pids(ClientId) of
        [] -> ok;
        [SessPid] -> close_session(SessPid);
        SessPids -> lists:foreach(fun close_session/1, SessPids)
    end;

close_session(SessPid) when is_pid(SessPid) ->
    emqx_session:close(SessPid).

%% @doc Register a session.
-spec(register_session(emqx_types:client_id()) -> ok).
register_session(ClientId) when is_binary(ClientId) ->
    register_session(ClientId, self()).

-spec(register_session(emqx_types:client_id(), pid()) -> ok).
register_session(ClientId, SessPid) when is_binary(ClientId), is_pid(SessPid) ->
    Session = {ClientId, SessPid},
    true = ets:insert(?SESSION_TAB, Session),
    emqx_sm_registry:register_session(Session).

%% @doc Unregister a session
-spec(unregister_session(emqx_types:client_id()) -> ok).
unregister_session(ClientId) when is_binary(ClientId) ->
    unregister_session(ClientId, self()).

-spec(unregister_session(emqx_types:client_id(), pid()) -> ok).
unregister_session(ClientId, SessPid) when is_binary(ClientId), is_pid(SessPid) ->
    Session = {ClientId, SessPid},
    true = ets:delete(?SESSION_STATS_TAB, Session),
    true = ets:delete(?SESSION_ATTRS_TAB, Session),
    true = ets:delete_object(?SESSION_P_TAB, Session),
    true = ets:delete_object(?SESSION_TAB, Session),
    emqx_sm_registry:unregister_session(Session).

%% @doc Get session attrs
-spec(get_session_attrs(emqx_types:client_id()) -> list(emqx_session:attr())).
get_session_attrs(ClientId) when is_binary(ClientId) ->
    case lookup_session_pids(ClientId) of
        [] -> [];
        [SessPid|_] -> get_session_attrs(ClientId, SessPid)
    end.

-spec(get_session_attrs(emqx_types:client_id(), pid()) -> list(emqx_session:attr())).
get_session_attrs(ClientId, SessPid) when is_binary(ClientId), is_pid(SessPid) ->
    emqx_tables:lookup_value(?SESSION_ATTRS_TAB, {ClientId, SessPid}, []).

%% @doc Set session attrs
-spec(set_session_attrs(emqx_types:client_id(), list(emqx_session:attr())) -> true).
set_session_attrs(ClientId, SessAttrs) when is_binary(ClientId) ->
    set_session_attrs(ClientId, self(), SessAttrs).

-spec(set_session_attrs(emqx_types:client_id(), pid(), list(emqx_session:attr())) -> true).
set_session_attrs(ClientId, SessPid, SessAttrs) when is_binary(ClientId), is_pid(SessPid) ->
    Session = {ClientId, SessPid},
    true = ets:insert(?SESSION_ATTRS_TAB, {Session, SessAttrs}),
    proplists:get_value(clean_start, SessAttrs, true) orelse ets:insert(?SESSION_P_TAB, Session).

%% @doc Get session stats
-spec(get_session_stats(emqx_types:client_id()) -> list(emqx_stats:stats())).
get_session_stats(ClientId) when is_binary(ClientId) ->
    case lookup_session_pids(ClientId) of
        [] -> [];
        [SessPid|_] ->
            get_session_stats(ClientId, SessPid)
    end.

-spec(get_session_stats(emqx_types:client_id(), pid()) -> list(emqx_stats:stats())).
get_session_stats(ClientId, SessPid) when is_binary(ClientId) ->
    emqx_tables:lookup_value(?SESSION_STATS_TAB, {ClientId, SessPid}, []).

%% @doc Set session stats
-spec(set_session_stats(emqx_types:client_id(), emqx_stats:stats()) -> true).
set_session_stats(ClientId, Stats) when is_binary(ClientId) ->
    set_session_stats(ClientId, self(), Stats).

-spec(set_session_stats(emqx_types:client_id(), pid(), emqx_stats:stats()) -> true).
set_session_stats(ClientId, SessPid, Stats) when is_binary(ClientId), is_pid(SessPid) ->
    ets:insert(?SESSION_STATS_TAB, {{ClientId, SessPid}, Stats}).

%% @doc Lookup session pid.
-spec(lookup_session_pids(emqx_types:client_id()) -> list(pid())).
lookup_session_pids(ClientId) ->
    case emqx_sm_registry:is_enabled() of
        true -> emqx_sm_registry:lookup_session(ClientId);
        false ->
            case emqx_tables:lookup_value(?SESSION_TAB, ClientId) of
                undefined -> [];
                SessPid when is_pid(SessPid) -> [SessPid]
            end
    end.

%% @doc Dispatch a message to the session.
-spec(dispatch(emqx_types:client_id(), emqx_topic:topic(), emqx_types:message()) -> any()).
dispatch(ClientId, Topic, Msg) ->
    case lookup_session_pids(ClientId) of
        [SessPid|_] when is_pid(SessPid) ->
            SessPid ! {dispatch, Topic, Msg};
        [] ->
            emqx_hooks:run('message.dropped', [#{client_id => ClientId}, Msg])
    end.

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

init([]) ->
    TabOpts = [public, set, {write_concurrency, true}],
    ok = emqx_tables:new(?SESSION_TAB, [{read_concurrency, true} | TabOpts]),
    ok = emqx_tables:new(?SESSION_P_TAB, TabOpts),
    ok = emqx_tables:new(?SESSION_ATTRS_TAB, TabOpts),
    ok = emqx_tables:new(?SESSION_STATS_TAB, TabOpts),
    ok = emqx_stats:update_interval(sess_stats, fun ?MODULE:stats_fun/0),
    {ok, #{}}.

handle_call(Req, _From, State) ->
    ?LOG(error, "[SM] Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?LOG(error, "[SM] Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info(Info, State) ->
    ?LOG(error, "[SM] Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    emqx_stats:cancel_update(sess_stats).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

clean_down(Session = {ClientId, SessPid}) ->
    case ets:member(?SESSION_TAB, ClientId)
         orelse ets:member(?SESSION_ATTRS_TAB, Session) of
        true ->
            unregister_session(ClientId, SessPid);
        false -> ok
    end.

stats_fun() ->
    safe_update_stats(?SESSION_TAB, 'sessions/count', 'sessions/max'),
    safe_update_stats(?SESSION_P_TAB, 'sessions/persistent/count', 'sessions/persistent/max').

safe_update_stats(Tab, Stat, MaxStat) ->
    case ets:info(Tab, size) of
        undefined -> ok;
        Size -> emqx_stats:setstat(Stat, MaxStat, Size)
    end.

