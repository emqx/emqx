%%%===================================================================
%%% Copyright (c) 2013-2018 EMQ Inc. All rights reserved.
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%===================================================================

-module(emqx_sm_registry).

-behaviour(gen_server).

-include("emqx.hrl").

%% API
-export([start_link/0]).

-export([is_enabled/0]).

-export([register_session/1, lookup_session/1, unregister_session/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(REGISTRY, ?MODULE).

-define(TAB, emqx_session_registry).

-define(LOCK, {?MODULE, cleanup_sessions}).

-record(global_session, {sid, pid}).

-record(state, {}).

-type(session_pid() :: pid()).

%% @doc Start the session manager.
-spec(start_link() -> {ok, pid()} | ignore | {error, term()}).
start_link() ->
    gen_server:start_link({local, ?REGISTRY}, ?MODULE, [], []).

-spec(is_enabled() -> boolean()).
is_enabled() ->
    ets:info(?TAB, name) =/= undefined.

-spec(lookup_session(client_id()) -> list({client_id(), session_pid()})).
lookup_session(ClientId) ->
    [{ClientId, SessionPid} || #global_session{pid = SessionPid}
                               <- mnesia:dirty_read(?TAB, ClientId)].

-spec(register_session({client_id(), session_pid()}) -> ok).
register_session({ClientId, SessionPid}) when is_binary(ClientId),
                                              is_pid(SessionPid) ->
    mnesia:dirty_write(?TAB, record(ClientId, SessionPid)).

-spec(unregister_session({client_id(), session_pid()}) -> ok).
unregister_session({ClientId, SessionPid}) when is_binary(ClientId),
                                                is_pid(SessionPid) ->
    mnesia:dirty_delete_object(?TAB, record(ClientId, SessionPid)).

record(ClientId, SessionPid) ->
    #global_session{sid = ClientId, pid = SessionPid}.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    ok = ekka_mnesia:create_table(?TAB, [
                {type, bag},
                {ram_copies, [node()]},
                {record_name, global_session},
                {attributes, record_info(fields, global_session)}]),
    ok = ekka_mnesia:copy_table(?TAB),
    ekka:monitor(membership),
    {ok, #state{}}.

handle_call(Req, _From, State) ->
    emqx_logger:error("[Registry] Unexpected request: ~p", [Req]),
    {reply, ignore, State}.

handle_cast(Msg, State) ->
    emqx_logger:error("[Registry] Unexpected msg: ~p", [Msg]),
    {noreply, State}.

handle_info({membership, {mnesia, down, Node}}, State) ->
    global:trans({?LOCK, self()},
                 fun() ->
                     mnesia:transaction(fun cleanup_sessions/1, [Node])
                 end),
    {noreply, State};

handle_info({membership, _Event}, State) ->
    {noreply, State};

handle_info(Info, State) ->
    emqx_logger:error("[Registry] Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

cleanup_sessions(Node) ->
    Pat = [{#global_session{pid = '$1', _ = '_'},
            [{'==', {node, '$1'}, Node}], ['$_']}],
    lists:foreach(fun(Session) ->
                      mnesia:delete_object(?TAB, Session)
                  end, mnesia:select(?TAB, Pat)).

