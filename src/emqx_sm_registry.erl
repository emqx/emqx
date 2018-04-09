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

-module(emqx_sm_registry).

-behaviour(gen_server).

-include("emqx.hrl").

%% API
-export([start_link/0]).

-export([register_session/1, lookup_session/1, unregister_session/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-define(TAB, session_registry).

-define(LOCK, {?MODULE, cleanup_sessions}).

-record(state, {}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link() ->
    ok = ekka_mnesia:create_table(?TAB, [
                {type, bag},
                {ram_copies, [node()]},
                {record_name, session},
                {attributes, record_info(fields, session)}]),
    ok = ekka_mnesia:copy_table(?TAB),
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec(lookup_session(client_id()) -> list(session())).
lookup_session(ClientId) ->
     mnesia:dirty_read(?TAB, ClientId).

-spec(register_session(session()) -> ok).
register_session(Session) when is_record(Session, session) ->
    mnesia:dirty_write(?TAB, Session).

-spec(unregister_session(session()) -> ok).
unregister_session(Session) when is_record(Session, session) ->
    mnesia:dirty_delete_object(?TAB, Session).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    ekka:monitor(membership),
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({membership, {mnesia, down, Node}}, State) ->
    global:trans({?LOCK, self()},
                 fun() ->
                     mnesia:transaction(fun cleanup_sessions/1, [Node])
                 end),
    {noreply, State};

handle_info({membership, _Event}, State) ->
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

cleanup_sessions(Node) ->
    Pat = [{#session{pid = '$1', _ = '_'},
            [{'==', {node, '$1'}, Node}], ['$_']}],
    lists:foreach(fun(Session) ->
                      mnesia:delete_object(?TAB, Session)
                  end, mnesia:select(?TAB, Pat)).

