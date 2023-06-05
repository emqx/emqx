%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_ds).

%% API:
%%   Messages:
-export([message_store/2, message_store/1, message_stats/0]).
%%   Iterator:
-export([iterator_update/2, iterator_next/1, iterator_stats/0]).
%%   Session:
-export([
    session_open/1,
    session_drop/1,
    session_suspend/1,
    session_add_iterator/2,
    session_del_iterator/2,
    session_stats/0
]).

%% internal exports:
-export([]).

-export_type([
    message_id/0,
    message_stats/0,
    message_store_opts/0,
    session_id/0,
    iterator_id/0,
    iterator/0
]).

-include("emqx_ds_int.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-type session_id() :: emqx_types:clientid().

-type iterator() :: term().

-opaque iterator_id() :: binary().

%%-type session() :: #session{}.

-type message_store_opts() :: #{}.

-type message_stats() :: #{}.

-type message_id() :: binary().

%%================================================================================
%% API funcions
%%================================================================================

%%--------------------------------------------------------------------------------
%% Message
%%--------------------------------------------------------------------------------
-spec message_store([emqx_types:message()], message_store_opts()) ->
    {ok, [message_id()]} | {error, _}.
message_store(_Msg, _Opts) ->
    %% TODO
    ok.

-spec message_store([emqx_types:message()]) -> {ok, [message_id()]} | {error, _}.
message_store(Msg) ->
    %% TODO
    message_store(Msg, #{}).

-spec message_stats() -> message_stats().
message_stats() ->
    #{}.

%%--------------------------------------------------------------------------------
%% Session
%%--------------------------------------------------------------------------------

%% @doc Called when a client connects. This function looks up a
%% session or creates a new one if previous one couldn't be found.
%%
%% This function also spawns replay agents for each iterator.
%%
%% Note: session API doesn't handle session takeovers, it's the job of
%% the broker.
-spec session_open(emqx_types:clientid()) -> {_New :: boolean(), session_id(), [iterator_id()]}.
session_open(ClientID) ->
    {atomic, Ret} =
        mria:transaction(
            ?DS_SHARD,
            fun() ->
                case mnesia:read(?SESSION_TAB, ClientID) of
                    [#session{iterators = Iterators}] ->
                        {false, ClientID, Iterators};
                    [] ->
                        Session = #session{id = ClientID, iterators = []},
                        mnesia:write(?SESSION_TAB, Session),
                        {true, ClientID, []}
                end
            end
        ),
    Ret.

%% @doc Called when a client reconnects with `clean session=true' or
%% during session GC
-spec session_drop(emqx_types:clientid()) -> ok.
session_drop(ClientID) ->
    {atomic, ok} = mnesia:transaction(
        ?DS_SHARD,
        fun() ->
            mnesia:delete(?SESSION_TAB, ClientID)
        end
    ),
    ok.

%% @doc Called when a client disconnects. This function terminates all
%% active processes related to the session.
-spec session_suspend(session_id()) -> ok | {error, session_not_found}.
session_suspend(_SessionId) ->
    %% TODO
    ok.

%% @doc Called when a client subscribes to a topic. Idempotent.
-spec session_add_iterator(session_id(), emqx_topic:words()) ->
    {ok, iterator_id()} | {error, session_not_found}.
session_add_iterator(_SessionId, _TopicFilter) ->
    %% TODO
    {ok, <<"">>}.

%% @doc Called when a client unsubscribes from a topic. Returns `true'
%% if the session contained the subscription or `false' if it wasn't
%% subscribed.
-spec session_del_iterator(session_id(), emqx_topic:words()) ->
    {ok, boolean()} | {error, session_not_found}.
session_del_iterator(_SessionId, _TopicFilter) ->
    %% TODO
    false.

-spec session_stats() -> #{}.
session_stats() ->
    #{}.

%%--------------------------------------------------------------------------------
%% Iterator (pull API)
%%--------------------------------------------------------------------------------

%% @doc Called when a client acks a message
-spec iterator_update(iterator_id(), iterator()) -> ok.
iterator_update(_IterId, _Iter) ->
    %% TODO
    ok.

%% @doc Called when a client acks a message
-spec iterator_next(iterator()) -> {value, emqx_types:message(), iterator()} | none | {error, _}.
iterator_next(_Iter) ->
    %% TODO
    ok.

-spec iterator_stats() -> #{}.
iterator_stats() ->
    #{}.

%%================================================================================
%% Internal functions
%%================================================================================
