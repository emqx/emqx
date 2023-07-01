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

%% TODOs
%% 1. Atomicity with `emqx_session_router` updates.
%% 2. Separate table for iterators (?)
%% 3. Error handling:
%%   * Are aborts possible? Do we need to handle them gracefully?

-module(emqx_ds_session).

-include("emqx_ds_int.hrl").

-export([
    open/1,
    find/1,
    drop/1,
    suspend/1
]).

-export([
    add_iterator/2,
    del_iterator/2
]).

-type id() :: emqx_types:clientid().

-opaque iterator_id() :: binary().
-type iterators() :: #{emqx_topic:words() => iterator_id()}.

%% @doc Called when a client connects. This function looks up a
%% session or creates a new one if previous one couldn't be found.
%%
%% This function also spawns replay agents for each iterator.
%%
%% Note: session API doesn't handle session takeovers, it's the job of
%% the broker.
-spec open(emqx_types:clientid()) -> {_New :: boolean(), id(), iterators()}.
open(ClientID) ->
    {atomic, Ret} = mria:transaction(
        ?DS_SHARD,
        fun() ->
            case mnesia:read(?SESSION_TAB, ClientID) of
                [#session{iterators = Iterators}] ->
                    {false, ClientID, Iterators};
                [] ->
                    Session = #session{id = ClientID, iterators = #{}},
                    mnesia:write(?SESSION_TAB, Session, write),
                    {true, ClientID, []}
            end
        end
    ),
    Ret.

-spec find(emqx_types:clientid()) -> {ok, id(), iterators()} | {error, session_not_found}.
find(ClientID) ->
    {atomic, Res} = mria:ro_transaction(
        ?DS_SHARD,
        fun() ->
            case mnesia:dirty_read(?SESSION_TAB, ClientID) of
                [#session{iterators = Iterators}] ->
                    {ok, ClientID, Iterators};
                [] ->
                    {error, session_not_found}
            end
        end
    ),
    Res.

%% @doc Called when a client reconnects with `clean session=true' or
%% during session GC
-spec drop(id()) -> ok.
drop(ID) ->
    {atomic, ok} = mria:transaction(
        ?DS_SHARD,
        fun() ->
            mnesia:delete({?SESSION_TAB, ID})
        end
    ),
    ok.

%% @doc Called when a client disconnects. This function terminates all
%% active processes related to the session.
-spec suspend(id()) -> ok | {error, session_not_found}.
suspend(_ID) ->
    %% TODO
    ok.

%% @doc Called when a client subscribes to a topic. Idempotent.
-spec add_iterator(id(), emqx_topic:words()) ->
    {ok, iterator_id()} | {error, session_not_found}.
add_iterator(ID, TopicFilter) ->
    % TODO
    % Do we need a transaction here? Seems unlikely. One possible scenario where
    % it might help is battling a rogue node that didn't let the session go for
    % some reason.
    case mnesia:dirty_read(?SESSION_TAB, ID) of
        [#session{iterators = #{TopicFilter := IteratorID}}] ->
            {ok, IteratorID};
        [#session{iterators = Iterators} = Session] ->
            IteratorID = <<"TODO">>,
            NIterators = Iterators#{TopicFilter => IteratorID},
            NSession = Session#session{iterators = NIterators},
            mria:dirty_write(?SESSION_TAB, NSession),
            {ok, IteratorID};
        [] ->
            {error, session_not_found}
    end.

%% @doc Called when a client unsubscribes from a topic. Returns `true'
%% if the session contained the subscription or `false' if it wasn't
%% subscribed.
-spec del_iterator(id(), emqx_topic:words()) ->
    {ok, boolean()} | {error, session_not_found}.
del_iterator(ID, TopicFilter) ->
    % TODO: Do we need a transaction here? Seems unlikely, see above.
    case mnesia:dirty_read(?SESSION_TAB, ID) of
        [#session{iterators = Iterators = #{TopicFilter := _}} = Session] ->
            % TODO: close iterator
            NIterators = maps:remove(TopicFilter, Iterators),
            NSession = Session#session{iterators = NIterators},
            mria:dirty_write(?SESSION_TAB, NSession),
            {ok, true};
        [#session{iterators = #{}}] ->
            {ok, false};
        [] ->
            {error, session_not_found}
    end.
