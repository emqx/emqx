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

-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% API:
-export([ensure_shard/2]).
%%   Messages:
-export([message_store/2, message_store/1, message_stats/0]).
%%   Iterator:
-export([iterator_update/2, iterator_next/1, iterator_stats/0]).
%%   Session:
-export([
    session_open/2,
    session_drop/1,
    session_suspend/1,
    session_add_iterator/3,
    session_get_iterator_id/2,
    session_del_iterator/2,
    session_stats/0
]).

%% internal exports:
-export([]).

-export_type([
    keyspace/0,
    message_id/0,
    message_stats/0,
    message_store_opts/0,
    session_id/0,
    replay/0,
    replay_id/0,
    iterator_id/0,
    iterator/0,
    shard/0,
    shard_id/0,
    topic/0,
    time/0
]).

-include("emqx_ds_int.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

%% Session
%% See also: `#session{}`.
-type session() :: #{
    id := emqx_ds:session_id(),
    created_at := _Millisecond :: non_neg_integer(),
    expires_at := _Millisecond :: non_neg_integer() | never,
    iterators := map(),
    props := map()
}.

%% Currently, this is the clientid.  We avoid `emqx_types:clientid()' because that can be
%% an atom, in theory (?).
-type session_id() :: binary().

-type iterator() :: term().

-type iterator_id() :: binary().

%%-type session() :: #session{}.

-type message_store_opts() :: #{}.

-type message_stats() :: #{}.

-type message_id() :: binary().

%% Parsed topic:
-type topic() :: list(binary()).

-type keyspace() :: atom().
-type shard_id() :: binary().
-type shard() :: {keyspace(), shard_id()}.

%% Timestamp
%% Earliest possible timestamp is 0.
%% TODO granularity?  Currently, we should always use micro second, as that's the unit we
%% use in emqx_guid.  Otherwise, the iterators won't match the message timestamps.
-type time() :: non_neg_integer().

-type replay_id() :: binary().

-type replay() :: {
    _TopicFilter :: emqx_topic:words(),
    _StartTime :: time()
}.

%%================================================================================
%% API funcions
%%================================================================================

-spec ensure_shard(shard(), emqx_ds_storage_layer:options()) ->
    ok | {error, _Reason}.
ensure_shard(Shard, Options) ->
    case emqx_ds_storage_layer_sup:start_shard(Shard, Options) of
        {ok, _Pid} ->
            ok;
        {error, {already_started, _Pid}} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------------------
%% Message
%%--------------------------------------------------------------------------------
-spec message_store([emqx_types:message()], message_store_opts()) ->
    {ok, [message_id()]} | {error, _}.
message_store(_Msg, _Opts) ->
    %% TODO
    {error, not_implemented}.

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
-spec session_open(session_id(), _Props :: map()) -> {_New :: boolean(), session()}.
session_open(SessionId, Props) ->
    transaction(fun() ->
        case mnesia:read(?SESSION_TAB, SessionId, write) of
            [Record = #session{}] ->
                Session = export_record(Record),
                IteratorRefs = session_read_iterators(SessionId),
                Iterators = export_iterators(IteratorRefs),
                {false, Session#{iterators => Iterators}};
            [] ->
                Session = export_record(session_create(SessionId, Props)),
                {true, Session#{iterators => #{}}}
        end
    end).

session_create(SessionId, Props) ->
    Session = #session{
        id = SessionId,
        created_at = erlang:system_time(millisecond),
        expires_at = never,
        props = Props
    },
    ok = mnesia:write(?SESSION_TAB, Session, write),
    Session.

%% @doc Called when a client reconnects with `clean session=true' or
%% during session GC
-spec session_drop(session_id()) -> ok.
session_drop(DSSessionId) ->
    transaction(fun() ->
        %% TODO: ensure all iterators from this clientid are closed?
        IteratorRefs = session_read_iterators(DSSessionId),
        ok = lists:foreach(fun session_del_iterator/1, IteratorRefs),
        ok = mnesia:delete(?SESSION_TAB, DSSessionId, write)
    end).

%% @doc Called when a client disconnects. This function terminates all
%% active processes related to the session.
-spec session_suspend(session_id()) -> ok | {error, session_not_found}.
session_suspend(_SessionId) ->
    %% TODO
    ok.

%% @doc Called when a client subscribes to a topic. Idempotent.
-spec session_add_iterator(session_id(), emqx_topic:words(), _Props :: map()) ->
    {ok, iterator(), _IsNew :: boolean()}.
session_add_iterator(DSSessionId, TopicFilter, Props) ->
    IteratorRefId = {DSSessionId, TopicFilter},
    transaction(fun() ->
        case mnesia:read(?ITERATOR_REF_TAB, IteratorRefId, write) of
            [] ->
                IteratorRef = session_insert_iterator(DSSessionId, TopicFilter, Props),
                Iterator = export_record(IteratorRef),
                ?tp(
                    ds_session_subscription_added,
                    #{iterator => Iterator, session_id => DSSessionId}
                ),
                {ok, Iterator, _IsNew = true};
            [#iterator_ref{} = IteratorRef] ->
                NIteratorRef = session_update_iterator(IteratorRef, Props),
                NIterator = export_record(NIteratorRef),
                ?tp(
                    ds_session_subscription_present,
                    #{iterator => NIterator, session_id => DSSessionId}
                ),
                {ok, NIterator, _IsNew = false}
        end
    end).

session_insert_iterator(DSSessionId, TopicFilter, Props) ->
    {IteratorId, StartMS} = new_iterator_id(DSSessionId),
    IteratorRef = #iterator_ref{
        ref_id = {DSSessionId, TopicFilter},
        it_id = IteratorId,
        start_time = StartMS,
        props = Props
    },
    ok = mnesia:write(?ITERATOR_REF_TAB, IteratorRef, write),
    IteratorRef.

session_update_iterator(IteratorRef, Props) ->
    NIteratorRef = IteratorRef#iterator_ref{props = Props},
    ok = mnesia:write(?ITERATOR_REF_TAB, NIteratorRef, write),
    NIteratorRef.

-spec session_get_iterator_id(session_id(), emqx_topic:words()) ->
    {ok, iterator_id()} | {error, not_found}.
session_get_iterator_id(DSSessionId, TopicFilter) ->
    IteratorRefId = {DSSessionId, TopicFilter},
    case mnesia:dirty_read(?ITERATOR_REF_TAB, IteratorRefId) of
        [] ->
            {error, not_found};
        [#iterator_ref{it_id = IteratorId}] ->
            {ok, IteratorId}
    end.

%% @doc Called when a client unsubscribes from a topic.
-spec session_del_iterator(session_id(), emqx_topic:words()) -> ok.
session_del_iterator(DSSessionId, TopicFilter) ->
    IteratorRefId = {DSSessionId, TopicFilter},
    transaction(fun() ->
        mnesia:delete(?ITERATOR_REF_TAB, IteratorRefId, write)
    end).

session_read_iterators(DSSessionId) ->
    % NOTE: somewhat convoluted way to trick dialyzer
    Pat = erlang:make_tuple(record_info(size, iterator_ref), '_', [
        {1, iterator_ref},
        {#iterator_ref.ref_id, {DSSessionId, '_'}}
    ]),
    mnesia:match_object(?ITERATOR_REF_TAB, Pat, read).

session_del_iterator(#iterator_ref{ref_id = IteratorRefId}) ->
    mnesia:delete(?ITERATOR_REF_TAB, IteratorRefId, write).

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
    none.

-spec iterator_stats() -> #{}.
iterator_stats() ->
    #{}.

%%================================================================================
%% Internal functions
%%================================================================================

-spec new_iterator_id(session_id()) -> {iterator_id(), time()}.
new_iterator_id(DSSessionId) ->
    NowMS = erlang:system_time(microsecond),
    IteratorId = <<DSSessionId/binary, (emqx_guid:gen())/binary>>,
    {IteratorId, NowMS}.

%%--------------------------------------------------------------------------------

transaction(Fun) ->
    {atomic, Res} = mria:transaction(?DS_MRIA_SHARD, Fun),
    Res.

%%--------------------------------------------------------------------------------

export_iterators(IteratorRefs) ->
    lists:foldl(
        fun(IteratorRef = #iterator_ref{ref_id = {_DSSessionId, TopicFilter}}, Acc) ->
            Acc#{TopicFilter => export_record(IteratorRef)}
        end,
        #{},
        IteratorRefs
    ).

export_record(#session{} = Record) ->
    export_record(Record, #session.id, [id, created_at, expires_at, props], #{});
export_record(#iterator_ref{} = Record) ->
    export_record(Record, #iterator_ref.it_id, [id, start_time, props], #{}).

export_record(Record, I, [Field | Rest], Acc) ->
    export_record(Record, I + 1, Rest, Acc#{Field => element(I, Record)});
export_record(_, _, [], Acc) ->
    Acc.
