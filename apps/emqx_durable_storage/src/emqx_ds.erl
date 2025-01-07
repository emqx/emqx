%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc Main interface module for `emqx_durable_storage' application.
%%
%% It takes care of forwarding calls to the underlying DBMS.
-module(emqx_ds).

-include_lib("emqx_durable_storage/include/emqx_ds.hrl").

%% Management API:
-export([
    register_backend/2,

    open_db/2,
    close_db/1,
    which_dbs/0,
    update_db_config/2,
    add_generation/1,
    list_generations_with_lifetimes/1,
    drop_generation/2,
    drop_db/1
]).

%% Message storage API:
-export([store_batch/2, store_batch/3]).

%% Message replay API:
-export([get_streams/3, make_iterator/4, next/3, poll/3]).

%% Message delete API:
-export([get_delete_streams/3, make_delete_iterator/4, delete_next/4]).

%% Misc. API:
-export([count/1]).
-export([timestamp_us/0]).

-export_type([
    create_db_opts/0,
    db_opts/0,
    db/0,
    time/0,
    topic_filter/0,
    topic/0,
    batch/0,
    dsbatch/0,
    operation/0,
    deletion/0,
    precondition/0,
    stream/0,
    delete_stream/0,
    delete_selector/0,
    rank_x/0,
    rank_y/0,
    stream_rank/0,
    iterator/0,
    delete_iterator/0,
    iterator_id/0,
    message_key/0,
    message_store_opts/0,
    next_result/1, next_result/0,
    delete_next_result/1, delete_next_result/0,
    store_batch_result/0,
    make_iterator_result/1, make_iterator_result/0,
    make_delete_iterator_result/1, make_delete_iterator_result/0,

    error/1,

    ds_specific_stream/0,
    ds_specific_iterator/0,
    ds_specific_generation_rank/0,
    ds_specific_delete_stream/0,
    ds_specific_delete_iterator/0,
    generation_rank/0,
    generation_info/0,

    poll_iterators/0,
    poll_opts/0
]).

%%================================================================================
%% Type declarations
%%================================================================================

-type db() :: atom().

%% Parsed topic.
-type topic() :: list(binary()).

%% Parsed topic filter.
-type topic_filter() :: list(binary() | '+' | '#' | '').

-type message() :: emqx_types:message().

%% Message matcher.
-type message_matcher(Payload) :: #message_matcher{payload :: Payload}.

%% A batch of storage operations.
-type batch() :: [operation()] | dsbatch().

-type dsbatch() :: #dsbatch{}.

-type operation() ::
    %% Store a message.
    message()
    %% Delete a message.
    %% Does nothing if the message does not exist.
    | deletion().

-type deletion() :: {delete, message_matcher('_')}.

%% Precondition.
%% Fails whole batch if the storage already has the matching message (`if_exists'),
%% or does not yet have (`unless_exists'). Here "matching" means that it either
%% just exists (when pattern is '_') or has exactly the same payload, rest of the
%% message fields are irrelevant.
%% Useful to construct batches with "compare-and-set" semantics.
%% Note: backends may not support this, but if they do only DBs with `atomic_batches'
%% enabled are expected to support preconditions in batches.
-type precondition() ::
    {if_exists | unless_exists, message_matcher(iodata() | '_')}.

-type rank_x() :: term().

-type rank_y() :: integer().

-type stream_rank() :: {rank_x(), rank_y()}.

%% TODO: Not implemented
-type iterator_id() :: term().

-opaque iterator() :: ds_specific_iterator().

-opaque delete_iterator() :: ds_specific_delete_iterator().

-opaque stream() :: ds_specific_stream().

-opaque delete_stream() :: ds_specific_delete_stream().

-type delete_selector() :: fun((emqx_types:message()) -> boolean()).

-type ds_specific_iterator() :: term().

-type ds_specific_stream() :: term().

-type ds_specific_generation_rank() :: term().

-type message_key() :: binary().

-type store_batch_result() :: ok | error(_).

-type make_iterator_result(Iterator) :: {ok, Iterator} | error(_).

-type make_iterator_result() :: make_iterator_result(iterator()).

-type next_result(Iterator) ::
    {ok, Iterator, [{message_key(), emqx_types:message()}]} | {ok, end_of_stream} | error(_).

-type next_result() :: next_result(iterator()).

-type ds_specific_delete_iterator() :: term().

-type ds_specific_delete_stream() :: term().

-type make_delete_iterator_result(DeleteIterator) :: {ok, DeleteIterator} | error(_).

-type make_delete_iterator_result() :: make_delete_iterator_result(delete_iterator()).

-type delete_next_result(DeleteIterator) ::
    {ok, DeleteIterator, non_neg_integer()} | {ok, end_of_stream} | {error, term()}.

-type delete_next_result() :: delete_next_result(delete_iterator()).

-type poll_iterators() :: [{_UserData, iterator()}].

-type error(Reason) :: {error, recoverable | unrecoverable, Reason}.

%% Timestamp
%% Each message must have unique timestamp.
%% Earliest possible timestamp is 0.
-type time() :: non_neg_integer().

-type message_store_opts() ::
    #{
        %% Whether to wait until the message storage has been acknowledged to return from
        %% `store_batch'.
        %% Default: `true'.
        sync => boolean()
    }.

%% This type specifies the backend and some generic options that
%% affect the semantics of DS operations.
%%
%% All backends MUST handle all options listed here; even if it means
%% throwing an exception that says that certain option is not
%% supported.
%%
%% How to add a new option:
%%
%% 1. Create a PR modifying this type and adding a stub implementation
%% to all backends that throws "option unsupported" error.
%%
%% 2. Get everyone fully on-board about the semantics and name of the
%% new option. Merge the PR.
%%
%% 3. Implement business logic reliant on the new option, and its
%% support in the backends.
%%
%% 4. If the new option is not supported by all backends, it's up to
%% the business logic to choose the right one.
%%
%% 5. Default value for the option MUST be set in `emqx_ds:open_db'
%% function. The backends SHOULD NOT make any assumptions about the
%% default values for common options.
-type create_db_opts() ::
    #{
        backend := atom(),
        %% `append_only' option ensures that the backend will take
        %% measures to avoid overwriting messages, even if their
        %% fields (topic, timestamp, GUID and client ID, ... or any
        %% combination of thereof) match. This option is `true' by
        %% default.
        %%
        %% When this flag is `false':
        %%
        %% - Messages published by the same client WILL be overwritten
        %% if thier topic and timestamp match.
        %%
        %% - Messages published with the same topic and timestamp by
        %% different clients MAY be overwritten.
        %%
        %% The API consumer must design the topic structure
        %% accordingly.
        append_only => boolean(),
        %% Whether the whole batch given to `store_batch' should be processed and
        %% inserted atomically as a unit, in isolation from other batches.
        %% Default: `false'.
        %% The whole batch must be crafted so that it belongs to a single shard (if
        %% applicable to the backend).
        atomic_batches => boolean(),
        %% Backend-specific options:
        _ => _
    }.

-type db_opts() :: #{
    %% See respective `create_db_opts()` fields.
    append_only => boolean(),
    atomic_batches => boolean()
}.

-type poll_opts() ::
    #{
        %% Expire poll request after this timeout
        timeout := pos_integer(),
        %% (Optional) Provide an explicit process alias for receiving
        %% replies. It must be created with `explicit_unalias' flag,
        %% otherwise replies will get lost. If not specified, DS will
        %% create a new alias.
        reply_to => reference()
    }.

%% An opaque term identifying a generation.  Each implementation will possibly add
%% information to this term to match its inner structure (e.g.: by embedding the shard id,
%% in the case of `emqx_ds_replication_layer').
-opaque generation_rank() :: ds_specific_generation_rank().

-type generation_info() :: #{
    created_at := time(),
    since := time(),
    until := time() | undefined
}.

-define(persistent_term(DB), {emqx_ds_db_backend, DB}).

-define(module(DB), (persistent_term:get(?persistent_term(DB)))).

%%================================================================================
%% Behavior callbacks
%%================================================================================

-callback open_db(db(), create_db_opts()) -> ok | {error, _}.

-callback close_db(db()) -> ok.

-callback add_generation(db()) -> ok | {error, _}.

-callback update_db_config(db(), create_db_opts()) -> ok | {error, _}.

-callback list_generations_with_lifetimes(db()) ->
    #{generation_rank() => generation_info()}.

-callback drop_generation(db(), generation_rank()) -> ok | {error, _}.

-callback drop_db(db()) -> ok | {error, _}.

-callback store_batch(db(), [emqx_types:message()], message_store_opts()) -> store_batch_result().

-callback get_streams(db(), topic_filter(), time()) -> [{stream_rank(), ds_specific_stream()}].

-callback make_iterator(db(), ds_specific_stream(), topic_filter(), time()) ->
    make_iterator_result(ds_specific_iterator()).

-callback next(db(), Iterator, pos_integer()) -> next_result(Iterator).

-callback poll(db(), poll_iterators(), poll_opts()) -> {ok, reference()}.

-callback get_delete_streams(db(), topic_filter(), time()) -> [ds_specific_delete_stream()].

-callback make_delete_iterator(db(), ds_specific_delete_stream(), topic_filter(), time()) ->
    make_delete_iterator_result(ds_specific_delete_iterator()).

-callback delete_next(db(), DeleteIterator, delete_selector(), pos_integer()) ->
    delete_next_result(DeleteIterator).

-callback count(db()) -> non_neg_integer().

-optional_callbacks([
    list_generations_with_lifetimes/1,
    drop_generation/2,

    get_delete_streams/3,
    make_delete_iterator/4,
    delete_next/4,

    count/1
]).

%%================================================================================
%% API functions
%%================================================================================

%% @doc Register DS backend.
-spec register_backend(atom(), module()) -> ok.
register_backend(Name, Module) ->
    persistent_term:put({emqx_ds_backend_module, Name}, Module).

%% @doc Different DBs are completely independent from each other. They
%% could represent something like different tenants.
-spec open_db(db(), create_db_opts()) -> ok.
open_db(DB, Opts = #{backend := Backend}) ->
    case persistent_term:get({emqx_ds_backend_module, Backend}, undefined) of
        undefined ->
            error({no_such_backend, Backend});
        Module ->
            persistent_term:put(?persistent_term(DB), Module),
            emqx_ds_sup:register_db(DB, Backend),
            ?module(DB):open_db(DB, set_db_defaults(Opts))
    end.

-spec close_db(db()) -> ok.
close_db(DB) ->
    emqx_ds_sup:unregister_db(DB),
    ?module(DB):close_db(DB).

-spec which_dbs() -> [{db(), _Backend :: atom()}].
which_dbs() ->
    emqx_ds_sup:which_dbs().

-spec add_generation(db()) -> ok.
add_generation(DB) ->
    ?module(DB):add_generation(DB).

-spec update_db_config(db(), create_db_opts()) -> ok.
update_db_config(DB, Opts) ->
    ?module(DB):update_db_config(DB, set_db_defaults(Opts)).

-spec list_generations_with_lifetimes(db()) -> #{generation_rank() => generation_info()}.
list_generations_with_lifetimes(DB) ->
    Mod = ?module(DB),
    call_if_implemented(Mod, list_generations_with_lifetimes, [DB], #{}).

-spec drop_generation(db(), ds_specific_generation_rank()) -> ok | {error, _}.
drop_generation(DB, GenId) ->
    Mod = ?module(DB),
    case erlang:function_exported(Mod, drop_generation, 2) of
        true ->
            Mod:drop_generation(DB, GenId);
        false ->
            {error, not_implemented}
    end.

-spec drop_db(db()) -> ok.
drop_db(DB) ->
    case persistent_term:get(?persistent_term(DB), undefined) of
        undefined ->
            ok;
        Module ->
            _ = persistent_term:erase(?persistent_term(DB)),
            Module:drop_db(DB)
    end.

-spec store_batch(db(), batch(), message_store_opts()) -> store_batch_result().
store_batch(DB, Msgs, Opts) ->
    ?module(DB):store_batch(DB, Msgs, Opts).

-spec store_batch(db(), batch()) -> store_batch_result().
store_batch(DB, Msgs) ->
    store_batch(DB, Msgs, #{}).

%% @doc Get a list of streams needed for replaying a topic filter.
%%
%% Motivation: under the hood, EMQX may store different topics at
%% different locations or even in different databases. A wildcard
%% topic filter may require pulling data from any number of locations.
%%
%% Stream is an abstraction exposed by `emqx_ds' that, on one hand,
%% reflects the notion that different topics can be stored
%% differently, but hides the implementation details.
%%
%% While having to work with multiple iterators to replay a topic
%% filter may be cumbersome, it opens up some possibilities:
%%
%% 1. It's possible to parallelize replays
%%
%% 2. Streams can be shared between different clients to implement
%% shared subscriptions
%%
%% IMPORTANT RULES:
%%
%% 0. There is no 1-to-1 mapping between MQTT topics and streams. One
%% stream can contain any number of MQTT topics.
%%
%% 1. New streams matching the topic filter and start time can appear
%% without notice, so the replayer must periodically call this
%% function to get the updated list of streams.
%%
%% 2. Streams may depend on one another. Therefore, care should be
%% taken while replaying them in parallel to avoid out-of-order
%% replay. This function returns stream together with its
%% "coordinate": `stream_rank()'.
%%
%% Stream rank is a tuple of two terms, let's call them X and Y. If
%% X coordinate of two streams is different, they are independent and
%% can be replayed in parallel. If it's the same, then the stream with
%% smaller Y coordinate should be replayed first. If Y coordinates are
%% equal, then the streams are independent.
%%
%% Stream is fully consumed when `next/3' function returns
%% `end_of_stream'. Then and only then the client can proceed to
%% replaying streams that depend on the given one.
-spec get_streams(db(), topic_filter(), time()) -> [{stream_rank(), stream()}].
get_streams(DB, TopicFilter, StartTime) ->
    ?module(DB):get_streams(DB, TopicFilter, StartTime).

-spec make_iterator(db(), stream(), topic_filter(), time()) -> make_iterator_result().
make_iterator(DB, Stream, TopicFilter, StartTime) ->
    ?module(DB):make_iterator(DB, Stream, TopicFilter, StartTime).

-spec next(db(), iterator(), pos_integer()) -> next_result().
next(DB, Iter, BatchSize) ->
    ?module(DB):next(DB, Iter, BatchSize).

%% @doc Schedule asynchrounous long poll of the iterators and return
%% immediately.
%%
%% Arguments:
%% 1. Name of DS DB
%% 2. List of tuples, where first element is an arbitrary tag that can
%%    be used to identify replies, and the second one is iterator.
%% 3. Poll options
%%
%% Return value: process alias that identifies the replies.
%%
%% Data will be sent to the caller process as messages wrapped in
%% `#poll_reply' record:
%% - `ref' field will be equal to the returned reference.
%% - `userdata' field will be equal to the iterator tag.
%% - `payload' will be of type `next_result()' or `poll_timeout' atom
%%
%% There are some important caveats:
%%
%% - Replies are sent on a best-effort basis. They may be lost for any
%% reason. Caller must be designed to tolerate and retry missed poll
%% replies.
%%
%% - There is no explicit lifetime management for poll workers. When
%% caller dies, its poll requests survive. It's assumed that orphaned
%% requests will naturally clean themselves out by timeout alone.
%% Therefore, timeout must not be too long.
%%
%% - But not too short either: if no data arrives to the stream before
%% timeout, the request is usually retried. This should not create a
%% busy loop. Also DS may silently drop requests due to overload. So
%% they should not be retried too early.
-spec poll(db(), poll_iterators(), poll_opts()) -> {ok, reference()}.
poll(DB, Iterators, PollOpts = #{timeout := Timeout}) when is_integer(Timeout), Timeout > 0 ->
    ?module(DB):poll(DB, Iterators, PollOpts).

-spec get_delete_streams(db(), topic_filter(), time()) -> [delete_stream()].
get_delete_streams(DB, TopicFilter, StartTime) ->
    Mod = ?module(DB),
    call_if_implemented(Mod, get_delete_streams, [DB, TopicFilter, StartTime], []).

-spec make_delete_iterator(db(), ds_specific_delete_stream(), topic_filter(), time()) ->
    make_delete_iterator_result().
make_delete_iterator(DB, Stream, TopicFilter, StartTime) ->
    Mod = ?module(DB),
    call_if_implemented(
        Mod, make_delete_iterator, [DB, Stream, TopicFilter, StartTime], {error, not_implemented}
    ).

-spec delete_next(db(), delete_iterator(), delete_selector(), pos_integer()) ->
    delete_next_result().
delete_next(DB, Iter, Selector, BatchSize) ->
    Mod = ?module(DB),
    call_if_implemented(
        Mod, delete_next, [DB, Iter, Selector, BatchSize], {error, not_implemented}
    ).

-spec count(db()) -> non_neg_integer() | {error, not_implemented}.
count(DB) ->
    Mod = ?module(DB),
    call_if_implemented(Mod, count, [DB], {error, not_implemented}).

%%================================================================================
%% Internal exports
%%================================================================================

-spec timestamp_us() -> time().
timestamp_us() ->
    erlang:system_time(microsecond).

%%================================================================================
%% Internal functions
%%================================================================================

set_db_defaults(Opts) ->
    Defaults = #{
        append_only => true,
        atomic_batches => false
    },
    maps:merge(Defaults, Opts).

call_if_implemented(Mod, Fun, Args, Default) ->
    case erlang:function_exported(Mod, Fun, length(Args)) of
        true ->
            apply(Mod, Fun, Args);
        false ->
            Default
    end.
