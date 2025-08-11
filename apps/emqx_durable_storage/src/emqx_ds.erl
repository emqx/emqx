%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds).

-moduledoc """
Main interface module for `emqx_durable_storage' application.

It takes care of forwarding calls to the underlying DBMS.
""".

-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include("emqx_ds_builtin_tx.hrl").

%% Management API:
-export([
    register_backend/2,
    set_shard_ready/3,
    set_db_ready/2,

    open_db/2,
    wait_db/3,
    is_shard_up/2,
    close_db/1,
    which_dbs/0,
    update_db_config/2,
    add_generation/1,
    shard_of/2,
    list_shards/1,
    list_slabs/1,
    drop_slab/2,
    drop_db/1
]).

%% Message storage API:
-export([store_batch/2, store_batch/3]).

%% Transactional API (low-level):
-export([new_tx/2, commit_tx/3]).

%% Message replay API:
-export([get_streams/3, get_streams/4, make_iterator/4, next/3]).
-export([subscribe/3, unsubscribe/2, suback/3, subscription_info/2]).

%% Message delete API:
-export([get_delete_streams/3, make_delete_iterator/4, delete_next/4]).

%% Misc. API:
-export([count/1]).
-export([timestamp_us/0]).
-export([topic_words/1]).

%% Transaction API:
-export([
    %% General "high-level" transaction API
    trans/2,
    tx_commit_outcome/3,
    reset_trans/1,

    tx_fold_topic/3,
    tx_fold_topic/4,
    tx_read/1,
    tx_read/2,

    tx_write/1,
    tx_del_topic/3,
    tx_del_topic/1,
    tx_ttv_assert_present/3,
    tx_ttv_assert_absent/2,
    tx_on_success/1
]).

%% Metadata serialization API:
-export([
    stream_to_binary/2,
    binary_to_stream/2,
    iterator_to_binary/2,
    binary_to_iterator/2
]).

%% Utility functions:
-export([
    dirty_read/2,
    fold_topic/4,

    make_multi_iterator/2,
    multi_iterator_next/4
]).

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
    get_streams_opts/0,
    get_streams_result/0,
    stream/0,
    delete_stream/0,
    delete_selector/0,
    shard/0,
    generation/0,
    slab/0,
    iterator/0,
    delete_iterator/0,
    iterator_id/0,
    message_key/0,
    message_store_opts/0,
    next_limit/0,
    next_result/1, next_result/0,
    delete_next_result/1, delete_next_result/0,
    topic_range/0,
    store_batch_result/0,
    make_iterator_result/1, make_iterator_result/0,
    make_delete_iterator_result/1, make_delete_iterator_result/0,

    error/1,

    fold_options/0,
    tx_fold_options/0,

    ds_specific_stream/0,
    ds_specific_iterator/0,
    ds_specific_delete_stream/0,
    ds_specific_delete_iterator/0,
    slab_info/0,

    poll_iterators/0,
    poll_opts/0,

    sub_opts/0,
    subscription_handle/0,
    sub_ref/0,
    sub_info/0,
    sub_seqno/0,
    sub_reply/0,

    ttv/0,
    payload/0,

    tx_serial/0,
    tx_context/0,
    transaction_opts/0,
    tx_ops/0,
    commit_result/0,

    multi_iterator/0,
    multi_iter_opts/0
]).

%%================================================================================
%% Type declarations
%%================================================================================

-doc "Identifier of the DB.".
-type db() :: atom().

-doc "Parsed topic.".
-type topic() :: list(binary()).

-doc "Parsed topic filter.".
-type topic_filter() :: list(binary() | '+' | '#' | '').

-type message() :: emqx_types:message().

-doc "Message matcher.".
-type message_matcher(Payload) :: #message_matcher{payload :: Payload}.

-doc "A batch of storage operations.".
-type batch() :: [operation()] | dsbatch().

-type dsbatch() :: #dsbatch{}.

-type operation() ::
    %% Store a message.
    message()
    %% Delete a message.
    %% Does nothing if the message does not exist.
    | deletion().

-type deletion() :: {delete, message_matcher('_')}.

-doc "Topic-Time-Value triple. It's used as the basic storage unit.".
-type ttv() :: {topic(), time(), binary()}.

-doc """
Precondition.
Fails whole batch if the storage already has the matching message (`if_exists'),
or does not yet have (`unless_exists'). Here "matching" means that it either
just exists (when pattern is '_') or has exactly the same payload, rest of the
message fields are irrelevant.
Useful to construct batches with "compare-and-set" semantics.
Note: backends may not support this, but if they do only DBs with `atomic_batches'
enabled are expected to support preconditions in batches.
""".
-type precondition() ::
    {if_exists | unless_exists, message_matcher(iodata() | '_')}.

-type shard() :: term().

-type generation() :: integer().

-type slab() :: {shard(), generation()}.

%% TODO: Not implemented
-type iterator_id() :: term().

-opaque iterator() :: ds_specific_iterator().

-opaque delete_iterator() :: ds_specific_delete_iterator().

-doc """
Options for limiting the number of streams.

- `shard`: Only query streams in the given shard.

- `generation_min`: Only return streams with generation >= generation_min.
""".
-type get_streams_opts() :: #{
    shard => shard(),
    generation_min => generation()
}.

-type get_streams_result() ::
    {
        _Streams :: [{slab(), stream()}],
        _Errors :: [{shard(), error(_)}]
    }.

-opaque stream() :: ds_specific_stream().

-opaque delete_stream() :: ds_specific_delete_stream().

-type delete_selector() :: fun((emqx_types:message()) -> boolean()).

-type ds_specific_iterator() :: term().

-type ds_specific_stream() :: term().

-type message_key() :: binary().

-type store_batch_result() :: ok | error(_).

-type make_iterator_result(Iterator) :: {ok, Iterator} | error(_).

-type make_iterator_result() :: make_iterator_result(iterator()).

-type next_limit() ::
    pos_integer() | {time, time(), pos_integer()}.

-type next_result(Iterator) ::
    {ok, Iterator, [payload()]} | {ok, end_of_stream} | error(_).

-type next_result() :: next_result(iterator()).

-type ds_specific_delete_iterator() :: term().

-type ds_specific_delete_stream() :: term().

-type make_delete_iterator_result(DeleteIterator) :: {ok, DeleteIterator} | error(_).

-type make_delete_iterator_result() :: make_delete_iterator_result(delete_iterator()).

-type delete_next_result(DeleteIterator) ::
    {ok, DeleteIterator, non_neg_integer()} | {ok, end_of_stream} | {error, term()}.

%% obsolete
-type poll_iterators() :: [{_UserData, iterator()}].

-type delete_next_result() :: delete_next_result(delete_iterator()).

-type topic_range() :: {topic(), time(), time() | ?ds_tx_ts_monotonic | infinity}.

-type error(Reason) :: {error, recoverable | unrecoverable, Reason}.

-doc """
Timestamp
Each message must have unique timestamp.
Earliest possible timestamp is 0.
""".
-type time() :: non_neg_integer().

-type tx_serial() :: binary().

-type message_store_opts() ::
    #{
        %% Whether to wait until the message storage has been acknowledged to return from
        %% `store_batch'.
        %% Default: `true'.
        sync => boolean(),
        %% Whether to store this batch as a single unit.  Currently not supported by
        %% builtin backends.
        %% Default: `false'.
        atomic => boolean()
    }.

-doc """
Common options for creation of a DS database.

- **`backend`** - specifies an underlying implementation of the
    durable storage. Mandatory. Currently the following backends are
    supported:

   - **`builtin_local`**: an embedded backend based on RocksDB without
       support for replication. Suitable for single-node deployments.

   - **`builtin_raft`**: an embedded backend based on RocksDB using
       Raft algorithm for data replication. Suitable for clusters.

- **`append_only`** - a semantic switch. If enabled, DS will prevent
  overwriting of records by automatically reassigning their
  timestamps. Timestamps of all messages within the shard will be
  monotonically increasing.

  Default: `true`.

- **`atomic_batches`** - deprecated in favor of **`store_ttv`**.
   Whether the whole batch given to `store_batch' should be processed
   and inserted atomically as a unit, in isolation from other batches.

   Default: `false'. The whole batch must be crafted so that it
   belongs to a single shard (if applicable to the backend).

- **`store_ttv`** - enables optimistic transactions.

  NOTE: this flag is meant to be temporary. Eventually the APIs
  should converge.

Note: all backends MUST handle all options listed here; even if it
means throwing an exception that says that certain option is not
supported.
""".
-type create_db_opts() ::
    #{
        backend := atom(),
        append_only => boolean(),
        atomic_batches => boolean(),
        %% Whether the DB stores values of type `#message{}' or
        %% topic-time-value triples.
        %%
        store_ttv => boolean(),
        %% Backend-specific options:
        _ => _
    }.

-doc """
See respective `create_db_opts()` fields.
""".
-type db_opts() :: #{
    append_only => boolean(),
    atomic_batches => boolean(),
    store_ttv => boolean()
}.

%% obsolete
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

-doc """
Options for the `subscribe` API.

- **`max_unacked`** - Maximum number of payloads that can remain
  unacked by the subscriber before the subscription is considered
  overloaded and is paused.
""".
-type sub_opts() ::
    #{
        max_unacked := pos_integer()
    }.

-type sub_info() ::
    #{
        seqno := sub_seqno(),
        acked := sub_seqno(),
        window := non_neg_integer(),
        stuck := boolean(),
        atom() => _
    }.

-type slab_info() :: #{
    created_at := time(),
    since := time(),
    until := time() | undefined
}.

%% Subscription:
-type subscription_handle() :: term().

-type sub_reply() :: #ds_sub_reply{}.

-type sub_ref() :: reference().

-type sub_seqno() :: non_neg_integer().

%% Low-level transaction types:

%% TODO: currently all backends use the same definition of context.
%% This may change in the future.
-opaque tx_context() :: emqx_ds_optimistic_tx:ctx().

-type tx_ops() :: #{
    %% Write operations:
    ?ds_tx_write => [{topic(), time() | ?ds_tx_ts_monotonic, binary() | ?ds_tx_serial}],
    %% Deletions:
    ?ds_tx_delete_topic => [topic_range()],
    %% Checked reads:
    ?ds_tx_read => [topic_range()],
    %% Preconditions:
    %%   List of objects that should be present in the database.
    ?ds_tx_expected => [{topic(), time(), binary() | '_'}],
    %%   List of objects that should NOT be present in the database.
    ?ds_tx_unexpected => [{topic(), time()}]
}.

-doc "Options for `trans/2` function.".
-type transaction_opts() :: #{
    db := db(),
    shard := shard() | {auto, _},

    generation := generation(),

    retries => non_neg_integer(),
    timeout => timeout(),
    sync => boolean(),
    retry_interval => non_neg_integer()
}.

-type transaction_result(Ret) ::
    {atomic, tx_serial(), Ret}
    | {nop, Ret}
    | {async, reference(), Ret}
    | error(_).

-type commit_result() :: {ok, tx_serial()} | error(_).

-type payload() :: emqx_types:message() | ttv().

-type fold_fun(Acc) :: fun(
    (
        slab(),
        stream(),
        payload(),
        Acc
    ) -> Acc
).

-type fold_options() ::
    #{
        db := db(),
        shard => shard(),
        generation => generation(),

        errors => crash | report | ignore,
        batch_size => pos_integer(),
        start_time => time(),
        end_time => time() | infinity
    }.

-type tx_fold_options() ::
    #{
        errors => crash | report,
        batch_size => pos_integer(),
        start_time => time(),
        end_time => time() | infinity
    }.

-record(fold_ctx, {
    db :: db(),
    tf :: topic_filter(),
    start_time :: time(),
    batch_size :: pos_integer(),
    errors :: crash | report | ignore
}).

-type fold_ctx() :: #fold_ctx{}.

-type fold_error() ::
    {shard, shard(), error(_)}
    | {stream, slab(), stream(), error(_)}.

-type fold_result(R) :: {R, [fold_error()]} | R.

-record(m_iter, {
    stream :: stream(),
    it :: iterator()
}).

-type multi_iterator() :: #m_iter{}.

-type multi_iter_opts() ::
    #{
        db := db(),
        generation => generation() | '_',
        shard => shard() | '_',
        start_time => time()
    }.

%% Internal:
-define(persistent_term(DB), {emqx_ds_db_backend, DB}).

-define(module(DB), (persistent_term:get(?persistent_term(DB)))).

-define(is_time_range(FROM, TO),
    (is_integer(FROM) andalso
        FROM >= 0 andalso
        (is_integer(TO) orelse TO =:= infinity orelse TO =:= ?ds_tx_ts_monotonic) andalso
        TO >= FROM)
).

%% Optvars:
-record(emqx_ds_db_ready_optvar, {db :: db()}).
-record(emqx_ds_shard_ready_optvar, {db :: db(), shard :: shard()}).

%%================================================================================
%% Behavior callbacks
%%================================================================================

-doc """
Create or open a DB.

NOTE: The backend must handle all options in `create_db_opts()` type,
even if it means throwing an error that certain option is unsupported.

NOTE: All `create_db_opts()` are present in the input map. The backend
must not assume the default values.
""".
-callback open_db(db(), create_db_opts()) -> ok | {error, _}.

-callback close_db(db()) -> ok.

-callback add_generation(db()) -> ok | {error, _}.

-callback update_db_config(db(), create_db_opts()) -> ok | {error, _}.

-callback list_slabs(db()) ->
    #{slab() => slab_info()}.

-callback drop_slab(db(), slab()) -> ok | {error, _}.

-callback drop_db(db()) -> ok | {error, _}.

-callback list_shards(db()) -> [shard()].

-callback shard_of(db(), emqx_types:clientid() | topic()) -> shard().

-callback store_batch(db(), [emqx_types:message()], message_store_opts()) -> store_batch_result().

%% Synchronous read API:
-callback get_streams(db(), topic_filter(), time(), get_streams_opts()) -> get_streams_result().

-callback make_iterator(db(), ds_specific_stream(), topic_filter(), time()) ->
    make_iterator_result(ds_specific_iterator()).

-callback next(db(), Iterator, pos_integer()) -> next_result(Iterator).

%% Deletion API:
-callback get_delete_streams(db(), topic_filter(), time()) -> [ds_specific_delete_stream()].

-callback make_delete_iterator(db(), ds_specific_delete_stream(), topic_filter(), time()) ->
    make_delete_iterator_result(ds_specific_delete_iterator()).

-callback delete_next(db(), DeleteIterator, delete_selector(), pos_integer()) ->
    delete_next_result(DeleteIterator).

%% Metadata API:
-callback stream_to_binary(db(), ds_specific_stream()) -> {ok, binary()} | {error, _}.
-callback iterator_to_binary(db(), ds_specific_iterator()) -> {ok, binary()} | {error, _}.

-callback binary_to_stream(db(), binary()) -> {ok, ds_specific_stream()} | {error, _}.
-callback binary_to_iterator(db(), binary()) -> {ok, ds_specific_iterator()} | {error, _}.

%% Statistics API:
-callback count(db()) -> non_neg_integer().

%% Blob transaction API:
-callback new_tx(db(), transaction_opts()) ->
    {ok, tx_context()} | error(_).

-callback commit_tx(db(), tx_context(), tx_ops()) -> reference().

-callback tx_commit_outcome({'DOWN', reference(), _, _, _}) ->
    commit_result().

-optional_callbacks([
    get_delete_streams/3,
    make_delete_iterator/4,
    delete_next/4,

    count/1
]).

%%================================================================================
%% API functions
%%================================================================================

-doc """
Internal API called by the backend application to register
itself as a viable backend.
""".
-doc #{title => <<"Backend API">>}.
-spec register_backend(atom(), module()) -> ok.
register_backend(Name, Module) ->
    persistent_term:put({emqx_ds_backend_module, Name}, Module).

-doc """
Create a new DS database or open an existing one.

Different DBs are completely independent from each other. They can
store different type of data or data from different tenants.

WARNING: this function may return before the DB is ready to be used.
Database is considered initialized once `wait_db/3` function returns.
This separation of functionality simplifies startup procedure of the
EMQX application.
""".
-spec open_db(db(), create_db_opts()) -> ok.
open_db(DB, UserOpts) ->
    Opts = #{backend := Backend} = set_db_defaults(UserOpts),
    %% Sanity checks:
    case Opts of
        #{store_ttv := true, append_only := true} ->
            %% Currently `append_only' semantic is not supported for
            %% TTV databases.
            error({incompatible_options, [store_ttv, append_only]});
        _ ->
            ok
    end,
    %% Call backend:
    case persistent_term:get({emqx_ds_backend_module, Backend}, undefined) of
        undefined ->
            error({no_such_backend, Backend});
        Module ->
            persistent_term:put(?persistent_term(DB), Module),
            emqx_ds_sup:register_db(DB, Backend),
            ?module(DB):open_db(DB, Opts)
    end.

-doc """
Block the process until `DB` is initialized.
""".
-spec wait_db(db(), [shard()] | all, timeout()) -> ok | timeout.
wait_db(DB, Shards0, Timeout) ->
    maybe
        {ok, _} ?= optvar:read(#emqx_ds_db_ready_optvar{db = DB}, Timeout),
        Shards =
            case Shards0 of
                all -> list_shards(DB);
                _ -> Shards0
            end,
        ok ?=
            optvar:wait_vars(
                [#emqx_ds_shard_ready_optvar{db = DB, shard = Shard} || Shard <- Shards], Timeout
            )
    else
        _ ->
            timeout
    end.

-doc """
Check status of the shard.
""".
-spec is_shard_up(db(), shard()) -> boolean().
is_shard_up(DB, Shard) ->
    optvar:is_set(#emqx_ds_shard_ready_optvar{db = DB, shard = Shard}).

-doc """
Internal API called by the backend application when DB becomes ready.
""".
-doc #{title => <<"Backend API">>, since => <<"6.0.0.">>}.
-spec set_shard_ready(db(), shard(), boolean()) -> ok.
set_shard_ready(DB, Shard, true) ->
    optvar:set(#emqx_ds_shard_ready_optvar{db = DB, shard = Shard}, true);
set_shard_ready(DB, Shard, false) ->
    optvar:unset(#emqx_ds_shard_ready_optvar{db = DB, shard = Shard}).

-doc """
Internal API called by the backend application when DB becomes ready.
""".
-doc #{title => <<"Backend API">>, since => <<"6.0.0.">>}.
-spec set_db_ready(db(), boolean()) -> ok.
set_db_ready(DB, true) ->
    optvar:set(#emqx_ds_db_ready_optvar{db = DB}, true);
set_db_ready(DB, false) ->
    optvar:unset(#emqx_ds_db_ready_optvar{db = DB}).

-doc """
Gracefully close a DB.
""".
-spec close_db(db()) -> ok.
close_db(DB) ->
    set_db_ready(DB, false),
    emqx_ds_sup:unregister_db(DB),
    ?module(DB):close_db(DB).

-doc """
List open DBs and their backends.
""".
-spec which_dbs() -> [{db(), _Backend :: atom()}].
which_dbs() ->
    emqx_ds_sup:which_dbs().

-doc """
Add a new generation within each shard.

DS encourages splitting data into "generations".

Generations provide a mechanism for changing data representation
without rendering previously written data unreadable, and for
efficient implementation of data retention policies.

Note: a collection of records identified by shard and generation ID is
called `slab`. All slabs with the same generation ID share the same
properties. Therefore, when we speak about a "generation" we usually
imply a set of all slabs in the same generation.

Each generation contains homogenious data. Data-at-rest representation
of each record in the generation is the same. This representation is
specified by a **layout**.

Layout consists of an implementation module (called layout engine) and
its settings. Once a slab is created, its layout is immutable. The
only way to change the layout is to call `update_db_config/2` to
change the default layout, followed by `add_generation/1`.

#### Use-cases for the generations

- For append-only DBs storing MQTT messages generations can be used to
  implement time-based data retention. The client code can
  periodically rotate the generations by calling this function and
  drop the old generations.

- DBs containing key-value data can use generations to handle schema
  updates. When internal implementation of the data changes, client
  code can create a new generation and spawn a background migration
  process that moves and transforms data to the new generation.

""".
-spec add_generation(db()) -> ok.
add_generation(DB) ->
    ?module(DB):add_generation(DB).

-doc """
Update settings of a database.

Note: certain settings, such as backend, number of shards and certain
semantic flags cannot be changed for the existing DBs.

""".
-spec update_db_config(db(), create_db_opts()) -> ok.
update_db_config(DB, Opts) ->
    ?module(DB):update_db_config(DB, set_db_defaults(Opts)).

-spec list_slabs(db()) -> #{slab() => slab_info()}.
list_slabs(DB) ->
    ?module(DB):list_slabs(DB).

-doc """
Delete an entire slab `Slab` from the database `DB`.

This operation is the most efficient way to delete data.
""".
-spec drop_slab(db(), slab()) -> ok | {error, _}.
drop_slab(DB, Slab) ->
    ?module(DB):drop_slab(DB, Slab).

-doc """
Drop DB and destroy all data stored there.
""".
-spec drop_db(db()) -> ok.
drop_db(DB) ->
    set_db_ready(DB, false),
    case persistent_term:get(?persistent_term(DB), undefined) of
        undefined ->
            ok;
        Module ->
            _ = persistent_term:erase(?persistent_term(DB)),
            Module:drop_db(DB)
    end.

-doc """
List all shards of the database.
""".
-spec list_shards(db()) -> [shard()].
list_shards(DB) ->
    ?module(DB):list_shards(DB).

-doc """
Get shard of a client ID or a topic.

WARNING: This function does NOT check the type of input, and may
return arbitrary result when called with topic as an argument for a
DB using client ID for sharding and vice versa.
""".
-spec shard_of(db(), emqx_types:clientid() | topic()) -> shard().
shard_of(DB, ClientId) ->
    ?module(DB):shard_of(DB, ClientId).

-spec store_batch(db(), batch(), message_store_opts()) -> store_batch_result().
store_batch(DB, Msgs, Opts) ->
    ?module(DB):store_batch(DB, Msgs, Opts).

-spec store_batch(db(), batch()) -> store_batch_result().
store_batch(DB, Msgs) ->
    store_batch(DB, Msgs, #{}).

-doc "Simplified version of `get_streams/4` that ignores the errors.".
-spec get_streams(db(), topic_filter(), time()) -> [{slab(), stream()}].
get_streams(DB, TopicFilter, StartTime) ->
    {Streams, _Errors} = get_streams(DB, TopicFilter, StartTime, #{}),
    Streams.

-doc """
Get a list of streams needed for replaying a topic filter.

When `shard` key is present in the options, this function will
query only the specified shard. Otherwise, it will query all
shards.

Motivation: under the hood, EMQX may store different topics at
different locations or even in different databases. A wildcard
topic filter may require pulling data from any number of locations.

Stream is an abstraction exposed by `emqx_ds` that, on one hand,
reflects the notion that different topics can be stored
differently, but hides the implementation details.

While having to work with multiple iterators to replay a topic
filter may be cumbersome, it opens up some possibilities:

1. It's possible to parallelize replays

2. Streams can be shared between different clients to implement shared
   subscriptions

#### IMPORTANT RULES:

0. There is no 1-to-1 mapping between MQTT topics and streams. One
   stream can contain any number of MQTT topics.

1. New streams matching the topic filter and start time may appear
   from time to time, so the reader must periodically call this
   function to get the updated list of streams.
   `emqx_ds_new_streams:watch/2` function can be used to get
   notifications about new streams.

2. Streams may depend on one another. Therefore, care should be taken
   while replaying them in parallel to avoid out-of-order replay. This
   function returns stream together with identifier of the slab
   containing it.

   Slab ID is a tuple of two terms: *shard* and *generation*. If two
   streams reside in slabs with different shard, they are independent
   and can be replayed in parallel. If shard is the same, then the
   stream with smaller generation should be replayed first. If both
   shard and generations are equal, then the streams are independent.

   Stream is fully consumed when `next/3' function returns
   `end_of_stream`. Then and only then the client can proceed to
   replaying streams that depend on the given one.

""".
-spec get_streams(db(), topic_filter(), time(), get_streams_opts()) -> get_streams_result().
get_streams(DB, TopicFilter, StartTime, Opts) ->
    ?module(DB):get_streams(DB, TopicFilter, StartTime, Opts).

-doc """
Make an iterator that can be used to traverse the given stream and topic filter.

Iterators can be used to read data using `subscribe/3` or `next/3` APIs.

StartTime: timestamp of the first payload *included* in the batch.
""".
-spec make_iterator(db(), stream(), topic_filter(), time()) -> make_iterator_result().
make_iterator(DB, Stream, TopicFilter, StartTime) ->
    ?module(DB):make_iterator(DB, Stream, TopicFilter, StartTime).

-doc """
Fetch a batch of payloads from the DB, starting from position marked by the iterator.

Size of the batch is controlled by the NextLimit parameter.

- When set to a positive integer Nmax, this function will return up to Nmax entries.

- When set to `{time, Time, NMax}`, iteration will stop either
  upon reaching a payload with timestamp >= `Time` (which *won't* be included in the batch) or
  when the batch size reaches NMax.
""".
-spec next(db(), iterator(), next_limit()) -> next_result(iterator()).
next(DB, It, NextLimit) ->
    ?module(DB):next(DB, It, NextLimit).

-doc """

A low-level API that subscribes current Erlang process to the messages
that follow `Iterator`.

NOTE: business-level applications are encouraged to use `emqx_ds_client` instead.
This helper module greatly simplifies management of subscriptions.

This function returns a subscription handle that can be used to to
manipulate the subscription (ack batches and unsubscribe), as well as
a monitor reference used to detect unexpected termination of the
subscription by the durable storage. The same reference is included in
the messages sent by DS to the subscriber.

Once subscribed, the client process will receive messages of type
`#ds_sub_reply{}`.

- `ref` field is equal to the `sub_ref()' returned by subscribe call.

- `size` field is equal to the number of messages in the payload. If
   payload = `{ok, end_of_stream}' or `{error, _, _}' then `size` = 1.

- `seqno` field contains sum of all `size`'s received by the
  subscription so far (including the current batch).

- `stuck` flag is set when subscription is paused for not keeping up
  with the acks.

- `lagging` flag is an implementation-defined indicator that the
   subscription is currently reading old data.

WARNING: Only use this API when monotonic timestamp policy is enforced
for the new data. Subscriptions advance their iterators forward and
only forward in time.

Subscribers are NOT notified about newly inserted data if its
timestamp is not greater than the current position of the subscription
iterator.

""".
-doc #{title => <<"Subscriptions">>, since => <<"5.9.0">>}.
-spec subscribe(db(), iterator(), sub_opts()) ->
    {ok, subscription_handle(), sub_ref()} | error(_).
subscribe(DB, Iterator, SubOpts) ->
    ?module(DB):subscribe(DB, Iterator, SubOpts).

-doc #{title => <<"Subscriptions">>, since => <<"5.9.0">>}.
-spec unsubscribe(db(), subscription_handle()) -> boolean().
unsubscribe(DB, SubRef) ->
    ?module(DB):unsubscribe(DB, SubRef).

-doc """

Acknowledge processing of payloads received via subscription `SubRef`
up to sequence number `SeqNo`. This way client can signal to DS that
it is ready to process more data. Subscriptions that do not keep up
with the acks are paused.

""".
-doc #{title => <<"Subscriptions">>, since => <<"5.9.0">>}.
-spec suback(db(), subscription_handle(), non_neg_integer()) -> ok.
suback(DB, SubRef, SeqNo) ->
    ?module(DB):suback(DB, SubRef, SeqNo).

-doc "Get information about the subscription.".
-spec subscription_info(db(), subscription_handle()) -> sub_info() | undefined.
subscription_info(DB, Handle) ->
    ?module(DB):subscription_info(DB, Handle).

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

-doc """
Low-level transaction API. Obtain context for an optimistic
transaction.
""".
-doc #{title => <<"Low-level transactions">>, since => <<"5.10.0">>}.
-spec new_tx(db(), transaction_opts()) -> {ok, tx_context()} | error(_).
new_tx(DB, Opts) ->
    ?module(DB):new_tx(DB, Opts).

-doc """

Low-level transaction API. Try to commit a set of
operations executed in a given transaction context. This function
returns immediately.

Outcome of the transaction is sent to the process that created
`TxContext` as a message.

This message matches `?ds_tx_commit_reply(Ref, Reply)` macro from
`emqx_ds.hrl`. Reply should be passed to `tx_commit_outcome`

function.

""".
-doc #{title => <<"Low-level transactions">>, since => <<"5.10.0">>}.
-spec commit_tx
    (db(), tx_context(), tx_ops()) -> reference();
    (db(), tx_context(), tx_ops()) -> reference().
commit_tx(DB, TxContext, TxOps) ->
    ?module(DB):commit_tx(DB, TxContext, TxOps).

-doc """

Process asynchronous DS transaction commit reply and return
the outcome of commit.

""".
-spec tx_commit_outcome(db(), reference(), term()) ->
    commit_result().
tx_commit_outcome(DB, Ref, ?ds_tx_commit_reply(Ref, Reply)) ->
    SideEffects = tx_pop_side_effects(Ref),
    case ?module(DB):tx_commit_outcome(Reply) of
        {ok, _} = Ok ->
            tx_eval_side_effects(SideEffects),
            Ok;
        Other ->
            Other
    end.

%%================================================================================
%% Utility functions
%%================================================================================

%% Transaction process dictionary keys:
-define(tx_ctx_db, emqx_ds_tx_db).
-define(tx_ctx, emqx_ds_tx_ctx).
-define(tx_ops_write, emqx_ds_tx_ctx_write).
-define(tx_ops_read, emqx_ds_tx_ctx_read).
-define(tx_ops_del_topic, emqx_ds_tx_ctx_del_topic).
-define(tx_ops_assert_present, emqx_ds_tx_ctx_assert_present).
-define(tx_ops_assert_absent, emqx_ds_tx_ctx_assert_absent).
-define(tx_ops_side_effect, emqx_ds_tx_ctx_side_effect).

%% Transaction throws:
-define(tx_reset, emqx_ds_tx_reset).

-doc """

Execute a function in the environment where writes, deletes
and asserts are deferred, and then committed atomically.

#### Order of operations

When transaction commits, its side effects may be applied in an
order different from their execution in the transaction fun.

The following is guaranteed, though:

0. Formally, reads and folds happen first.

1. Preconditions are checked first

2. Deletions are applied (in any order)

3. Finally, writes are applied (in any order)

#### Limitations

- Transactions can't span multiple shards and/or generations.

- This function is not as sophisticated as, say, mnesia. In
  particular, transaction side effects become visible _eventually_
  after the successful commit. They are not visible inside the
  transaction environment.

- Currently this API is supported only for DBs created with
  `store_ttv = true`.

- Transaction API is entirely optimistic, and it's not at all
  optimized to handle conflicts. When DS _suspects_ a potential
  conflict, it refuses to commit any or all transactions involved in
  it. If a lot of conflicts is expected, it's up to user to create an
  external locking mechanism.

#### Options

- **`db`**: name of the database. Mandatory.

- **`shard`**: Specify the shard directly. If set to `{auto, Term}`,
  then the shard is derived by calling `shard_of(DB, Term)`.
  Mandatory.

- **`generation`**: Specify generation for the transaction. Mandatory.

- **`sync`**: If set to `false`, this function will return immediately
  without waiting for commit. Commit outcome will be sent as a
  message. `true` by default.

- **`timeout`**: sets timeout waiting for the commit in milliseconds.
  If set to a positive integer, function may return
  `{error, unrecoverable, timeout}`.

  Default: 5s.

- **`retries`**: Automatically retry the transaction if transaction
  results in a recoverable error. This option specifies number of
  retries. Transaction restart behavior depends on the `sync` flag.
  Sync transactions are always restarted, while async transactions are
  only restarted for errors that happen before the commit stage.

  Default: 0.

- **`retry_interval`**: set pause between the retries in milliseconds.

  Default: 1s.

#### Return values

- When transaction doesn't have any side effects (writes, deletes or
  asserts), `{nop, Ret}` is returned regardless of other factors.
  `Ret` is the return value of the transaction fun.

- When `sync` option is set to `false`, this function returns
  `{async, Ref, Ret}` tuple where `Ref` is a reference and `Ret`
  is return value of the transaction fun or error tuple if the
  transaction fails before the commit.

  Result of the commit is sent to the caller asynchronously as a
  message that should be matched using `?tx_commit_reply(Ref, Reply)`
  macro. This macro binds `Reply` to a variable that should be passed
  to `emqx_ds:check_commit_reply` function to get the outcome of the
  async commit. For example:

  ```erlang
  {async, Ref, Ret} = emqx_ds:trans(#{sync => false}, Fun),
  receive
    ?tx_commit_reply(Ref, Reply) ->
       CommitOutcome = emqx_ds:tx_commit_outcome(Ref, Reply)
  end.
  ```

  WARNING: `?tx_commit_reply(Ref, Reply)` has the same structure as a
  monitor `'DOWN'` message. Therefore, in a selective receive or
  `gen_*` callbacks it should be matched before other `'DOWN'`
  messages. Also, indiscriminate flushing such messages must be
  avoided.

- If `sync` is `true`, `{atomic, Serial, Ret}' tuple is returned on
  successful commit. `Serial` is a shard-unique monotonically
  increasing binary identifying the transaction.

- Errors are returned as usual for DS.

- Precondition failures result in
  `{error, unrecoverable, {precondition_failed, _}}' commit
  outcome.

""".
-doc #{title => <<"Transactions">>, since => <<"5.10.0">>}.
-spec trans(
    transaction_opts(),
    fun(() -> Ret)
) ->
    transaction_result(Ret).
trans(UserOpts = #{db := DB, shard := _, generation := _}, Fun) ->
    Defaults = #{
        timeout => 5_000,
        sync => true,
        retries => 0,
        retry_interval => 1_000
    },
    Opts = maps:merge(Defaults, UserOpts),
    case tx_ctx() of
        undefined ->
            #{retries := Retries} = Opts,
            trans_maybe_retry(DB, Fun, Opts, Retries);
        _ ->
            ?err_unrec(nested_transaction)
    end.

-doc """

Schedule a transactional write of a given value to the topic. Value
can be a binary or `?ds_tx_serial`. The latter is replaced with the
transaction serial.

If time is set to a special value `?ds_tx_ts_monotonic`, then at the
time of the commit timestamp of the record will be replaced with a
unique, monotonically increasing timestamp (in microseconds).

NOTE: topics used in TTV interface are not MQTT topics: they don't
have to be proper unicode and levels can contain slashes.

""".
-doc #{title => <<"Transactions">>, since => <<"5.10.0">>}.
-spec tx_write({topic(), time() | ?ds_tx_ts_monotonic, binary() | ?ds_tx_serial}) -> ok.
tx_write({Topic, Time, Value}) ->
    case
        is_topic(Topic) andalso
            (is_integer(Time) orelse Time =:= ?ds_tx_ts_monotonic) andalso
            (is_binary(Value) orelse Value =:= ?ds_tx_serial)
    of
        true ->
            tx_push_op(?tx_ops_write, {Topic, Time, Value});
        false ->
            error(badarg)
    end.

-doc "Equivalent to `tx_del_topic(TopicFilter, 0, infinity)`.".
-doc #{title => <<"Transactions">>, since => <<"5.10.0">>}.
-spec tx_del_topic(topic_filter()) -> ok.
tx_del_topic(TopicFilter) ->
    tx_del_topic(TopicFilter, 0, infinity).

-doc """

Schedule a transactional deletion of all values stored in
topics matching the filter and with timestamps within `[From, To)` half-open interval.

Deletion is performed on the latest version of the data.

NOTE: Use `<<>>` instead of `''` for empty topic levels

""".
-doc #{title => <<"Transactions">>, since => <<"6.0.0">>}.
-spec tx_del_topic(topic_filter(), time(), time() | ?ds_tx_ts_monotonic | infinity) -> ok.
tx_del_topic(TopicFilter, From, To) ->
    case is_topic_filter(TopicFilter) of
        true when ?is_time_range(From, To) ->
            tx_push_op(?tx_ops_del_topic, {TopicFilter, From, To});
        false ->
            error(badarg)
    end.

-doc """

Add a transaction precondition that asserts presence of an
object with a certain value in the given topic and with the given
timestamp. If object is absent or its value is different, the whole
transaction is aborted.

This operation is considered side effect.

#### Arguments

1. Topic of the object

2. Timestamp of the object

3. Expected value (binary) or atom `_' that works as a wildcard.

""".
-doc #{title => <<"Transactions">>, since => <<"5.10.0">>}.
-spec tx_ttv_assert_present(topic(), time(), binary() | '_') -> ok.
tx_ttv_assert_present(Topic, Time, Value) ->
    case is_topic(Topic) andalso is_integer(Time) andalso (Value =:= '_' orelse is_binary(Value)) of
        true ->
            tx_push_op(?tx_ops_assert_present, {Topic, Time, Value});
        false ->
            error(badarg)
    end.

-doc """

Inverse of `tx_ttv_assert_present`. Add a transaction
precondition that asserts _absense_ of value in the topic.

This operation is considered side effect.

""".
-doc #{title => <<"Transactions">>, since => <<"5.10.0">>}.
-spec tx_ttv_assert_absent(topic(), time()) -> ok.
tx_ttv_assert_absent(Topic, Time) ->
    case is_topic(Topic) andalso is_integer(Time) of
        true ->
            tx_push_op(?tx_ops_assert_absent, {Topic, Time});
        false ->
            error(badarg)
    end.

-doc """
This API attaches a side effect to the transaction.

Side effect is a callback that is executed locally (in the context of
the current process) iff the transaction was successfully committed.

WARNING: When transaction is started with `sync => false`, this
function leaves data in the process dictionary of the transaction
initiator.

Therefore, `tx_commit_outcome/3` MUST be called in the same process.
""".
-doc #{title => <<"Transactions">>, since => <<"6.0.0">>}.
-spec tx_on_success(fun(() -> _)) -> ok.
tx_on_success(Fun) ->
    tx_push_op(?tx_ops_side_effect, Fun).

-doc "Serialize stream to a compact binary representation.".
-doc #{title => <<"Metadata">>, since => <<"6.0.0">>}.
-spec stream_to_binary(db(), stream()) -> {ok, binary()} | {error, _}.
stream_to_binary(DB, Stream) ->
    ?module(DB):stream_to_binary(DB, Stream).

-doc "De-serialize a binary produced by `stream_to_binary/2`.".
-doc #{title => <<"Metadata">>, since => <<"6.0.0">>}.
-spec binary_to_stream(db(), binary()) -> {ok, stream()} | {error, _}.
binary_to_stream(DB, Stream) ->
    ?module(DB):binary_to_stream(DB, Stream).

-doc "Serialize iterator to a compact binary representation.".
-doc #{title => <<"Metadata">>, since => <<"6.0.0">>}.
-spec iterator_to_binary(db(), iterator() | end_of_stream) -> {ok, binary()} | {error, _}.
iterator_to_binary(DB, Iterator) ->
    ?module(DB):iterator_to_binary(DB, Iterator).

-doc "De-serialize a binary produced by `iterator_to_binary/2`.".
-doc #{title => <<"Metadata">>, since => <<"6.0.0">>}.
-spec binary_to_iterator(db(), binary()) -> {ok, iterator() | end_of_stream} | {error, _}.
binary_to_iterator(DB, Iterator) ->
    ?module(DB):binary_to_iterator(DB, Iterator).

-doc "Restart the transaction.".
-doc #{title => <<"Transactions">>, since => <<"5.10.0">>}.
-spec reset_trans(term()) -> no_return().
reset_trans(Reason) ->
    throw({?tx_reset, Reason}).

-doc "Return _all_ messages matching the topic-filter as a list.".
-doc #{title => <<"Utility functions">>, since => <<"5.10.0">>}.
-spec dirty_read(db() | fold_options(), topic_filter()) ->
    fold_result([payload()]).
dirty_read(DB, TopicFilter) when is_atom(DB) ->
    dirty_read(#{db => DB}, TopicFilter);
dirty_read(#{db := _} = Opts, TopicFilter) ->
    fold_topic(fun dirty_read_topic_fun/4, [], TopicFilter, Opts).

-doc """

Transactional version of `dirty_read/2`. Return **all** messages
matching the topic-filter as a list.

This operation is considered a side effect.

NOTE: Transactional reads are always limited to the shard and
generation of the surrounding transaction.

WARNING: This operation conflicts with _any_ write to the topics
matching the filter.

""".
-doc #{title => <<"Transactions">>, since => <<"5.10.0">>}.
-spec tx_read(tx_fold_options(), topic_filter()) ->
    fold_result([payload()]).
tx_read(Opts, TopicFilter) ->
    tx_fold_topic(fun dirty_read_topic_fun/4, [], TopicFilter, Opts).

-doc #{
    equiv => tx_read(#{}, TopicFilter),
    since => <<"5.10.0">>
}.
-spec tx_read(topic_filter()) -> [payload()].
tx_read(TopicFilter) ->
    tx_read(#{}, TopicFilter).

-doc """

A helper function for iterating over values in the storage matching
the topic filter. It is a wrapper over `get_streams`, `make_iterator`
and `next`.

#### Arguments

- **`Fun`** Fold function that takes the following parameters:

  - Slab: a tuple consisting of shard and generation

  - Stream: stream identifier

  - DSKey: message key in the stream

  - Payload: either `#message' record or {Topic, Time, Value} triple.

  - Acc: accumulator

  This function should return the updated accumulator.

- **`Acc`** Initial value of the accumulator

- **`Opts`** A map that contains additional options

#### Options

- **`db`** (mandatory): DS DB

- **`errors`**: Error handling style.

  + When set to `crash', fold throws an exception on read errors. On
    success it returns the accumulator.

  + When set to `ignore', fold returns the accumulator, but errors are
    ignored.

  + When set to `report', fold returns a tuple: `{Acc, Errors}' where
    `Errors' is a list of read errors.

  Default: `crash`

- **`batch_size`**: Maximum size of batches fetched from DS.

- **`start_time`**: Same as in `get_streams` and `make_iterator`.

  Default: 0.

- **`shard`**: If specified, data is read only from one shard. Useful
  when the caller knows location of the data and wants to avoid extra
  reads from other shards.

- **`generation`**: If specified, data is read only from one
  generation.

""".
-doc #{title => <<"Utility functions">>, since => <<"5.10.0">>}.
-spec fold_topic(
    fold_fun(Acc),
    Acc,
    topic_filter(),
    fold_options()
) -> fold_result(Acc).
fold_topic(Fun, AccIn, TopicFilter, UserOpts = #{db := DB}) ->
    %% Merge config and make fold context:
    Defaults = #{
        batch_size => 100,
        generation => '_',
        errors => crash,
        start_time => 0
    },
    #{
        batch_size := BatchSize0,
        generation := GenerationMatcher,
        errors := ErrorHandling,
        start_time := StartTime
    } = maps:merge(Defaults, UserOpts),
    BatchSize =
        case maps:get(end_time, UserOpts, infinity) of
            infinity ->
                BatchSize0;
            EndTime when is_integer(EndTime) ->
                {time, EndTime, BatchSize0}
        end,
    Ctx = #fold_ctx{
        db = DB,
        tf = TopicFilter,
        start_time = StartTime,
        batch_size = BatchSize,
        errors = ErrorHandling
    },
    %% Get streams:
    GetStreamOpts =
        case UserOpts of
            #{shard := ReqShard} -> #{shard => ReqShard};
            _ -> #{}
        end,
    {Streams, ShardErrors0} = get_streams(DB, TopicFilter, StartTime, GetStreamOpts),
    ShardErrors = [{shard, Shard, Err} || {Shard, Err} <- ShardErrors0],
    %% Create iterators:
    {Iterators, MakeIteratorErrors} =
        lists:foldl(
            fun({Slab = {_, Generation}, Stream}, {Acc, ErrAcc}) ->
                case
                    (GenerationMatcher =:= '_' orelse GenerationMatcher =:= Generation) andalso
                        make_iterator(DB, Stream, TopicFilter, StartTime)
                of
                    false ->
                        {Acc, ErrAcc};
                    {ok, It} ->
                        {[{Slab, Stream, It} | Acc], ErrAcc};
                    Err ->
                        {Acc, [{stream, Slab, Stream, Err} | ErrAcc]}
                end
            end,
            {[], ShardErrors},
            Streams
        ),
    %% Fold over data:
    case MakeIteratorErrors =/= [] andalso ErrorHandling =:= crash of
        true ->
            %% Don't bother scanning the data, we already failed:
            Result = undefined,
            Errors = MakeIteratorErrors;
        false ->
            {Result, Errors} = fold_streams(Fun, AccIn, MakeIteratorErrors, Iterators, Ctx)
    end,
    %% Return result:
    case {Errors, ErrorHandling} of
        {_, ignore} ->
            Result;
        {[], crash} ->
            Result;
        {_, crash} ->
            error(Errors);
        {_, report} ->
            {Result, Errors}
    end.

-doc """

Create a "multi-iterator" that combines iterators from multiple
streams. Data from the streams can be consumed using
`multi_iterator_next/4` function. When scan reaches end of the stream,
multi-iterator automatically switches to the next stream.

This method of reading data frees the user from having to manage
multiple streams and iterators at the cost of efficiency. Generally,
multi-iterators are less efficient than subscriptions, regular
iterators or `fold_topic/4`.

WARNING: Multi-iterators operate on live, changing data rather than
snapshots. Don't use this API if you need any degree of consistency.

""".
-doc #{title => <<"Utility functions">>, since => <<"6.0.0">>}.
-spec make_multi_iterator(multi_iter_opts(), topic_filter()) -> multi_iterator() | '$end_of_table'.
make_multi_iterator(UserOpts = #{db := _}, TF) ->
    Opts = multi_iter_opts(UserOpts),
    case multi_iter_get_streams(Opts, TF) of
        [Stream | _] ->
            do_multi_iter_make_it(Opts, TF, {ok, Stream});
        [] ->
            '$end_of_table'
    end.

-doc """

Consume a batch of data from the multi-iterator.

Options and TopicFilter parameters should be the same as ones used to
create the iterator.

""".
-doc #{title => <<"Utility functions">>, since => <<"6.0.0">>}.
-spec multi_iterator_next(
    multi_iter_opts(),
    topic_filter(),
    multi_iterator(),
    pos_integer()
) ->
    {[payload()], multi_iterator() | '$end_of_table'}.
multi_iterator_next(UserOpts, TF, It = #m_iter{}, N) ->
    do_multi_iterator_next(multi_iter_opts(UserOpts), TF, It, N, []);
multi_iterator_next(_, _, '$end_of_table', _) ->
    {[], '$end_of_table'}.

-doc """

Transactional version of `fold_topic`. Performs fold over **all**
messages matching the topic-filter in the shard and generation
specified by the surrounding transaction.

This operation is considered a side effect.

WARNING: This operation conflicts with _any_ write to the topics
matching the filter.

""".
-doc #{title => <<"Transactions">>, since => <<"5.10.0">>}.
-spec tx_fold_topic(
    fold_fun(Acc),
    Acc,
    topic_filter(),
    tx_fold_options()
) -> fold_result(Acc).
tx_fold_topic(Fun, Acc, TopicFilter, UserOpts) ->
    case tx_ctx() of
        #kv_tx_ctx{shard = Shard, generation = Generation} ->
            StartTime = maps:get(start_time, UserOpts, 0),
            EndTime = maps:get(end_time, UserOpts, infinity),
            Opts = UserOpts#{db => tx_ctx_db(), shard => Shard, generation => Generation},
            tx_push_op(?tx_ops_read, {TopicFilter, StartTime, EndTime}),
            case fold_topic(Fun, Acc, TopicFilter, Opts#{errors => report}) of
                {Result, []} ->
                    Result;
                {_, [_ | _] = Errors} ->
                    reset_trans(Errors)
            end;
        undefined ->
            error(not_a_transaction)
    end.

-doc #{
    title => <<"Transactions">>,
    since => <<"5.10.0">>,
    equiv => tx_fold_topic(Fun, Acc, TopicFilter, #{})
}.
-spec tx_fold_topic(
    fold_fun(Acc),
    Acc,
    topic_filter()
) -> Acc.
tx_fold_topic(Fun, Acc, TopicFilter) ->
    tx_fold_topic(Fun, Acc, TopicFilter, #{}).

%%================================================================================
%% Internal exports
%%================================================================================

-spec timestamp_us() -> time().
timestamp_us() ->
    erlang:system_time(microsecond).

-doc """

Version of `emqx_topic:words()' that doesn't transform empty topic into atom ''.

""".
-spec topic_words(binary()) -> topic().
topic_words(<<>>) -> [];
topic_words(Bin) -> emqx_topic:words(Bin).

%%================================================================================
%% Internal functions
%%================================================================================

set_db_defaults(Opts = #{store_ttv := true}) ->
    Defaults = #{
        append_only => false,
        atomic_batches => true
    },
    maps:merge(Defaults, Opts);
set_db_defaults(Opts) ->
    Defaults = #{
        append_only => true,
        atomic_batches => false,
        store_ttv => false
    },
    maps:merge(Defaults, Opts).

call_if_implemented(Mod, Fun, Args, Default) ->
    case erlang:function_exported(Mod, Fun, length(Args)) of
        true ->
            apply(Mod, Fun, Args);
        false ->
            Default
    end.

-spec trans_maybe_retry(
    db(),
    fun(() -> Ret),
    transaction_opts(),
    non_neg_integer()
) ->
    transaction_result(Ret).
trans_maybe_retry(DB, Fun, Opts = #{retry_interval := RetryInterval}, Retries) ->
    case trans_inner(DB, Fun, Opts) of
        ?err_rec(Reason) when Retries > 0 ->
            ?tp(
                debug,
                emqx_ds_tx_retry,
                #{
                    db => DB,
                    tx_fun => Fun,
                    opts => Opts,
                    attempts_left => Retries,
                    reason => Reason
                }
            ),
            timer:sleep(RetryInterval),
            trans_maybe_retry(DB, Fun, Opts, Retries - 1);
        Other ->
            Other
    end.

-spec trans_inner(
    db(),
    fun(() -> Ret),
    transaction_opts()
) ->
    transaction_result(Ret).
trans_inner(DB, Fun, Opts) ->
    try
        case new_tx(DB, Opts) of
            {ok, Ctx} ->
                _ = put(?tx_ctx_db, DB),
                _ = put(?tx_ctx, Ctx),
                _ = put(?tx_ops_write, []),
                _ = put(?tx_ops_read, []),
                _ = put(?tx_ops_del_topic, []),
                _ = put(?tx_ops_assert_present, []),
                _ = put(?tx_ops_assert_absent, []),
                _ = put(?tx_ops_side_effect, []),
                Ret = Fun(),
                Tx = #{
                    ?ds_tx_write => lists:reverse(erase(?tx_ops_write)),
                    ?ds_tx_read => erase(?tx_ops_read),
                    ?ds_tx_delete_topic => erase(?tx_ops_del_topic),
                    ?ds_tx_expected => erase(?tx_ops_assert_present),
                    ?ds_tx_unexpected => erase(?tx_ops_assert_absent)
                },
                case Tx of
                    #{
                        ?ds_tx_write := [],
                        ?ds_tx_read := [],
                        ?ds_tx_delete_topic := [],
                        ?ds_tx_expected := [],
                        ?ds_tx_unexpected := []
                    } ->
                        %% Nothing to commit, apply side effects now:
                        tx_eval_side_effects(erase(?tx_ops_side_effect)),
                        {nop, Ret};
                    _ ->
                        Ref = commit_tx(DB, Ctx, Tx),
                        tx_maybe_save_side_effects(Ref),
                        trans_maybe_wait_outcome(DB, Ref, Ret, Opts)
                end;
            Err ->
                Err
        end
    catch
        {?tx_reset, Reason} ->
            ?err_rec({?tx_reset, Reason})
    after
        _ = erase(?tx_ctx_db),
        _ = erase(?tx_ctx),
        _ = erase(?tx_ops_write),
        _ = erase(?tx_ops_read),
        _ = erase(?tx_ops_del_topic),
        _ = erase(?tx_ops_assert_present),
        _ = erase(?tx_ops_assert_absent),
        _ = erase(?tx_ops_side_effect)
    end.

trans_maybe_wait_outcome(DB, Ref, Ret, #{sync := true}) ->
    receive
        ?ds_tx_commit_reply(Ref, Reply) ->
            case tx_commit_outcome(DB, Ref, Reply) of
                {ok, CommitTXId} ->
                    {atomic, CommitTXId, Ret};
                Err ->
                    Err
            end
    end;
trans_maybe_wait_outcome(_DB, Ref, Ret, #{sync := false}) ->
    {async, Ref, Ret}.

tx_ctx() ->
    get(?tx_ctx).

tx_ctx_db() ->
    get(?tx_ctx_db).

tx_push_op(K, A) ->
    put(K, [A | get(K)]),
    ok.

tx_pop_side_effects(Ref) ->
    case erase({?tx_ops_side_effect, Ref}) of
        undefined -> [];
        L -> lists:reverse(L)
    end.

tx_maybe_save_side_effects(Ref) ->
    case erase(?tx_ops_side_effect) of
        [] ->
            ok;
        L ->
            put({?tx_ops_side_effect, Ref}, L)
    end.

tx_eval_side_effects(L) ->
    lists:foreach(fun(F) -> F() end, L).

-spec fold_streams(
    fold_fun(Acc), Acc, [error(_)], [{slab(), stream(), iterator()}], fold_ctx()
) -> {Acc, [fold_error()]}.
fold_streams(_Fun, Acc, AccErrors, [], _Ctx) ->
    {Acc, AccErrors};
fold_streams(Fun, Acc0, AccErrors0, [{Slab, Stream, It} | Rest], Ctx) ->
    {Acc, AccErrors} = fold_iterator(Fun, Acc0, AccErrors0, Slab, Stream, It, Ctx),
    fold_streams(Fun, Acc, AccErrors, Rest, Ctx).

-spec fold_iterator(
    fold_fun(Acc), Acc, [error(_)], slab(), stream(), iterator(), fold_ctx()
) -> {Acc, [fold_error()]}.
fold_iterator(Fun, Acc0, AccErrors, Slab, Stream, It0, Ctx) ->
    #fold_ctx{db = DB, batch_size = BatchSize} = Ctx,
    case next(DB, It0, BatchSize) of
        {ok, _It, []} ->
            {Acc0, AccErrors};
        {ok, end_of_stream} ->
            {Acc0, AccErrors};
        {ok, It, Batch} ->
            Acc = lists:foldl(
                fun(Msg, A) ->
                    Fun(Slab, Stream, Msg, A)
                end,
                Acc0,
                Batch
            ),
            fold_iterator(Fun, Acc, AccErrors, Slab, Stream, It, Ctx);
        Err ->
            {Acc0, [{stream, Slab, Stream, Err} | AccErrors]}
    end.

is_topic([]) ->
    true;
is_topic([B | Rest]) when is_binary(B) ->
    is_topic(Rest);
is_topic(_) ->
    false.

is_topic_filter([]) ->
    true;
is_topic_filter(['#']) ->
    true;
is_topic_filter([L | Rest]) when is_binary(L); L =:= '+' ->
    is_topic_filter(Rest);
is_topic_filter(_) ->
    false.

dirty_read_topic_fun(_Slab, _Stream, Object, Acc) ->
    [Object | Acc].

-spec multi_iter_opts(multi_iter_opts()) -> multi_iter_opts().
multi_iter_opts(UserOpts) ->
    maps:merge(
        #{shard => '_', generation => '_', start_time => 0},
        UserOpts
    ).

-spec do_multi_iterator_next(multi_iter_opts(), topic_filter(), multi_iterator(), pos_integer(), [
    payload()
]) -> {[payload()], multi_iterator() | '$end_of_table'}.
do_multi_iterator_next(_Opts, _TF, '$end_of_table', _N, Acc) ->
    {Acc, '$end_of_table'};
do_multi_iterator_next(_Opts, _TF, MIt, N, Acc) when N =< 0 ->
    {Acc, MIt};
do_multi_iterator_next(Opts = #{db := DB}, TF, MIt0 = #m_iter{it = It0, stream = Stream}, N, Acc0) ->
    Result = next(DB, It0, N),
    Len =
        case Result of
            {ok, _, Batch0} ->
                length(Batch0);
            _ ->
                0
        end,
    case Result of
        {ok, It, Batch} when Len >= N ->
            {Acc0 ++ Batch, MIt0#m_iter{it = It}};
        Other ->
            Acc =
                case Other of
                    {ok, _, Batch} ->
                        Acc0 ++ Batch;
                    _ ->
                        Acc0
                end,
            MIt = do_multi_iter_make_it(Opts, TF, multi_iter_next_stream(Opts, TF, Stream)),
            do_multi_iterator_next(Opts, TF, MIt, N - Len, Acc)
    end.

multi_iter_next_stream(Opts, TopicFilter, Stream) ->
    F = fun
        F([S1, S2 | _]) when S1 =:= Stream ->
            {ok, S2};
        F([_ | Rest]) ->
            F(Rest);
        F(_) ->
            '$end_of_table'
    end,
    F(multi_iter_get_streams(Opts, TopicFilter)).

multi_iter_get_streams(
    #{db := DB, start_time := StartTime, shard := Shard, generation := Gen}, TopicFilter
) ->
    lists:sort(
        lists:filtermap(
            fun({{S, G}, Stream}) ->
                (Shard =:= '_' orelse Shard =:= S) andalso
                    (Gen =:= '_' orelse Gen =:= G) andalso
                    {true, Stream}
            end,
            emqx_ds:get_streams(DB, TopicFilter, StartTime)
        )
    ).

do_multi_iter_make_it(_Opts, _TopicFilter, '$end_of_table') ->
    '$end_of_table';
do_multi_iter_make_it(#{db := DB, start_time := StartTime}, TopicFilter, {ok, Stream}) ->
    {ok, It} = make_iterator(DB, Stream, TopicFilter, StartTime),
    #m_iter{
        stream = Stream,
        it = It
    }.
