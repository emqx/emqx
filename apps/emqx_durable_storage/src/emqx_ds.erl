%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    shard_of/2,
    list_generations_with_lifetimes/1,
    drop_generation/2,
    drop_db/1
]).

%% Message storage API:
-export([store_batch/2, store_batch/3]).

%% Transactional API (low-level):
%% FIXME: do not export?
-export([new_blob_tx/2, commit_blob_tx/3]).

%% Message replay API:
-export([get_streams/3, get_streams/4, make_iterator/4, next/3]).
-export([subscribe/3, unsubscribe/2, suback/3, subscription_info/2]).

%% Message delete API:
-export([get_delete_streams/3, make_delete_iterator/4, delete_next/4]).

%% Misc. API:
-export([count/1]).
-export([timestamp_us/0]).
-export([topic_words/1]).

%% Utility functions:
-export([
    dirty_read/2,
    dirty_read/3,

    fold_topic/4,
    fold_topic/5,
    fold_topic/6,

    trans/2,
    tx_blob_write/2,
    tx_del_topic/1,
    tx_blob_assert/2,
    tx_blob_assert_not/1,
    reset_trans/0
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
    next_result/1, next_result/0,
    delete_next_result/1, delete_next_result/0,
    store_batch_result/0,
    make_iterator_result/1, make_iterator_result/0,
    make_delete_iterator_result/1, make_delete_iterator_result/0,

    error/1,

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

    kv_pair/0,
    kv_matcher/0,

    tx_context/0,
    transaction_opts/0,
    blob_tx_ops/0,
    commit_result/0
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

-type kv_pair() :: {topic(), binary()}.

-type kv_matcher() :: {message_key(), binary() | '_'}.

%% Precondition.
%% Fails whole batch if the storage already has the matching message (`if_exists'),
%% or does not yet have (`unless_exists'). Here "matching" means that it either
%% just exists (when pattern is '_') or has exactly the same payload, rest of the
%% message fields are irrelevant.
%% Useful to construct batches with "compare-and-set" semantics.
%% Note: backends may not support this, but if they do only DBs with `atomic_batches'
%% enabled are expected to support preconditions in batches.
-type precondition(A) :: {if_exists | unless_exists, A}.

-type precondition() :: precondition(message_matcher('_')).

-type shard() :: term().

-type generation() :: integer().

-type slab() :: {shard(), generation()}.

%% TODO: Not implemented
-type iterator_id() :: term().

-opaque iterator() :: ds_specific_iterator().

-opaque delete_iterator() :: ds_specific_delete_iterator().

-type get_streams_opts() :: #{
    shard => shard()
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

-type next_result(Iterator) ::
    {ok, Iterator, [{message_key(), emqx_types:message()}]} | {ok, end_of_stream} | error(_).

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
        sync => boolean(),
        %% Whether to store this batch as a single unit.  Currently not supported by
        %% builtin backends.
        %% Default: `false'.
        atomic => boolean()
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
        %% Whether the DB stores values of type `#message{}' or `#ds_blob{}'
        store_kv => boolean(),
        %% Backend-specific options:
        _ => _
    }.

-type db_opts() :: #{
    %% See respective `create_db_opts()` fields.
    append_only => boolean(),
    atomic_batches => boolean(),
    store_kv => boolean()
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

-type sub_opts() ::
    #{
        %% Maximum number of unacked batches before subscription is
        %% considered overloaded and removed from the active queues:
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

-type sub_ref() :: reference().

-type sub_seqno() :: non_neg_integer().

%% Low-level transaction types:
-type tx_context() :: term().

%% Note: the only guarantee about order of operations is that all
%% deletions are executed before the writes.
-type blob_tx_ops() :: #{
    %% Write operations:
    ?ds_tx_write => [kv_pair()],
    %% Deletions:
    ?ds_tx_delete_topic => [topic_filter()],
    %% Preconditions:
    %%   List of objects that should be present in the database.
    ?ds_tx_expected => [kv_matcher()],
    %%   List of objects that should NOT be present in the database.
    ?ds_tx_unexpected => [topic()]
}.

-type transaction_opts() :: #{
    db := db(),
    %% Either `owner' or `shard' field should be present. If `owner'
    %% option is given, current sharding strategy will be used to
    %% determine the shard.
    %%
    %% WARNING: combination of `owner' and `generation' options is
    %% dangerous since number of shards may change, and shard
    %% allocation for older generations could be different.
    owner => emqx_types:clientid(),
    shard => shard(),

    %% If not specified, the last generation will be used:
    generation => generation(),

    %% Options that govern retry of recoverable errors:
    retries => non_neg_integer(),
    retry_interval => non_neg_integer()
}.

-type commit_result() :: ok | error(_).

-type fold_fun(Acc) :: fun(
    (
        slab(),
        stream(),
        message_key(),
        emqx_types:message() | kv_pair(),
        Acc
    ) -> Acc
).

-type fold_options() ::
    #{
        errors => crash | report,
        batch_size => pos_integer(),
        shard => shard(),
        generation => generation()
    }.

-record(fold_ctx, {
    db :: db(),
    tf :: topic_filter(),
    start_time :: time(),
    batch_size :: pos_integer(),
    errors :: crash | report
}).

-type fold_ctx() :: #fold_ctx{}.

-type fold_error() ::
    {shard, shard(), error(_)}
    | {stream, slab(), stream(), error(_)}.

-type fold_result(R) :: {R, [fold_error()]} | R.

%% Internal:
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
    #{slab() => slab_info()}.

-callback drop_generation(db(), slab()) -> ok | {error, _}.

-callback drop_db(db()) -> ok | {error, _}.

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

%% Statistics API:
-callback count(db()) -> non_neg_integer().

%% Blob transaction API:
-callback new_blob_tx(db(), transaction_opts()) ->
    {ok, tx_context()} | error(_).

-callback commit_blob_tx(db(), tx_context(), blob_tx_ops()) -> commit_result().

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
open_db(DB, UserOpts) ->
    Opts = #{backend := Backend} = set_db_defaults(UserOpts),
    %% Santiy checks:
    case Opts of
        #{store_kv := true, append_only := true} ->
            %% Blobs don't have a builtin timestamp field, so we
            %% cannot set it automatically:
            error({incompatible_options, [store_kv, append_only]});
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

-spec list_generations_with_lifetimes(db()) -> #{slab() => slab_info()}.
list_generations_with_lifetimes(DB) ->
    Mod = ?module(DB),
    call_if_implemented(Mod, list_generations_with_lifetimes, [DB], #{}).

-spec drop_generation(db(), generation()) -> ok | {error, _}.
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

%% @doc Get shard of a client ID or a topic.
%%
%% WARNING: This function does NOT check the type of input, and may
%% return arbitrary result when called with topic as an argument for a
%% DB using client ID for sharding and vice versa.
-spec shard_of(db(), emqx_types:clientid() | topic()) -> shard().
shard_of(DB, ClientId) ->
    ?module(DB):shard_of(DB, ClientId).

-spec store_batch(db(), batch(), message_store_opts()) -> store_batch_result().
store_batch(DB, Msgs, Opts) ->
    ?module(DB):store_batch(DB, Msgs, Opts).

-spec store_batch(db(), batch()) -> store_batch_result().
store_batch(DB, Msgs) ->
    store_batch(DB, Msgs, #{}).

%% @doc Simplified version of `get_streams/4' that ignores the errors.
-spec get_streams(db(), topic_filter(), time()) -> [{slab(), stream()}].
get_streams(DB, TopicFilter, StartTime) ->
    {Streams, _Errors} = get_streams(DB, TopicFilter, StartTime, #{}),
    Streams.

%% @doc Get a list of streams needed for replaying a topic filter.
%%
%% When `shard' key is present in the options, this function will
%% query only the specified shard. Otherwise, it will query all
%% shards.
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
%% replay. This function returns stream together with identifier of
%% the slab containing it.
%%
%% Slab ID is a tuple of two terms: *shard* and *generation*. If two
%% streams reside in slabs with different shard, they are independent
%% and can be replayed in parallel. If shard is the same, then the
%% stream with smaller generation should be replayed first. If both
%% shard and generations are equal, then the streams are independent.
%%
%% Stream is fully consumed when `next/3' function returns
%% `end_of_stream'. Then and only then the client can proceed to
%% replaying streams that depend on the given one.
-spec get_streams(db(), topic_filter(), time(), get_streams_opts()) -> get_streams_result().
get_streams(DB, TopicFilter, StartTime, Opts) ->
    ?module(DB):get_streams(DB, TopicFilter, StartTime, Opts).

-spec make_iterator(db(), stream(), topic_filter(), time()) -> make_iterator_result().
make_iterator(DB, Stream, TopicFilter, StartTime) ->
    ?module(DB):make_iterator(DB, Stream, TopicFilter, StartTime).

-spec next(db(), iterator(), pos_integer()) -> next_result().
next(DB, Iter, BatchSize) ->
    ?module(DB):next(DB, Iter, BatchSize).

%% @doc "Multi-poll" API: subscribe current process to the messages
%% that follow `Iterator'.
%%
%% This function returns subscription handle that can be used to to
%% manipulate the subscription (ack batches and unsubscribe), as well
%% as a monitor reference that used to detect unexpected termination
%% of the subscription on the DS side. The same reference is included
%% in the `#poll_reply{}' messages sent by DS to the subscriber.
%%
%% Once subscribed, the client process will receive messages of type
%% `#poll_reply{}':
%%
%% - `ref' field is equal to the `sub_ref()' returned by subscribe
%% call.
%%
%% - `size' field is equal to the number of messages in the payload.
%% If payload = `{ok, end_of_stream}' or `{error, _, _}' then `size' =
%% 1.
%%
%% - `seqno' field contains sum of all `size's received by the
%% subscription so far (including the current batch).
%%
%% - `stuck' flag is set when subscription is paused for not keeping
%% up with the acks.
%%
%% - `lagging' flag is an implementation-defined indicator that the
%% subscription is currently reading old data.
-spec subscribe(db(), iterator(), sub_opts()) ->
    {ok, subscription_handle(), sub_ref()} | error(_).
subscribe(DB, Iterator, SubOpts) ->
    ?module(DB):subscribe(DB, Iterator, SubOpts).

-spec unsubscribe(db(), subscription_handle()) -> boolean().
unsubscribe(DB, SubRef) ->
    ?module(DB):unsubscribe(DB, SubRef).

%% @doc Acknowledge processing of a message with a given sequence
%% number. This way client can signal to DS that it is ready to
%% process more data. Subscriptions that do not keep up with the acks
%% are paused.
-spec suback(db(), subscription_handle(), non_neg_integer()) -> ok.
suback(DB, SubRef, SeqNo) ->
    ?module(DB):suback(DB, SubRef, SeqNo).

%% @doc Get information about the subscription.
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

%% @hidden Low-level transaction API. Obtain context for an optimistic
%% transaction that allows to execute a set of operations atomically.
%%
%% Note: transactions always apply to the *current* generation. Old
%% generations are immutable.
-spec new_blob_tx(db(), transaction_opts()) -> {ok, tx_context()} | error(_).
new_blob_tx(DB, Opts) ->
    ?module(DB):new_blob_tx(DB, Opts).

%% @hidden Low-level transaction API. Try to commit a set of operations
%% executed in a given transaction context.
-spec commit_blob_tx(db(), tx_context(), blob_tx_ops()) ->
    commit_result().
commit_blob_tx(DB, TxContext, TxOps) ->
    ?module(DB):commit_blob_tx(DB, TxContext, TxOps).

%%================================================================================
%% Utility functions
%%================================================================================

%% Transaction process dictionary keys:
-define(tx_ops_write, emqx_ds_tx_ctx_write).
-define(tx_ops_del_topic, emqx_ds_tx_ctx_del_topic).
-define(tx_ops_expected, emqx_ds_tx_ctx_expected).
-define(tx_ops_unexpected, emqx_ds_tx_ctx_unexpected).

%% Transaction throws:
-define(tx_reset, emqx_ds_tx_reset).

%% @doc Execute a function in an environment where write and delete
%% operations and asserts are deferred and then committed atomically.
%%
%% NOTE: This function is not as sophisticated as mnesia. In
%% particular, side effects of writes and deletes are not visible
%% until the commit.
-spec trans(
    transaction_opts(),
    fun(() -> Ret)
) ->
    {ok, Ret} | error(_).
trans(Opts = #{db := DB}, Fun) ->
    case is_trans() of
        false ->
            Retries = maps:get(retries, Opts, 0),
            trans(DB, Fun, Opts, Retries);
        true ->
            ?err_unrec(nested_transaction)
    end.

-spec tx_blob_write(topic(), binary()) -> ok.
tx_blob_write(Topic, Value) ->
    ct:pal("OHAYO))) ~p", [Topic]),
    case is_topic(Topic) andalso is_binary(Value) of
        true ->
            tx_push_op(?tx_ops_write, {Topic, Value});
        false ->
            error(badarg)
    end.

-spec tx_del_topic(topic_filter()) -> ok.
tx_del_topic(TopicFilter) ->
    case is_topic_filter(TopicFilter) of
        true ->
            tx_push_op(?tx_ops_del_topic, TopicFilter);
        false ->
            error(badarg)
    end.

-spec tx_blob_assert(topic(), binary() | '_') -> ok.
tx_blob_assert(Topic, Value) ->
    case is_topic(Topic) andalso (Value =:= '_' orelse is_binary(Value)) of
        true ->
            tx_push_op(?tx_ops_expected, {Topic, Value});
        false ->
            error(badarg)
    end.

-spec tx_blob_assert_not(topic()) -> ok.
tx_blob_assert_not(Topic) ->
    case is_topic(Topic) of
        true ->
            tx_push_op(?tx_ops_unexpected, Topic);
        false ->
            error(badarg)
    end.

%% @doc Restart `trans'
-spec reset_trans() -> no_return().
reset_trans() ->
    throw(?tx_reset).

-spec dirty_read(db(), topic_filter()) ->
    fold_result([kv_pair() | emqx_types:message()]).
dirty_read(DB, TopicFilter) ->
    dirty_read(DB, TopicFilter, #{}).

-spec dirty_read(db(), topic_filter(), fold_options()) ->
    fold_result([kv_pair() | emqx_types:message()]).
dirty_read(DB, TopicFilter, Opts) ->
    Fun = fun(_Slab, _Stream, _DSKey, Object, Acc) -> [Object | Acc] end,
    fold_topic(Fun, [], DB, TopicFilter, 0, Opts).

-spec fold_topic(fold_fun(Acc), Acc, db(), topic_filter()) -> fold_result(Acc).
fold_topic(Fun, Acc, DB, TopicFilter) ->
    fold_topic(Fun, Acc, DB, TopicFilter, 0).

-spec fold_topic(fold_fun(Acc), Acc, db(), topic_filter(), time()) ->
    fold_result(Acc).
fold_topic(Fun, Acc, DB, TopicFilter, StartTime) ->
    fold_topic(Fun, Acc, DB, TopicFilter, StartTime, #{}).

-spec fold_topic(
    fold_fun(Acc),
    Acc,
    db(),
    topic_filter(),
    time(),
    fold_options()
) -> fold_result(Acc).
fold_topic(Fun, AccIn, DB, TopicFilter, StartTime, UserOpts) ->
    %% Merge config and make fold context:
    Defaults = #{
        batch_size => 100,
        generation => '_',
        errors => crash
    },
    #{
        batch_size := BatchSize,
        generation := GenerationMatcher,
        errors := ErrorHandling
    } = maps:merge(Defaults, UserOpts),
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
    {Iterators, Errors} =
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
    case {Errors, ErrorHandling} of
        {_, report} ->
            {fold_streams(Fun, AccIn, Iterators, Ctx), Errors};
        {[], crash} ->
            fold_streams(Fun, AccIn, Iterators, Ctx);
        {_, crash} ->
            error(Errors)
    end.

%%================================================================================
%% Internal exports
%%================================================================================

-spec timestamp_us() -> time().
timestamp_us() ->
    erlang:system_time(microsecond).

%% @doc Version of `emqx_topic:words()' that doesn't transform empty
%% topic into atom ''
-spec topic_words(binary()) -> topic().
topic_words(<<>>) -> [];
topic_words(Bin) -> emqx_topic:words(Bin).

%%================================================================================
%% Internal functions
%%================================================================================

set_db_defaults(Opts = #{store_kv := true}) ->
    Defaults = #{
        append_only => false,
        atomic_batches => true
    },
    maps:merge(Defaults, Opts);
set_db_defaults(Opts) ->
    Defaults = #{
        append_only => true,
        atomic_batches => false,
        store_kv => false
    },
    maps:merge(Defaults, Opts).

call_if_implemented(Mod, Fun, Args, Default) ->
    case erlang:function_exported(Mod, Fun, length(Args)) of
        true ->
            apply(Mod, Fun, Args);
        false ->
            Default
    end.

-spec trans(
    db(),
    fun(() -> Ret),
    transaction_opts(),
    non_neg_integer()
) ->
    {ok, Ret} | error(_).
trans(DB, Fun, Opts, Retries) ->
    _ = put(?tx_ops_write, []),
    _ = put(?tx_ops_del_topic, []),
    _ = put(?tx_ops_expected, []),
    _ = put(?tx_ops_unexpected, []),
    RetryTimeout = maps:get(retry_interval, Opts, 1_000),
    try
        case new_blob_tx(DB, Opts) of
            {ok, Ctx} ->
                Ret = Fun(),
                Tx = #{
                    ?ds_tx_write => erase(?tx_ops_write),
                    ?ds_tx_delete_topic => erase(?tx_ops_del_topic),
                    ?ds_tx_expected => erase(?tx_ops_expected),
                    ?ds_tx_unexpected => erase(?tx_ops_unexpected)
                },
                case Tx of
                    #{
                        ?ds_tx_write := [],
                        ?ds_tx_delete_topic := [],
                        ?ds_tx_expected := [],
                        ?ds_tx_unexpected := []
                    } ->
                        %% Nothing to commit:
                        {ok, Ret};
                    _ ->
                        case commit_blob_tx(DB, Ctx, Tx) of
                            ok ->
                                {ok, Ret};
                            ?err_unrec(_) = Err ->
                                Err;
                            ?err_rec(_) when Retries > 0 ->
                                timer:sleep(RetryTimeout),
                                trans(DB, Fun, Opts, Retries - 1)
                        end
                end;
            ?err_rec(_) ->
                timer:sleep(RetryTimeout),
                trans(DB, Fun, Opts, Retries - 1);
            ?err_unrec(_) = Err ->
                Err
        end
    catch
        ?tx_reset when Retries > 0 ->
            timer:sleep(RetryTimeout),
            trans(DB, Fun, Opts, Retries - 1)
    after
        _ = erase(?tx_ops_write),
        _ = erase(?tx_ops_del_topic),
        _ = erase(?tx_ops_expected),
        _ = erase(?tx_ops_unexpected)
    end.

is_trans() ->
    case get(?tx_ops_write) of
        undefined ->
            false;
        _ ->
            true
    end.

tx_push_op(K, A) ->
    put(K, [A | get(K)]),
    ok.

-spec fold_streams(
    fold_fun(Acc), Acc, [{slab(), stream(), iterator()}], fold_ctx()
) -> Acc.
fold_streams(_Fun, Acc, [], _Ctx) ->
    Acc;
fold_streams(Fun, Acc0, [{Slab, Stream, It} | Rest], Ctx) ->
    Acc = fold_iterator(Fun, Acc0, Slab, Stream, It, Ctx),
    fold_streams(Fun, Acc, Rest, Ctx).

-spec fold_iterator(
    fold_fun(Acc), Acc, slab(), stream(), iterator(), fold_ctx()
) -> Acc.
fold_iterator(Fun, Acc0, Slab, Stream, It0, Ctx) ->
    #fold_ctx{db = DB, batch_size = BatchSize} = Ctx,
    case next(DB, It0, BatchSize) of
        {ok, _It, []} ->
            Acc0;
        {ok, end_of_stream} ->
            Acc0;
        {ok, It, Batch} ->
            Acc = lists:foldl(
                fun({MsgKey, Msg}, A) ->
                    Fun(Slab, Stream, MsgKey, Msg, A)
                end,
                Acc0,
                Batch
            ),
            fold_iterator(Fun, Acc, Slab, Stream, It, Ctx)
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
is_topic_filter(['+' | Rest]) ->
    is_topic_filter(Rest);
is_topic_filter([B | Rest]) when is_binary(B) ->
    is_topic_filter(Rest);
is_topic_filter(_) ->
    false.
