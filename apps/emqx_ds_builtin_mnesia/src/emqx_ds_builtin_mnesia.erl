%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_ds_builtin_mnesia).

-feature(maybe_expr, enable).

-behaviour(emqx_ds).

%% `emqx_ds' API
-export([
    open_db/2,
    close_db/1,
    drop_db/1,
    store_batch/3,
    get_streams/3,
    get_delete_streams/3,
    make_iterator/4,
    make_delete_iterator/4,
    update_iterator/3,
    next/3,
    delete_next/4,
    %% This backend has no generations, so these are to satisfy the API
    add_generation/1,
    update_db_config/2
]).

%% `emqx_ds_precondition' API
-export([lookup_message/2]).

%% internal exports
-export([execute_ops_txn/2, do_store_batch_transaction/3]).

-export_type([db_opts/0, iterator/0, delete_iterator/0]).

-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(MRIA_SHARD, ?MODULE).

-define(data_ns, data).
-define(top_key_ns, top_key).
-define(data_key(TOPIC_WORDS, ID), {?data_ns, {TOPIC_WORDS, ID}}).
-define(data_key(TOPIC_WORDS), ?data_key(TOPIC_WORDS, [])).

-define(insert_op(K, V), {insert, K, V}).
-define(delete_op(K), {delete, K}).

-define(tag, 1).
-define(top_key, 2).
-define(filter, 3).
-define(last_seen_key, 4).

-define(IT, 61).
-define(DELETE_IT, 62).

-opaque iterator() ::
    #{
        ?tag := ?IT,
        ?top_key := binary(),
        ?filter := emqx_ds:topic_filter(),
        ?last_seen_key := none | key()
    }.

-opaque delete_iterator() ::
    #{
        ?tag := ?DELETE_IT,
        ?top_key := binary(),
        ?filter := emqx_ds:topic_filter(),
        ?last_seen_key => none | key()
    }.

-type db_opts() ::
    #{
        backend := builtin_mnesia,
        %% Inherited from `emqx_ds:generic_db_opts()`.
        atomic_batches => boolean()
    }.

-define(stream(INNER), [2 | INNER]).
-define(delete_stream(INNER), [3 | INNER]).

-type key() :: ?data_key([binary()]).

-record(kv, {k :: key(), v :: term()}).

-type precondition_context() :: #{
    db := emqx_ds:db()
}.

%%================================================================================
%% API functions
%%================================================================================

%%================================================================================
%% `emqx_ds' API
%%================================================================================

-spec open_db(emqx_ds:db(), db_opts()) -> ok | {error, _}.
open_db(DB, CreateOpts) ->
    ok = ensure_table(DB, CreateOpts),
    case emqx_ds_builtin_mnesia_sup:start_db(DB, CreateOpts) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            ok;
        {error, Err} ->
            {error, Err}
    end.

-spec close_db(emqx_ds:db()) -> ok.
close_db(DB) ->
    emqx_ds_builtin_mnesia_sup:stop_db(DB).

-spec drop_db(emqx_ds:db()) -> ok | {error, _}.
drop_db(DB) ->
    close_db(DB),
    drop_table(DB),
    ok.

-spec store_batch(emqx_ds:db(), emqx_ds:batch(), emqx_ds:message_store_opts()) ->
    emqx_ds:store_batch_result().
store_batch(DB, Batch, Opts) ->
    case Batch of
        #dsbatch{} ->
            store_batch_transaction(DB, Batch, Opts);
        _ when is_list(Batch) ->
            store_batch_dirty(DB, Batch, Opts)
    end.

-spec get_streams(emqx_ds:db(), emqx_ds:topic_filter(), emqx_ds:time()) ->
    [{emqx_ds:stream_rank(), emqx_ds:ds_specific_stream()}].
get_streams(DB, TopicFilter, _StartTime) ->
    OnMatchFn = fun(K) -> {{0, 0}, ?stream(K)} end,
    do_get_streams(DB, TopicFilter, OnMatchFn).

-spec make_iterator(
    emqx_ds:db(), emqx_ds:ds_specific_stream(), emqx_ds:topic_filter(), emqx_ds:time()
) ->
    emqx_ds:make_iterator_result(iterator()).
make_iterator(_DB, ?stream(TopKey), TopicFilter, _StartTime) ->
    {ok, #{?tag => ?IT, ?filter => TopicFilter, ?top_key => TopKey, ?last_seen_key => none}}.

-spec update_iterator(emqx_ds:db(), iterator(), emqx_ds:message_key()) ->
    emqx_ds:make_iterator_result(iterator()).
update_iterator(_DB, #{?tag := ?IT} = Iter0, TopicBin) ->
    TopicWords = emqx_topic:words(TopicBin),
    Key = ?data_key(TopicWords),
    {ok, Iter0#{?last_seen_key => Key}}.

-spec next(emqx_ds:db(), iterator(), pos_integer()) -> emqx_ds:next_result(iterator()).
next(DB, #{?tag := ?IT} = Iter0, N) ->
    do_next(DB, Iter0, N).

-spec get_delete_streams(emqx_ds:db(), emqx_ds:topic_filter(), emqx_ds:time()) ->
    [emqx_ds:ds_specific_delete_stream()].
get_delete_streams(DB, TopicFilter, _StartTime) ->
    OnMatchFn = fun(K) -> ?delete_stream(K) end,
    do_get_streams(DB, TopicFilter, OnMatchFn).

-spec make_delete_iterator(
    emqx_ds:db(), emqx_ds:ds_specific_delete_stream(), emqx_ds:topic_filter(), emqx_ds:time()
) ->
    emqx_ds:make_delete_iterator_result(delete_iterator()).
make_delete_iterator(_DB, ?delete_stream(TopKey), TopicFilter, _StartTime) ->
    {ok, #{?tag => ?DELETE_IT, ?filter => TopicFilter, ?top_key => TopKey, ?last_seen_key => none}}.

-spec delete_next(emqx_ds:db(), delete_iterator(), emqx_ds:delete_selector(), pos_integer()) ->
    emqx_ds:delete_next_result(delete_iterator()).
delete_next(DB, Iter, SelectorFn, N) ->
    do_delete_next(DB, Iter, SelectorFn, N).

add_generation(_DB) ->
    {error, not_supported}.

update_db_config(_DB, _Opts) ->
    {error, not_supported}.

%%================================================================================
%% `emqx_ds_precondition' API
%%================================================================================

-spec lookup_message(precondition_context(), emqx_ds_precondition:matcher()) ->
    emqx_types:message() | not_found | emqx_ds:error(_).
lookup_message(Ctx, #message_matcher{topic = TopicBin}) ->
    #{db := DB} = Ctx,
    Table = table_name(DB),
    TopicWords = emqx_topic:words(TopicBin),
    Key = ?data_key(TopicWords),
    case mnesia:read(Table, Key, write) of
        [] ->
            not_found;
        [#kv{v = V}] ->
            V
    end.

%%================================================================================
%% Internal exports
%%================================================================================

execute_ops_txn(Table, PreparedOps) ->
    lists:foreach(
        fun
            (?insert_op(K, V)) ->
                mnesia:write(Table, #kv{k = K, v = V}, write);
            (?delete_op(K)) ->
                mnesia:delete(Table, K, write)
        end,
        PreparedOps
    ).

do_store_batch_transaction(DB, #dsbatch{} = Batch, _Opts) ->
    Table = table_name(DB),
    #dsbatch{
        preconditions = Preconditions,
        operations = Operations
    } = Batch,
    Ctx = #{db => DB},
    case emqx_ds_precondition:verify(?MODULE, Ctx, Preconditions) of
        ok ->
            PreparedOps = prepare_ops(Operations),
            execute_ops_txn(Table, PreparedOps),
            ok;
        {precondition_failed, _} = PreconditionFailed ->
            {error, unrecoverable, PreconditionFailed}
    end.

%%================================================================================
%% Internal functions
%%================================================================================

-spec do_next(emqx_ds:db(), Iterator, pos_integer()) ->
    emqx_ds:next_result(Iterator)
when
    Iterator :: iterator() | delete_iterator().
do_next(DB, Iter0, N) ->
    #{
        ?filter := TopicFilter,
        ?top_key := TopKey,
        ?last_seen_key := LastSeenKey0
    } = Iter0,
    Table = table_name(DB),
    T0 = erlang:monotonic_time(microsecond),
    {LastSeenKey, Results} = do_next_loop(Table, TopKey, TopicFilter, LastSeenKey0, N, []),
    T1 = erlang:monotonic_time(microsecond),
    emqx_ds_builtin_metrics:observe_next_time(DB, T1 - T0),
    {ok, Iter0#{?last_seen_key := LastSeenKey}, Results}.

do_next_loop(_Table, _TopKey, _TopicFilter, LastSeenKey, 0, Acc) ->
    {LastSeenKey, lists:reverse(Acc)};
do_next_loop(Table, TopKey, TopicFilter, LastSeenKey, N, Acc) ->
    case next_step(Table, TopKey, LastSeenKey, TopicFilter) of
        none ->
            {LastSeenKey, lists:reverse(Acc)};
        {ok, ?data_key(TopicWords) = NextKey, Val} ->
            BinKey = emqx_topic:join(TopicWords),
            do_next_loop(Table, TopKey, TopicFilter, NextKey, N - 1, [{BinKey, Val} | Acc])
    end.

next_step(Table, TopKey, CurrKey, TopicFilter) ->
    maybe
        {ok, ?data_key(Topic) = Key} ?= next_key(CurrKey, TopKey, TopicFilter, Table),
        true ?= emqx_topic:match(Topic, TopicFilter),
        [#kv{k = Key, v = Val}] ?= mnesia:dirty_read(Table, Key),
        {ok, Key, Val}
    else
        _ ->
            none
    end.

next_key('$end_of_table', _TopKey, _TopicFilter, _Table) ->
    none;
next_key(none, TopKey, TopicFilter, Table) ->
    %% Try to seek to the first concrete key.
    %% Optimizing for topic pattern `$topkey/$clientid/$id'.
    %% The stream already is `$topkey', so we may drop the first level from filter (could
    %% be '+' or '#').
    ConcretePrefix =
        case TopicFilter of
            [] ->
                %% Impossible?
                [];
            [_ | RestTF] ->
                concrete_prefix(RestTF)
        end,
    next_key(?data_key([TopKey | ConcretePrefix], {}), TopKey, TopicFilter, Table);
next_key(?data_key([TopKey | _], _) = Key, TopKey, TopicFilter, Table) ->
    case mnesia:dirty_next(Table, Key) of
        ?data_key([TopKey | _] = Topic) = NextKey ->
            case emqx_topic:match(Topic, TopicFilter) of
                true ->
                    {ok, NextKey};
                false ->
                    next_key(mnesia:dirty_next(Table, NextKey), TopKey, TopicFilter, Table)
            end;
        _ ->
            none
    end;
next_key(_Key, _TopKey, _TopicFilter, _Table) ->
    none.

concrete_prefix([Bin | Rest]) when is_binary(Bin) ->
    [Bin | concrete_prefix(Rest)];
concrete_prefix(_) ->
    [].

-spec do_delete_next(emqx_ds:db(), delete_iterator(), emqx_ds:delete_selector(), pos_integer()) ->
    emqx_ds:delete_next_result(delete_iterator()).
do_delete_next(DB, Iter0 = #{?tag := ?DELETE_IT}, SelectorFn, N) ->
    Table = table_name(DB),
    {ok, Iter, Msgs} = do_next(DB, Iter0, N),
    KeysToDelete =
        lists:filtermap(
            fun({TopicBin, Message}) ->
                case SelectorFn(Message) of
                    true ->
                        K = ?data_key(emqx_topic:words(TopicBin)),
                        {true, K};
                    false ->
                        false
                end
            end,
            Msgs
        ),
    ok = mria:async_dirty(?MRIA_SHARD, fun() ->
        lists:foreach(fun(K) -> mnesia:delete(Table, K, write) end, KeysToDelete)
    end),
    NumDeleted = length(KeysToDelete),
    {ok, Iter, NumDeleted}.

store_batch_transaction(DB, #dsbatch{} = Batch, Opts) ->
    Result = mria:transaction(
        ?MRIA_SHARD,
        fun ?MODULE:do_store_batch_transaction/3,
        [DB, Batch, Opts]
    ),
    case Result of
        {atomic, Res} -> Res;
        {aborted, Reason} -> {error, unrecoverable, Reason}
    end.

store_batch_dirty(_DB, [], _Opts) ->
    ok;
store_batch_dirty(DB, [_ | _] = Batch, Opts) ->
    Table = table_name(DB),
    Activity =
        case maps:get(sync, Opts, true) of
            true -> sync_dirty;
            false -> async_dirty
        end,
    PreparedOps = prepare_ops(Batch),
    try mria:Activity(?MRIA_SHARD, fun ?MODULE:execute_ops_txn/2, [Table, PreparedOps]) of
        ok ->
            ok
    catch
        Kind:Reason:Stacktrace ->
            logger:warning(#{
                msg => "ds_builtin_mnesia_store_batch_dirty_error",
                activity => Activity,
                kind => Kind,
                reason => Reason,
                stacktrace => Stacktrace
            }),
            {error, unrecoverable, {Kind, Reason}}
    end.

prepare_ops(Batch) ->
    {TopKeysSet, PreparedBatch} =
        lists:foldl(
            fun(Op, {TopKeyAcc, OpAcc}) ->
                case prepare_op(Op) of
                    ?insert_op(?data_key(TopicWords), _) = PreparedOp ->
                        TopKey = top_key(TopicWords),
                        {TopKeyAcc#{TopKey => true}, [PreparedOp | OpAcc]};
                    PreparedOp ->
                        {TopKeyAcc, [PreparedOp | OpAcc]}
                end
            end,
            {#{}, []},
            Batch
        ),
    TopKeyOps = maps:fold(
        fun(TopKey, _, Acc) ->
            [?insert_op({?top_key_ns, TopKey}, true) | Acc]
        end,
        [],
        TopKeysSet
    ),
    TopKeyOps ++ PreparedBatch.

top_key([]) ->
    '';
top_key([TopKey | _]) ->
    TopKey.

prepare_op(#message{} = Message) ->
    TopicWords = emqx_topic:words(Message#message.topic),
    Key = ?data_key(TopicWords),
    ?insert_op(Key, Message);
prepare_op({delete, #message_matcher{topic = Topic}}) ->
    TopicWords = emqx_topic:words(Topic),
    Key = ?data_key(TopicWords),
    ?delete_op(Key).

table_name(DB) ->
    list_to_atom("ds_tab_" ++ atom_to_list(DB)).

ensure_table(DB, _Opts) ->
    Table = table_name(DB),
    ok = mria:create_table(Table, [
        {rlog_shard, ?MRIA_SHARD},
        {type, ordered_set},
        {storage, disc_copies},
        {record_name, kv},
        {attributes, record_info(fields, kv)}
    ]),
    ok = mria:wait_for_tables([Table]),
    ok.

drop_table(_DB) ->
    %% TODO: implement `mria:delete_table'.
    %% Table = table_name(DB),
    %% case mnesia:delete_table(Table) of
    %%     {atomic, ok} -> ok;
    %%     {aborted, {no_exists, _}} -> ok
    %% end.
    ok.

do_get_streams(DB, TopicFilter, OnMatchFn) ->
    MS = ets:fun2ms(fun(#kv{k = {?top_key_ns, K}, v = _}) -> K end),
    TopKeys = mnesia:dirty_select(table_name(DB), MS),
    MatchesFilterTop = fun(TopKey) ->
        case TopicFilter of
            ['#'] ->
                true;
            ['+' | _] ->
                true;
            [TopKeyF | _] ->
                TopKeyF =:= TopKey;
            _ ->
                false
        end
    end,
    lists:filtermap(
        fun(K) ->
            case MatchesFilterTop(K) of
                true -> {true, OnMatchFn(K)};
                false -> false
            end
        end,
        TopKeys
    ).
