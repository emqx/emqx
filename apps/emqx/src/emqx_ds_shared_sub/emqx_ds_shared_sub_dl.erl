%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_shared_sub_dl).
-moduledoc """
Data layer for the shared durable subscriptions.

This module abstracts all CRUD operations on the shared sub leader durable state.
""".

-behaviour(emqx_ds_pmap).

%% API:
-export([
    open_db/0,
    wait_db/0,
    close_db/0,

    mk_id/1,
    mk_id/2,

    create_new/1,
    commit/2,
    open/1,
    destroy/1,
    is_dirty/1,
    exists/1,
    dirty_read_props/1,

    set_created_at/2,
    get_start_time/1,
    set_start_time/2,
    get_strategy/1,
    set_strategy/2,
    get_realloc_timeout/1,
    set_realloc_timeout/2,
    get_revocation_timeout/1,
    set_revocation_timeout/2,

    set_current_generation/3,
    get_current_generation/2,

    set_stream_state/4,
    get_stream_state/3,
    del_stream_state/3,
    fold_stream_states/3,

    make_iterator/0,
    iterator_next/2
]).

%% behavior callbacks:
-export([pmap_encode_key/3, pmap_decode_key/2, pmap_encode_val/3, pmap_decode_val/3]).

%% internal exports:
-export([]).

-export_type([t/0, id/0, cursor/0]).

-include("../gen_src/DSSharedSub.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds_pmap.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include("emqx_persistent_message.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(DB, shared_subs).

-type id() :: binary().

-define(properties, properties).
-define(top_properties, <<"prop">>).
-define(generations, generations).
-define(top_generations, <<"gens">>).
-define(iterators, iterators).
-define(top_iterators, <<"it">>).

-define(prop_created_at, <<"created_at">>).
-define(prop_start_time, <<"start_time">>).
-define(prop_strategy, <<"strat">>).
-define(prop_realloc_timeout, <<"to_realloc">>).
-define(prop_revocation_timeout, <<"to_revoke">>).

-type t() :: #{
    id := id(),
    ?collection_dirty := boolean(),
    ?collection_guard := emqx_ds_pmap:guard() | undefined,
    ?properties := emqx_ds_pmap:pmap(atom(), term()),
    ?generations := emqx_ds_pmap:pmap(emqx_ds:shard(), emqx_ds:generation()),
    ?iterators := emqx_ds_pmap:pmap(
        {emqx_ds:slab(), emqx_ds:stream()}, emqx_ds_shared_sub_leader:stream_state()
    )
}.

-type lifetime() :: new | up.

%% base62-encoded binary
-type cursor() :: binary().

%%================================================================================
%% API functions
%%================================================================================

-spec open_db() -> ok.
open_db() ->
    Config = emqx_ds_schema:db_config_shared_subs(),
    emqx_ds:open_db(?DB, Config#{
        atomic_batches => true,
        append_only => false,
        store_ttv => true,
        storage => emqx_ds_pmap:storage_opts(#{})
    }).

-spec wait_db() -> ok.
wait_db() ->
    emqx_ds:wait_db(?DB, all, infinity).

-spec close_db() -> ok.
close_db() ->
    emqx_ds:close_db(?DB).

-spec mk_id(emqx_types:share()) -> id().
mk_id(#share{group = ShareGroup, topic = Topic}) ->
    mk_id(ShareGroup, Topic).

-spec mk_id(_GroupName :: binary(), emqx_types:topic()) -> id().
mk_id(Group, Topic) ->
    'DSSharedSub':encode('Id', #'Id'{
        group = Group,
        topic = Topic
    }).

-spec dirty_read_props(id()) -> emqx_ds_shared_sub:info() | undefined.
dirty_read_props(Id) ->
    Map = emqx_ds_pmap:dirty_read(?MODULE, ?top_properties, Id, trans_opts(Id)),
    case Map of
        #{?prop_created_at := CreatedAt, ?prop_start_time := StartTime} ->
            #'Id'{group = Group, topic = Topic} = 'DSSharedSub':decode('Id', Id),
            #{
                id => Id,
                created_at => CreatedAt,
                start_time => StartTime,
                group => Group,
                topic => Topic
            };
        #{} ->
            undefined
    end.

%%--------------------------------------------------------------------------------
%% Operations over entire collections
%%--------------------------------------------------------------------------------

-spec create_new(id()) -> t().
create_new(Id) ->
    #{
        id => Id,
        ?collection_dirty => true,
        ?collection_guard => undefined,
        ?properties => emqx_ds_pmap:new_pmap(?MODULE, ?top_properties),
        ?generations => emqx_ds_pmap:new_pmap(?MODULE, ?top_generations),
        ?iterators => emqx_ds_pmap:new_pmap(?MODULE, ?top_iterators)
    }.

-spec commit(lifetime(), t()) -> t().
commit(up, Rec = #{?collection_dirty := false}) ->
    Rec;
commit(
    Lifetime,
    Rec0 = #{
        id := Id,
        ?collection_guard := OldGuard,
        ?properties := Properties,
        ?generations := Generations,
        ?iterators := Iterators
    }
) ->
    NewGuard = Lifetime =/= up,
    Result = emqx_ds:trans(
        trans_opts(Id),
        fun() ->
            emqx_ds_pmap:tx_assert_guard(Id, OldGuard),
            NewGuard andalso emqx_ds_pmap:tx_write_guard(Id, ?ds_tx_serial),
            Rec0#{
                ?properties := emqx_ds_pmap:tx_commit(Id, Properties),
                ?generations := emqx_ds_pmap:tx_commit(Id, Generations),
                ?iterators := emqx_ds_pmap:tx_commit(Id, Iterators),
                ?unset_dirty
            }
        end
    ),
    case Result of
        {atomic, TXSerial, Rec} when NewGuard ->
            Rec#{?collection_guard := TXSerial};
        {atomic, _, Rec} ->
            Rec;
        {nop, Rec} ->
            Rec;
        Err ->
            error({failed_to_commit_state, Err})
    end.

-spec open(id()) -> {ok, t()} | undefined.
open(Id) ->
    case emqx_ds:trans(trans_opts(Id), fun() -> open_tx(Id) end) of
        {atomic, _, Ret} ->
            Ret;
        {nop, Ret} ->
            Ret
    end.

-spec destroy(id()) -> ok | emqx_ds:error(_).
destroy(Id) ->
    Result = emqx_ds:trans(
        trans_opts(Id),
        fun() ->
            emqx_ds_pmap:tx_delete_guard(Id),
            emqx_ds_pmap:tx_destroy(Id, ?top_properties),
            emqx_ds_pmap:tx_destroy(Id, ?top_generations),
            emqx_ds_pmap:tx_destroy(Id, ?top_iterators)
        end
    ),
    case Result of
        {atomic, _, _} ->
            ok;
        {error, _, _} = Err ->
            Err
    end.

-spec is_dirty(t()) -> boolean().
is_dirty(#{?collection_dirty := Dirty}) ->
    Dirty.

-spec exists(id()) -> boolean().
exists(Id) ->
    case emqx_ds:dirty_read(trans_opts(Id), ?guard_topic(Id)) of
        [] -> false;
        [_] -> true
    end.

%%--------------------------------------------------------------------------------
%% CRUD
%%--------------------------------------------------------------------------------

-spec set_created_at(integer(), t()) -> t().
set_created_at(T, Rec) ->
    emqx_ds_pmap:collection_put(?properties, ?prop_created_at, T, Rec).

-spec set_start_time(integer(), t()) -> t().
set_start_time(V, Rec) ->
    emqx_ds_pmap:collection_put(?properties, ?prop_start_time, V, Rec).

-spec get_start_time(t()) -> integer() | undefined.
get_start_time(Rec) ->
    emqx_ds_pmap:collection_get(?properties, ?prop_start_time, Rec).

-spec set_strategy(emqx_ds_shared_sub:strategy() | undefined, t()) -> t().
set_strategy(Module, Rec) ->
    emqx_ds_pmap:collection_put(?properties, ?prop_strategy, Module, Rec).

-spec get_strategy(t()) -> emqx_ds_shared_sub:strategy() | undefined.
get_strategy(Rec) ->
    emqx_ds_pmap:collection_get(?properties, ?prop_strategy, Rec).

-spec get_realloc_timeout(t()) -> non_neg_integer() | undefined.
get_realloc_timeout(Rec) ->
    emqx_ds_pmap:collection_get(?properties, ?prop_realloc_timeout, Rec).

-spec set_realloc_timeout(non_neg_integer() | undefined, t()) -> t().
set_realloc_timeout(Timeout, Rec) ->
    emqx_ds_pmap:collection_put(?properties, ?prop_realloc_timeout, Timeout, Rec).

-spec get_revocation_timeout(t()) -> non_neg_integer() | undefined.
get_revocation_timeout(Rec) ->
    emqx_ds_pmap:collection_get(?properties, ?prop_revocation_timeout, Rec).

-spec set_revocation_timeout(non_neg_integer() | undefined, t()) -> t().
set_revocation_timeout(Timeout, Rec) ->
    emqx_ds_pmap:collection_put(?properties, ?prop_revocation_timeout, Timeout, Rec).

-spec get_current_generation(emqx_ds:shard(), t()) -> emqx_ds:generation() | undefined.
get_current_generation(Shard, Rec) ->
    emqx_ds_pmap:collection_get(?generations, Shard, Rec).

-spec set_current_generation(emqx_ds:shard(), emqx_ds:generation(), t()) -> t().
set_current_generation(Shard, Generation, Rec) ->
    emqx_ds_pmap:collection_put(?generations, Shard, Generation, Rec).

-spec set_stream_state(
    emqx_ds:slab(), emqx_ds:stream(), emqx_ds_shared_sub_leader:stream_state(), t()
) -> t().
set_stream_state(Slab, Stream, State, Rec) ->
    emqx_ds_pmap:collection_put(?iterators, {Slab, Stream}, State, Rec).

-spec get_stream_state(emqx_ds:slab(), emqx_ds:stream(), t()) ->
    emqx_ds_shared_sub_leader:stream_state() | undefined.
get_stream_state(Slab, Stream, Rec) ->
    emqx_ds_pmap:collection_get(?iterators, {Slab, Stream}, Rec).

-spec del_stream_state(emqx_ds:slab(), emqx_ds:stream(), t()) -> t().
del_stream_state(Slab, Stream, Rec) ->
    emqx_ds_pmap:collection_del(?iterators, {Slab, Stream}, Rec).

-spec fold_stream_states(
    fun((emqx_ds:slab(), emqx_ds:stream(), emqx_ds_shared_sub_leader:stream_state(), A) -> A),
    A,
    t()
) ->
    A.
fold_stream_states(Fun, Acc0, Rec) ->
    emqx_ds_pmap:collection_fold(
        ?iterators,
        fun({Slab, Stream}, SState, Acc) ->
            Fun(Slab, Stream, SState, Acc)
        end,
        Acc0,
        Rec
    ).

%%--------------------------------------------------------------------------------
%% Iteration over shared sub states
%%--------------------------------------------------------------------------------

-spec make_iterator() -> cursor().
make_iterator() ->
    it2cursor(emqx_ds:make_multi_iterator(multi_iter_opts(), ?guard_topic('+'))).

-spec iterator_next(cursor(), pos_integer()) -> {[id()], cursor()}.
iterator_next(Cursor, Limit) ->
    {TTVs, It} = emqx_ds:multi_iterator_next(
        multi_iter_opts(), ?guard_topic('+'), cursor2it(Cursor), Limit
    ),
    Batch = lists:filtermap(
        fun({?guard_topic(Id), _, _}) ->
            case dirty_read_props(Id) of
                undefined ->
                    false;
                #{} = Props ->
                    {true, Props}
            end
        end,
        TTVs
    ),
    {Batch, it2cursor(It)}.

%%================================================================================
%% behavior callbacks
%%================================================================================

pmap_encode_key(?top_properties, Key, _Val) ->
    Key;
pmap_encode_key(?top_generations, Key, _Val) ->
    Key;
pmap_encode_key(?top_iterators, Key, _Val) ->
    {{Shard, Gen}, Stream} = Key,
    {ok, StreamBin} = emqx_ds:stream_to_binary(messages, Stream),
    'DSSharedSub':encode('StreamId', #'StreamId'{
        shard = Shard,
        generation = Gen,
        stream = StreamBin
    });
pmap_encode_key(_, Key, _Val) ->
    term_to_binary(Key).

pmap_decode_key(?top_properties, Key) ->
    Key;
pmap_decode_key(?top_generations, Key) ->
    Key;
pmap_decode_key(?top_iterators, Key) ->
    #'StreamId'{
        shard = Shard,
        generation = Gen,
        stream = StreamBin
    } = 'DSSharedSub':decode('StreamId', Key),
    {ok, Stream} = emqx_ds:binary_to_stream(messages, StreamBin),
    {{Shard, Gen}, Stream};
pmap_decode_key(_, KeyBin) ->
    binary_to_term(KeyBin).

pmap_encode_val(?top_iterators, _, Val) ->
    #'StreamState'{iterator = It} = Val,
    {ok, ItBin} = emqx_ds:iterator_to_binary(?PERSISTENT_MESSAGE_DB, It),
    'DSSharedSub':encode('StreamState', Val#'StreamState'{
        iterator = ItBin
    });
pmap_encode_val(_, _Key, Val) ->
    term_to_binary(Val).

pmap_decode_val(?top_iterators, _, ValBin) ->
    Val0 = #'StreamState'{iterator = ItBin} = 'DSSharedSub':decode('StreamState', ValBin),
    {ok, It} = emqx_ds:binary_to_iterator(?PERSISTENT_MESSAGE_DB, ItBin),
    Val0#'StreamState'{
        iterator = It
    };
pmap_decode_val(_, _Key, ValBin) ->
    binary_to_term(ValBin).

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

-spec open_tx(id()) -> {ok, t()} | undefined.
open_tx(Id) ->
    case emqx_ds_pmap:tx_guard(Id) of
        undefined ->
            undefined;
        Guard ->
            Rec = #{
                id => Id,
                ?collection_guard => Guard,
                ?collection_dirty => false,
                ?properties => emqx_ds_pmap:tx_restore(?MODULE, ?top_properties, Id),
                ?generations => emqx_ds_pmap:tx_restore(?MODULE, ?top_generations, Id),
                ?iterators => emqx_ds_pmap:tx_restore(?MODULE, ?top_iterators, Id)
            },
            {ok, Rec}
    end.

-spec trans_opts(id()) -> emqx_ds:transaction_opts().
trans_opts(Id) ->
    #{
        db => ?DB,
        shard => emqx_ds:shard_of(?DB, Id),
        generation => 1
    }.

it2cursor('$end_of_table') ->
    <<>>;
it2cursor(It) ->
    {ok, Bin} = emqx_ds:multi_iterator_to_binary(It),
    emqx_base62:encode(Bin).

cursor2it(<<>>) ->
    '$end_of_table';
cursor2it(Bin) ->
    {ok, It} = emqx_ds:binary_to_multi_iterator(emqx_base62:decode(Bin)),
    It.

-spec multi_iter_opts() -> emqx_ds:multi_iter_opts().
multi_iter_opts() ->
    #{
        db => ?DB,
        generation => 1
    }.
