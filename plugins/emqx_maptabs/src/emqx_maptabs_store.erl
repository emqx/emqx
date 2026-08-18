%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_maptabs_store).

-moduledoc """
Durable storage for mapping tables, replicated to every node and
recovered by mria after a node restart. Split over two tables (see
the header file): the disc_copies index is the authority on
existence and version, the rocksdb blob table keeps the JSON
payloads on disk. Every write updates both in one transaction. The
per-node ETS caches are derived from this storage (see
`emqx_maptabs_server`).
""".

-include("emqx_maptabs.hrl").

-export([
    create_tables/0,
    put/2,
    delete/1,
    get/1,
    versions/0,
    count/0
]).

-spec create_tables() -> ok.
create_tables() ->
    ok = mria:create_table(?MAPTABS_INDEX_TAB, [
        {type, set},
        {rlog_shard, ?MAPTABS_SHARD},
        {storage, disc_copies},
        {record_name, maptab_index},
        {attributes, record_info(fields, maptab_index)}
    ]),
    ok = mria:create_table(?MAPTABS_TAB, [
        {type, set},
        {rlog_shard, ?MAPTABS_SHARD},
        {storage, rocksdb_copies},
        {record_name, maptab},
        {attributes, record_info(fields, maptab)}
    ]),
    ok = mria:wait_for_tables([?MAPTABS_INDEX_TAB, ?MAPTABS_TAB]).

%% The caller is responsible for validating the JSON before storing it.
-spec put(binary(), binary()) -> ok | {error, term()}.
put(Name, Json) ->
    Index = #maptab_index{
        name = Name,
        version = emqx_maptabs_loader:version(Json),
        updated_at = erlang:system_time(second)
    },
    Blob = #maptab{name = Name, json = Json},
    transaction(fun() ->
        ok = mnesia:write(?MAPTABS_TAB, Blob, write),
        ok = mnesia:write(?MAPTABS_INDEX_TAB, Index, write)
    end).

-spec delete(binary()) -> ok | {error, term()}.
delete(Name) ->
    transaction(fun() ->
        ok = mnesia:delete(?MAPTABS_INDEX_TAB, Name, write),
        ok = mnesia:delete(?MAPTABS_TAB, Name, write)
    end).

%% Only indexed blobs are served: a blob without an index entry does
%% not exist.
-spec get(binary()) -> {ok, #maptab{}} | {error, not_found}.
get(Name) ->
    case mnesia:dirty_read(?MAPTABS_INDEX_TAB, Name) of
        [#maptab_index{}] ->
            case mnesia:dirty_read(?MAPTABS_TAB, Name) of
                [Rec] -> {ok, Rec};
                [] -> {error, not_found}
            end;
        [] ->
            {error, not_found}
    end.

%% `{Name, Version}' of every stored table, from the index only.
-spec versions() -> #{binary() => binary()}.
versions() ->
    MS = [{#maptab_index{name = '$1', version = '$2', _ = '_'}, [], [{{'$1', '$2'}}]}],
    maps:from_list(mnesia:dirty_select(?MAPTABS_INDEX_TAB, MS)).

-spec count() -> non_neg_integer().
count() ->
    mnesia:table_info(?MAPTABS_INDEX_TAB, size).

transaction(Fun) ->
    case mria:transaction(?MAPTABS_SHARD, Fun) of
        {atomic, _} -> ok;
        {aborted, Reason} -> {error, #{reason => storage_transaction_failed, detail => Reason}}
    end.
