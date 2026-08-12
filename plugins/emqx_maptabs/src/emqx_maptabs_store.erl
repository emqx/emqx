%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Durable storage for mapping tables: a mria table with one record per
%% mapping table, replicated to every node and recovered by mria after
%% a node restart. The per-node ETS caches are derived from it (see
%% emqx_maptabs_server).
-module(emqx_maptabs_store).

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
    ok = mria:create_table(?MAPTABS_TAB, [
        {type, set},
        {rlog_shard, ?MAPTABS_SHARD},
        {storage, disc_copies},
        {record_name, maptab},
        {attributes, record_info(fields, maptab)}
    ]),
    ok = mria:wait_for_tables([?MAPTABS_TAB]).

%% The caller is responsible for validating the JSON before storing it.
-spec put(binary(), binary()) -> ok | {error, term()}.
put(Name, Json) ->
    Rec = #maptab{
        name = Name,
        json = Json,
        version = emqx_maptabs_loader:version(Json),
        updated_at = erlang:system_time(second)
    },
    transaction(fun() -> mnesia:write(?MAPTABS_TAB, Rec, write) end).

-spec delete(binary()) -> ok | {error, term()}.
delete(Name) ->
    transaction(fun() -> mnesia:delete(?MAPTABS_TAB, Name, write) end).

-spec get(binary()) -> {ok, #maptab{}} | {error, not_found}.
get(Name) ->
    case mnesia:dirty_read(?MAPTABS_TAB, Name) of
        [Rec] -> {ok, Rec};
        [] -> {error, not_found}
    end.

%% `{Name, Version}' of every stored table, without loading the JSON
%% payloads.
-spec versions() -> #{binary() => binary()}.
versions() ->
    MS = [{#maptab{name = '$1', version = '$2', _ = '_'}, [], [{{'$1', '$2'}}]}],
    maps:from_list(mnesia:dirty_select(?MAPTABS_TAB, MS)).

-spec count() -> non_neg_integer().
count() ->
    mnesia:table_info(?MAPTABS_TAB, size).

transaction(Fun) ->
    case mria:transaction(?MAPTABS_SHARD, Fun) of
        {atomic, _} -> ok;
        {aborted, Reason} -> {error, #{reason => storage_transaction_failed, detail => Reason}}
    end.
