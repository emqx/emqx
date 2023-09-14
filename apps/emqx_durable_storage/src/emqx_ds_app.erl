%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_app).

-dialyzer({nowarn_function, storage/0}).

-export([start/2, storage/0]).

-include("emqx_ds_int.hrl").

start(_Type, _Args) ->
    init_mnesia(),
    emqx_ds_sup:start_link().

init_mnesia() ->
    ok = mria:create_table(
        ?SESSION_TAB,
        [
            {rlog_shard, ?DS_SHARD},
            {type, set},
            {storage, storage()},
            {record_name, session},
            {attributes, record_info(fields, session)}
        ]
    ),
    ok = mria:create_table(
        ?ITERATOR_REF_TAB,
        [
            {rlog_shard, ?DS_SHARD},
            {type, ordered_set},
            {storage, storage()},
            {record_name, iterator_ref},
            {attributes, record_info(fields, iterator_ref)}
        ]
    ),
    ok.

storage() ->
    %% FIXME: This is a temporary workaround to avoid crashes when starting on Windows
    case mria:rocksdb_backend_available() of
        true ->
            rocksdb_copies;
        _ ->
            disc_copies
    end.
