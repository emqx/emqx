%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_app).

-export([start/2]).

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
            {storage, rocksdb_copies},
            {record_name, session},
            {attributes, record_info(fields, session)}
        ]
    ).
