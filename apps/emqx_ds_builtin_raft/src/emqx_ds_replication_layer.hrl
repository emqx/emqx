%%--------------------------------------------------------------------
%% Copyright (c) 2022, 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-ifndef(EMQX_DS_REPLICATION_LAYER_HRL).
-define(EMQX_DS_REPLICATION_LAYER_HRL, true).

%% # "Record" integer keys.  We use maps with integer keys to avoid persisting and sending
%% records over the wire.

%% tags:
-define(STREAM, 1).
-define(IT, 2).
-define(BATCH, 3).
-define(DELETE_IT, 4).

%% keys:
-define(tag, 1).
-define(shard, 2).
-define(enc, 3).

%% ?BATCH
-define(batch_operations, 2).
-define(batch_preconditions, 4).
-define(timestamp, 3).

%% add_generation / update_config
-define(config, 2).
-define(since, 3).

%% drop_generation
-define(generation, 2).

%% custom events
-define(payload, 2).
-define(now, 3).

-record(sub_handle, {
    shard, server, ref
}).

-endif.
