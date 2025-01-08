%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-define(HSTREAMDB_DEFAULT_PORT, 6570).

-define(DEFAULT_GRPC_TIMEOUT_RAW, <<"30s">>).
-define(DEFAULT_GRPC_FLUSH_TIMEOUT, 10000).
-define(DEFAULT_MAX_BATCHES, 500).
-define(DEFAULT_BATCH_INTERVAL, 500).
-define(DEFAULT_BATCH_INTERVAL_RAW, <<"500ms">>).
-define(DEFAULT_AGG_POOL_SIZE, 8).
-define(DEFAULT_WRITER_POOL_SIZE, 8).
-define(DEFAULT_GRPC_FLUSH_TIMEOUT_RAW, <<"10s">>).
