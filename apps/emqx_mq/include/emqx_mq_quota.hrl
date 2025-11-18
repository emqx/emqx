%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_MQ_QUOTA_HRL).
-define(EMQX_MQ_QUOTA_HRL, true).

-define(QUOTA_INDEX_UPDATE(TIMESTAMP_US, BYTE_UPDATE, COUNT_UPDATE),
    {TIMESTAMP_US, BYTE_UPDATE, COUNT_UPDATE}
).

-define(QUOTA_INDEX_TS, 1).

-define(DEFAULT_QUOTA_BUFFER_POOL_SIZE, 10).
-define(DEFAULT_QUOTA_BUFFER_MAX_SIZE, 100).
-define(DEFAULT_QUOTA_BUFFER_FLUSH_INTERVAL, 1000).

-endif.
