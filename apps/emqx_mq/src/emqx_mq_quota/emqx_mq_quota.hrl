%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_MQ_QUOTA_HRL).
-define(EMQX_MQ_QUOTA_HRL, true).

-define(QUOTA_INDEX_UPDATE(TIMESTAMP_US, BYTE_UPDATE, COUNT_UPDATE),
    {TIMESTAMP_US, BYTE_UPDATE, COUNT_UPDATE}
).

-endif.
