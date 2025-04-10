%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-ifndef(RESOURCE_BUFFER_WORKER_INTERNAL_HRL).
-define(RESOURCE_BUFFER_WORKER_INTERNAL_HRL, true).

-define(QUERY(FROM, REQUEST, SENT, EXPIRE_AT, REQ_CONTEXT, TRACE_CTX),
    {query, FROM, REQUEST, SENT, EXPIRE_AT, REQ_CONTEXT, TRACE_CTX}
).

-define(ack, ack).
-define(nack, nack).

-endif.
