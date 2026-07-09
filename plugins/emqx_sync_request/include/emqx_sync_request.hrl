%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_SYNC_REQUEST_HRL).
-define(EMQX_SYNC_REQUEST_HRL, true).

-define(PLUGIN_NAME, emqx_sync_request).
-define(SERVICE, emqx_sync_request).

-define(REQ_TAB, emqx_sync_request_requests).
-define(PENDING_TAB, emqx_sync_request_pending).
-define(PENDING_BY_REQ_TAB, emqx_sync_request_pending_by_req).

-define(HEADER, emqx_sync_request).

-define(DEFAULT_TIMEOUT, <<"10s">>).
-define(DEFAULT_MAX_TIMEOUT, <<"60s">>).
-define(DEFAULT_MAX_INFLIGHT, 10000).
-define(DEFAULT_MAX_PAYLOAD_SIZE, <<"64KB">>).

-define(CODE_OK, <<"OK">>).
-define(CODE_BAD_REQUEST, <<"BAD_REQUEST">>).
-define(CODE_NO_SUBSCRIBERS, <<"NO_SUBSCRIBERS">>).
-define(CODE_CONFLICT, <<"CONFLICT">>).
-define(CODE_TOO_MANY_REQUESTS, <<"TOO_MANY_REQUESTS">>).
-define(CODE_SERVICE_UNAVAILABLE, <<"SERVICE_UNAVAILABLE">>).
-define(CODE_INTERNAL_ERROR, <<"INTERNAL_ERROR">>).
-define(CODE_TIMEOUT, <<"TIMEOUT">>).

-endif.
