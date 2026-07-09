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

-define(STATUS_OK, <<"OK">>).
-define(STATUS_BAD_REQUEST, <<"BAD_REQUEST">>).
-define(STATUS_CONFLICT, <<"CONFLICT">>).
-define(STATUS_TOO_MANY_REQUESTS, <<"TOO_MANY_REQUESTS">>).
-define(STATUS_UNAVAILABLE, <<"UNAVAILABLE">>).
-define(STATUS_INTERNAL_ERROR, <<"INTERNAL_ERROR">>).
-define(STATUS_TIMEOUT, <<"TIMEOUT">>).
-define(STATUS_OFFLINE, <<"OFFLINE">>).

-endif.
