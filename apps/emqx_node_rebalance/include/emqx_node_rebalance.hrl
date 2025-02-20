%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-define(DEFAULT_CONN_EVICT_RATE, 500).
-define(DEFAULT_SESS_EVICT_RATE, 500).

%% sec
-define(DEFAULT_WAIT_HEALTH_CHECK, 60).
%% sec
-define(DEFAULT_WAIT_TAKEOVER, 60).

-define(DEFAULT_ABS_CONN_THRESHOLD, 1000).
-define(DEFAULT_ABS_SESS_THRESHOLD, 1000).

-define(DEFAULT_REL_CONN_THRESHOLD, 1.1).
-define(DEFAULT_REL_SESS_THRESHOLD, 1.1).

-define(EVICT_INTERVAL, 1000).

-define(DEFAULT_CONN_EVICT_RPC_TIMEOUT, 15000).
-define(DEFAULT_SESS_EVICT_RPC_TIMEOUT, 15000).

-define(EVACUATION_FILENAME, <<".evacuation">>).
