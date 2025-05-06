%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-define(APP, emqx_exproto).

-define(TCP_SOCKOPTS, [
    binary,
    {packet, raw},
    {reuseaddr, true},
    {backlog, 512},
    {nodelay, true}
]).

-define(UDP_SOCKOPTS, []).

%%--------------------------------------------------------------------
%% gRPC result code

-define(RESP_UNKNOWN, 'UNKNOWN').
-define(RESP_SUCCESS, 'SUCCESS').
-define(RESP_CONN_PROCESS_NOT_ALIVE, 'CONN_PROCESS_NOT_ALIVE').
-define(RESP_PARAMS_TYPE_ERROR, 'PARAMS_TYPE_ERROR').
-define(RESP_REQUIRED_PARAMS_MISSED, 'REQUIRED_PARAMS_MISSED').
-define(RESP_PERMISSION_DENY, 'PERMISSION_DENY').
-define(IS_GRPC_RESULT_CODE(C),
    (C =:= ?RESP_SUCCESS orelse
        C =:= ?RESP_CONN_PROCESS_NOT_ALIVE orelse
        C =:= ?RESP_REQUIRED_PARAMS_MISSED orelse
        C =:= ?RESP_PERMISSION_DENY)
).
