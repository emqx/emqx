%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_BRIDGE_IOTDB_HRL).
-define(EMQX_BRIDGE_IOTDB_HRL, true).

-define(VSN_1_3_X, 'v1.3.x').
-define(VSN_1_2_X, 'v1.2.x').
-define(VSN_1_1_X, 'v1.1.x').
-define(VSN_1_0_X, 'v1.0.x').
-define(VSN_0_13_X, 'v0.13.x').

-define(THRIFT_HOST_OPTIONS, #{
    default_port => 6667
}).

-define(PROTOCOL_V1, 'protocol_v1').
-define(PROTOCOL_V2, 'protocol_v2').
-define(PROTOCOL_V3, 'protocol_v3').

-define(THRIFT_NOT_SUPPORT_ASYNC_MSG, <<"The Thrift backend does not support asynchronous calls">>).

-type driver() :: resetapi | thrift.

-endif.
