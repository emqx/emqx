%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-ifndef(EMQX_BRIDGE_SYSKEEPER).
-define(EMQX_BRIDGE_SYSKEEPER, true).

-define(TYPE_HANDSHAKE, 0).
-define(TYPE_FORWARD, 1).
-define(TYPE_HEARTBEAT, 2).

-type packet_type() :: handshake | forward | heartbeat.
-type packet_data() :: none | binary() | [binary()].
-type packet_type_val() :: ?TYPE_HANDSHAKE..?TYPE_HEARTBEAT.

-endif.
