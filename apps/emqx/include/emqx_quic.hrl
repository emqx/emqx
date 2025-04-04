%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_QUIC_HRL).
-define(EMQX_QUIC_HRL, true).

%% MQTT Over QUIC Shutdown Error code.
-define(MQTT_QUIC_CONN_NOERROR, 0).
-define(MQTT_QUIC_CONN_ERROR_CTRL_STREAM_DOWN, 1).
-define(MQTT_QUIC_CONN_ERROR_OVERLOADED, 2).

%% Prod SAFE timeout, better than `infinity` or
%% 5000 (gen_server default timeout)
%% Covering the unknown scenarios.
-define(QUIC_SAFE_TIMEOUT, 3000).
-endif.
