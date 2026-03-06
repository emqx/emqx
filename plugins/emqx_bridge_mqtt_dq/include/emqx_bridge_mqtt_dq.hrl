%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_BRIDGE_MQTT_DQ_HRL).
-define(EMQX_BRIDGE_MQTT_DQ_HRL, true).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(LOG(Level, Data),
    ?SLOG(Level, maps:merge(#{tag => "MQTT_DQ", domain => [mqtt_dq]}, (Data)))
).

-endif.
