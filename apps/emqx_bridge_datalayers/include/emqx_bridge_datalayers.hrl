%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-define(DATALAYERS_DEFAULT_PORT, 8361).

%% datalayers servers don't need parse
-define(DATALAYERS_HOST_OPTIONS, #{
    default_port => ?DATALAYERS_DEFAULT_PORT
}).

-define(DATALAYERS_DRIVER_TYPE_INFLUX, influxdb_v1).
-define(DATALAYERS_DRIVER_TYPE_ARROW_FLIGHT, arrow_flight).
