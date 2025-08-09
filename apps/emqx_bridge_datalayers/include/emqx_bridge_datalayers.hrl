%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-define(DATALAYERS_DEFAULT_ARROW_PORT, 8360).
-define(DATALAYERS_DEFAULT_HTTP_PORT, 8361).

%% datalayers servers don't need parse
-define(DATALAYERS_HOST_ARROW_OPTIONS, #{
    default_port => ?DATALAYERS_DEFAULT_ARROW_PORT
}).

-define(DATALAYERS_HOST_HTTP_OPTIONS, #{
    default_port => ?DATALAYERS_DEFAULT_HTTP_PORT
}).

-define(DATALAYERS_DRIVER_TYPE_INFLUX, influxdb_v1).
-define(DATALAYERS_DRIVER_TYPE_ARROW_FLIGHT, arrow_flight).
