%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(__EMQX_BRIDGE_ICEBERG_HRL__).
-define(__EMQX_BRIDGE_ICEBERG_HRL__, true).

-define(CONNECTOR_TYPE, iceberg).
-define(CONNECTOR_TYPE_BIN, <<"iceberg">>).

-define(ACTION_TYPE, iceberg).
-define(ACTION_TYPE_BIN, <<"iceberg">>).

-record(unpartitioned, {}).
-record(partitioned, {fields}).

-define(avro_schema, avro_schema).
-define(iceberg_schema, iceberg_schema).
-define(loaded_table, loaded_table).
-define(partition_spec, partition_spec).
-define(partition_spec_id, partition_spec_id).

%% END ifndef(__EMQX_BRIDGE_ICEBERG_HRL__)
-endif.
