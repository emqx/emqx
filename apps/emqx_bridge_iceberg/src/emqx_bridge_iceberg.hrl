%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(__EMQX_BRIDGE_ICEBERG_HRL__).
-define(__EMQX_BRIDGE_ICEBERG_HRL__, true).

-define(CONNECTOR_TYPE, iceberg).
-define(CONNECTOR_TYPE_BIN, <<"iceberg">>).

-define(ACTION_TYPE, iceberg).
-define(ACTION_TYPE_BIN, <<"iceberg">>).

-define(ROOT_AVRO_TYPE, <<"__root">>).

-record(unpartitioned, {}).
-record(partitioned, {fields}).

-define(avro_schema, avro_schema).
-define(get_fn, get_fn).
-define(iceberg_schema, iceberg_schema).
-define(id, id).
-define(loaded_table, loaded_table).
-define(name, name).
-define(num_records, num_records).
-define(partition_spec, partition_spec).
-define(partition_spec_id, partition_spec_id).
-define(raw, raw).
-define(result_type, result_type).
-define(source_type, source_type).
-define(transform_fn, transform_fn).

%% END ifndef(__EMQX_BRIDGE_ICEBERG_HRL__)
-endif.
