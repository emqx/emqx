%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(__EMQX_BRIDGE_S3TABLES_HRL__).
-define(__EMQX_BRIDGE_S3TABLES_HRL__, true).

-define(CONNECTOR_TYPE, s3tables).
-define(CONNECTOR_TYPE_BIN, <<"s3tables">>).

-define(ACTION_TYPE, s3tables).
-define(ACTION_TYPE_BIN, <<"s3tables">>).

-define(ROOT_AVRO_TYPE, <<"__root">>).

-record(unpartitioned, {}).
-record(partitioned, {fields}).

-define(action_res_id, action_res_id).
-define(avro_schema, avro_schema).
-define(base_path, base_path).
-define(bucket, bucket).
-define(client, client).
-define(data_file_key, data_file_key).
-define(data_size, data_size).
-define(get_fn, get_fn).
-define(iceberg_schema, iceberg_schema).
-define(id, id).
-define(inner_transfer, inner_transfer).
-define(loaded_table, loaded_table).
-define(location_client, location_client).
-define(n_attempt, n_attempt).
-define(name, name).
-define(namespace, namespace).
-define(num_records, num_records).
-define(partition_spec, partition_spec).
-define(partition_spec_id, partition_spec_id).
-define(raw, raw).
-define(result_type, result_type).
-define(s3_client, s3_client).
-define(s3_client_config, s3_client_config).
-define(s3_transfer_state, s3_transfer_state).
-define(schema_id, schema_id).
-define(seq_num, seq_num).
-define(source_type, source_type).
-define(table, table).
-define(table_uuid, table_uuid).
-define(transform_fn, transform_fn).
-define(write_uuid, write_uuid).

-define(MANIFEST_ENTRY_PT_KEY, {emqx_bridge_s3tables, manifest_entry}).
-define(MANIFEST_FILE_PT_KEY, {emqx_bridge_s3tables, manifest_file}).

%% END ifndef(__EMQX_BRIDGE_S3TABLES_HRL__)
-endif.
