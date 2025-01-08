%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_SCHEMA_REGISTRY_HRL).
-define(EMQX_SCHEMA_REGISTRY_HRL, true).

-define(CONF_KEY_ROOT, schema_registry).
-define(CONF_KEY_ROOT_BIN, <<"schema_registry">>).
-define(CONF_KEY_PATH, [?CONF_KEY_ROOT]).

%% Note: this has the `_ee_' segment for backwards compatibility.
-define(SCHEMA_REGISTRY_SHARD, emqx_ee_schema_registry_shard).
-define(PROTOBUF_CACHE_TAB, emqx_ee_schema_registry_protobuf_cache_tab).

%% ETS table for serde build results.
-define(SERDE_TAB, emqx_schema_registry_serde_tab).

-define(EMQX_SCHEMA_REGISTRY_SPARKPLUGB_SCHEMA_NAME,
    <<"__CiYAWBja87PleCyKZ58h__SparkPlug_B_BUILT-IN">>
).
-type schema_name() :: binary().
-type schema_source() :: binary().

-type serde_args() :: list().

-type encoded_data() :: iodata().
-type decoded_data() :: map().

-type serde_type() :: avro | protobuf | json.
-type serde_opts() :: map().

-record(serde, {
    name :: schema_name(),
    type :: serde_type(),
    eval_context :: term(),
    %% for future use
    extra = #{}
}).
-type serde() :: #serde{}.

-record(protobuf_cache, {
    fingerprint,
    module,
    module_binary
}).
-type protobuf_cache() :: #protobuf_cache{
    fingerprint :: binary(),
    module :: module(),
    module_binary :: binary()
}.

-endif.
