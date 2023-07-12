%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_SCHEMA_REGISTRY_HRL).
-define(EMQX_SCHEMA_REGISTRY_HRL, true).

-define(CONF_KEY_ROOT, schema_registry).
-define(CONF_KEY_PATH, [?CONF_KEY_ROOT]).

%% Note: this has the `_ee_' segment for backwards compatibility.
-define(SCHEMA_REGISTRY_SHARD, emqx_ee_schema_registry_shard).
-define(SERDE_TAB, emqx_ee_schema_registry_serde_tab).
-define(PROTOBUF_CACHE_TAB, emqx_ee_schema_registry_protobuf_cache_tab).

-define(EMQX_SCHEMA_REGISTRY_SPARKPLUGB_SCHEMA_NAME,
    <<"__CiYAWBja87PleCyKZ58h__SparkPlug_B_BUILT-IN">>
).
-type schema_name() :: binary().
-type schema_source() :: binary().

-type encoded_data() :: iodata().
-type decoded_data() :: map().
-type serializer() ::
    fun((decoded_data()) -> encoded_data())
    | fun((decoded_data(), term()) -> encoded_data()).
-type deserializer() ::
    fun((encoded_data()) -> decoded_data())
    | fun((encoded_data(), term()) -> decoded_data()).
-type destructor() :: fun(() -> ok).
-type serde_type() :: avro.
-type serde_opts() :: map().

-record(serde, {
    name :: schema_name(),
    serializer :: serializer(),
    deserializer :: deserializer(),
    destructor :: destructor()
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

-type serde_map() :: #{
    name := schema_name(),
    serializer := serializer(),
    deserializer := deserializer(),
    destructor := destructor()
}.

-endif.
