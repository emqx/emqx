%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_PLUGINS_HRL).
-define(EMQX_PLUGINS_HRL, true).

-define(CONF_ROOT, plugins).

-define(PLUGIN_SERDE_TAB, emqx_plugins_schema_serde_tab).

-define(CONFIG_FORMAT_BIN, config_format_bin).
-define(CONFIG_FORMAT_MAP, config_format_map).

-define(plugin_conf_not_found, plugin_conf_not_found).
-define(plugin_without_config_schema, plugin_without_config_schema).
-define(fresh_install, fresh_install).
-define(normal, normal).

-type schema_name() :: binary().
-type avsc_path() :: string().

-type encoded_data() :: iodata().
-type decoded_data() :: map().

%% "my_plugin-0.1.0"
-type name_vsn() :: binary() | string().
%% the parse result of the JSON info file
-type schema_json_map() :: map().
-type i18n_json_map() :: map().
-type plugin_config_map() :: map().
-type position() :: no_move | front | rear | {before, name_vsn()} | {behind, name_vsn()}.

-endif.
