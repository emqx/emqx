%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
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
-type plugin_info() :: map().
-type schema_json_map() :: map().
-type i18n_json_map() :: map().
-type raw_plugin_config_content() :: binary().
-type plugin_config_map() :: map().
-type position() :: no_move | front | rear | {before, name_vsn()} | {behind, name_vsn()}.

-record(plugin_schema_serde, {
    name :: schema_name(),
    eval_context :: term(),
    %% TODO: fields to mark schema import status
    %% scheam_imported :: boolean(),
    %% for future use
    extra = []
}).
-type plugin_schema_serde() :: #plugin_schema_serde{}.

-endif.
