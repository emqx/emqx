%%--------------------------------------------------------------------
%% Copyright (c) 2021-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-define(CONFIG_FORMAT_AVRO, config_format_avro).
-define(CONFIG_FORMAT_MAP, config_format_map).

-type schema_name() :: binary().
-type avsc_path() :: string().

-type encoded_data() :: iodata().
-type decoded_data() :: map().

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
