%%--------------------------------------------------------------------
%% Copyright (c) 2022-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-ifndef(EMQX_CONF_HRL).
-define(EMQX_CONF_HRL, true).

-define(CLUSTER_RPC_SHARD, emqx_cluster_rpc_shard).

-define(CLUSTER_MFA, cluster_rpc_mfa).
-define(CLUSTER_COMMIT, cluster_rpc_commit).
-define(DEFAULT_INIT_TXN_ID, -1).

-record(cluster_rpc_mfa, {
    tnx_id :: pos_integer(),
    mfa :: {module(), atom(), [any()]},
    created_at :: calendar:datetime(),
    initiator :: node()
}).

-record(cluster_rpc_commit, {
    node :: node(),
    tnx_id :: pos_integer() | '$1'
}).

-define(READONLY_KEYS, [cluster, rpc, node]).

-define(CE_AUTHZ_SOURCE_SCHEMA_MODS, [
    emqx_authz_file_schema,
    emqx_authz_mnesia_schema,
    emqx_authz_http_schema,
    emqx_authz_redis_schema,
    emqx_authz_mysql_schema,
    emqx_authz_postgresql_schema,
    emqx_authz_mongodb_schema,
    emqx_authz_ldap_schema
]).

-define(EE_AUTHZ_SOURCE_SCHEMA_MODS, []).

-define(CE_AUTHN_PROVIDER_SCHEMA_MODS, [
    emqx_authn_mnesia_schema,
    emqx_authn_mysql_schema,
    emqx_authn_postgresql_schema,
    emqx_authn_mongodb_schema,
    emqx_authn_redis_schema,
    emqx_authn_http_schema,
    emqx_authn_jwt_schema,
    emqx_authn_scram_mnesia_schema,
    emqx_authn_ldap_schema
]).

-define(EE_AUTHN_PROVIDER_SCHEMA_MODS, [
    emqx_gcp_device_authn_schema
]).

-if(?EMQX_RELEASE_EDITION == ee).

-define(AUTHZ_SOURCE_SCHEMA_MODS, ?CE_AUTHZ_SOURCE_SCHEMA_MODS ++ ?EE_AUTHZ_SOURCE_SCHEMA_MODS).
-define(AUTHN_PROVIDER_SCHEMA_MODS,
    (?CE_AUTHN_PROVIDER_SCHEMA_MODS ++ ?EE_AUTHN_PROVIDER_SCHEMA_MODS)
).

-else.

-define(AUTHZ_SOURCE_SCHEMA_MODS, ?CE_AUTHZ_SOURCE_SCHEMA_MODS).
-define(AUTHN_PROVIDER_SCHEMA_MODS, ?CE_AUTHN_PROVIDER_SCHEMA_MODS).

-endif.

-endif.
