%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-ifndef(EMQX_AUTHN_SOURCES_HRL).
-define(EMQX_AUTHN_SOURCES_HRL, true).

-define(PROVIDER_SCHEMA_MODS, [
    emqx_authn_mnesia_schema,
    emqx_authn_mysql_schema,
    emqx_authn_postgresql_schema,
    emqx_authn_mongodb_schema,
    emqx_authn_redis_schema,
    emqx_authn_http_schema,
    emqx_authn_jwt_schema,
    emqx_authn_scram_mnesia_schema
]).

-define(EE_PROVIDER_SCHEMA_MODS, [
    emqx_authn_ldap_schema,
    emqx_authn_ldap_bind_schema,
    emqx_gcp_device_authn_schema
]).

-endif.
