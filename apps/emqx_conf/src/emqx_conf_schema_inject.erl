%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_conf_schema_inject).

-export([schemas/0]).

schemas() ->
    mria() ++
        cluster_linking() ++
        authn() ++
        authz() ++
        bridges() ++
        customized().

mria() ->
    [emqx_enterprise_schema].

cluster_linking() ->
    [emqx_cluster_link_schema].

authn() ->
    [{emqx_authn_schema, authn_mods()}].

authn_mods() ->
    [
        emqx_authn_mnesia_schema,
        emqx_authn_mysql_schema,
        emqx_authn_postgresql_schema,
        emqx_authn_mongodb_schema,
        emqx_authn_redis_schema,
        emqx_authn_http_schema,
        emqx_authn_jwt_schema,
        emqx_authn_scram_mnesia_schema,
        emqx_authn_ldap_schema,
        emqx_gcp_device_authn_schema,
        emqx_authn_scram_restapi_schema,
        emqx_authn_kerberos_schema,
        emqx_authn_cinfo_schema
    ].

authz() ->
    [{emqx_authz_schema, authz_mods()}].

authz_mods() ->
    [
        emqx_authz_file_schema,
        emqx_authz_mnesia_schema,
        emqx_authz_http_schema,
        emqx_authz_redis_schema,
        emqx_authz_mysql_schema,
        emqx_authz_postgresql_schema,
        emqx_authz_mongodb_schema,
        emqx_authz_ldap_schema
    ].

bridges() ->
    [
        emqx_bridge_disk_log_connector_schema,
        emqx_bridge_mqtt_connector_schema,
        emqx_bridge_snowflake_aggregated_connector_schema
    ].

%% Add more schemas here.
customized() ->
    [].
