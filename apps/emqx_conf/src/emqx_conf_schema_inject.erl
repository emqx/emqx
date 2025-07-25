%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_conf_schema_inject).

-export([schemas/0]).

schemas() ->
    schemas(emqx_release:edition()).

schemas(Edition) ->
    mria(Edition) ++
        cluster_linking(Edition) ++
        authn(Edition) ++
        authz() ++
        shared_subs(Edition) ++
        bridges(Edition) ++
        namespaced_root_keys() ++
        customized(Edition).

mria(ee) ->
    [emqx_enterprise_schema].

cluster_linking(ee) ->
    [emqx_cluster_link_schema].

authn(Edition) ->
    [{emqx_authn_schema, authn_mods(Edition)}].

authn_mods(ee) ->
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

shared_subs(ee) ->
    [emqx_ds_shared_sub_schema].

bridges(ee) ->
    [
        emqx_bridge_disk_log_connector_schema,
        emqx_bridge_mqtt_connector_schema,
        emqx_bridge_snowflake_connector_schema
    ].

namespaced_root_keys() ->
    [
        emqx_connector_schema,
        emqx_bridge_v2_schema,
        emqx_rule_engine_schema
    ].

%% Add more schemas here.
customized(_) ->
    [].
