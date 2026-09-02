-module(emqx_bridge_nats_connector_schema).
-behaviour(hocon_schema).
-behaviour(emqx_connector_examples).
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include("emqx_bridge_nats.hrl").
-export([namespace/0, roots/0, fields/1, desc/1, connector_examples/1]).

namespace() -> "connector_nats".
roots() -> [].
fields("get_connector") ->
    emqx_connector_schema:api_fields("get_connector", ?CONNECTOR_TYPE, fields(connector_config));
fields("put_connector") ->
    emqx_connector_schema:api_fields("put_connector", ?CONNECTOR_TYPE, fields(connector_config));
fields("post_connector") ->
    emqx_connector_schema:api_fields("post_connector", ?CONNECTOR_TYPE, fields(connector_config));
fields("config_connector") ->
    emqx_connector_schema:common_fields() ++ fields(connector_config);
fields(connector_config) ->
    [
        {servers,
            emqx_schema:servers_sc(
                #{default => <<"127.0.0.1:4222">>, desc => ?DESC("servers")},
                #{default_port => 4222}
            )},
        {pool_size, hoconsc:mk(pos_integer(), #{default => 8, desc => ?DESC("pool_size")})},
        {connect_timeout,
            hoconsc:mk(emqx_schema:timeout_duration_ms(), #{
                default => <<"5s">>, desc => ?DESC("connect_timeout")
            })},
        {username,
            hoconsc:mk(binary(), #{required => false, default => <<>>, desc => ?DESC("username")})},
        {password,
            emqx_schema_secret:mk(#{required => false, default => <<>>, desc => ?DESC("password")})},
        {token,
            emqx_schema_secret:mk(#{required => false, default => <<>>, desc => ?DESC("token")})},
        {auth_type,
            hoconsc:mk(
                hoconsc:enum([none, user_password, token, nkey, jwt, creds_file]),
                #{default => none, desc => ?DESC("auth_type")}
            )},
        {public_key,
            hoconsc:mk(binary(), #{required => false, default => <<>>, desc => ?DESC("public_key")})},
        {jwt, emqx_schema_secret:mk(#{required => false, default => <<>>, desc => ?DESC("jwt")})},
        {nkey_seed,
            emqx_schema_secret:mk(#{required => false, default => <<>>, desc => ?DESC("nkey_seed")})},
        {credentials_file,
            hoconsc:mk(binary(), #{
                required => false, default => <<>>, desc => ?DESC("credentials_file")
            })}
    ] ++ emqx_connector_schema:resource_opts() ++ emqx_connector_schema_lib:ssl_fields().
desc("config_connector") -> ?DESC("config_connector");
desc(_) -> undefined.

connector_examples(Method) ->
    [#{?CONNECTOR_TYPE_BIN => #{summary => <<"NATS Connector">>, value => example(Method)}}].
example(post) ->
    maps:merge(example(put), #{type => ?CONNECTOR_TYPE_BIN, name => <<"nats_connector">>});
example(get) ->
    maps:merge(example(put), #{status => <<"connected">>, node_status => []});
example(put) ->
    #{
        enable => true,
        description => <<"NATS connector">>,
        servers => <<"127.0.0.1:4222">>,
        pool_size => 8,
        connect_timeout => <<"5s">>,
        username => <<>>,
        password => <<>>,
        token => <<>>,
        auth_type => none,
        public_key => <<>>,
        jwt => <<>>,
        nkey_seed => <<>>,
        credentials_file => <<>>,
        ssl => #{enable => false},
        resource_opts => #{health_check_interval => <<"30s">>}
    }.
