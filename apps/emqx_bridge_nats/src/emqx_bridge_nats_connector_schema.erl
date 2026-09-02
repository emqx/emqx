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
        {authentication,
            hoconsc:mk(
                hoconsc:union(fun authentication_selector/1),
                #{default => none, desc => ?DESC("authentication")}
            )},
        {tls_handshake,
            hoconsc:mk(
                hoconsc:enum([starttls, first]),
                #{default => starttls, desc => ?DESC("tls_handshake")}
            )}
    ] ++ emqx_connector_schema:resource_opts() ++ emqx_connector_schema_lib:ssl_fields();
fields(auth_user_password) ->
    [
        {mechanism, hoconsc:mk(user_password, #{required => true, desc => ?DESC("mechanism")})},
        {username, hoconsc:mk(binary(), #{required => true, desc => ?DESC("username")})},
        {password, emqx_schema_secret:mk(#{required => true, desc => ?DESC("password")})}
    ];
fields(auth_token) ->
    [
        {mechanism, hoconsc:mk(token, #{required => true, desc => ?DESC("mechanism")})},
        {token, emqx_schema_secret:mk(#{required => true, desc => ?DESC("token")})}
    ];
fields(auth_nkey) ->
    [
        {mechanism, hoconsc:mk(nkey, #{required => true, desc => ?DESC("mechanism")})},
        {public_key,
            hoconsc:mk(binary(), #{required => false, default => <<>>, desc => ?DESC("public_key")})},
        {nkey_seed, emqx_schema_secret:mk(#{required => true, desc => ?DESC("nkey_seed")})}
    ];
fields(auth_jwt) ->
    [
        {mechanism, hoconsc:mk(jwt, #{required => true, desc => ?DESC("mechanism")})},
        {public_key,
            hoconsc:mk(binary(), #{required => false, default => <<>>, desc => ?DESC("public_key")})},
        {jwt, emqx_schema_secret:mk(#{required => true, desc => ?DESC("jwt")})},
        {nkey_seed, emqx_schema_secret:mk(#{required => true, desc => ?DESC("nkey_seed")})}
    ];
fields(auth_creds_file) ->
    [
        {mechanism, hoconsc:mk(creds_file, #{required => true, desc => ?DESC("mechanism")})},
        {credentials_file,
            hoconsc:mk(binary(), #{required => true, desc => ?DESC("credentials_file")})}
    ].

authentication_selector(all_union_members) ->
    [
        none,
        hoconsc:ref(?MODULE, auth_user_password),
        hoconsc:ref(?MODULE, auth_token),
        hoconsc:ref(?MODULE, auth_nkey),
        hoconsc:ref(?MODULE, auth_jwt),
        hoconsc:ref(?MODULE, auth_creds_file)
    ];
authentication_selector({value, Value}) when is_atom(Value) ->
    authentication_selector({value, atom_to_binary(Value)});
authentication_selector({value, <<"none">>}) ->
    [none];
authentication_selector({value, #{<<"mechanism">> := Mechanism}}) ->
    case emqx_utils_conv:bin(Mechanism) of
        <<"user_password">> ->
            [hoconsc:ref(?MODULE, auth_user_password)];
        <<"token">> ->
            [hoconsc:ref(?MODULE, auth_token)];
        <<"nkey">> ->
            [hoconsc:ref(?MODULE, auth_nkey)];
        <<"jwt">> ->
            [hoconsc:ref(?MODULE, auth_jwt)];
        <<"creds_file">> ->
            [hoconsc:ref(?MODULE, auth_creds_file)];
        _ ->
            throw(#{
                field_name => mechanism,
                expected => "user_password | token | nkey | jwt | creds_file"
            })
    end;
authentication_selector({value, Value}) ->
    throw(#{field_name => authentication, reason => {not_a_map, Value}}).

desc("config_connector") -> ?DESC("config_connector");
desc(auth_user_password) -> ?DESC("auth_user_password");
desc(auth_token) -> ?DESC("auth_token");
desc(auth_nkey) -> ?DESC("auth_nkey");
desc(auth_jwt) -> ?DESC("auth_jwt");
desc(auth_creds_file) -> ?DESC("auth_creds_file");
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
        authentication => none,
        ssl => #{enable => false},
        resource_opts => #{health_check_interval => <<"30s">>}
    }.
