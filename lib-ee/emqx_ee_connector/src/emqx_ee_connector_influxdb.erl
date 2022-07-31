%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ee_connector_influxdb).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").

-import(hoconsc, [mk/2, enum/1, ref/2]).

-behaviour(emqx_resource).

%% callbacks of behaviour emqx_resource
-export([
    on_start/2,
    on_stop/2,
    on_query/4,
    on_get_status/2
]).

-export([
    fields/1,
    connector_examples/1
]).

%% -------------------------------------------------------------------------------------------------
%% resource callback

on_start(InstId, Config) ->
    start_client(InstId, Config).

on_stop(_InstId, #{client := Client}) ->
    influxdb:stop_client(Client).

on_query(_InstId, {send_message, _Data}, _AfterQuery, _State) ->
    ok.

on_get_status(_InstId, #{client := Client}) ->
    case influxdb:is_alive(Client) of
        true ->
            connected;
        false ->
            disconnected
    end.

%% -------------------------------------------------------------------------------------------------
%% schema

fields("put_udp") ->
    fields(influxdb_udp);
fields("put_api_v1") ->
    fields(influxdb_api_v1);
fields("put_api_v2") ->
    fields(influxdb_api_v2);
fields("get_udp") ->
    Key = influxdb_udp,
    fields(Key) ++ type_name_field(Key);
fields("get_api_v1") ->
    Key = influxdb_api_v1,
    fields(Key) ++ type_name_field(Key);
fields("get_api_v2") ->
    Key = influxdb_api_v2,
    fields(Key) ++ type_name_field(Key);
fields("post_udp") ->
    Key = influxdb_udp,
    fields(Key) ++ type_name_field(Key);
fields("post_api_v1") ->
    Key = influxdb_api_v1,
    fields(Key) ++ type_name_field(Key);
fields("post_api_v2") ->
    Key = influxdb_api_v2,
    fields(Key) ++ type_name_field(Key);
fields(basic) ->
    [
        {host,
            mk(binary(), #{required => true, default => <<"120.0.0.1">>, desc => ?DESC("host")})},
        {port, mk(pos_integer(), #{required => true, default => 8086, desc => ?DESC("port")})},
        {precision,
            mk(enum([ns, us, ms, s, m, h]), #{
                required => false, default => ms, desc => ?DESC("precision")
            })},
        {pool_size, mk(pos_integer(), #{required => true, desc => ?DESC("pool_size")})}
    ];
fields(influxdb_udp) ->
    fields(basic);
fields(influxdb_api_v1) ->
    [
        {database, mk(binary(), #{required => true, desc => ?DESC("database")})},
        {username, mk(binary(), #{required => true, desc => ?DESC("username")})},
        {password, mk(binary(), #{required => true, desc => ?DESC("password")})}
    ] ++ emqx_connector_schema_lib:ssl_fields() ++ fields(basic);
fields(influxdb_api_v2) ->
    [
        {bucket, mk(binary(), #{required => true, desc => ?DESC("bucket")})},
        {org, mk(binary(), #{required => true, desc => ?DESC("org")})},
        {token, mk(binary(), #{required => true, desc => ?DESC("token")})}
    ] ++ emqx_connector_schema_lib:ssl_fields() ++ fields(basic).

type_name_field(Type) ->
    [
        {type, mk(Type, #{required => true, desc => ?DESC("type")})},
        {name, mk(binary(), #{required => true, desc => ?DESC("name")})}
    ].

connector_examples(Method) ->
    [
        #{
            <<"influxdb_udp">> => #{
                summary => <<"InfluxDB UDP Connector">>,
                value => values(udp, Method)
            }
        },
        #{
            <<"influxdb_api_v1">> => #{
                summary => <<"InfluxDB HTTP API V1 Connector">>,
                value => values(api_v1, Method)
            }
        },
        #{
            <<"influxdb_api_v2">> => #{
                summary => <<"InfluxDB HTTP API V2 Connector">>,
                value => values(api_v2, Method)
            }
        }
    ].

values(Protocol, get) ->
    values(Protocol, post);
values(Protocol, post) ->
    Type = list_to_atom("influxdb_" ++ atom_to_list(Protocol)),
    maps:merge(values(Protocol, put), #{type => Type, name => <<"connector">>});
values(udp, put) ->
    #{
        host => <<"127.0.0.1">>,
        port => 8089,
        precision => ms,
        pool_size => 8
    };
values(api_v1, put) ->
    #{
        host => <<"127.0.0.1">>,
        port => 8086,
        precision => ms,
        pool_size => 8,
        database => <<"my_db">>,
        username => <<"my_user">>,
        password => <<"my_password">>,
        ssl => #{enable => false}
    };
values(api_v2, put) ->
    #{
        host => <<"127.0.0.1">>,
        port => 8086,
        precision => ms,
        pool_size => 8,
        bucket => <<"my_bucket">>,
        org => <<"my_org">>,
        token => <<"my_token">>,
        ssl => #{enable => false}
    }.
%% -------------------------------------------------------------------------------------------------
%% internal functions

start_client(InstId, Config) ->
    ClientConfig = client_config(InstId, Config),
    ?SLOG(info, #{
        msg => "starting influxdb connector",
        connector => InstId,
        config => Config,
        client_config => ClientConfig
    }),
    try
        do_start_client(InstId, ClientConfig, Config)
    catch
        E:R:S ->
            ?SLOG(error, #{
                msg => "start influxdb connector error",
                connector => InstId,
                error => E,
                reason => R,
                stack => S
            }),
            {error, R}
    end.

do_start_client(InstId, ClientConfig, Config = #{egress := #{payload := PayloadBin}}) ->
    case influxdb:start_client(ClientConfig) of
        {ok, Client} ->
            case influxdb:is_alive(Client) of
                true ->
                    Payload = emqx_plugin_libs_rule:preproc_tmpl(PayloadBin),
                    ?SLOG(info, #{
                        msg => "starting influxdb connector success",
                        connector => InstId,
                        client => Client
                    }),
                    #{client => Client, payload => Payload};
                false ->
                    ?SLOG(error, #{
                        msg => "starting influxdb connector failed",
                        connector => InstId,
                        client => Client,
                        reason => "client is not alive"
                    }),
                    {error, influxdb_client_not_alive}
            end;
        {error, {already_started, Client0}} ->
            ?SLOG(info, #{
                msg => "starting influxdb connector,find already started client",
                connector => InstId,
                old_client => Client0
            }),
            _ = influxdb:stop_client(Client0),
            do_start_client(InstId, ClientConfig, Config);
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "starting influxdb connector failed",
                connector => InstId,
                reason => Reason
            }),
            {error, Reason}
    end.

client_config(
    InstId,
    Config = #{
        host := Host,
        port := Port,
        pool_size := PoolSize
    }
) ->
    [
        {host, Host},
        {port, Port},
        {pool_size, PoolSize},
        {pool, binary_to_atom(InstId, utf8)},
        {precision, maps:get(precision, Config, ms)}
    ] ++ protocol_config(Config).

protocol_config(#{
    protocol := udp
}) ->
    [
        {protocol, udp}
    ];
protocol_config(#{
    protocol := api_v1,
    username := Username,
    password := Password,
    database := DB,
    ssl := SSL
}) ->
    [
        {protocol, http},
        {version, v1},
        {username, Username},
        {password, Password},
        {database, DB},
        {ssl, SSL}
    ] ++ ssl_config(SSL);
protocol_config(#{
    protocol := api_v2,
    bucket := Bucket,
    org := Org,
    token := Token,
    ssl := SSL
}) ->
    [
        {protocol, http},
        {version, v2},
        {bucket, Bucket},
        {org, Org},
        {token, Token},
        {ssl, SSL}
    ] ++ ssl_config(SSL).

ssl_config(#{enable := false}) ->
    [
        {https_enabled, false}
    ];
ssl_config(SSL = #{enable := true}) ->
    [
        {https_enabled, true},
        {transport, ssl}
    ] ++ maps:to_list(maps:remove(enable, SSL)).
