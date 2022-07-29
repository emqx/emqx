%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ee_connector_influxdb).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").

-import(hoconsc, [mk/2, enum/1, ref/2]).

-behaviour(emqx_resource).

-define(PUT_FIELDS_FILTER, fun({Name, _}) -> not lists:member(Name, [type, name]) end).

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

on_stop(_InstId, _State) ->
    ok.

on_query(_InstId, {send_message, _Data}, _AfterQuery, _State) ->
    ok.

on_get_status(_InstId, _State) ->
    % connected;
    disconnected.

%% -------------------------------------------------------------------------------------------------
%% schema

fields("put_udp") ->
    lists:filter(?PUT_FIELDS_FILTER, fields(udp));
fields("put_api_v1") ->
    lists:filter(?PUT_FIELDS_FILTER, fields(api_v1));
fields("put_api_v2") ->
    lists:filter(?PUT_FIELDS_FILTER, fields(api_v2));
fields("get_udp") ->
    fields(udp);
fields("get_api_v1") ->
    fields(api_v1);
fields("get_api_v2") ->
    fields(api_v2);
fields("post_udp") ->
    fields(udp);
fields("post_api_v1") ->
    fields(api_v1);
fields("post_api_v2") ->
    fields(api_v2);
fields(basic) ->
    [
        {host,
            mk(binary(), #{required => true, default => <<"120.0.0.1">>, desc => ?DESC("host")})},
        {port, mk(pos_integer(), #{required => true, default => 8086, desc => ?DESC("port")})},
        {precision,
            mk(enum([ns, us, ms, s, m, h]), #{
                required => false, default => ms, desc => ?DESC("precision")
            })},
        {pool_size, mk(pos_integer(), #{required => true, desc => ?DESC("pool_size")})},
        {type, mk(enum([influxdb]), #{required => true, desc => ?DESC("type")})},
        {name, mk(binary(), #{required => true, desc => ?DESC("name")})}
    ];
fields(udp) ->
    [
        {protocol, mk(enum([udp]), #{required => true, desc => ?DESC("protocol_udp")})}
    ] ++ fields(basic);
fields(api_v1) ->
    [
        {protocol, mk(enum([api_v1]), #{required => true, desc => ?DESC("protocol_api_v1")})},
        {database, mk(binary(), #{required => true, desc => ?DESC("database")})},
        {username, mk(binary(), #{required => true, desc => ?DESC("username")})},
        {password, mk(binary(), #{required => true, desc => ?DESC("password")})}
    ] ++ emqx_connector_schema_lib:ssl_fields() ++ fields(basic);
fields(api_v2) ->
    [
        {protocol, mk(enum([api_v2]), #{required => true, desc => ?DESC("protocol_api_v2")})},
        {bucket, mk(binary(), #{required => true, desc => ?DESC("bucket")})},
        {org, mk(binary(), #{required => true, desc => ?DESC("org")})},
        {token, mk(binary(), #{required => true, desc => ?DESC("token")})}
    ] ++ emqx_connector_schema_lib:ssl_fields() ++ fields(basic).

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
    ConnectorName = list_to_binary(io_lib:format("~p_connector", [Protocol])),
    maps:merge(values(Protocol, put), #{type => influxdb, name => ConnectorName});
values(udp, put) ->
    #{
        protocol => udp,
        host => <<"127.0.0.1">>,
        port => 8089,
        precision => ms,
        pool_size => 8
    };
values(api_v1, put) ->
    #{
        protocol => api_v1,
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
        protocol => api_v2,
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
    io:format("InstId ~p~n", [InstId]),
    client_config(InstId, Config).

% ClientConfig = client_config(InstId, Config),
% case influxdb:start_client(ClientConfig) of
%     {ok, Client} ->
%         true = influxdb:is_alive(Client),
%         maybe_pool_size(Client, Params);
%     {error, {already_started, Client0}} ->
%         _ = influxdb:stop_client(Client0),
%         {ok, Client} = influxdb:start_client(Options),
%         true = influxdb:is_alive(Client),
%         maybe_pool_size(Client, Params);
%     {error, Reason} ->
%         logger:log(error, "Initiate influxdb failed ~0p", [Reason]),
%         error({start_pool_failed, ResId})
% end.

client_config(
    _InstId,
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
        {pool, atom_pname_todo},
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
