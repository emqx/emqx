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
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_batch_query/3,
    on_get_status/2
]).

-export([
    namespace/0,
    fields/1,
    desc/1,
    connector_examples/1
]).

%% -------------------------------------------------------------------------------------------------
%% resource callback
callback_mode() -> always_sync.

on_start(InstId, Config) ->
    start_client(InstId, Config).

on_stop(_InstId, #{client := Client}) ->
    influxdb:stop_client(Client).

on_query(InstId, {send_message, Data}, _State = #{write_syntax := SyntaxLines, client := Client}) ->
    case data_to_points(Data, SyntaxLines) of
        {ok, Points} ->
            do_query(InstId, Client, Points);
        {error, ErrorPoints} = Err ->
            log_error_points(InstId, ErrorPoints),
            Err
    end.

%% Once a Batched Data trans to points failed.
%% This batch query failed
on_batch_query(InstId, BatchData, State = #{write_syntax := SyntaxLines, client := Client}) ->
    case on_get_status(InstId, State) of
        connected ->
            case parse_batch_data(InstId, BatchData, SyntaxLines) of
                {ok, Points} ->
                    do_query(InstId, Client, Points);
                {error, Reason} ->
                    {error, Reason}
            end;
        disconnected ->
            {resource_down, disconnected}
    end.

on_get_status(_InstId, #{client := Client}) ->
    case influxdb:is_alive(Client) of
        true ->
            connected;
        false ->
            disconnected
    end.

%% -------------------------------------------------------------------------------------------------
%% schema
namespace() -> connector_influxdb.

fields("udp_get") ->
    Key = influxdb_udp,
    fields(Key) ++ type_name_field(Key);
fields("udp_post") ->
    Key = influxdb_udp,
    fields(Key) ++ type_name_field(Key);
fields("udp_put") ->
    fields(influxdb_udp);
fields("api_v1_get") ->
    Key = influxdb_api_v1,
    fields(Key) ++ type_name_field(Key);
fields("api_v1_post") ->
    Key = influxdb_api_v1,
    fields(Key) ++ type_name_field(Key);
fields("api_v1_put") ->
    fields(influxdb_api_v1);
fields("api_v2_get") ->
    Key = influxdb_api_v2,
    fields(Key) ++ type_name_field(Key);
fields("api_v2_post") ->
    Key = influxdb_api_v2,
    fields(Key) ++ type_name_field(Key);
fields("api_v2_put") ->
    fields(influxdb_api_v2);
fields(basic) ->
    [
        {host,
            mk(binary(), #{required => true, default => <<"127.0.0.1">>, desc => ?DESC("host")})},
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

desc(influxdb_udp) ->
    ?DESC("influxdb_udp");
desc(influxdb_api_v1) ->
    ?DESC("influxdb_api_v1");
desc(influxdb_api_v2) ->
    ?DESC("influxdb_api_v2").

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

do_start_client(
    InstId,
    ClientConfig,
    Config = #{
        egress := #{
            write_syntax := Lines
        }
    }
) ->
    case influxdb:start_client(ClientConfig) of
        {ok, Client} ->
            case influxdb:is_alive(Client) of
                true ->
                    State = #{
                        client => Client,
                        write_syntax => to_config(Lines)
                    },
                    ?SLOG(info, #{
                        msg => "starting influxdb connector success",
                        connector => InstId,
                        client => Client,
                        state => State
                    }),
                    {ok, State};
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
        {host, binary_to_list(Host)},
        {port, Port},
        {pool_size, PoolSize},
        {pool, binary_to_atom(InstId, utf8)},
        {precision, atom_to_binary(maps:get(precision, Config, ms), utf8)}
    ] ++ protocol_config(Config).

%% api v2 config
protocol_config(#{
    username := Username,
    password := Password,
    database := DB,
    ssl := SSL
}) ->
    [
        {protocol, http},
        {version, v1},
        {username, binary_to_list(Username)},
        {password, binary_to_list(Password)},
        {database, binary_to_list(DB)}
    ] ++ ssl_config(SSL);
%% api v1 config
protocol_config(#{
    bucket := Bucket,
    org := Org,
    token := Token,
    ssl := SSL
}) ->
    [
        {protocol, http},
        {version, v2},
        {bucket, binary_to_list(Bucket)},
        {org, binary_to_list(Org)},
        {token, Token}
    ] ++ ssl_config(SSL);
%% udp config
protocol_config(_) ->
    [
        {protocol, udp}
    ].

ssl_config(#{enable := false}) ->
    [
        {https_enabled, false}
    ];
ssl_config(SSL = #{enable := true}) ->
    [
        {https_enabled, true},
        {transport, ssl}
    ] ++ maps:to_list(maps:remove(enable, SSL)).

%% -------------------------------------------------------------------------------------------------
%% Query

do_query(InstId, Client, Points) ->
    case influxdb:write(Client, Points) of
        ok ->
            ?SLOG(debug, #{
                msg => "influxdb write point success",
                connector => InstId,
                points => Points
            });
        {error, Reason} = Err ->
            ?SLOG(error, #{
                msg => "influxdb write point failed",
                connector => InstId,
                reason => Reason
            }),
            Err
    end.

%% -------------------------------------------------------------------------------------------------
%% Tags & Fields Config Trans

to_config(Lines) ->
    to_config(Lines, []).

to_config([], Acc) ->
    lists:reverse(Acc);
to_config(
    [
        #{
            measurement := Measurement,
            timestamp := Timestamp,
            tags := Tags,
            fields := Fields
        }
        | Rest
    ],
    Acc
) ->
    Res = #{
        measurement => emqx_plugin_libs_rule:preproc_tmpl(Measurement),
        timestamp => emqx_plugin_libs_rule:preproc_tmpl(Timestamp),
        tags => to_kv_config(Tags),
        fields => to_kv_config(Fields)
    },
    to_config(Rest, [Res | Acc]).

to_kv_config(KVfields) ->
    maps:fold(fun to_maps_config/3, #{}, proplists:to_map(KVfields)).

to_maps_config(K, V, Res) ->
    NK = emqx_plugin_libs_rule:preproc_tmpl(bin(K)),
    NV = emqx_plugin_libs_rule:preproc_tmpl(bin(V)),
    Res#{NK => NV}.

%% -------------------------------------------------------------------------------------------------
%% Tags & Fields Data Trans
parse_batch_data(InstId, BatchData, SyntaxLines) ->
    {Points, Errors} = lists:foldl(
        fun({send_message, Data}, {AccIn, ErrAccIn}) ->
            case data_to_points(Data, SyntaxLines) of
                {ok, Points} ->
                    {[Points | AccIn], ErrAccIn};
                {error, ErrorPoints} ->
                    log_error_points(InstId, ErrorPoints),
                    {AccIn, ErrAccIn + 1}
            end
        end,
        {[], 0},
        BatchData
    ),
    case Errors of
        0 ->
            {ok, Points};
        _ ->
            ?SLOG(error, #{
                msg => io_lib:format("InfluxDB trans point failed, count: ~p", [Errors]),
                connector => InstId,
                reason => points_trans_failed
            }),
            {error, points_trans_failed}
    end.

data_to_points(Data, SyntaxLines) ->
    lines_to_points(Data, SyntaxLines, [], []).

%% When converting multiple rows data into InfluxDB Line Protocol, they are considered to be strongly correlated.
%% And once a row fails to convert, all of them are considered to have failed.
lines_to_points(_, [], Points, ErrorPoints) ->
    case ErrorPoints of
        [] ->
            {ok, Points};
        _ ->
            %% ignore trans succeeded points
            {error, ErrorPoints}
    end;
lines_to_points(
    Data,
    [
        #{
            measurement := Measurement,
            timestamp := Timestamp,
            tags := Tags,
            fields := Fields
        }
        | Rest
    ],
    ResultPointsAcc,
    ErrorPointsAcc
) ->
    TransOptions = #{return => rawlist, var_trans => fun data_filter/1},
    case emqx_plugin_libs_rule:proc_tmpl(Timestamp, Data, TransOptions) of
        [TimestampInt] when is_integer(TimestampInt) ->
            {_, EncodeTags} = maps:fold(fun maps_config_to_data/3, {Data, #{}}, Tags),
            {_, EncodeFields} = maps:fold(fun maps_config_to_data/3, {Data, #{}}, Fields),
            Point = #{
                measurement => emqx_plugin_libs_rule:proc_tmpl(Measurement, Data),
                timestamp => TimestampInt,
                tags => EncodeTags,
                fields => EncodeFields
            },
            lines_to_points(Data, Rest, [Point | ResultPointsAcc], ErrorPointsAcc);
        BadTimestamp ->
            lines_to_points(Data, Rest, ResultPointsAcc, [
                {error, {bad_timestamp, BadTimestamp}} | ErrorPointsAcc
            ])
    end.

maps_config_to_data(K, V, {Data, Res}) ->
    KTransOptions = #{return => full_binary},
    VTransOptions = #{return => rawlist, var_trans => fun data_filter/1},
    NK = emqx_plugin_libs_rule:proc_tmpl(K, Data, KTransOptions),
    NV = emqx_plugin_libs_rule:proc_tmpl(V, Data, VTransOptions),
    case {NK, NV} of
        {[undefined], _} ->
            {Data, Res};
        {_, [undefined]} ->
            {Data, Res};
        _ ->
            {Data, Res#{NK => value_type(NV)}}
    end.

value_type([Int, <<"i">>]) when
    is_integer(Int)
->
    {int, Int};
value_type([UInt, <<"u">>]) ->
    {uint, UInt};
value_type([<<"t">>]) ->
    't';
value_type([<<"T">>]) ->
    'T';
value_type([true]) ->
    'true';
value_type([<<"TRUE">>]) ->
    'TRUE';
value_type([<<"True">>]) ->
    'True';
value_type([<<"f">>]) ->
    'f';
value_type([<<"F">>]) ->
    'F';
value_type([false]) ->
    'false';
value_type([<<"FALSE">>]) ->
    'FALSE';
value_type([<<"False">>]) ->
    'False';
value_type(Val) ->
    Val.

data_filter(undefined) -> undefined;
data_filter(Int) when is_integer(Int) -> Int;
data_filter(Number) when is_number(Number) -> Number;
data_filter(Bool) when is_boolean(Bool) -> Bool;
data_filter(Data) -> bin(Data).

bin(Data) -> emqx_plugin_libs_rule:bin(Data).

%% helper funcs
log_error_points(InstId, Errs) ->
    lists:foreach(
        fun({error, Reason}) ->
            ?SLOG(error, #{
                msg => "influxdb trans point failed",
                connector => InstId,
                reason => Reason
            })
        end,
        Errs
    ).
