%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ee_connector_influxdb).

-include("emqx_ee_connector.hrl").
-include_lib("emqx_connector/include/emqx_connector.hrl").

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-import(hoconsc, [mk/2, enum/1, ref/2]).

-behaviour(emqx_resource).

%% callbacks of behaviour emqx_resource
-export([
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_batch_query/3,
    on_query_async/4,
    on_batch_query_async/4,
    on_get_status/2
]).

-export([
    namespace/0,
    fields/1,
    desc/1
]).

%% influxdb servers don't need parse
-define(INFLUXDB_HOST_OPTIONS, #{
    host_type => hostname,
    default_port => ?INFLUXDB_DEFAULT_PORT
}).

%% -------------------------------------------------------------------------------------------------
%% resource callback
callback_mode() -> async_if_possible.

on_start(InstId, Config) ->
    start_client(InstId, Config).

on_stop(_InstId, #{client := Client}) ->
    influxdb:stop_client(Client).

on_query(InstId, {send_message, Data}, _State = #{write_syntax := SyntaxLines, client := Client}) ->
    case data_to_points(Data, SyntaxLines) of
        {ok, Points} ->
            ?tp(
                influxdb_connector_send_query,
                #{points => Points, batch => false, mode => sync}
            ),
            do_query(InstId, Client, Points);
        {error, ErrorPoints} = Err ->
            ?tp(
                influxdb_connector_send_query_error,
                #{batch => false, mode => sync, error => ErrorPoints}
            ),
            log_error_points(InstId, ErrorPoints),
            Err
    end.

%% Once a Batched Data trans to points failed.
%% This batch query failed
on_batch_query(InstId, BatchData, _State = #{write_syntax := SyntaxLines, client := Client}) ->
    case parse_batch_data(InstId, BatchData, SyntaxLines) of
        {ok, Points} ->
            ?tp(
                influxdb_connector_send_query,
                #{points => Points, batch => true, mode => sync}
            ),
            do_query(InstId, Client, Points);
        {error, Reason} ->
            ?tp(
                influxdb_connector_send_query_error,
                #{batch => true, mode => sync, error => Reason}
            ),
            {error, Reason}
    end.

on_query_async(
    InstId,
    {send_message, Data},
    {ReplayFun, Args},
    _State = #{write_syntax := SyntaxLines, client := Client}
) ->
    case data_to_points(Data, SyntaxLines) of
        {ok, Points} ->
            ?tp(
                influxdb_connector_send_query,
                #{points => Points, batch => false, mode => async}
            ),
            do_async_query(InstId, Client, Points, {ReplayFun, Args});
        {error, ErrorPoints} = Err ->
            ?tp(
                influxdb_connector_send_query_error,
                #{batch => false, mode => async, error => ErrorPoints}
            ),
            log_error_points(InstId, ErrorPoints),
            Err
    end.

on_batch_query_async(
    InstId,
    BatchData,
    {ReplayFun, Args},
    #{write_syntax := SyntaxLines, client := Client}
) ->
    case parse_batch_data(InstId, BatchData, SyntaxLines) of
        {ok, Points} ->
            ?tp(
                influxdb_connector_send_query,
                #{points => Points, batch => true, mode => async}
            ),
            do_async_query(InstId, Client, Points, {ReplayFun, Args});
        {error, Reason} ->
            ?tp(
                influxdb_connector_send_query_error,
                #{batch => true, mode => async, error => Reason}
            ),
            {error, Reason}
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

fields(common) ->
    [
        {server, fun server/1},
        {precision,
            mk(enum([ns, us, ms, s, m, h]), #{
                required => false, default => ms, desc => ?DESC("precision")
            })}
    ];
fields(influxdb_udp) ->
    fields(common);
fields(influxdb_api_v1) ->
    fields(common) ++
        [
            {database, mk(binary(), #{required => true, desc => ?DESC("database")})},
            {username, mk(binary(), #{desc => ?DESC("username")})},
            {password, mk(binary(), #{desc => ?DESC("password"), format => <<"password">>})}
        ] ++ emqx_connector_schema_lib:ssl_fields();
fields(influxdb_api_v2) ->
    fields(common) ++
        [
            {bucket, mk(binary(), #{required => true, desc => ?DESC("bucket")})},
            {org, mk(binary(), #{required => true, desc => ?DESC("org")})},
            {token, mk(binary(), #{required => true, desc => ?DESC("token")})}
        ] ++ emqx_connector_schema_lib:ssl_fields().

server(type) -> emqx_schema:ip_port();
server(required) -> true;
server(validator) -> [?NOT_EMPTY("the value of the field 'server' cannot be empty")];
server(converter) -> fun to_server_raw/1;
server(default) -> <<"127.0.0.1:8086">>;
server(desc) -> ?DESC("server");
server(_) -> undefined.

desc(common) ->
    ?DESC("common");
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
            ?tp(influxdb_connector_start_exception, #{error => {E, R}}),
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
        write_syntax := Lines
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
                    ?tp(influxdb_connector_start_failed, #{error => influxdb_client_not_alive}),
                    ?SLOG(error, #{
                        msg => "starting influxdb connector failed",
                        connector => InstId,
                        client => Client,
                        reason => "client is not alive"
                    }),
                    {error, influxdb_client_not_alive}
            end;
        {error, {already_started, Client0}} ->
            ?tp(influxdb_connector_start_already_started, #{}),
            ?SLOG(info, #{
                msg => "restarting influxdb connector, found already started client",
                connector => InstId,
                old_client => Client0
            }),
            _ = influxdb:stop_client(Client0),
            do_start_client(InstId, ClientConfig, Config);
        {error, Reason} ->
            ?tp(influxdb_connector_start_failed, #{error => Reason}),
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
        server := {Host, Port}
    }
) ->
    [
        {host, str(Host)},
        {port, Port},
        {pool_size, erlang:system_info(schedulers)},
        {pool, binary_to_atom(InstId, utf8)},
        {precision, atom_to_binary(maps:get(precision, Config, ms), utf8)}
    ] ++ protocol_config(Config).

%% api v1 config
protocol_config(#{
    username := Username,
    password := Password,
    database := DB,
    ssl := SSL
}) ->
    [
        {protocol, http},
        {version, v1},
        {username, str(Username)},
        {password, str(Password)},
        {database, str(DB)}
    ] ++ ssl_config(SSL);
%% api v2 config
protocol_config(#{
    bucket := Bucket,
    org := Org,
    token := Token,
    ssl := SSL
}) ->
    [
        {protocol, http},
        {version, v2},
        {bucket, str(Bucket)},
        {org, str(Org)},
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
            ?tp(influxdb_connector_do_query_failure, #{error => Reason}),
            ?SLOG(error, #{
                msg => "influxdb write point failed",
                connector => InstId,
                reason => Reason
            }),
            Err
    end.

do_async_query(InstId, Client, Points, ReplayFunAndArgs) ->
    ?SLOG(info, #{
        msg => "influxdb write point async",
        connector => InstId,
        points => Points
    }),
    ok = influxdb:write_async(Client, Points, ReplayFunAndArgs).

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
        fun({send_message, Data}, {ListOfPoints, ErrAccIn}) ->
            case data_to_points(Data, SyntaxLines) of
                {ok, Points} ->
                    {[Points | ListOfPoints], ErrAccIn};
                {error, ErrorPoints} ->
                    log_error_points(InstId, ErrorPoints),
                    {ListOfPoints, ErrAccIn + 1}
            end
        end,
        {[], 0},
        BatchData
    ),
    case Errors of
        0 ->
            {ok, lists:flatten(Points)};
        _ ->
            ?SLOG(error, #{
                msg => io_lib:format("InfluxDB trans point failed, count: ~p", [Errors]),
                connector => InstId,
                reason => points_trans_failed
            }),
            {error, points_trans_failed}
    end.

-spec data_to_points(map(), [
    #{
        fields := [{binary(), binary()}],
        measurement := binary(),
        tags := [{binary(), binary()}],
        timestamp := binary()
    }
]) -> {ok, [map()]} | {error, term()}.
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
    KTransOptions = #{return => rawlist, var_trans => fun key_filter/1},
    VTransOptions = #{return => rawlist, var_trans => fun data_filter/1},
    NK0 = emqx_plugin_libs_rule:proc_tmpl(K, Data, KTransOptions),
    NV = emqx_plugin_libs_rule:proc_tmpl(V, Data, VTransOptions),
    case {NK0, NV} of
        {[undefined], _} ->
            {Data, Res};
        %% undefined value in normal format [undefined] or int/uint format [undefined, <<"i">>]
        {_, [undefined | _]} ->
            {Data, Res};
        _ ->
            NK = list_to_binary(NK0),
            {Data, Res#{NK => value_type(NV)}}
    end.

value_type([Int, <<"i">>]) when
    is_integer(Int)
->
    {int, Int};
value_type([UInt, <<"u">>]) when
    is_integer(UInt)
->
    {uint, UInt};
value_type([Float]) when is_float(Float) ->
    Float;
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

key_filter(undefined) -> undefined;
key_filter(Value) -> emqx_plugin_libs_rule:bin(Value).

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

%% ===================================================================
%% typereflt funcs

-spec to_server_raw(string() | binary()) ->
    {string(), pos_integer()}.
to_server_raw(<<"http://", Server/binary>>) ->
    emqx_connector_schema_lib:parse_server(Server, ?INFLUXDB_HOST_OPTIONS);
to_server_raw(<<"https://", Server/binary>>) ->
    emqx_connector_schema_lib:parse_server(Server, ?INFLUXDB_HOST_OPTIONS);
to_server_raw(Server) ->
    emqx_connector_schema_lib:parse_server(Server, ?INFLUXDB_HOST_OPTIONS).

str(A) when is_atom(A) ->
    atom_to_list(A);
str(B) when is_binary(B) ->
    binary_to_list(B);
str(S) when is_list(S) ->
    S.

%%===================================================================
%% eunit tests
%%===================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

to_server_raw_test_() ->
    [
        ?_assertEqual(
            {"foobar", 1234},
            to_server_raw(<<"http://foobar:1234">>)
        ),
        ?_assertEqual(
            {"foobar", 1234},
            to_server_raw(<<"https://foobar:1234">>)
        ),
        ?_assertEqual(
            {"foobar", 1234},
            to_server_raw(<<"foobar:1234">>)
        )
    ].

%% for coverage
desc_test_() ->
    [
        ?_assertMatch(
            {desc, _, _},
            desc(common)
        ),
        ?_assertMatch(
            {desc, _, _},
            desc(influxdb_udp)
        ),
        ?_assertMatch(
            {desc, _, _},
            desc(influxdb_api_v1)
        ),
        ?_assertMatch(
            {desc, _, _},
            desc(influxdb_api_v2)
        ),
        ?_assertMatch(
            {desc, _, _},
            server(desc)
        ),
        ?_assertMatch(
            connector_influxdb,
            namespace()
        )
    ].
-endif.
