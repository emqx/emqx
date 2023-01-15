%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    {ReplyFun, Args},
    _State = #{write_syntax := SyntaxLines, client := Client}
) ->
    case data_to_points(Data, SyntaxLines) of
        {ok, Points} ->
            ?tp(
                influxdb_connector_send_query,
                #{points => Points, batch => false, mode => async}
            ),
            do_async_query(InstId, Client, Points, {ReplyFun, Args});
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
    {ReplyFun, Args},
    #{write_syntax := SyntaxLines, client := Client}
) ->
    case parse_batch_data(InstId, BatchData, SyntaxLines) of
        {ok, Points} ->
            ?tp(
                influxdb_connector_send_query,
                #{points => Points, batch => true, mode => async}
            ),
            do_async_query(InstId, Client, Points, {ReplyFun, Args});
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
        {server, server()},
        {precision,
            %% The influxdb only supports these 4 precision:
            %% See "https://github.com/influxdata/influxdb/blob/
            %% 6b607288439a991261307518913eb6d4e280e0a7/models/points.go#L487" for
            %% more information.
            mk(enum([ns, us, ms, s]), #{
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
            {password,
                mk(binary(), #{
                    desc => ?DESC("password"),
                    format => <<"password">>,
                    sensitive => true,
                    converter => fun emqx_schema:password_converter/2
                })}
        ] ++ emqx_connector_schema_lib:ssl_fields();
fields(influxdb_api_v2) ->
    fields(common) ++
        [
            {bucket, mk(binary(), #{required => true, desc => ?DESC("bucket")})},
            {org, mk(binary(), #{required => true, desc => ?DESC("org")})},
            {token, mk(binary(), #{required => true, desc => ?DESC("token")})}
        ] ++ emqx_connector_schema_lib:ssl_fields().

server() ->
    Meta = #{
        required => false,
        default => <<"127.0.0.1:8086">>,
        desc => ?DESC("server"),
        converter => fun convert_server/2
    },
    emqx_schema:servers_sc(Meta, ?INFLUXDB_HOST_OPTIONS).

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
    Config = #{write_syntax := Lines}
) ->
    case influxdb:start_client(ClientConfig) of
        {ok, Client} ->
            case influxdb:is_alive(Client) of
                true ->
                    State = #{
                        client => Client,
                        write_syntax => to_config(
                            Lines, proplists:get_value(precision, ClientConfig)
                        )
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
        server := Server
    }
) ->
    {Host, Port} = emqx_schema:parse_server(Server, ?INFLUXDB_HOST_OPTIONS),
    [
        {host, str(Host)},
        {port, Port},
        {pool_size, erlang:system_info(schedulers)},
        {pool, binary_to_atom(InstId, utf8)},
        {precision, atom_to_binary(maps:get(precision, Config, ms), utf8)}
    ] ++ protocol_config(Config).

%% api v1 config
protocol_config(
    #{
        database := DB,
        ssl := SSL
    } = Config
) ->
    [
        {protocol, http},
        {version, v1},
        {database, str(DB)}
    ] ++ username(Config) ++
        password(Config) ++ ssl_config(SSL);
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

username(#{username := Username}) ->
    [{username, str(Username)}];
username(_) ->
    [].

password(#{password := Password}) ->
    [{password, str(Password)}];
password(_) ->
    [].

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

do_async_query(InstId, Client, Points, ReplyFunAndArgs) ->
    ?SLOG(info, #{
        msg => "influxdb write point async",
        connector => InstId,
        points => Points
    }),
    ok = influxdb:write_async(Client, Points, ReplyFunAndArgs).

%% -------------------------------------------------------------------------------------------------
%% Tags & Fields Config Trans

to_config(Lines, Precision) ->
    to_config(Lines, [], Precision).

to_config([], Acc, _Precision) ->
    lists:reverse(Acc);
to_config([Item0 | Rest], Acc, Precision) ->
    Ts = maps:get(timestamp, Item0, undefined),
    Item = #{
        measurement => emqx_plugin_libs_rule:preproc_tmpl(maps:get(measurement, Item0)),
        timestamp => preproc_tmpl_timestamp(Ts, Precision),
        tags => to_kv_config(maps:get(tags, Item0)),
        fields => to_kv_config(maps:get(fields, Item0))
    },
    to_config(Rest, [Item | Acc], Precision).

preproc_tmpl_timestamp(undefined, <<"ns">>) ->
    erlang:system_time(nanosecond);
preproc_tmpl_timestamp(undefined, <<"us">>) ->
    erlang:system_time(microsecond);
preproc_tmpl_timestamp(undefined, <<"ms">>) ->
    erlang:system_time(millisecond);
preproc_tmpl_timestamp(undefined, <<"s">>) ->
    erlang:system_time(second);
preproc_tmpl_timestamp(Ts, _) when is_integer(Ts) ->
    Ts;
preproc_tmpl_timestamp(Ts, _) when is_binary(Ts); is_list(Ts) ->
    emqx_plugin_libs_rule:preproc_tmpl(Ts).

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
        timestamp := emqx_plugin_libs_rule:tmpl_token() | integer()
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
lines_to_points(Data, [#{timestamp := Ts} = Item | Rest], ResultPointsAcc, ErrorPointsAcc) when
    is_list(Ts)
->
    TransOptions = #{return => rawlist, var_trans => fun data_filter/1},
    case emqx_plugin_libs_rule:proc_tmpl(Ts, Data, TransOptions) of
        [TsInt] when is_integer(TsInt) ->
            Item1 = Item#{timestamp => TsInt},
            continue_lines_to_points(Data, Item1, Rest, ResultPointsAcc, ErrorPointsAcc);
        BadTs ->
            lines_to_points(Data, Rest, ResultPointsAcc, [
                {error, {bad_timestamp, BadTs}} | ErrorPointsAcc
            ])
    end;
lines_to_points(Data, [#{timestamp := Ts} = Item | Rest], ResultPointsAcc, ErrorPointsAcc) when
    is_integer(Ts)
->
    continue_lines_to_points(Data, Item, Rest, ResultPointsAcc, ErrorPointsAcc).

continue_lines_to_points(Data, Item, Rest, ResultPointsAcc, ErrorPointsAcc) ->
    case line_to_point(Data, Item) of
        #{fields := Fields} when map_size(Fields) =:= 0 ->
            %% influxdb client doesn't like empty field maps...
            ErrorPointsAcc1 = [{error, no_fields} | ErrorPointsAcc],
            lines_to_points(Data, Rest, ResultPointsAcc, ErrorPointsAcc1);
        Point ->
            lines_to_points(Data, Rest, [Point | ResultPointsAcc], ErrorPointsAcc)
    end.

line_to_point(
    Data,
    #{
        measurement := Measurement,
        tags := Tags,
        fields := Fields
    } = Item
) ->
    {_, EncodedTags} = maps:fold(fun maps_config_to_data/3, {Data, #{}}, Tags),
    {_, EncodedFields} = maps:fold(fun maps_config_to_data/3, {Data, #{}}, Fields),
    Item#{
        measurement => emqx_plugin_libs_rule:proc_tmpl(Measurement, Data),
        tags => EncodedTags,
        fields => EncodedFields
    }.

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

convert_server(<<"http://", Server/binary>>, HoconOpts) ->
    convert_server(Server, HoconOpts);
convert_server(<<"https://", Server/binary>>, HoconOpts) ->
    convert_server(Server, HoconOpts);
convert_server(Server, HoconOpts) ->
    emqx_schema:convert_servers(Server, HoconOpts).

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
            hocon_schema:field_schema(server(), desc)
        ),
        ?_assertMatch(
            connector_influxdb,
            namespace()
        )
    ].
-endif.
