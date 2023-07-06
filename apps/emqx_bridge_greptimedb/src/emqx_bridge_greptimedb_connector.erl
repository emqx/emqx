%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_greptimedb_connector).

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
    on_get_status/2
]).

-export([
    roots/0,
    namespace/0,
    fields/1,
    desc/1
]).

%% only for test
-export([is_unrecoverable_error/1]).

-type ts_precision() :: ns | us | ms | s.

%% Allocatable resources
-define(greptime_client, greptime_client).

-define(GREPTIMEDB_DEFAULT_PORT, 4000).

-define(DEFAULT_DB, <<"public">>).

-define(GREPTIMEDB_HOST_OPTIONS, #{
    default_port => ?GREPTIMEDB_DEFAULT_PORT
}).

-define(DEFAULT_TIMESTAMP_TMPL, "${timestamp}").

%% -------------------------------------------------------------------------------------------------
%% resource callback
callback_mode() -> always_sync.

on_start(InstId, Config) ->
    %% InstID as pool would be handled by greptimedb client
    %% so there is no need to allocate pool_name here
    %% See: greptimedb:start_client/1
    start_client(InstId, Config).

on_stop(InstId, _State) ->
    case emqx_resource:get_allocated_resources(InstId) of
        #{?greptime_client := Client} ->
            greptimedb:stop_client(Client);
        _ ->
            ok
    end.

on_query(InstId, {send_message, Data}, _State = #{write_syntax := SyntaxLines, client := Client}) ->
    case data_to_points(Data, SyntaxLines) of
        {ok, Points} ->
            ?tp(
                greptimedb_connector_send_query,
                #{points => Points, batch => false, mode => sync}
            ),
            do_query(InstId, Client, Points);
        {error, ErrorPoints} ->
            ?tp(
                greptimedb_connector_send_query_error,
                #{batch => false, mode => sync, error => ErrorPoints}
            ),
            log_error_points(InstId, ErrorPoints),
            {error, {unrecoverable_error, ErrorPoints}}
    end.

%% Once a Batched Data trans to points failed.
%% This batch query failed
on_batch_query(InstId, BatchData, _State = #{write_syntax := SyntaxLines, client := Client}) ->
    case parse_batch_data(InstId, BatchData, SyntaxLines) of
        {ok, Points} ->
            ?tp(
                greptimedb_connector_send_query,
                #{points => Points, batch => true, mode => sync}
            ),
            do_query(InstId, Client, Points);
        {error, Reason} ->
            ?tp(
                greptimedb_connector_send_query_error,
                #{batch => true, mode => sync, error => Reason}
            ),
            {error, {unrecoverable_error, Reason}}
    end.

on_get_status(_InstId, #{client := Client}) ->
    case greptimedb:is_alive(Client) of
        true ->
            connected;
        false ->
            disconnected
    end.

%% -------------------------------------------------------------------------------------------------
%% schema
namespace() -> connector_greptimedb.

roots() ->
    [
        {config, #{
            type => hoconsc:union(
                [
                    hoconsc:ref(?MODULE, greptimedb_grpc_v1)
                ]
            )
        }}
    ].

fields(common) ->
    [
        {server, server()},
        {precision,
            %% The greptimedb only supports these 4 precision:
            %% See "https://github.com/influxdata/greptimedb/blob/
            %% 6b607288439a991261307518913eb6d4e280e0a7/models/points.go#L487" for
            %% more information.
            mk(enum([ns, us, ms, s]), #{
                required => false, default => ms, desc => ?DESC("precision")
            })}
    ];
fields(greptimedb_grpc_v1) ->
    fields(common) ++
        [
            {dbname, mk(binary(), #{required => true, desc => ?DESC("dbname")})}
        ] ++ emqx_connector_schema_lib:ssl_fields().

server() ->
    Meta = #{
        required => false,
        default => <<"127.0.0.1:4000">>,
        desc => ?DESC("server"),
        converter => fun convert_server/2
    },
    emqx_schema:servers_sc(Meta, ?GREPTIMEDB_HOST_OPTIONS).

desc(common) ->
    ?DESC("common");
desc(greptimedb_grpc_v1) ->
    ?DESC("greptimedb_grpc_v1").

%% -------------------------------------------------------------------------------------------------
%% internal functions

start_client(InstId, Config) ->
    ClientConfig = client_config(InstId, Config),
    ?SLOG(info, #{
        msg => "starting greptimedb connector",
        connector => InstId,
        config => emqx_utils:redact(Config),
        client_config => emqx_utils:redact(ClientConfig)
    }),
    try do_start_client(InstId, ClientConfig, Config) of
        Res = {ok, #{client := Client}} ->
            ok = emqx_resource:allocate_resource(InstId, ?greptime_client, Client),
            Res;
        {error, Reason} ->
            {error, Reason}
    catch
        E:R:S ->
            ?tp(greptimedb_connector_start_exception, #{error => {E, R}}),
            ?SLOG(warning, #{
                msg => "start greptimedb connector error",
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
    Precision = maps:get(precision, Config, ms),
    case greptimedb:start_client(ClientConfig) of
        {ok, Client} ->
            case greptimedb:is_alive(Client, true) of
                true ->
                    State = #{
                        client => Client,
                        dbname => proplists:get_value(dbname, ClientConfig, ?DEFAULT_DB),
                        write_syntax => to_config(Lines, Precision)
                    },
                    ?SLOG(info, #{
                        msg => "starting greptimedb connector success",
                        connector => InstId,
                        client => redact_auth(Client),
                        state => redact_auth(State)
                    }),
                    {ok, State};
                {false, Reason} ->
                    ?tp(greptimedb_connector_start_failed, #{
                        error => greptimedb_client_not_alive, reason => Reason
                    }),
                    ?SLOG(warning, #{
                        msg => "failed_to_start_greptimedb_connector",
                        connector => InstId,
                        client => redact_auth(Client),
                        reason => Reason
                    }),
                    %% no leak
                    _ = greptimedb:stop_client(Client),
                    {error, greptimedb_client_not_alive}
            end;
        {error, {already_started, Client0}} ->
            ?tp(greptimedb_connector_start_already_started, #{}),
            ?SLOG(info, #{
                msg => "restarting greptimedb connector, found already started client",
                connector => InstId,
                old_client => redact_auth(Client0)
            }),
            _ = greptimedb:stop_client(Client0),
            do_start_client(InstId, ClientConfig, Config);
        {error, Reason} ->
            ?tp(greptimedb_connector_start_failed, #{error => Reason}),
            ?SLOG(warning, #{
                msg => "failed_to_start_greptimedb_connector",
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
    #{hostname := Host, port := Port} = emqx_schema:parse_server(Server, ?GREPTIMEDB_HOST_OPTIONS),
    [
        {endpoints, [{http, str(Host), Port}]},
        {pool_size, erlang:system_info(schedulers)},
        {pool, InstId},
        {pool_type, random},
        {timeunit, maps:get(precision, Config, ms)}
    ] ++ protocol_config(Config).

protocol_config(
    #{
        dbname := DbName,
        ssl := SSL
    } = Config
) ->
    [
        {dbname, DbName}
    ] ++ auth(Config) ++
        ssl_config(SSL).

ssl_config(#{enable := false}) ->
    [
        {https_enabled, false}
    ];
ssl_config(SSL = #{enable := true}) ->
    [
        {https_enabled, true},
        {transport, ssl},
        {transport_opts, emqx_tls_lib:to_client_opts(SSL)}
    ].

auth(#{username := Username, password := Password}) ->
    [
        {auth, {basic, #{username => Username, password => Password}}}
    ];
auth(_) ->
    [].

redact_auth(Term) ->
    emqx_utils:redact(Term, fun is_auth_key/1).

is_auth_key(Key) when is_binary(Key) ->
    string:equal("authorization", Key, true);
is_auth_key(_) ->
    false.

%% -------------------------------------------------------------------------------------------------
%% Query
do_query(InstId, Client, Points) ->
    case greptimedb:write_batch(Client, Points) of
        {ok, _} ->
            ?SLOG(debug, #{
                msg => "greptimedb write point success",
                connector => InstId,
                points => Points
            });
        {error, {401, _, _}} ->
            ?tp(greptimedb_connector_do_query_failure, #{error => <<"authorization failure">>}),
            ?SLOG(error, #{
                msg => "greptimedb_authorization_failed",
                client => redact_auth(Client),
                connector => InstId
            }),
            {error, {unrecoverable_error, <<"authorization failure">>}};
        {error, Reason} = Err ->
            ?tp(greptimedb_connector_do_query_failure, #{error => Reason}),
            ?SLOG(error, #{
                msg => "greptimedb write point failed",
                connector => InstId,
                reason => Reason
            }),
            case is_unrecoverable_error(Err) of
                true ->
                    {error, {unrecoverable_error, Reason}};
                false ->
                    {error, {recoverable_error, Reason}}
            end
    end.

%% -------------------------------------------------------------------------------------------------
%% Tags & Fields Config Trans

to_config(Lines, Precision) ->
    to_config(Lines, [], Precision).

to_config([], Acc, _Precision) ->
    lists:reverse(Acc);
to_config([Item0 | Rest], Acc, Precision) ->
    Ts0 = maps:get(timestamp, Item0, undefined),
    {Ts, FromPrecision, ToPrecision} = preproc_tmpl_timestamp(Ts0, Precision),
    Item = #{
        measurement => emqx_placeholder:preproc_tmpl(maps:get(measurement, Item0)),
        timestamp => Ts,
        precision => {FromPrecision, ToPrecision},
        tags => to_kv_config(maps:get(tags, Item0)),
        fields => to_kv_config(maps:get(fields, Item0))
    },
    to_config(Rest, [Item | Acc], Precision).

%% pre-process the timestamp template
%% returns a tuple of three elements:
%% 1. The timestamp template itself.
%% 2. The source timestamp precision (ms if the template ${timestamp} is used).
%% 3. The target timestamp precision (configured for the client).
preproc_tmpl_timestamp(undefined, Precision) ->
    %% not configured, we default it to the message timestamp
    preproc_tmpl_timestamp(?DEFAULT_TIMESTAMP_TMPL, Precision);
preproc_tmpl_timestamp(Ts, Precision) when is_integer(Ts) ->
    %% a const value is used which is very much unusual, but we have to add a special handling
    {Ts, Precision, Precision};
preproc_tmpl_timestamp(Ts, Precision) when is_list(Ts) ->
    preproc_tmpl_timestamp(iolist_to_binary(Ts), Precision);
preproc_tmpl_timestamp(<<?DEFAULT_TIMESTAMP_TMPL>> = Ts, Precision) ->
    {emqx_placeholder:preproc_tmpl(Ts), ms, Precision};
preproc_tmpl_timestamp(Ts, Precision) when is_binary(Ts) ->
    %% a placehold is in use. e.g. ${payload.my_timestamp}
    %% we can only hope it the value will be of the same precision in the configs
    {emqx_placeholder:preproc_tmpl(Ts), Precision, Precision}.

to_kv_config(KVfields) ->
    maps:fold(fun to_maps_config/3, #{}, proplists:to_map(KVfields)).

to_maps_config(K, V, Res) ->
    NK = emqx_placeholder:preproc_tmpl(bin(K)),
    NV = emqx_placeholder:preproc_tmpl(bin(V)),
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
                msg => io_lib:format("Greptimedb trans point failed, count: ~p", [Errors]),
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
        timestamp := emqx_placeholder:tmpl_token() | integer(),
        precision := {From :: ts_precision(), To :: ts_precision()}
    }
]) -> {ok, [map()]} | {error, term()}.
data_to_points(Data, SyntaxLines) ->
    lines_to_points(Data, SyntaxLines, [], []).

%% When converting multiple rows data into Greptimedb Line Protocol, they are considered to be strongly correlated.
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
    case parse_timestamp(emqx_placeholder:proc_tmpl(Ts, Data, TransOptions)) of
        {ok, TsInt} ->
            Item1 = Item#{timestamp => TsInt},
            continue_lines_to_points(Data, Item1, Rest, ResultPointsAcc, ErrorPointsAcc);
        {error, BadTs} ->
            lines_to_points(Data, Rest, ResultPointsAcc, [
                {error, {bad_timestamp, BadTs}} | ErrorPointsAcc
            ])
    end;
lines_to_points(Data, [#{timestamp := Ts} = Item | Rest], ResultPointsAcc, ErrorPointsAcc) when
    is_integer(Ts)
->
    continue_lines_to_points(Data, Item, Rest, ResultPointsAcc, ErrorPointsAcc).

parse_timestamp([TsInt]) when is_integer(TsInt) ->
    {ok, TsInt};
parse_timestamp([TsBin]) ->
    try
        {ok, binary_to_integer(TsBin)}
    catch
        _:_ ->
            {error, TsBin}
    end.

continue_lines_to_points(Data, Item, Rest, ResultPointsAcc, ErrorPointsAcc) ->
    case line_to_point(Data, Item) of
        #{fields := Fields} when map_size(Fields) =:= 0 ->
            %% greptimedb client doesn't like empty field maps...
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
        fields := Fields,
        timestamp := Ts,
        precision := Precision
    } = Item
) ->
    {_, EncodedTags} = maps:fold(fun maps_config_to_data/3, {Data, #{}}, Tags),
    {_, EncodedFields} = maps:fold(fun maps_config_to_data/3, {Data, #{}}, Fields),
    TableName = emqx_placeholder:proc_tmpl(Measurement, Data),
    {TableName, [
        maps:without([precision], Item#{
            tags => EncodedTags,
            fields => EncodedFields,
            timestamp => maybe_convert_time_unit(Ts, Precision)
        })
    ]}.

maybe_convert_time_unit(Ts, {FromPrecision, ToPrecision}) ->
    erlang:convert_time_unit(Ts, time_unit(FromPrecision), time_unit(ToPrecision)).

time_unit(s) -> second;
time_unit(ms) -> millisecond;
time_unit(us) -> microsecond;
time_unit(ns) -> nanosecond.

maps_config_to_data(K, V, {Data, Res}) ->
    KTransOptions = #{return => rawlist, var_trans => fun key_filter/1},
    VTransOptions = #{return => rawlist, var_trans => fun data_filter/1},
    NK0 = emqx_placeholder:proc_tmpl(K, Data, KTransOptions),
    NV = emqx_placeholder:proc_tmpl(V, Data, VTransOptions),
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
    greptimedb_values:int64_value(Int);
value_type([UInt, <<"u">>]) when
    is_integer(UInt)
->
    greptimedb_values:uint64_value(UInt);
value_type([Float]) when is_float(Float) ->
    Float;
value_type([<<"t">>]) ->
    greptimedb_values:boolean_value(true);
value_type([<<"T">>]) ->
    greptimedb_values:boolean_value(true);
value_type([true]) ->
    greptimedb_values:boolean_value(true);
value_type([<<"TRUE">>]) ->
    greptimedb_values:boolean_value(true);
value_type([<<"True">>]) ->
    greptimedb_values:boolean_value(true);
value_type([<<"f">>]) ->
    greptimedb_values:boolean_value(false);
value_type([<<"F">>]) ->
    greptimedb_values:boolean_value(false);
value_type([false]) ->
    greptimedb_values:boolean_value(false);
value_type([<<"FALSE">>]) ->
    greptimedb_values:boolean_value(false);
value_type([<<"False">>]) ->
    greptimedb_values:boolean_value(false);
value_type(Val) ->
    #{values => #{string_values => Val, datatype => 'STRING'}}.

key_filter(undefined) -> undefined;
key_filter(Value) -> emqx_utils_conv:bin(Value).

data_filter(undefined) -> undefined;
data_filter(Int) when is_integer(Int) -> Int;
data_filter(Number) when is_number(Number) -> Number;
data_filter(Bool) when is_boolean(Bool) -> Bool;
data_filter(Data) -> bin(Data).

bin(Data) -> emqx_utils_conv:bin(Data).

%% helper funcs
log_error_points(InstId, Errs) ->
    lists:foreach(
        fun({error, Reason}) ->
            ?SLOG(error, #{
                msg => "greptimedb trans point failed",
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

is_unrecoverable_error({error, {unrecoverable_error, _}}) ->
    true;
is_unrecoverable_error(_) ->
    false.

%%===================================================================
%% eunit tests
%%===================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

is_auth_key_test_() ->
    [
        ?_assert(is_auth_key(<<"Authorization">>)),
        ?_assertNot(is_auth_key(<<"Something">>)),
        ?_assertNot(is_auth_key(89))
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
            desc(greptimedb_grpc_v1)
        ),
        ?_assertMatch(
            {desc, _, _},
            hocon_schema:field_schema(server(), desc)
        ),
        ?_assertMatch(
            connector_greptimedb,
            namespace()
        )
    ].
-endif.
