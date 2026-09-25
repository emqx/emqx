%%--------------------------------------------------------------------
%% Copyright (c) 2022-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_greptimedb_connector).

-include_lib("emqx_connector/include/emqx_connector.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-import(hoconsc, [mk/2, enum/1, ref/2]).

-behaviour(emqx_resource).

%% callbacks of behaviour emqx_resource
-export([
    resource_type/0,
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channel_status/3,
    on_get_channels/1,
    on_query/3,
    on_batch_query/3,
    on_query_async/4,
    on_batch_query_async/4,
    on_get_status/2,
    on_format_query_result/1
]).
-export([reply_callback/2, batch_reply_callback/3]).

-export([
    roots/0,
    namespace/0,
    fields/1,
    desc/1
]).

-export([precision_field/0]).

%% only for test
-ifdef(TEST).
-export([is_unrecoverable_error/1]).
-endif.

-type ts_precision() :: ns | us | ms | s.

%% Allocatable resources
-define(greptime_client, greptime_client).

-define(GREPTIMEDB_DEFAULT_PORT, 4001).
-define(INT64_MIN, -16#8000000000000000).
-define(INT64_MAX, 16#7FFFFFFFFFFFFFFF).
-define(UINT64_MAX, 16#FFFFFFFFFFFFFFFF).

-define(DEFAULT_DB, <<"public">>).

-define(GREPTIMEDB_HOST_OPTIONS, #{
    default_port => ?GREPTIMEDB_DEFAULT_PORT
}).

-define(DEFAULT_TIMESTAMP_TMPL, "${timestamp}").

-define(AUTO_RECONNECT_S, 1).

-define(CONNECT_TIMEOUT, 5_000).

%% -------------------------------------------------------------------------------------------------
%% resource callback
resource_type() -> greptimedb.

callback_mode() -> async_if_possible.

on_add_channel(
    _InstanceId,
    #{channels := Channels} = OldState,
    ChannelId,
    #{parameters := Parameters} = ChannelConfig0
) ->
    #{write_syntax := WriteSyntaxTmpl} = Parameters,
    Precision = maps:get(precision, Parameters, ms),
    ChannelConfig = maps:merge(
        Parameters,
        ChannelConfig0#{
            precision => Precision,
            write_syntax => to_config(WriteSyntaxTmpl, Precision)
        }
    ),
    {ok, OldState#{
        channels => Channels#{ChannelId => ChannelConfig}
    }}.

on_remove_channel(_InstanceId, #{channels := Channels} = State, ChannelId) ->
    NewState = State#{channels => maps:remove(ChannelId, Channels)},
    {ok, NewState}.

on_get_channel_status(InstanceId, _ChannelId, State) ->
    case on_get_status(InstanceId, State) of
        ?status_connected -> ?status_connected;
        _ -> ?status_connecting
    end.

on_get_channels(InstanceId) ->
    emqx_bridge_v2:get_channels_for_connector(InstanceId).

on_start(InstId, Config) ->
    %% InstID as pool would be handled by greptimedb client
    %% so there is no need to allocate pool_name here
    %% See: greptimedb:start_client/1
    start_client(InstId, Config).

on_stop(InstId, #{client := Client}) ->
    Res = greptimedb:stop_client(Client),
    ?tp(greptimedb_client_stopped, #{instance_id => InstId}),
    Res;
on_stop(InstId, _State) ->
    case emqx_resource:get_allocated_resources(InstId) of
        #{?greptime_client := Client} ->
            Res = greptimedb:stop_client(Client),
            ?tp(greptimedb_client_stopped, #{instance_id => InstId}),
            Res;
        _ ->
            ok
    end.

on_query(InstId, {Channel, Message}, State) ->
    #{
        channels := #{Channel := #{write_syntax := SyntaxLines}},
        client := Client,
        dbname := DbName
    } = State,
    case data_to_points(Message, DbName, SyntaxLines) of
        {ok, Points} ->
            ?tp(
                greptimedb_connector_send_query,
                #{points => Points, batch => false, mode => sync}
            ),
            do_query(InstId, Channel, Client, Points);
        {error, ErrorPoints} ->
            ?tp(
                greptimedb_connector_send_query_error,
                #{batch => false, mode => sync, error => ErrorPoints}
            ),
            log_error_points(InstId, ErrorPoints),
            unrecoverable_transformation_error(ErrorPoints)
    end.

on_batch_query(InstId, [{Channel, _} | _] = BatchData, State) ->
    #{
        channels := #{Channel := #{write_syntax := SyntaxLines}},
        client := Client,
        dbname := DbName
    } = State,
    case parse_batch_data(InstId, DbName, BatchData, SyntaxLines) of
        {ok, Points} ->
            ?tp(
                greptimedb_connector_send_query,
                #{points => Points, batch => true, mode => sync}
            ),
            do_query(InstId, Channel, Client, Points);
        {ok, Points, BatchResults} ->
            ?tp(
                greptimedb_connector_send_query_error,
                #{batch => true, mode => sync, error => points_trans_failed}
            ),
            case Points of
                [] ->
                    merge_batch_result(ok, BatchResults);
                _ ->
                    ?tp(
                        greptimedb_connector_send_query,
                        #{points => Points, batch => true, mode => sync}
                    ),
                    Result = do_query(InstId, Channel, Client, Points),
                    merge_batch_result(Result, BatchResults)
            end
    end.

on_query_async(InstId, {Channel, Message}, {ReplyFun, Args}, State) ->
    #{
        channels := #{Channel := #{write_syntax := SyntaxLines}},
        client := Client,
        dbname := DbName
    } = State,
    case data_to_points(Message, DbName, SyntaxLines) of
        {ok, Points} ->
            ?tp(
                greptimedb_connector_send_query,
                #{points => Points, batch => false, mode => async}
            ),
            do_async_query(Channel, Client, Points, {ReplyFun, Args});
        {error, ErrorPoints} ->
            ?tp(
                greptimedb_connector_send_query_error,
                #{batch => false, mode => async, error => ErrorPoints}
            ),
            log_error_points(InstId, ErrorPoints),
            unrecoverable_transformation_error(ErrorPoints)
    end.

on_batch_query_async(InstId, [{Channel, _} | _] = BatchData, {ReplyFun, Args}, State) ->
    #{
        channels := #{Channel := #{write_syntax := SyntaxLines}},
        client := Client,
        dbname := DbName
    } = State,
    case parse_batch_data(InstId, DbName, BatchData, SyntaxLines) of
        {ok, Points} ->
            ?tp(
                greptimedb_connector_send_query,
                #{points => Points, batch => true, mode => async}
            ),
            do_async_query(Channel, Client, Points, {ReplyFun, Args});
        {ok, Points, BatchResults} ->
            ?tp(
                greptimedb_connector_send_query_error,
                #{batch => true, mode => async, error => points_trans_failed}
            ),
            ReplyFunAndArgs = {ReplyFun, Args},
            case Points of
                [] ->
                    emqx_resource:apply_reply_fun(
                        ReplyFunAndArgs, merge_batch_result(ok, BatchResults)
                    ),
                    ok;
                _ ->
                    ?tp(
                        greptimedb_connector_send_query,
                        #{points => Points, batch => true, mode => async}
                    ),
                    do_async_batch_query(Channel, Client, Points, BatchResults, ReplyFunAndArgs)
            end
    end.

on_get_status(_InstId, #{client := Client}) ->
    case greptimedb:is_alive(Client) of
        true ->
            ?status_connected;
        false ->
            ?status_disconnected
    end.

%% -------------------------------------------------------------------------------------------------
%% schema
namespace() -> connector_greptimedb.

roots() ->
    [
        {config, #{
            type => hoconsc:union(
                [
                    hoconsc:ref(?MODULE, greptimedb)
                ]
            )
        }}
    ].

fields("connector") ->
    [
        server_field(),
        {ttl,
            mk(binary(), #{
                required => false,
                desc => ?DESC("ttl")
            })},
        {ts_column,
            mk(binary(), #{
                required => false,
                desc => ?DESC("connector_ts_column")
            })}
    ] ++
        credentials_fields() ++
        emqx_connector_schema_lib:ssl_fields();
%% ============ begin: schema for old bridge configs ============
fields(common) ->
    [
        server_field(),
        precision_field()
    ];
fields(greptimedb) ->
    fields(common) ++
        credentials_fields() ++
        emqx_connector_schema_lib:ssl_fields().
%% ============ end: schema for old bridge configs ============

desc(common) ->
    ?DESC("common");
desc(greptimedb) ->
    ?DESC("greptimedb").

precision_field() ->
    {precision,
        %% The greptimedb only supports these 4 precision
        mk(enum([ns, us, ms, s]), #{
            required => false, default => ms, desc => ?DESC("precision")
        })}.

server_field() ->
    {server, server()}.

server() ->
    Meta = #{
        required => false,
        default => <<"127.0.0.1:4001">>,
        desc => ?DESC("server"),
        converter => fun convert_server/2
    },
    emqx_schema:servers_sc(Meta, ?GREPTIMEDB_HOST_OPTIONS).

credentials_fields() ->
    [
        {dbname, mk(binary(), #{required => true, desc => ?DESC("dbname")})},
        {username, mk(binary(), #{desc => ?DESC("username")})},
        {password, emqx_schema_secret:mk(#{desc => ?DESC("password")})}
    ].

%% -------------------------------------------------------------------------------------------------
%% internal functions

start_client(InstId, Config) ->
    ClientConfig = client_config(InstId, Config),
    ?SLOG(info, #{
        msg => "starting_greptimedb_connector",
        connector => InstId,
        config => emqx_utils:redact(Config),
        client_config => emqx_utils:redact(ClientConfig)
    }),
    try do_start_client(InstId, ClientConfig, Config) of
        Res = {ok, #{client := Client}} ->
            ok = emqx_resource:allocate_resource(InstId, ?MODULE, ?greptime_client, Client),
            Res;
        {error, Reason} ->
            {error, Reason}
    catch
        E:R:S ->
            ?tp(greptimedb_connector_start_exception, #{error => {E, R}}),
            ?SLOG(warning, #{
                msg => "start_greptimedb_connector_error",
                connector => InstId,
                error => E,
                reason => emqx_utils:redact(R),
                stack => emqx_utils:redact(S)
            }),
            {error, R}
    end.

do_start_client(
    InstId,
    ClientConfig,
    Config
) ->
    case greptimedb:start_client(ClientConfig) of
        {ok, Client} ->
            case greptimedb:is_alive(Client, true) of
                true ->
                    State = #{
                        client => Client,
                        dbname => proplists:get_value(dbname, ClientConfig, ?DEFAULT_DB),
                        channels => #{}
                    },
                    ?SLOG(info, #{
                        msg => "starting_greptimedb_connector_success",
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
                msg => "restarting_greptimedb_connector_found_already_started_client",
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

grpc_opts() ->
    #{
        sync_start => true,
        connect_timeout => ?CONNECT_TIMEOUT
    }.

client_config(
    InstId,
    Config = #{
        server := Server
    }
) ->
    Hints =
        case maps:find(ttl, Config) of
            {ok, TimeToLive} -> #{<<"ttl">> => TimeToLive};
            _ -> #{}
        end,
    #{hostname := Host, port := Port} = emqx_schema:parse_server(Server, ?GREPTIMEDB_HOST_OPTIONS),
    TsColumn =
        case maps:get(ts_column, Config, undefined) of
            undefined ->
                [];
            TsColumn0 ->
                [{ts_column, TsColumn0}]
        end,
    TsColumn ++
        [
            {endpoints, [{scheme(Config), str(Host), Port}]},
            {pool_size, erlang:system_info(schedulers)},
            {pool, InstId},
            {pool_type, random},
            {auto_reconnect, ?AUTO_RECONNECT_S},
            {grpc_hints, Hints},
            {grpc_opts, grpc_opts()}
        ] ++ protocol_config(Config).

protocol_config(
    #{
        dbname := DbName,
        ssl := SSL
    } = Config
) ->
    [
        {dbname, str(DbName)}
    ] ++ auth(Config) ++
        ssl_config(SSL).

ssl_config(#{enable := false}) ->
    [
        {https_enabled, false}
    ];
ssl_config(SSL = #{enable := true}) ->
    [
        {https_enabled, true},
        {ssl_opts, emqx_tls_lib:to_client_opts(SSL)}
    ].

scheme(#{ssl := #{enable := true}}) ->
    https;
scheme(#{ssl := #{enable := false}}) ->
    http.

auth(#{username := Username, password := Password}) ->
    [
        %% TODO: teach `greptimedb` to accept 0-arity closures as passwords.
        {auth, {basic, #{username => str(Username), password => emqx_secret:unwrap(Password)}}}
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
do_query(InstId, Channel, Client, Points) ->
    emqx_trace:rendered_action_template(Channel, #{points => Points}),
    case greptimedb:write_batch(Client, Points) of
        {ok, #{response := {affected_rows, #{value := Rows}}}} ->
            ?SLOG(debug, #{
                msg => "greptimedb_write_point_success",
                connector => InstId,
                affected_rows => Rows
            }),
            {ok, {affected_rows, Rows}};
        {error, {unauth, _, _}} ->
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
                msg => "greptimedb_write_point_failed",
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

on_format_query_result({ok, {affected_rows, Rows}}) ->
    #{result => ok, affected_rows => Rows};
on_format_query_result(Result) ->
    Result.

do_async_query(Channel, Client, Points, ReplyFunAndArgs) ->
    emqx_trace:rendered_action_template(Channel, #{points => Points}),
    WrappedReplyFunAndArgs = {fun ?MODULE:reply_callback/2, [ReplyFunAndArgs]},
    ok = greptimedb:async_write_batch(Client, Points, WrappedReplyFunAndArgs).

do_async_batch_query(Channel, Client, Points, BatchResults, ReplyFunAndArgs) ->
    emqx_trace:rendered_action_template(Channel, #{points => Points}),
    WrappedReplyFunAndArgs = {
        fun ?MODULE:batch_reply_callback/3, [ReplyFunAndArgs, BatchResults]
    },
    ok = greptimedb:async_write_batch(Client, Points, WrappedReplyFunAndArgs).

reply_callback(ReplyFunAndArgs, Result0) ->
    Result = classify_query_result(Result0),
    emqx_resource:apply_reply_fun(ReplyFunAndArgs, Result).

batch_reply_callback(ReplyFunAndArgs, BatchResults, Result0) ->
    Result = classify_query_result(Result0),
    emqx_resource:apply_reply_fun(
        ReplyFunAndArgs, merge_batch_result(Result, BatchResults)
    ).

classify_query_result({error, {unauth, _, _}}) ->
    ?tp(greptimedb_connector_do_query_failure, #{error => <<"authorization failure">>}),
    {error, {unrecoverable_error, <<"authorization failure">>}};
classify_query_result({error, Reason} = Error) ->
    case is_unrecoverable_error(Error) of
        true ->
            {error, {unrecoverable_error, Reason}};
        false ->
            {error, {recoverable_error, Reason}}
    end;
classify_query_result(Result) ->
    Result.

merge_batch_result({error, _} = Error, _BatchResults) ->
    Error;
merge_batch_result(Result, BatchResults) ->
    lists:map(
        fun
            (valid) -> Result;
            ({error, _} = Error) -> Error
        end,
        BatchResults
    ).

unrecoverable_transformation_error(ErrorPoints) ->
    {error, {unrecoverable_error, ErrorPoints}}.

%% -------------------------------------------------------------------------------------------------
%% Tags & Fields Config Trans

to_config(Lines, Precision) ->
    to_config(Lines, [], Precision).

to_config([], Acc, _Precision) ->
    lists:reverse(Acc);
to_config([Item0 | Rest], Acc, Precision) ->
    Ts0 = maps:get(timestamp, Item0, ?DEFAULT_TIMESTAMP_TMPL),
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
    lists:foldl(
        fun({K, V}, Acc) -> to_maps_config(K, V, Acc) end,
        #{},
        KVfields
    ).

to_maps_config(K, V, Res) ->
    NK = emqx_placeholder:preproc_tmpl(bin(K)),
    NV = preproc_quoted(V),
    Res#{NK => NV}.

preproc_quoted({quoted, V}) ->
    {quoted, emqx_placeholder:preproc_tmpl(bin(V))};
preproc_quoted(V) ->
    emqx_placeholder:preproc_tmpl(bin(V)).

proc_quoted({quoted, V}, Data, TransOpts) ->
    {quoted, emqx_placeholder:proc_tmpl(V, Data, TransOpts)};
proc_quoted(V, Data, TransOpts) ->
    emqx_placeholder:proc_tmpl(V, Data, TransOpts).

%% -------------------------------------------------------------------------------------------------
%% Tags & Fields Data Trans
parse_batch_data(InstId, DbName, BatchData, SyntaxLines) ->
    {Points, BatchResults, Errors} = lists:foldl(
        fun({_, Data}, {ListOfPoints, BatchResultsAcc, ErrAccIn}) ->
            case data_to_points(Data, DbName, SyntaxLines) of
                {ok, Points} ->
                    {[Points | ListOfPoints], [valid | BatchResultsAcc], ErrAccIn};
                {error, ErrorPoints} ->
                    log_error_points(InstId, ErrorPoints),
                    Error = {error, {unrecoverable_error, points_trans_failed}},
                    {ListOfPoints, [Error | BatchResultsAcc], ErrAccIn + 1}
            end
        end,
        {[], [], 0},
        BatchData
    ),
    case Errors of
        0 ->
            {ok, lists:flatten(Points)};
        _ ->
            ?SLOG(error, #{
                msg => "greptimedb_trans_point_failed",
                error_count => Errors,
                connector => InstId,
                reason => points_trans_failed
            }),
            {ok, lists:flatten(Points), lists:reverse(BatchResults)}
    end.

-spec data_to_points(
    map(),
    binary(),
    [
        #{
            fields := [{binary(), binary()}],
            measurement := binary(),
            tags := [{binary(), binary()}],
            timestamp := emqx_placeholder:tmpl_token() | integer(),
            precision := {From :: ts_precision(), To :: ts_precision()}
        }
    ]
) -> {ok, [map()]} | {error, term()}.
data_to_points(Data, DbName, SyntaxLines) ->
    lines_to_points(Data, DbName, SyntaxLines, [], []).

%% When converting multiple rows data into Greptimedb Line Protocol, they are considered to be strongly correlated.
%% And once a row fails to convert, all of them are considered to have failed.
lines_to_points(_Data, _DbName, [], Points, ErrorPoints) ->
    case ErrorPoints of
        [] ->
            {ok, Points};
        _ ->
            %% ignore trans succeeded points
            {error, ErrorPoints}
    end;
lines_to_points(
    Data, DbName, [#{timestamp := Ts} = Item | Rest], ResultPointsAcc, ErrorPointsAcc
) when
    is_list(Ts)
->
    TransOptions = #{return => rawlist, var_trans => fun data_filter/1},
    case parse_timestamp(emqx_placeholder:proc_tmpl(Ts, Data, TransOptions)) of
        {ok, TsInt} ->
            Item1 = Item#{timestamp => TsInt},
            continue_lines_to_points(Data, DbName, Item1, Rest, ResultPointsAcc, ErrorPointsAcc);
        {error, BadTs} ->
            lines_to_points(Data, DbName, Rest, ResultPointsAcc, [
                {error, {bad_timestamp, BadTs}} | ErrorPointsAcc
            ])
    end;
lines_to_points(
    Data, DbName, [#{timestamp := Ts} = Item | Rest], ResultPointsAcc, ErrorPointsAcc
) when
    is_integer(Ts)
->
    continue_lines_to_points(Data, DbName, Item, Rest, ResultPointsAcc, ErrorPointsAcc).

parse_timestamp([TsInt]) when is_integer(TsInt) ->
    {ok, TsInt};
parse_timestamp([TsBin]) ->
    try
        {ok, binary_to_integer(TsBin)}
    catch
        _:_ ->
            {error, TsBin}
    end.

continue_lines_to_points(Data, DbName, Item, Rest, ResultPointsAcc, ErrorPointsAcc) ->
    case line_to_point(Data, DbName, Item) of
        {ok, {_, [#{fields := Fields}]}} when map_size(Fields) =:= 0 ->
            %% greptimedb client doesn't like empty field maps...
            ErrorPointsAcc1 = [{error, no_fields} | ErrorPointsAcc],
            lines_to_points(Data, DbName, Rest, ResultPointsAcc, ErrorPointsAcc1);
        {ok, Point} ->
            lines_to_points(Data, DbName, Rest, [Point | ResultPointsAcc], ErrorPointsAcc);
        {error, Reason} ->
            lines_to_points(Data, DbName, Rest, ResultPointsAcc, [
                {error, Reason} | ErrorPointsAcc
            ])
    end.

line_to_point(
    Data,
    DbName,
    #{
        measurement := Measurement,
        tags := Tags,
        fields := Fields,
        timestamp := Ts,
        precision := {_, ToPrecision} = Precision
    } = Item
) ->
    case config_to_data(Data, Tags) of
        {ok, EncodedTags} ->
            case config_to_data(Data, Fields) of
                {ok, EncodedFields} ->
                    case convert_timestamp(Ts, Precision) of
                        {ok, Timestamp} ->
                            TableName = emqx_placeholder:proc_tmpl(Measurement, Data),
                            Metric = #{
                                dbname => DbName, table => TableName, timeunit => ToPrecision
                            },
                            {ok,
                                {Metric, [
                                    maps:without([precision, measurement], Item#{
                                        tags => EncodedTags,
                                        fields => EncodedFields,
                                        timestamp => Timestamp
                                    })
                                ]}};
                        {error, _} = Error ->
                            Error
                    end;
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

maybe_convert_time_unit(Ts, {FromPrecision, ToPrecision}) ->
    erlang:convert_time_unit(Ts, time_unit(FromPrecision), time_unit(ToPrecision)).

convert_timestamp(Ts, Precision) ->
    Timestamp = maybe_convert_time_unit(Ts, Precision),
    case is_int64(Timestamp) of
        true -> {ok, Timestamp};
        false -> {error, {bad_timestamp, Ts}}
    end.

time_unit(s) -> second;
time_unit(ms) -> millisecond;
time_unit(us) -> microsecond;
time_unit(ns) -> nanosecond.

config_to_data(Data, Config) ->
    maps:fold(
        fun(K, V, Acc) -> maps_config_to_data(K, V, Data, Acc) end,
        {ok, #{}},
        Config
    ).

maps_config_to_data(_K, _V, _Data, {error, _} = Error) ->
    Error;
maps_config_to_data(K, V, Data, {ok, Res}) ->
    KTransOptions = #{return => rawlist, var_trans => fun key_filter/1},
    VTransOptions = #{return => rawlist, var_trans => fun data_filter/1},
    NK0 = emqx_placeholder:proc_tmpl(K, Data, KTransOptions),
    NV = proc_quoted(V, Data, VTransOptions),
    case {NK0, NV} of
        {[undefined], _} ->
            {ok, Res};
        %% undefined value in normal format [undefined] or int/uint format [undefined, <<"i">>]
        {_, [undefined | _]} ->
            {ok, Res};
        _ ->
            NK = list_to_binary(NK0),
            case value_type(NV) of
                {ok, Value} ->
                    {ok, Res#{NK => Value}};
                {error, _} = Error ->
                    Error
            end
    end.

value_type([Int, <<"i">>]) when
    is_integer(Int), Int >= ?INT64_MIN, Int =< ?INT64_MAX
->
    {ok, greptimedb_values:int64_value(Int)};
value_type([Invalid, <<"i">>]) ->
    {error, {invalid_integer_value, Invalid}};
value_type([UInt, <<"u">>]) when
    is_integer(UInt), UInt >= 0, UInt =< ?UINT64_MAX
->
    {ok, greptimedb_values:uint64_value(UInt)};
value_type([Invalid, <<"u">>]) ->
    {error, {invalid_unsigned_integer_value, Invalid}};
value_type([<<"t">>]) ->
    {ok, greptimedb_values:boolean_value(true)};
value_type([<<"T">>]) ->
    {ok, greptimedb_values:boolean_value(true)};
value_type([true]) ->
    {ok, greptimedb_values:boolean_value(true)};
value_type([<<"TRUE">>]) ->
    {ok, greptimedb_values:boolean_value(true)};
value_type([<<"True">>]) ->
    {ok, greptimedb_values:boolean_value(true)};
value_type([<<"f">>]) ->
    {ok, greptimedb_values:boolean_value(false)};
value_type([<<"F">>]) ->
    {ok, greptimedb_values:boolean_value(false)};
value_type([false]) ->
    {ok, greptimedb_values:boolean_value(false)};
value_type([<<"FALSE">>]) ->
    {ok, greptimedb_values:boolean_value(false)};
value_type([<<"False">>]) ->
    {ok, greptimedb_values:boolean_value(false)};
value_type([Float]) when is_float(Float) ->
    {ok, Float};
value_type([Int]) when is_integer(Int) ->
    try
        {ok, greptimedb_values:float64_value(float(Int))}
    catch
        error:badarg ->
            {error, {invalid_float_value, Int}}
    end;
value_type(Val0) ->
    try unicode:characters_to_binary(Val0, utf8) of
        Val1 when is_binary(Val1) ->
            {ok, greptimedb_values:string_value([Val1])};
        _Error ->
            {error, {invalid_string_value, Val0}}
    catch
        error:badarg ->
            {error, {invalid_string_value, Val0}}
    end.

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
                msg => "greptimedb_trans_point_failed",
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

is_int64(Int) ->
    is_integer(Int) andalso Int >= ?INT64_MIN andalso Int =< ?INT64_MAX.

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

integer_point_validation_test() ->
    Lines = emqx_bridge_influxdb:to_influx_lines(
        <<"rtc,channel=${channel} e2e_delay=${m.e2e_delay}i">>
    ),
    SyntaxLines = to_config(Lines, ms),
    Data0 = #{
        <<"channel">> => <<"repro-channel">>,
        <<"timestamp">> => 1_789_360_000_000
    },
    ValidData = Data0#{<<"m">> => #{<<"e2e_delay">> => 470}},
    InvalidData = Data0#{<<"m">> => #{<<"e2e_delay">> => 470.5}},
    {ok, [{_, [#{fields := Fields}]}]} =
        data_to_points(ValidData, <<"public">>, SyntaxLines),
    ?assertEqual(
        #{value_data => {i64_value, 470}},
        maps:get(<<"e2e_delay">>, Fields)
    ),
    ?assertEqual({ok, #{value_data => {f64_value, 470.0}}}, value_type([470])),
    InvalidErrorPoints = [{error, {invalid_integer_value, 470.5}}],
    ?assertEqual(
        {error, InvalidErrorPoints}, data_to_points(InvalidData, <<"public">>, SyntaxLines)
    ),
    {ok, ValidPoints, BatchResults} =
        parse_batch_data(
            <<"connector:test">>,
            <<"public">>,
            [{channel, ValidData}, {channel, InvalidData}],
            SyntaxLines
        ),
    ?assertMatch([_], ValidPoints),
    InvalidResult = {error, {unrecoverable_error, points_trans_failed}},
    ?assertEqual([valid, InvalidResult], BatchResults),
    ?assertEqual(
        [{ok, written}, InvalidResult],
        merge_batch_result({ok, written}, BatchResults)
    ),
    ?assertEqual(
        {error, {recoverable_error, timeout}},
        merge_batch_result({error, {recoverable_error, timeout}}, BatchResults)
    ),
    Self = self(),
    ReplyFunAndArgs = {fun(Result) -> Self ! Result end, []},
    ok = batch_reply_callback(ReplyFunAndArgs, BatchResults, {ok, written}),
    receive
        CallbackResult ->
            ?assertEqual([{ok, written}, InvalidResult], CallbackResult)
    after 1_000 ->
        error(callback_timeout)
    end,
    AllInvalidBatch = [{channel, InvalidData}, {channel, InvalidData}],
    State = #{
        channels => #{channel => #{write_syntax => SyntaxLines}},
        client => unused,
        dbname => <<"public">>
    },
    SingleInvalidResult = {error, {unrecoverable_error, InvalidErrorPoints}},
    ?assertEqual(
        SingleInvalidResult,
        on_query(<<"connector:test">>, {channel, InvalidData}, State)
    ),
    ?assertEqual(
        SingleInvalidResult,
        on_query_async(
            <<"connector:test">>, {channel, InvalidData}, ReplyFunAndArgs, State
        )
    ),
    ?assertEqual(
        [InvalidResult, InvalidResult],
        on_batch_query(<<"connector:test">>, AllInvalidBatch, State)
    ),
    ?assertEqual(
        ok,
        on_batch_query_async(
            <<"connector:test">>, AllInvalidBatch, ReplyFunAndArgs, State
        )
    ),
    receive
        AllInvalidResult ->
            ?assertEqual([InvalidResult, InvalidResult], AllInvalidResult)
    after 1_000 ->
        error(callback_timeout)
    end,
    ?assertEqual(
        {error, {invalid_string_value, [470.5, <<"x">>]}},
        value_type([470.5, <<"x">>])
    ).

numeric_range_validation_test() ->
    ?assertEqual(
        {ok, #{value_data => {i64_value, ?INT64_MIN}}},
        value_type([?INT64_MIN, <<"i">>])
    ),
    ?assertEqual(
        {ok, #{value_data => {i64_value, ?INT64_MAX}}},
        value_type([?INT64_MAX, <<"i">>])
    ),
    ?assertEqual(
        {error, {invalid_integer_value, ?INT64_MIN - 1}},
        value_type([?INT64_MIN - 1, <<"i">>])
    ),
    ?assertEqual(
        {error, {invalid_integer_value, ?INT64_MAX + 1}},
        value_type([?INT64_MAX + 1, <<"i">>])
    ),
    ?assertEqual(
        {ok, #{value_data => {u64_value, ?UINT64_MAX}}},
        value_type([?UINT64_MAX, <<"u">>])
    ),
    ?assertEqual(
        {error, {invalid_unsigned_integer_value, -1}},
        value_type([-1, <<"u">>])
    ),
    ?assertEqual(
        {error, {invalid_unsigned_integer_value, ?UINT64_MAX + 1}},
        value_type([?UINT64_MAX + 1, <<"u">>])
    ),
    TooLargeForFloat64 = 1 bsl 1024,
    ?assertEqual(
        {error, {invalid_float_value, TooLargeForFloat64}},
        value_type([TooLargeForFloat64])
    ),
    ?assertEqual({ok, ?INT64_MIN}, convert_timestamp(?INT64_MIN, {ns, ns})),
    ?assertEqual({ok, ?INT64_MAX}, convert_timestamp(?INT64_MAX, {ns, ns})),
    ?assertEqual(
        {error, {bad_timestamp, ?INT64_MIN - 1}},
        convert_timestamp(?INT64_MIN - 1, {ns, ns})
    ),
    ?assertEqual(
        {error, {bad_timestamp, ?INT64_MAX + 1}},
        convert_timestamp(?INT64_MAX + 1, {ns, ns})
    ),
    MillisecondOverflow = ?INT64_MAX div 1_000_000 + 1,
    ?assertEqual(
        {error, {bad_timestamp, MillisecondOverflow}},
        convert_timestamp(MillisecondOverflow, {ms, ns})
    ).

%% for coverage
desc_test_() ->
    [
        ?_assertMatch(
            {desc, _, _},
            desc(common)
        ),
        ?_assertMatch(
            {desc, _, _},
            desc(greptimedb)
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
