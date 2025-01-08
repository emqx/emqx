%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_influxdb_connector).

-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx_connector/include/emqx_connector.hrl").

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
-export([reply_callback/2]).

-export([
    roots/0,
    namespace/0,
    fields/1,
    desc/1
]).

-export([transform_bridge_v1_config_to_connector_config/1]).

-export([precision_field/0]).

%% only for test
-export([is_unrecoverable_error/1]).

-type ts_precision() :: ns | us | ms | s.

%% Allocatable resources
-define(influx_client, influx_client).

-define(INFLUXDB_DEFAULT_PORT, 8086).

%% influxdb servers don't need parse
-define(INFLUXDB_HOST_OPTIONS, #{
    default_port => ?INFLUXDB_DEFAULT_PORT
}).

-define(DEFAULT_TIMESTAMP_TMPL, "${timestamp}").

-define(set_tag, set_tag).
-define(set_field, set_field).

-define(IS_HTTP_ERROR(STATUS_CODE),
    (is_integer(STATUS_CODE) andalso
        (STATUS_CODE < 200 orelse STATUS_CODE >= 300))
).

-define(DEFAULT_POOL_SIZE, 8).

%% -------------------------------------------------------------------------------------------------
%% resource callback
resource_type() -> influxdb.

callback_mode() -> async_if_possible.

on_add_channel(
    _InstanceId,
    #{channels := Channels, client := Client} = OldState,
    ChannelId,
    #{parameters := Parameters} = ChannelConfig0
) ->
    #{write_syntax := WriteSytaxTmpl} = Parameters,
    Precision = maps:get(precision, Parameters, ms),
    ChannelConfig = maps:merge(
        Parameters,
        ChannelConfig0#{
            channel_client => influxdb:update_precision(Client, Precision),
            write_syntax => to_config(WriteSytaxTmpl, Precision)
        }
    ),
    {ok, OldState#{
        channels => maps:put(ChannelId, ChannelConfig, Channels)
    }}.

on_remove_channel(_InstanceId, #{channels := Channels} = State, ChannelId) ->
    NewState = State#{channels => maps:remove(ChannelId, Channels)},
    {ok, NewState}.

on_get_channel_status(InstanceId, _ChannelId, State) ->
    case on_get_status(InstanceId, State) of
        connected -> connected;
        _ -> connecting
    end.

on_get_channels(InstanceId) ->
    emqx_bridge_v2:get_channels_for_connector(InstanceId).

on_start(InstId, Config) ->
    %% InstID as pool would be handled by influxdb client
    %% so there is no need to allocate pool_name here
    %% ehttpc for influxdb-v1/v2,
    %% ecpool for influxdb-udp
    %% See: influxdb:start_client/1
    start_client(InstId, Config).

on_stop(InstId, _State) ->
    case emqx_resource:get_allocated_resources(InstId) of
        #{?influx_client := Client} ->
            Res = influxdb:stop_client(Client),
            ?tp(influxdb_client_stopped, #{instance_id => InstId}),
            Res;
        _ ->
            ok
    end.

on_query(InstId, {Channel, Message}, #{channels := ChannelConf}) ->
    #{write_syntax := SyntaxLines} = maps:get(Channel, ChannelConf),
    #{channel_client := Client} = maps:get(Channel, ChannelConf),
    case data_to_points(Message, SyntaxLines) of
        {ok, Points} ->
            ?tp(
                influxdb_connector_send_query,
                #{points => Points, batch => false, mode => sync}
            ),
            do_query(InstId, Channel, Client, Points);
        {error, ErrorPoints} ->
            ?tp(
                influxdb_connector_send_query_error,
                #{batch => false, mode => sync, error => ErrorPoints}
            ),
            log_error_points(InstId, ErrorPoints),
            {error, {unrecoverable_error, ErrorPoints}}
    end.

%% Once a Batched Data trans to points failed.
%% This batch query failed
on_batch_query(InstId, BatchData, #{channels := ChannelConf}) ->
    [{Channel, _} | _] = BatchData,
    #{write_syntax := SyntaxLines} = maps:get(Channel, ChannelConf),
    #{channel_client := Client} = maps:get(Channel, ChannelConf),
    case parse_batch_data(InstId, BatchData, SyntaxLines) of
        {ok, Points} ->
            ?tp(
                influxdb_connector_send_query,
                #{points => Points, batch => true, mode => sync}
            ),
            do_query(InstId, Channel, Client, Points);
        {error, Reason} ->
            ?tp(
                influxdb_connector_send_query_error,
                #{batch => true, mode => sync, error => Reason}
            ),
            {error, {unrecoverable_error, Reason}}
    end.

on_query_async(
    InstId,
    {Channel, Message},
    {ReplyFun, Args},
    #{channels := ChannelConf}
) ->
    #{write_syntax := SyntaxLines} = maps:get(Channel, ChannelConf),
    #{channel_client := Client} = maps:get(Channel, ChannelConf),
    case data_to_points(Message, SyntaxLines) of
        {ok, Points} ->
            ?tp(
                influxdb_connector_send_query,
                #{points => Points, batch => false, mode => async}
            ),
            do_async_query(InstId, Channel, Client, Points, {ReplyFun, Args});
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
    #{channels := ChannelConf}
) ->
    [{Channel, _} | _] = BatchData,
    #{write_syntax := SyntaxLines} = maps:get(Channel, ChannelConf),
    #{channel_client := Client} = maps:get(Channel, ChannelConf),
    case parse_batch_data(InstId, BatchData, SyntaxLines) of
        {ok, Points} ->
            ?tp(
                influxdb_connector_send_query,
                #{points => Points, batch => true, mode => async}
            ),
            do_async_query(InstId, Channel, Client, Points, {ReplyFun, Args});
        {error, Reason} ->
            ?tp(
                influxdb_connector_send_query_error,
                #{batch => true, mode => async, error => Reason}
            ),
            {error, {unrecoverable_error, Reason}}
    end.

on_format_query_result(Result) ->
    emqx_bridge_http_connector:on_format_query_result(Result).

on_get_status(_InstId, #{client := Client}) ->
    case influxdb:is_alive(Client) andalso ok =:= influxdb:check_auth(Client) of
        true ->
            ?status_connected;
        false ->
            ?status_disconnected
    end.

transform_bridge_v1_config_to_connector_config(BridgeV1Config) ->
    IndentKeys = [username, password, database, token, bucket, org],
    ConnConfig0 = maps:without([write_syntax, precision], BridgeV1Config),
    ConnConfig1 =
        case emqx_utils_maps:indent(parameters, IndentKeys, ConnConfig0) of
            #{parameters := #{database := _} = Params} = Conf ->
                Conf#{parameters => Params#{influxdb_type => influxdb_api_v1}};
            #{parameters := #{bucket := _} = Params} = Conf ->
                Conf#{parameters => Params#{influxdb_type => influxdb_api_v2}}
        end,
    emqx_utils_maps:update_if_present(
        resource_opts,
        fun emqx_connector_schema:project_to_connector_resource_opts/1,
        ConnConfig1
    ).

%% -------------------------------------------------------------------------------------------------
%% schema
namespace() -> connector_influxdb.

roots() ->
    [
        {config, #{
            type => hoconsc:ref(?MODULE, "connector")
        }}
    ].

fields("connector") ->
    [
        server_field(),
        pool_size_field(),
        parameter_field()
    ] ++ emqx_connector_schema_lib:ssl_fields();
fields("connector_influxdb_api_v1") ->
    [influxdb_type_field(influxdb_api_v1) | influxdb_api_v1_fields()];
fields("connector_influxdb_api_v2") ->
    [influxdb_type_field(influxdb_api_v2) | influxdb_api_v2_fields()];
%% ============ begin: schema for old bridge configs ============
fields(influxdb_api_v1) ->
    fields(common) ++ influxdb_api_v1_fields();
fields(influxdb_api_v2) ->
    fields(common) ++ influxdb_api_v2_fields();
fields(common) ->
    [
        server_field(),
        precision_field(),
        pool_size_field()
    ] ++ emqx_connector_schema_lib:ssl_fields().
%% ============ end: schema for old bridge configs ============

influxdb_type_field(Type) ->
    {influxdb_type, #{
        required => true,
        type => Type,
        default => Type,
        desc => ?DESC(atom_to_list(Type))
    }}.

server_field() ->
    {server, server()}.

precision_field() ->
    {precision,
        %% The influxdb only supports these 4 precision:
        %% See "https://github.com/influxdata/influxdb/blob/
        %% 6b607288439a991261307518913eb6d4e280e0a7/models/points.go#L487" for
        %% more information.
        mk(enum([ns, us, ms, s]), #{
            required => false, default => ms, desc => ?DESC("precision")
        })}.

pool_size_field() ->
    {pool_size,
        mk(
            integer(),
            #{
                required => false,
                default => ?DEFAULT_POOL_SIZE,
                desc => ?DESC("pool_size")
            }
        )}.

parameter_field() ->
    {parameters,
        mk(
            hoconsc:union([
                ref(?MODULE, "connector_" ++ T)
             || T <- ["influxdb_api_v1", "influxdb_api_v2"]
            ]),
            #{required => true, desc => ?DESC("influxdb_parameters")}
        )}.

influxdb_api_v1_fields() ->
    [
        {database, mk(binary(), #{required => true, desc => ?DESC("database")})},
        {username, mk(binary(), #{desc => ?DESC("username")})},
        {password, emqx_schema_secret:mk(#{desc => ?DESC("password")})}
    ].

influxdb_api_v2_fields() ->
    [
        {bucket, mk(binary(), #{required => true, desc => ?DESC("bucket")})},
        {org, mk(binary(), #{required => true, desc => ?DESC("org")})},
        {token, emqx_schema_secret:mk(#{required => true, desc => ?DESC("token")})}
    ].

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
desc(parameters) ->
    ?DESC("influxdb_parameters");
desc("influxdb_parameters") ->
    ?DESC("influxdb_parameters");
desc(influxdb_api_v1) ->
    ?DESC("influxdb_api_v1");
desc(influxdb_api_v2) ->
    ?DESC("influxdb_api_v2");
desc("connector") ->
    ?DESC("connector");
desc("connector_influxdb_api_v1") ->
    ?DESC("influxdb_api_v1");
desc("connector_influxdb_api_v2") ->
    ?DESC("influxdb_api_v2").

%% -------------------------------------------------------------------------------------------------
%% internal functions

start_client(InstId, Config) ->
    ClientConfig = client_config(InstId, Config),
    ?SLOG(info, #{
        msg => "starting_influxdb_connector",
        connector => InstId,
        config => emqx_utils:redact(Config),
        client_config => emqx_utils:redact(ClientConfig)
    }),
    try do_start_client(InstId, ClientConfig, Config) of
        Res = {ok, #{client := Client}} ->
            ok = emqx_resource:allocate_resource(InstId, ?influx_client, Client),
            Res;
        {error, Reason} ->
            {error, Reason}
    catch
        E:R:S ->
            ?tp(influxdb_connector_start_exception, #{error => {E, R}}),
            ?SLOG(warning, #{
                msg => "start_influxdb_connector_error",
                connector => InstId,
                error => E,
                reason => R,
                stack => S
            }),
            {error, R}
    end.

do_start_client(InstId, ClientConfig, Config) ->
    case influxdb:start_client(ClientConfig) of
        {ok, Client} ->
            case influxdb:is_alive(Client, true) of
                true ->
                    case influxdb:check_auth(Client) of
                        ok ->
                            State = #{client => Client, channels => #{}},
                            ?SLOG(info, #{
                                msg => "starting_influxdb_connector_success",
                                connector => InstId,
                                client => redact_auth(Client),
                                state => redact_auth(State)
                            }),
                            {ok, State};
                        Error ->
                            ?tp(influxdb_connector_start_failed, #{error => auth_error}),
                            ?SLOG(warning, #{
                                msg => "failed_to_start_influxdb_connector",
                                error => Error,
                                connector => InstId,
                                client => redact_auth(Client),
                                reason => auth_error
                            }),
                            %% no leak
                            _ = influxdb:stop_client(Client),
                            {error, connect_ok_but_auth_failed}
                    end;
                {false, Reason} ->
                    ?tp(influxdb_connector_start_failed, #{
                        error => influxdb_client_not_alive, reason => Reason
                    }),
                    ?SLOG(warning, #{
                        msg => "failed_to_start_influxdb_connector",
                        connector => InstId,
                        client => redact_auth(Client),
                        reason => Reason
                    }),
                    %% no leak
                    _ = influxdb:stop_client(Client),
                    {error, {connect_failed, Reason}}
            end;
        {error, {already_started, Client0}} ->
            ?tp(influxdb_connector_start_already_started, #{}),
            ?SLOG(info, #{
                msg => "restarting_influxdb_connector_found_already_started_client",
                connector => InstId,
                old_client => redact_auth(Client0)
            }),
            _ = influxdb:stop_client(Client0),
            do_start_client(InstId, ClientConfig, Config);
        {error, Reason} ->
            ?tp(influxdb_connector_start_failed, #{error => Reason}),
            ?SLOG(warning, #{
                msg => "failed_to_start_influxdb_connector",
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
    #{hostname := Host, port := Port} = emqx_schema:parse_server(Server, ?INFLUXDB_HOST_OPTIONS),
    [
        {host, str(Host)},
        {port, Port},
        {pool_size, maps:get(pool_size, Config, ?DEFAULT_POOL_SIZE)},
        {pool, InstId}
    ] ++ protocol_config(Config).

%% api v1 config
protocol_config(#{
    parameters := #{influxdb_type := influxdb_api_v1, database := DB} = Params, ssl := SSL
}) ->
    [
        {protocol, http},
        {version, v1},
        {database, str(DB)}
    ] ++ username(Params) ++ password(Params) ++ ssl_config(SSL);
%% api v2 config
protocol_config(#{
    parameters := #{influxdb_type := influxdb_api_v2, bucket := Bucket, org := Org, token := Token},
    ssl := SSL
}) ->
    [
        {protocol, http},
        {version, v2},
        {bucket, str(Bucket)},
        {org, str(Org)},
        %% TODO: teach `influxdb` to accept 0-arity closures as passwords.
        {token, emqx_secret:unwrap(Token)}
    ] ++ ssl_config(SSL).

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

username(#{username := Username}) ->
    [{username, str(Username)}];
username(_) ->
    [].

password(#{password := Password}) ->
    %% TODO: teach `influxdb` to accept 0-arity closures as passwords.
    [{password, str(emqx_secret:unwrap(Password))}];
password(_) ->
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
    emqx_trace:rendered_action_template(Channel, #{points => Points, is_async => false}),
    case influxdb:write(Client, Points) of
        ok ->
            ?SLOG(debug, #{
                msg => "influxdb_write_point_success",
                connector => InstId,
                points => Points
            });
        {error, {401, _, _}} ->
            ?tp(influxdb_connector_do_query_failure, #{error => <<"authorization failure">>}),
            ?SLOG(error, #{
                msg => "influxdb_authorization_failed",
                client => redact_auth(Client),
                connector => InstId
            }),
            {error, {unrecoverable_error, <<"authorization failure">>}};
        {error, Reason} = Err ->
            ?tp(influxdb_connector_do_query_failure, #{error => Reason}),
            ?SLOG(error, #{
                msg => "influxdb_write_point_failed",
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

do_async_query(InstId, Channel, Client, Points, ReplyFunAndArgs) ->
    ?SLOG(info, #{
        msg => "influxdb_write_point_async",
        connector => InstId,
        points => Points
    }),
    emqx_trace:rendered_action_template(Channel, #{points => Points, is_async => true}),
    WrappedReplyFunAndArgs = {fun ?MODULE:reply_callback/2, [ReplyFunAndArgs]},
    {ok, _WorkerPid} = influxdb:write_async(Client, Points, WrappedReplyFunAndArgs).

reply_callback(ReplyFunAndArgs, {error, Reason} = Error) ->
    case is_unrecoverable_error(Error) of
        true ->
            Result = {error, {unrecoverable_error, Reason}},
            emqx_resource:apply_reply_fun(ReplyFunAndArgs, Result);
        false ->
            Result = {error, {recoverable_error, Reason}},
            emqx_resource:apply_reply_fun(ReplyFunAndArgs, Result)
    end;
reply_callback(ReplyFunAndArgs, {ok, 401, _, _}) ->
    ?tp(influxdb_connector_do_query_failure, #{error => <<"authorization failure">>}),
    Result = {error, {unrecoverable_error, <<"authorization failure">>}},
    emqx_resource:apply_reply_fun(ReplyFunAndArgs, Result);
reply_callback(ReplyFunAndArgs, {ok, Code, _, Body}) when ?IS_HTTP_ERROR(Code) ->
    ?tp(influxdb_connector_do_query_failure, #{error => Body}),
    Result = {error, {unrecoverable_error, Body}},
    emqx_resource:apply_reply_fun(ReplyFunAndArgs, Result);
reply_callback(ReplyFunAndArgs, Result) ->
    ?tp(influxdb_connector_do_query_ok, #{result => Result}),
    emqx_resource:apply_reply_fun(ReplyFunAndArgs, Result).

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
    Res#{NK => preproc_quoted(V)}.

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
parse_batch_data(InstId, BatchData, SyntaxLines) ->
    {Points, Errors} = lists:foldl(
        fun({_, Data}, {ListOfPoints, ErrAccIn}) ->
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
                msg => "influxdb_trans_point_failed",
                error_count => Errors,
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
            {error, {non_integer_timestamp, TsBin}}
    end;
parse_timestamp(InvalidTs) ->
    %% The timestamp field must be a single integer or a single placeholder. i.e. the
    %%   following is not allowed:
    %%   - weather,location=us-midwest,season=summer temperature=82 ${timestamp}00
    {error, {unsupported_placeholder_usage_for_timestamp, InvalidTs}}.

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
        fields := Fields,
        timestamp := Ts,
        precision := Precision
    } = Item
) ->
    {_, EncodedTags, _} = maps:fold(fun maps_config_to_data/3, {Data, #{}, ?set_tag}, Tags),
    {_, EncodedFields, _} = maps:fold(fun maps_config_to_data/3, {Data, #{}, ?set_field}, Fields),
    maps:without([precision], Item#{
        measurement => emqx_placeholder:proc_tmpl(Measurement, Data),
        tags => EncodedTags,
        fields => EncodedFields,
        timestamp => maybe_convert_time_unit(Ts, Precision)
    }).

maybe_convert_time_unit(Ts, {FromPrecision, ToPrecision}) ->
    erlang:convert_time_unit(Ts, time_unit(FromPrecision), time_unit(ToPrecision)).

time_unit(s) -> second;
time_unit(ms) -> millisecond;
time_unit(us) -> microsecond;
time_unit(ns) -> nanosecond.

maps_config_to_data(K, V, {Data, Res, SetType}) ->
    KTransOptions = #{return => rawlist, var_trans => fun key_filter/1},
    VTransOptions = #{return => rawlist, var_trans => fun data_filter/1},
    NK = emqx_placeholder:proc_tmpl(K, Data, KTransOptions),
    NV = proc_quoted(V, Data, VTransOptions),
    case {NK, NV} of
        {[undefined], _} ->
            {Data, Res, SetType};
        %% undefined value in normal format [undefined] or int/uint format [undefined, <<"i">>]
        {_, [undefined | _]} ->
            {Data, Res, SetType};
        {_, {quoted, [undefined | _]}} ->
            {Data, Res, SetType};
        _ ->
            NRes = Res#{
                list_to_binary(NK) => value_type(NV, #{
                    tmpl_type => tmpl_type(V), set_type => SetType
                })
            },
            {Data, NRes, SetType}
    end.

value_type([Number], #{set_type := ?set_tag}) when is_number(Number) ->
    %% all `tag` values are treated as string
    %% See also: https://docs.influxdata.com/influxdb/v2/reference/syntax/line-protocol/#tag-set
    emqx_utils_conv:bin(Number);
value_type([Str], #{set_type := ?set_tag}) when is_binary(Str) ->
    Str;
value_type({quoted, ValList}, _) ->
    {string_list, ValList};
value_type([Int, <<"i">>], #{tmpl_type := mixed}) when is_integer(Int) ->
    {int, Int};
value_type([UInt, <<"u">>], #{tmpl_type := mixed}) when is_integer(UInt) ->
    {uint, UInt};
%% write `1`, `1.0`, `-1.0` all as float
%% see also: https://docs.influxdata.com/influxdb/v2.7/reference/syntax/line-protocol/#float
value_type([Number], #{set_type := ?set_field}) when is_number(Number) ->
    {float, Number};
value_type([<<"t">>], _) ->
    't';
value_type([<<"T">>], _) ->
    'T';
value_type([true], _) ->
    'true';
value_type([<<"TRUE">>], _) ->
    'TRUE';
value_type([<<"True">>], _) ->
    'True';
value_type([<<"f">>], _) ->
    'f';
value_type([<<"F">>], _) ->
    'F';
value_type([false], _) ->
    'false';
value_type([<<"FALSE">>], _) ->
    'FALSE';
value_type([<<"False">>], _) ->
    'False';
value_type([Str], #{tmpl_type := variable}) when is_binary(Str) ->
    Str;
value_type([Str], #{tmpl_type := literal, set_type := ?set_field}) when is_binary(Str) ->
    %% if Str is a literal string suffixed with `i` or `u`, we should convert it to int/uint.
    %% otherwise, we should convert it to float.
    NumStr = binary:part(Str, 0, byte_size(Str) - 1),
    case binary:part(Str, byte_size(Str), -1) of
        <<"i">> ->
            maybe_convert_to_integer(NumStr, Str, int);
        <<"u">> ->
            maybe_convert_to_integer(NumStr, Str, uint);
        _ ->
            maybe_convert_to_float_str(Str)
    end;
value_type(Str, _) ->
    Str.

tmpl_type([{str, _}]) ->
    literal;
tmpl_type([{var, _}]) ->
    variable;
tmpl_type(_) ->
    mixed.

maybe_convert_to_integer(NumStr, String, Type) ->
    try
        Int = binary_to_integer(NumStr),
        {Type, Int}
    catch
        error:badarg ->
            maybe_convert_to_integer_f(NumStr, String, Type)
    end.

maybe_convert_to_integer_f(NumStr, String, Type) ->
    try
        Float = binary_to_float(NumStr),
        {Type, erlang:floor(Float)}
    catch
        error:badarg ->
            String
    end.

maybe_convert_to_float_str(NumStr) ->
    try
        _ = binary_to_float(NumStr),
        %% NOTE: return a {float, String} to avoid precision loss when converting to float
        {float, NumStr}
    catch
        error:badarg ->
            maybe_convert_to_float_str_i(NumStr)
    end.

maybe_convert_to_float_str_i(NumStr) ->
    try
        _ = binary_to_integer(NumStr),
        {float, NumStr}
    catch
        error:badarg ->
            NumStr
    end.

key_filter(undefined) -> undefined;
key_filter(Value) -> bin(Value).

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
                msg => "influxdb_trans_point_failed",
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
convert_server(Server0, HoconOpts) ->
    Server = string:trim(Server0, trailing, "/"),
    emqx_schema:convert_servers(Server, HoconOpts).

str(A) when is_atom(A) ->
    atom_to_list(A);
str(B) when is_binary(B) ->
    binary_to_list(B);
str(S) when is_list(S) ->
    S.

is_unrecoverable_error({error, {unrecoverable_error, _}}) ->
    true;
is_unrecoverable_error({error, {Code, _}}) when ?IS_HTTP_ERROR(Code) ->
    true;
is_unrecoverable_error({error, {Code, _, _Body}}) when ?IS_HTTP_ERROR(Code) ->
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
