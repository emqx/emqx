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
            write_syntax =>
                emqx_bridge_greptimedb_utils:parse_write_syntax(WriteSyntaxTmpl, Precision)
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
    case parse_single_data(InstId, DbName, Message, SyntaxLines) of
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
            {error, {unrecoverable_error, ErrorPoints}}
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
            %% Some messages transformed; a batch where none did is an error.
            ?tp(
                greptimedb_connector_send_query,
                #{points => Points, batch => true, mode => sync}
            ),
            Result = do_query(InstId, Channel, Client, Points),
            merge_batch_result(Result, BatchResults);
        {error, Reason} ->
            ?tp(
                greptimedb_connector_send_query_error,
                #{batch => true, mode => sync, error => Reason}
            ),
            {error, {unrecoverable_error, Reason}}
    end.

on_query_async(InstId, {Channel, Message}, {ReplyFun, Args}, State) ->
    #{
        channels := #{Channel := #{write_syntax := SyntaxLines}},
        client := Client,
        dbname := DbName
    } = State,
    case parse_single_data(InstId, DbName, Message, SyntaxLines) of
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
            {error, {unrecoverable_error, ErrorPoints}}
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
            %% Some messages transformed; a batch where none did is an error.
            ?tp(
                greptimedb_connector_send_query,
                #{points => Points, batch => true, mode => async}
            ),
            do_async_batch_query(Channel, Client, Points, BatchResults, ReplyFunAndArgs);
        {error, Reason} ->
            ?tp(
                greptimedb_connector_send_query_error,
                #{batch => true, mode => async, error => Reason}
            ),
            {error, {unrecoverable_error, Reason}}
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
        ttl_field(),
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
        ttl_field(),
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

ttl_field() ->
    {ttl,
        mk(binary(), #{
            required => false,
            desc => ?DESC("ttl")
        })}.

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

%% -------------------------------------------------------------------------------------------------
%% Tags & Fields Data Trans

parse_batch_data(InstId, DbName, BatchData, SyntaxLines) ->
    emqx_bridge_greptimedb_utils:parse_batch_data(InstId, DbName, BatchData, SyntaxLines, erlang).

parse_single_data(InstId, DbName, Data, SyntaxLines) ->
    emqx_bridge_greptimedb_utils:parse_single_data(InstId, DbName, Data, SyntaxLines, erlang).

%% helper funcs

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

mk(Type, Sc) -> hoconsc:mk(Type, Sc).
enum(Types) -> hoconsc:enum(Types).

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
    SyntaxLines = emqx_bridge_greptimedb_utils:parse_write_syntax(Lines, ms),
    Data0 = #{
        <<"channel">> => <<"repro-channel">>,
        <<"timestamp">> => 1_789_360_000_000
    },
    ValidData = Data0#{<<"m">> => #{<<"e2e_delay">> => 470}},
    InvalidData = Data0#{<<"m">> => #{<<"e2e_delay">> => 470.5}},
    {ok, [{_, [#{fields := Fields}]}]} =
        emqx_bridge_greptimedb_utils:data_to_points(ValidData, <<"public">>, SyntaxLines, erlang),
    ?assertEqual(
        #{value_data => {i64_value, 470}},
        maps:get(<<"e2e_delay">>, Fields)
    ),
    ?assertEqual(
        {ok, #{value_data => {f64_value, 470.0}}}, emqx_bridge_greptimedb_utils:value_type([470])
    ),
    InvalidErrorPoints = [{error, {invalid_integer_value, 470.5}}],
    ?assertEqual(
        {error, InvalidErrorPoints},
        emqx_bridge_greptimedb_utils:data_to_points(InvalidData, <<"public">>, SyntaxLines, erlang)
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
    %% This branch reports a transformation failure as `points_trans_failed'
    %% with the last error, rather than the list of error points.
    TransFailed = {points_trans_failed, #{last_error => {invalid_integer_value, 470.5}}},
    SingleInvalidResult = {error, {unrecoverable_error, TransFailed}},
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
    %% Nothing in the batch transformed, so the batch fails as a whole.
    ?assertEqual(
        SingleInvalidResult,
        on_batch_query(<<"connector:test">>, AllInvalidBatch, State)
    ),
    ?assertEqual(
        SingleInvalidResult,
        on_batch_query_async(
            <<"connector:test">>, AllInvalidBatch, ReplyFunAndArgs, State
        )
    ),
    ?assertEqual(
        {error, {invalid_string_value, [470.5, <<"x">>]}},
        emqx_bridge_greptimedb_utils:value_type([470.5, <<"x">>])
    ).

numeric_range_validation_test() ->
    ?assertEqual(
        {ok, #{value_data => {i64_value, ?INT64_MIN}}},
        emqx_bridge_greptimedb_utils:value_type([?INT64_MIN, <<"i">>])
    ),
    ?assertEqual(
        {ok, #{value_data => {i64_value, ?INT64_MAX}}},
        emqx_bridge_greptimedb_utils:value_type([?INT64_MAX, <<"i">>])
    ),
    ?assertEqual(
        {error, {invalid_integer_value, ?INT64_MIN - 1}},
        emqx_bridge_greptimedb_utils:value_type([?INT64_MIN - 1, <<"i">>])
    ),
    ?assertEqual(
        {error, {invalid_integer_value, ?INT64_MAX + 1}},
        emqx_bridge_greptimedb_utils:value_type([?INT64_MAX + 1, <<"i">>])
    ),
    ?assertEqual(
        {ok, #{value_data => {u64_value, ?UINT64_MAX}}},
        emqx_bridge_greptimedb_utils:value_type([?UINT64_MAX, <<"u">>])
    ),
    ?assertEqual(
        {error, {invalid_unsigned_integer_value, -1}},
        emqx_bridge_greptimedb_utils:value_type([-1, <<"u">>])
    ),
    ?assertEqual(
        {error, {invalid_unsigned_integer_value, ?UINT64_MAX + 1}},
        emqx_bridge_greptimedb_utils:value_type([?UINT64_MAX + 1, <<"u">>])
    ),
    TooLargeForFloat64 = 1 bsl 1024,
    ?assertEqual(
        {error, {invalid_float_value, TooLargeForFloat64}},
        emqx_bridge_greptimedb_utils:value_type([TooLargeForFloat64])
    ),
    ?assertEqual(
        {ok, ?INT64_MIN}, emqx_bridge_greptimedb_utils:convert_timestamp(?INT64_MIN, {ns, ns})
    ),
    ?assertEqual(
        {ok, ?INT64_MAX}, emqx_bridge_greptimedb_utils:convert_timestamp(?INT64_MAX, {ns, ns})
    ),
    ?assertEqual(
        {error, {bad_timestamp, ?INT64_MIN - 1}},
        emqx_bridge_greptimedb_utils:convert_timestamp(?INT64_MIN - 1, {ns, ns})
    ),
    ?assertEqual(
        {error, {bad_timestamp, ?INT64_MAX + 1}},
        emqx_bridge_greptimedb_utils:convert_timestamp(?INT64_MAX + 1, {ns, ns})
    ),
    MillisecondOverflow = ?INT64_MAX div 1_000_000 + 1,
    ?assertEqual(
        {error, {bad_timestamp, MillisecondOverflow}},
        emqx_bridge_greptimedb_utils:convert_timestamp(MillisecondOverflow, {ms, ns})
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
