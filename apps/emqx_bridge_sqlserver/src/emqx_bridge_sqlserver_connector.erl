%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_sqlserver_connector).

-feature(maybe_expr, enable).

-behaviour(emqx_resource).

-include("emqx_bridge_sqlserver.hrl").

-include_lib("kernel/include/file.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-elvis([{elvis_text_style, line_length, #{skip_comments => whole_line}}]).

%%====================================================================
%% Exports
%%====================================================================

%% Hocon config schema exports
-export([
    roots/0,
    fields/1,
    namespace/0
]).

%% callbacks for behaviour emqx_resource
-export([
    resource_type/0,
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_batch_query/3,
    on_get_status/2,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channels/1,
    on_get_channel_status/3,
    on_format_query_result/1
]).

%% `ecpool_worker' API
-export([
    connect/1,
    disconnect/1
]).

%% Internal exports used to execute code with ecpool worker
-export([do_get_status/1, worker_do_insert/3, get_servername/1]).

-export([parse_server/1]).

-import(emqx_utils_conv, [str/1]).
-import(hoconsc, [mk/2, enum/1, ref/2]).

-define(ACTION_SEND_MESSAGE, send_message).

-define(SYNC_QUERY_MODE, handover).

%% We use -1 to differentiate between default port and explicitly defined port.
-define(SQLSERVER_HOST_OPTIONS, #{
    default_port => -1
}).

-define(REQUEST_TTL(RESOURCE_OPTS),
    maps:get(request_ttl, RESOURCE_OPTS, ?DEFAULT_REQUEST_TTL)
).

-define(BATCH_INSERT_TEMP, batch_insert_temp).

-define(BATCH_INSERT_PART, batch_insert_part).
-define(BATCH_PARAMS_TOKENS, batch_insert_tks).

-define(FILE_MODE_755, 33261).
%% 32768 + 8#00400 + 8#00200 + 8#00100 + 8#00040 + 8#00010 + 8#00004 + 8#00001
%% See also
%% https://www.erlang.org/doc/man/file.html#read_file_info-2

%% Copied from odbc reference page
%% https://www.erlang.org/doc/man/odbc.html

%% as returned by connect/2
-type connection_reference() :: term().
-type time_out() :: milliseconds() | infinity.
-type sql() :: string() | binary().
-type milliseconds() :: pos_integer().
%% Tuple of column values e.g. one row of the result set.
%% it's a variable size tuple of column values.
-type row() :: tuple().
%% Some kind of explanation of what went wrong
-type common_reason() :: connection_closed | extended_error() | term().
%% extended error type with ODBC
%% and native database error codes, as well as the base reason that would have been
%% returned had extended_errors not been enabled.
-type extended_error() :: {string(), integer(), _Reason :: term()}.
%% Name of column in the result set
-type col_name() :: string().
%% e.g. a list of the names of the selected columns in the result set.
-type col_names() :: [col_name()].
%% A list of rows from the result set.
-type rows() :: list(row()).

%% -type result_tuple() :: {updated, n_rows()} | {selected, col_names(), rows()}.
-type updated_tuple() :: {updated, n_rows()}.
-type selected_tuple() :: {selected, col_names(), rows()}.
%% The number of affected rows for UPDATE,
%% INSERT, or DELETE queries. For other query types the value
%% is driver defined, and hence should be ignored.
-type n_rows() :: integer().

%% These type was not used in this module, but we may use it later
%% -type odbc_data_type() ::
%%     sql_integer
%%     | sql_smallint
%%     | sql_tinyint
%%     | {sql_decimal, precision(), scale()}
%%     | {sql_numeric, precision(), scale()}
%%     | {sql_char, size()}
%%     | {sql_wchar, size()}
%%     | {sql_varchar, size()}
%%     | {sql_wvarchar, size()}
%%     | {sql_float, precision()}
%%     | {sql_wlongvarchar, size()}
%%     | {sql_float, precision()}
%%     | sql_real
%%     | sql_double
%%     | sql_bit
%%     | atom().
%% -type precision() :: integer().
%% -type scale() :: integer().
%% -type size() :: integer().

-type state() :: #{
    installed_channels := map(),
    pool_name := binary(),
    resource_opts := map()
}.

%%====================================================================
%% Configuration and default values
%%====================================================================

namespace() -> sqlserver.

roots() ->
    [{config, #{type => hoconsc:ref(?MODULE, config)}}].

fields(config) ->
    [
        {server, server()}
        | add_default_username(emqx_connector_schema_lib:relational_db_fields())
    ].

add_default_username(Fields) ->
    lists:map(
        fun
            ({username, OrigUsernameFn}) ->
                {username, add_default_fn(OrigUsernameFn, <<"sa">>)};
            (Field) ->
                Field
        end,
        Fields
    ).

add_default_fn(OrigFn, Default) ->
    fun
        (default) -> Default;
        (Field) -> OrigFn(Field)
    end.

server() ->
    hoconsc:mk(
        string(),
        #{
            desc => ?DESC("server"),
            required => true,
            converter => fun emqx_schema:convert_servers/2,
            validator => fun server_validator/1
        }
    ).

server_validator(Str) ->
    BaseValidator = emqx_schema:servers_validator(?SQLSERVER_HOST_OPTIONS, _Required = true),
    ok = BaseValidator(Str),
    _ = parse_server(Str),
    ok.

parse_server(undefined) ->
    undefined;
parse_server(Str) ->
    Parsed = emqx_schema:parse_server(Str, ?SQLSERVER_HOST_OPTIONS),
    split_named_instance(Parsed).

split_named_instance(#{hostname := Hostname0} = Parsed) ->
    HasExplicitPort = maps:get(port, Parsed) =/= -1,
    case string:tokens(Hostname0, "\\") of
        [_] when HasExplicitPort ->
            Parsed;
        [_] ->
            Parsed#{port => ?SQLSERVER_DEFAULT_PORT};
        [Server, InstanceName] when HasExplicitPort ->
            Parsed#{hostname => Server, instance_name => InstanceName};
        [_Server, _InstanceName] ->
            throw("must_explicitly_define_port_when_using_named_instances");
        [_, _, _ | _] ->
            throw("bad_server_name_and_instance_name")
    end.

%%====================================================================
%% Callbacks defined in emqx_resource
%%====================================================================
resource_type() -> sqlserver.

callback_mode() -> always_sync.

on_start(
    InstanceId = PoolName,
    #{
        server := Server,
        username := Username,
        driver := Driver,
        database := Database,
        pool_size := PoolSize,
        resource_opts := ResourceOpts
    } = Config
) ->
    ?SLOG(info, #{
        msg => "starting_sqlserver_connector",
        connector => InstanceId,
        config => emqx_utils:redact(Config)
    }),

    ODBCDir = code:priv_dir(odbc),
    OdbcserverDir = filename:join(ODBCDir, "bin/odbcserver"),
    {ok, Info = #file_info{mode = Mode}} = file:read_file_info(OdbcserverDir),
    case ?FILE_MODE_755 =:= Mode of
        true ->
            ok;
        false ->
            _ = file:write_file_info(OdbcserverDir, Info#file_info{mode = ?FILE_MODE_755}),
            ok
    end,

    %% odbc connection string required
    ServerBin = to_bin(Server),
    ConnectOptions = [
        {server, ServerBin},
        {username, Username},
        {password, maps:get(password, Config, emqx_secret:wrap(""))},
        {driver, Driver},
        {database, Database},
        {pool_size, PoolSize},
        {auto_reconnect, 2},
        {on_disconnect, {?MODULE, disconnect, []}}
    ],
    ParsedServer = parse_server(Server),

    State = #{
        %% also InstanceId
        server => ParsedServer,
        pool_name => PoolName,
        installed_channels => #{},
        resource_opts => ResourceOpts
    },
    case emqx_resource_pool:start(PoolName, ?MODULE, ConnectOptions) of
        ok ->
            {ok, State};
        {error, Reason} ->
            ?tp(
                sqlserver_connector_start_failed,
                #{error => Reason}
            ),
            {error, Reason}
    end.

on_add_channel(_InstId, OldState, ChannelId, #{parameters := Params}) ->
    #{installed_channels := InstalledChannels} = OldState,
    case parse_sql_template(Params) of
        {ok, Templs} ->
            ChannelState = #{
                sql_templates => Templs,
                channel_conf => Params
            },
            NewInstalledChannels = maps:put(ChannelId, ChannelState, InstalledChannels),
            %% Update state
            NewState = OldState#{installed_channels => NewInstalledChannels},
            {ok, NewState};
        {error, Reason} ->
            {error, Reason}
    end.

on_remove_channel(_InstId, #{installed_channels := InstalledChannels} = OldState, ChannelId) ->
    NewInstalledChannels = maps:remove(ChannelId, InstalledChannels),
    %% Update state
    NewState = OldState#{installed_channels => NewInstalledChannels},
    {ok, NewState}.

on_get_channel_status(InstanceId, ChannelId, #{installed_channels := Channels} = State) ->
    case maps:find(ChannelId, Channels) of
        {ok, _} ->
            on_get_status(InstanceId, State);
        error ->
            ?status_disconnected
    end.

on_get_channels(ResId) ->
    emqx_bridge_v2:get_channels_for_connector(ResId).

on_stop(InstanceId, _State) ->
    ?tp(
        sqlserver_connector_on_stop,
        #{instance_id => InstanceId}
    ),
    ?SLOG(info, #{
        msg => "stopping_sqlserver_connector",
        connector => InstanceId
    }),
    emqx_resource_pool:stop(InstanceId).

-spec on_query(
    resource_id(),
    Query :: {channel_id(), map()},
    state()
) ->
    ok
    | {ok, list()}
    | {error, {recoverable_error, term()}}
    | {error, term()}.
on_query(ResourceId, {_ChannelId, _Msg} = Query, State) ->
    ?TRACE(
        "SINGLE_QUERY_SYNC",
        "bridge_sqlserver_received",
        #{requests => Query, connector => ResourceId, state => State}
    ),
    do_query(ResourceId, Query, ?SYNC_QUERY_MODE, State).

-spec on_batch_query(
    resource_id(),
    [{channel_id(), map()}],
    state()
) ->
    ok
    | {ok, list()}
    | {error, {recoverable_error, term()}}
    | {error, term()}.
on_batch_query(ResourceId, BatchRequests, State) ->
    ?TRACE(
        "BATCH_QUERY_SYNC",
        "bridge_sqlserver_received",
        #{requests => BatchRequests, connector => ResourceId, state => State}
    ),
    do_query(ResourceId, BatchRequests, ?SYNC_QUERY_MODE, State).

on_format_query_result({ok, Rows}) ->
    #{result => ok, rows => Rows};
on_format_query_result(Result) ->
    Result.

on_get_status(_InstanceId, #{pool_name := PoolName} = ConnState) ->
    Results = emqx_resource_pool:health_check_workers(
        PoolName,
        {?MODULE, do_get_status, []},
        _Timeout = 5000,
        #{return_values => true}
    ),
    status_result(Results, ConnState).

status_result({error, timeout}, _ConnState) ->
    {?status_connecting, <<"timeout_checking_connections">>};
status_result({ok, []}, _ConnState) ->
    %% ecpool will auto-restart after delay
    {?status_connecting, <<"connection_pool_not_initialized">>};
status_result({ok, Results}, ConnState) ->
    case lists:filter(fun(S) -> S =/= ok end, Results) of
        [] ->
            case validate_connected_instance_name(ConnState) of
                ok ->
                    ?status_connected;
                {error, #{expected := ExpectedInstanceName, got := InstanceName}} ->
                    Msg = iolist_to_binary(
                        io_lib:format(
                            "connected instance does not match desired instance name;"
                            " expected ~s; connected to: ~s",
                            [
                                format_instance_name(ExpectedInstanceName),
                                format_instance_name(InstanceName)
                            ]
                        )
                    ),
                    {?status_disconnected, {unhealthy_target, Msg}};
                error ->
                    %% Could not infer instance name; assume ok
                    ?status_connected
            end;
        [{error, Reason} | _] ->
            {?status_connecting, Reason}
    end.

format_instance_name(undefined) ->
    <<"no instance name">>;
format_instance_name(Name) ->
    Name.

%%====================================================================
%% ecpool callback fns
%%====================================================================

-spec connect(Options :: list()) -> {ok, connection_reference()} | {error, term()}.
connect(Options) ->
    ConnectStr = lists:concat(conn_str(Options, [])),
    %% Note: we don't use `emqx_secret:wrap/1` here because its return type is opaque, and
    %% dialyzer then complains that it's being fed to a function that doesn't expect
    %% something opaque...
    ConnectStrWrapped = fun() -> ConnectStr end,
    Opts = proplists:get_value(options, Options, []),
    odbc:connect(ConnectStrWrapped, Opts).

-spec disconnect(connection_reference()) -> ok | {error, term()}.
disconnect(ConnectionPid) ->
    odbc:disconnect(ConnectionPid).

-spec do_get_status(connection_reference()) -> ok | {error, term()}.
do_get_status(Conn) ->
    case execute(Conn, <<"SELECT 1">>) of
        {selected, [[]], [{1}]} ->
            ok;
        Other ->
            _ = disconnect(Conn),
            {error, #{
                cause => "unexpected_SELECT_1_result",
                result => Other
            }}
    end.

get_servername(Conn) ->
    case execute(Conn, <<"select @@servername">>) of
        {selected, _, [{RawName}]} ->
            {ok, RawName};
        _ ->
            error
    end.

%%====================================================================
%% Internal Functions
%%====================================================================

%% TODO && FIXME:
%% About the connection string attribute `Encrypt`:
%% The default value is `yes` in odbc version 18.0+ and `no` in previous versions.
%% And encrypted connections always verify the server's certificate.
%% So `Encrypt=YES;TrustServerCertificate=YES` must be set in the connection string
%% when connecting to a server that has a self-signed certificate.
%% See also:
%% 'https://learn.microsoft.com/en-us/sql/connect/odbc/
%%      dsn-connection-string-attribute?source=recommendations&view=sql-server-ver16#encrypt'
conn_str([], Acc) ->
    lists:join(";", ["Encrypt=YES", "TrustServerCertificate=YES" | Acc]);
conn_str([{driver, Driver} | Opts], Acc) ->
    conn_str(Opts, ["Driver=" ++ str(Driver) | Acc]);
conn_str([{server, Server} | Opts], Acc) ->
    #{hostname := Host, port := Port} = Parsed = parse_server(Server),
    InstanceNameStr =
        case maps:find(instance_name, Parsed) of
            {ok, InstanceName} -> "\\" ++ InstanceName;
            error -> ""
        end,
    conn_str(Opts, [
        "Server=" ++ str(Host) ++ InstanceNameStr ++ "," ++ str(Port) | Acc
    ]);
conn_str([{database, Database} | Opts], Acc) ->
    conn_str(Opts, ["Database=" ++ str(Database) | Acc]);
conn_str([{username, Username} | Opts], Acc) ->
    conn_str(Opts, ["UID=" ++ str(Username) | Acc]);
conn_str([{password, Password} | Opts], Acc) ->
    conn_str(Opts, ["PWD=" ++ str(emqx_secret:unwrap(Password)) | Acc]);
conn_str([{_, _} | Opts], Acc) ->
    conn_str(Opts, Acc).

%% Query with singe & batch sql statement
-spec do_query(
    resource_id(),
    Query :: {channel_id(), map()} | [{channel_id(), map()}],
    ApplyMode :: handover,
    state()
) ->
    {ok, list()}
    | {error, {recoverable_error, term()}}
    | {error, {unrecoverable_error, term()}}
    | {error, term()}.
do_query(ResourceId, Query, ApplyMode, State) ->
    #{pool_name := PoolName, installed_channels := Channels} = State,
    ?TRACE(
        "SINGLE_QUERY_SYNC",
        "sqlserver_connector_received",
        #{query => Query, connector => ResourceId, state => State}
    ),
    ChannelId = get_channel_id(Query),
    QueryTuple = get_query_tuple(Query),
    #{sql_templates := Templates} = ChannelState = maps:get(ChannelId, Channels),
    ChannelConf = maps:get(channel_conf, ChannelState, #{}),
    %% only insert sql statement for single query and batch query
    Result =
        case apply_template(QueryTuple, Templates, ChannelConf) of
            {?ACTION_SEND_MESSAGE, SQL} ->
                emqx_trace:rendered_action_template(ChannelId, #{
                    sql => SQL
                }),
                ecpool:pick_and_do(
                    PoolName,
                    {?MODULE, worker_do_insert, [SQL, State]},
                    ApplyMode
                );
            QueryTuple ->
                {error, {unrecoverable_error, invalid_query}};
            _ ->
                {error, {unrecoverable_error, failed_to_apply_sql_template}}
        end,
    handle_result(Result, ResourceId, Query).

handle_result({error, {recoverable_error, _} = Reason} = Result, _ResourceId, _Query) ->
    ?tp(sqlserver_connector_query_return, #{error => Reason}),
    Result;
handle_result({error, Reason}, ResourceId, Query) ->
    ?tp(sqlserver_connector_query_return, #{error => Reason}),
    ?SLOG(error, #{
        msg => "sqlserver_connector_do_query_failed",
        connector => ResourceId,
        query => Query,
        reason => Reason
    }),
    case Reason of
        ecpool_empty -> {error, {recoverable_error, ecpool_empty}};
        _ -> {error, Reason}
    end;
handle_result(Result, _, _) ->
    ?tp(sqlserver_connector_query_return, #{result => Result}),
    Result.

worker_do_insert(Conn, SQL, #{resource_opts := ResourceOpts, pool_name := ResourceId}) ->
    LogMeta = #{connector => ResourceId},
    try
        case execute(Conn, SQL, ?REQUEST_TTL(ResourceOpts)) of
            {selected, Rows, _} ->
                {ok, Rows};
            {updated, _} ->
                ok;
            {error, ErrStr} ->
                IsConnectionClosedError = is_connection_closed_error(ErrStr),
                IsConnectionBrokenError = is_connection_broken_error(ErrStr),
                ErrStr1 =
                    case is_table_or_view_not_found_error(ErrStr) of
                        true ->
                            <<"table_or_view_not_found">>;
                        false ->
                            ErrStr
                    end,
                {LogLevel, Err} =
                    case IsConnectionClosedError orelse IsConnectionBrokenError of
                        true ->
                            {info, {recoverable_error, <<"connection_closed">>}};
                        false ->
                            {error, {unrecoverable_error, {invalid_request, ErrStr1}}}
                    end,
                ?SLOG(LogLevel, LogMeta#{msg => "invalid_request", reason => ErrStr1}),
                {error, Err}
        end
    catch
        _Type:Reason:St ->
            ?SLOG(error, LogMeta#{msg => "invalid_request", reason => Reason, stacktrace => St}),
            {error, {unrecoverable_error, {invalid_request, Reason}}}
    end.

%% Potential race condition: if an insert request is made while the connection is being
%% cut by the remote server, this error might be returned.
%% References:
%% https://learn.microsoft.com/en-us/sql/relational-databases/errors-events/mssqlserver-17194-database-engine-error?view=sql-server-ver16
is_connection_closed_error(MsgStr) ->
    case re:run(MsgStr, <<"0x2746[^0-9a-fA-F]?">>, [{capture, none}]) of
        match ->
            true;
        nomatch ->
            false
    end.

%% Potential race condition: if an insert request is made while the connection is being
%% cut by the remote server, this error might be returned.
%% References:
%% https://learn.microsoft.com/en-us/sql/connect/odbc/connection-resiliency?view=sql-server-ver16
is_connection_broken_error(MsgStr) ->
    case re:run(MsgStr, <<"SQLSTATE IS: IMC0[1-6]">>, [{capture, none}]) of
        match ->
            true;
        nomatch ->
            false
    end.

%% https://learn.microsoft.com/en-us/sql/odbc/reference/appendixes/appendix-a-odbc-error-codes?view=sql-server-ver15
%% In some occasions, non-printable bytes/chars may be returned in the error message,
%% making it non-loggable.
%% See also: https://emqx.atlassian.net/browse/EMQX-14171
is_table_or_view_not_found_error(MsgStr) ->
    case re:run(MsgStr, <<"SQLSTATE IS: 42S02">>, [{capture, none}]) of
        match ->
            true;
        nomatch ->
            false
    end.

-spec execute(connection_reference(), sql()) ->
    updated_tuple()
    | selected_tuple()
    | [updated_tuple()]
    | [selected_tuple()]
    | {error, common_reason()}.
execute(Conn, SQL) ->
    odbc:sql_query(Conn, str(SQL)).

-spec execute(connection_reference(), sql(), time_out()) ->
    updated_tuple()
    | selected_tuple()
    | [updated_tuple()]
    | [selected_tuple()]
    | {error, common_reason()}.
execute(Conn, SQL, Timeout) ->
    odbc:sql_query(Conn, str(SQL), Timeout).

get_channel_id([{ChannelId, _Req} | _]) ->
    ChannelId;
get_channel_id({ChannelId, _Req}) ->
    ChannelId.

get_query_tuple({_ChannelId, {QueryType, Data}} = _Query) ->
    {QueryType, Data};
get_query_tuple({_ChannelId, Data} = _Query) ->
    {send_message, Data};
get_query_tuple([{_ChannelId, {_QueryType, _Data}} | _]) ->
    error(
        {unrecoverable_error,
            {invalid_request, <<"The only query type that supports batching is insert.">>}}
    );
get_query_tuple([_InsertQuery | _] = Reqs) ->
    lists:map(fun get_query_tuple/1, Reqs).

%% for bridge data to sql server
parse_sql_template(Config) ->
    RawSQLTemplates =
        case maps:get(sql, Config, undefined) of
            undefined -> #{};
            <<>> -> #{};
            SQLTemplate -> #{?ACTION_SEND_MESSAGE => SQLTemplate}
        end,
    try
        {ok, parse_sql_template(maps:to_list(RawSQLTemplates), #{})}
    catch
        throw:Reason ->
            {error, Reason}
    end.

parse_sql_template([{Key, H} | T], BatchInsertTks) ->
    case emqx_utils_sql:get_statement_type(H) of
        select ->
            parse_sql_template(T, BatchInsertTks);
        insert ->
            case emqx_utils_sql:split_insert(H) of
                {ok, {InsertPart, Values}} ->
                    Tks = #{
                        ?BATCH_INSERT_PART => InsertPart,
                        ?BATCH_PARAMS_TOKENS => emqx_placeholder:preproc_tmpl(Values)
                    },
                    parse_sql_template(T, BatchInsertTks#{Key => Tks});
                {ok, {_InsertPart, _Values, OnClause}} ->
                    throw(<<"The 'ON' clause is not supported in SQLServer: ", OnClause/binary>>);
                {error, Reason} ->
                    ?SLOG(error, #{msg => "split_sql_failed", sql => H, reason => Reason}),
                    parse_sql_template(T, BatchInsertTks)
            end;
        Type when is_atom(Type) ->
            ?SLOG(error, #{msg => "detect_sql_type_unsupported", sql => H, type => Type}),
            parse_sql_template(T, BatchInsertTks);
        {error, Reason} ->
            ?SLOG(error, #{msg => "detect_sql_type_failed", sql => H, reason => Reason}),
            parse_sql_template(T, BatchInsertTks)
    end;
parse_sql_template([], BatchInsertTks) ->
    #{
        ?BATCH_INSERT_TEMP => BatchInsertTks
    }.

%% single insert
apply_template(
    {?ACTION_SEND_MESSAGE = _Key, _Msg} = Query, Templates, ChannelConf
) ->
    %% TODO: fix emqx_placeholder:proc_tmpl/2
    %% it won't add single quotes for string
    apply_template([Query], Templates, ChannelConf);
%% batch inserts
apply_template(
    [{?ACTION_SEND_MESSAGE = Key, _Msg} | _T] = BatchReqs,
    #{?BATCH_INSERT_TEMP := BatchInsertsTks} = _Templates,
    ChannelConf
) ->
    case maps:get(Key, BatchInsertsTks, undefined) of
        undefined ->
            BatchReqs;
        #{?BATCH_INSERT_PART := BatchInserts, ?BATCH_PARAMS_TOKENS := BatchParamsTks} ->
            BatchParams = [proc_msg(BatchParamsTks, Msg, ChannelConf) || {_, Msg} <- BatchReqs],
            Values = erlang:iolist_to_binary(lists:join($,, BatchParams)),
            SQL = <<BatchInserts/binary, " values ", Values/binary>>,
            {Key, SQL}
    end;
apply_template(Query, Templates, _) ->
    %% TODO: more detail information
    ?SLOG(error, #{msg => "apply_sql_template_failed", query => Query, templates => Templates}),
    {error, failed_to_apply_sql_template}.

proc_msg(Tokens, Msg, #{undefined_vars_as_null := true}) ->
    emqx_placeholder:proc_sqlserver_param_str2(Tokens, Msg);
proc_msg(Tokens, Msg, _) ->
    emqx_placeholder:proc_sqlserver_param_str(Tokens, Msg).

to_bin(B) when is_binary(B) ->
    B;
to_bin(List) when is_list(List) ->
    unicode:characters_to_binary(List, utf8).

validate_connected_instance_name(#{server := ParsedServer} = ConnState) ->
    ExpectedInstanceName = maps:get(instance_name, ParsedServer, undefined),
    maybe
        {ok, InstanceName} ?= infer_instance_name(ConnState),
        InstanceName1 = emqx_maybe:apply(fun to_bin/1, InstanceName),
        ExpectedInstanceName1 = emqx_maybe:apply(fun to_bin/1, ExpectedInstanceName),
        case InstanceName1 == ExpectedInstanceName1 of
            true ->
                ok;
            false ->
                {error, #{expected => ExpectedInstanceName, got => InstanceName}}
        end
    end.

infer_instance_name(#{pool_name := PoolName} = _ConnState) ->
    maybe
        {ok, RawName} ?= ecpool:pick_and_do(PoolName, {?MODULE, get_servername, []}, handover),
        {ok, Decoded} ?= try_decode_raw_str(RawName),
        get_instance_name(Decoded)
    end.

try_decode_raw_str(Raw) when is_binary(Raw) ->
    case io_lib:printable_unicode_list(binary_to_list(Raw)) of
        true ->
            {ok, Raw};
        false ->
            Decoded = unicode:characters_to_binary(Raw, {utf16, little}, utf8),
            case io_lib:printable_unicode_list(binary_to_list(Decoded)) of
                true ->
                    {ok, Decoded};
                false ->
                    error
            end
    end.

get_instance_name(ServerName) ->
    case binary:split(ServerName, <<"\\">>) of
        [_Server, InstanceName] ->
            {ok, InstanceName};
        [_] ->
            {ok, undefined};
        _ ->
            error
    end.
