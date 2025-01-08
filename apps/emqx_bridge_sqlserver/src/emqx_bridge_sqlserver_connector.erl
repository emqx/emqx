%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_sqlserver_connector).

-behaviour(emqx_resource).

-include("emqx_bridge_sqlserver.hrl").

-include_lib("kernel/include/file.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

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
-export([do_get_status/1, worker_do_insert/3]).

-import(emqx_utils_conv, [str/1]).
-import(hoconsc, [mk/2, enum/1, ref/2]).

-define(ACTION_SEND_MESSAGE, send_message).

-define(SYNC_QUERY_MODE, handover).

-define(SQLSERVER_HOST_OPTIONS, #{
    default_port => ?SQLSERVER_DEFAULT_PORT
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
    Meta = #{desc => ?DESC("server")},
    emqx_schema:servers_sc(Meta, ?SQLSERVER_HOST_OPTIONS).

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
    ConnectOptions = [
        {server, to_bin(Server)},
        {username, Username},
        {password, maps:get(password, Config, emqx_secret:wrap(""))},
        {driver, Driver},
        {database, Database},
        {pool_size, PoolSize},
        {on_disconnect, {?MODULE, disconnect, []}}
    ],

    State = #{
        %% also InstanceId
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
        {ok, _} -> on_get_status(InstanceId, State);
        error -> ?status_disconnected
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

on_get_status(_InstanceId, #{pool_name := PoolName} = _State) ->
    Health = emqx_resource_pool:health_check_workers(
        PoolName,
        {?MODULE, do_get_status, []}
    ),
    status_result(Health).

status_result(_Status = true) -> ?status_connected;
status_result(_Status = false) -> ?status_connecting.
%% TODO:
%% case for disconnected

%%====================================================================
%% ecpool callback fns
%%====================================================================

-spec connect(Options :: list()) -> {ok, connection_reference()} | {error, term()}.
connect(Options) ->
    ConnectStr = lists:concat(conn_str(Options, [])),
    Opts = proplists:get_value(options, Options, []),
    odbc:connect(ConnectStr, Opts).

-spec disconnect(connection_reference()) -> ok | {error, term()}.
disconnect(ConnectionPid) ->
    odbc:disconnect(ConnectionPid).

-spec do_get_status(connection_reference()) -> Result :: boolean().
do_get_status(Conn) ->
    case execute(Conn, <<"SELECT 1">>) of
        {selected, [[]], [{1}]} -> true;
        _ -> false
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
    #{hostname := Host, port := Port} = emqx_schema:parse_server(Server, ?SQLSERVER_HOST_OPTIONS),
    conn_str(Opts, ["Server=" ++ str(Host) ++ "," ++ str(Port) | Acc]);
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
                ?SLOG(error, LogMeta#{msg => "invalid_request", reason => ErrStr}),
                {error, {unrecoverable_error, {invalid_request, ErrStr}}}
        end
    catch
        _Type:Reason:St ->
            ?SLOG(error, LogMeta#{msg => "invalid_request", reason => Reason, stacktrace => St}),
            {error, {unrecoverable_error, {invalid_request, Reason}}}
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

to_bin(List) when is_list(List) ->
    unicode:characters_to_binary(List, utf8).
