%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_dameng_connector).

-behaviour(emqx_resource).

-include("emqx_bridge_dameng.hrl").

-include_lib("kernel/include/file.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-elvis([{elvis_text_style, line_length, #{limit => 120, skip_comments => whole_line}}]).

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
-export([
    do_get_status/1,
    worker_do_insert/4,
    worker_do_literal/4,
    worker_describe/3,
    parse_server/2,
    parse_sql_template/1,
    build_param_params/4
]).

%% Exported for tests
-export([
    build_conn_map/1,
    ensure_odbcserver_executable/1
]).

-define(ACTION_SEND_MESSAGE, send_message).

-define(SYNC_QUERY_MODE, handover).

%% We use -1 to differentiate between default port and explicitly defined port.
-define(DAMENG_HOST_OPTIONS, #{
    default_port => -1
}).

-define(REQUEST_TTL(RESOURCE_OPTS),
    maps:get(request_ttl, RESOURCE_OPTS, ?DEFAULT_REQUEST_TTL)
).

-define(FILE_MODE_755, 33261).
%% 32768 + 8#00400 + 8#00200 + 8#00100 + 8#00040 + 8#00010 + 8#00004 + 8#00001

-type state() :: #{
    pool_name := binary(),
    installed_channels := map(),
    resource_opts := map()
}.

%%====================================================================
%% Configuration and default values
%%====================================================================

namespace() -> dameng.

roots() ->
    [{config, #{type => hoconsc:ref(?MODULE, config)}}].

fields(config) ->
    [
        {server, server()},
        {port,
            hoconsc:mk(
                integer(),
                #{desc => ?DESC("port"), default => ?DAMENG_DEFAULT_PORT}
            )},
        {username,
            hoconsc:mk(
                binary(),
                %% No default: leaving it unset lets a DSN entry supply the
                %% credentials instead of overriding them with `SYSDBA'.
                #{desc => ?DESC("username"), required => false}
            )},
        {password, emqx_connector_schema_lib:password_field()},
        {driver,
            hoconsc:mk(
                binary(),
                #{desc => ?DESC("driver"), default => ?DAMENG_DEFAULT_DRIVER}
            )},
        {dsn,
            hoconsc:mk(
                binary(),
                #{desc => ?DESC("dsn"), default => <<>>}
            )},
        {charset,
            hoconsc:mk(
                binary(),
                #{desc => ?DESC("charset"), default => ?DAMENG_DEFAULT_CHARSET}
            )},
        {ssl_path,
            hoconsc:mk(
                binary(),
                #{desc => ?DESC("ssl_path"), default => <<>>}
            )},
        {ssl_pwd, emqx_connector_schema_lib:password_field(#{desc => ?DESC("ssl_pwd")})},
        {pool_size, fun emqx_connector_schema_lib:pool_size/1}
    ].

server() ->
    hoconsc:mk(
        string(),
        #{
            desc => ?DESC("server"),
            %% Required unless `dsn' is set (see `build_conn_map/1').
            required => false,
            converter => fun emqx_schema:convert_servers/2,
            validator => fun server_validator/1
        }
    ).

server_validator(Str) ->
    BaseValidator = emqx_schema:servers_validator(?DAMENG_HOST_OPTIONS, _Required = false),
    ok = BaseValidator(Str),
    _ = parse_server(Str, ?DAMENG_DEFAULT_PORT),
    ok.

%%====================================================================
%% Callbacks defined in emqx_resource
%%====================================================================

resource_type() -> dameng.

callback_mode() -> always_sync.

on_start(InstanceId = PoolName, #{pool_size := PoolSize, resource_opts := ResourceOpts} = Config) ->
    ?SLOG(info, #{
        msg => "starting_dameng_connector",
        connector => InstanceId,
        config => emqx_utils:redact(Config)
    }),
    ensure_odbcserver_executable(),
    case build_conn_map(Config) of
        {ok, ConnMap} ->
            ConnectOptions = [
                {conn_map, ConnMap},
                {pool_size, PoolSize},
                {auto_reconnect, 2},
                {on_disconnect, {?MODULE, disconnect, []}}
            ],
            State = #{
                pool_name => PoolName,
                installed_channels => #{},
                resource_opts => ResourceOpts
            },
            case emqx_resource_pool:start(PoolName, ?MODULE, ConnectOptions) of
                ok ->
                    {ok, State};
                {error, Reason} ->
                    ?tp(
                        dameng_connector_start_failed,
                        #{error => Reason}
                    ),
                    {error, Reason}
            end;
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "invalid_dameng_connector_config",
                connector => InstanceId,
                reason => Reason
            }),
            ?tp(dameng_connector_start_failed, #{error => Reason}),
            {error, Reason}
    end.

on_stop(InstanceId, _State) ->
    ?tp(
        dameng_connector_on_stop,
        #{instance_id => InstanceId}
    ),
    ?SLOG(info, #{
        msg => "stopping_dameng_connector",
        connector => InstanceId
    }),
    emqx_resource_pool:stop(InstanceId).

on_add_channel(_InstId, OldState, ChannelId, #{parameters := Params} = _ChannelConfig) ->
    #{installed_channels := InstalledChannels, pool_name := PoolName} = OldState,
    ResourceOpts = maps:get(resource_opts, OldState, #{}),
    case parse_sql_template(Params) of
        {ok, ChannelState0} ->
            case resolve_column_types(PoolName, ChannelState0, ResourceOpts) of
                {ok, ColumnTypes} ->
                    ChannelState = ChannelState0#{column_types => ColumnTypes},
                    NewInstalledChannels = maps:put(ChannelId, ChannelState, InstalledChannels),
                    NewState = OldState#{installed_channels => NewInstalledChannels},
                    ?tp(dameng_connector_channel_added, #{channel_id => ChannelId}),
                    {ok, NewState};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

on_remove_channel(_InstId, #{installed_channels := InstalledChannels} = OldState, ChannelId) ->
    NewInstalledChannels = maps:remove(ChannelId, InstalledChannels),
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

on_format_query_result({ok, Rows}) ->
    #{result => ok, rows => Rows};
on_format_query_result(Result) ->
    Result.

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
    do_query(ResourceId, BatchRequests, ?SYNC_QUERY_MODE, State).

on_get_status(_InstanceId, #{pool_name := PoolName} = ConnState) ->
    Opts = #{
        check_fn => {?MODULE, do_get_status, []},
        on_success_fn => fun() -> on_get_status_continue(ConnState) end,
        timeout => 5_000
    },
    emqx_resource_pool:common_health_check_workers(PoolName, Opts).

on_get_status_continue(_ConnState) ->
    ?status_connected.

%%====================================================================
%% ecpool callback fns
%%====================================================================

-spec connect(Options :: list()) -> {ok, term()} | {error, term()}.
connect(Options) ->
    ConnMap = proplists:get_value(conn_map, Options, #{}),
    emqx_odbc:connect(ConnMap).

-spec disconnect(term()) -> ok | {error, term()}.
disconnect(Conn) ->
    emqx_odbc:disconnect(Conn).

-spec do_get_status(term()) -> ok | {error, term()}.
do_get_status(Conn) ->
    case emqx_odbc:sql_query(Conn, <<"SELECT 1">>, 5_000) of
        {selected, _Cols, [{1}]} ->
            ok;
        Other ->
            _ = disconnect(Conn),
            {error, #{cause => "unexpected_SELECT_1_result", result => Other}}
    end.

%%====================================================================
%% Internal Functions
%%====================================================================

build_conn_map(Config0) ->
    %% The connector config reaches this function either from the checked
    %% config (atom keys) or from a persisted/namespaced config (binary keys).
    Config = emqx_utils_maps:binary_key_map(Config0),
    case {dsn(Config), maps:get(<<"server">>, Config, undefined)} of
        {undefined, undefined} ->
            %% A DSN carries the server and the credentials, otherwise they must
            %% be configured explicitly.
            {error, {invalid_config, <<"either 'dsn' or 'server' must be configured">>}};
        {Dsn, Server} ->
            #{hostname := Host, port := Port} =
                parse_server(Server, maps:get(<<"port">>, Config, ?DAMENG_DEFAULT_PORT)),
            {ok,
                maps:merge(
                    #{
                        server => Host,
                        port => Port,
                        driver => maps:get(<<"driver">>, Config, ?DAMENG_DEFAULT_DRIVER),
                        dsn => Dsn,
                        charset => maps:get(<<"charset">>, Config, ?DAMENG_DEFAULT_CHARSET),
                        extra_conn_attrs => extra_conn_attrs(Config)
                    },
                    credentials(Config, Dsn)
                )}
    end.

%% The DM8 ODBC driver performs the TLS handshake itself, so the connector only
%% passes the certificate directory and the private key password through as
%% connection string attributes; EMQX does not handle certificates or enable TLS
%% by itself, and a DM8 server that does not require TLS simply ignores them.
%% They are dropped when unset (or blank), so existing connectors keep the exact
%% connection string they used before.
extra_conn_attrs(Config) ->
    lists:filtermap(
        fun({Key, BinKey}) ->
            case maps:get(BinKey, Config, undefined) of
                undefined -> false;
                null -> false;
                <<>> -> false;
                "" -> false;
                Value -> {true, {Key, Value}}
            end
        end,
        [{"SSL_PATH", <<"ssl_path">>}, {"SSL_PWD", <<"ssl_pwd">>}]
    ).

%% A DSN entry already carries the credentials, so they are only overridden when
%% configured explicitly; otherwise the driver reads them from `odbc.ini'.
%% Without a DSN the connector must supply them, so the DM default user is used.
credentials(Config, undefined) ->
    maps:merge(
        #{username => maps:get(<<"username">>, Config, ?DAMENG_DEFAULT_USERNAME)},
        configured(<<"password">>, password, Config)
    );
credentials(Config, _Dsn) ->
    maps:merge(
        configured(<<"username">>, username, Config),
        configured(<<"password">>, password, Config)
    ).

configured(BinKey, AtomKey, Config) ->
    case maps:get(BinKey, Config, undefined) of
        undefined -> #{};
        null -> #{};
        <<>> -> #{};
        "" -> #{};
        Value -> #{AtomKey => Value}
    end.

dsn(Config) ->
    case maps:get(<<"dsn">>, Config, undefined) of
        undefined -> undefined;
        <<>> -> undefined;
        "" -> undefined;
        Dsn -> Dsn
    end.

%%===================
%% SQL template parsing
%%===================

parse_sql_template(#{sql := SQL} = Params) ->
    case emqx_utils_sql:get_statement_type(SQL) of
        insert ->
            parse_insert_template(SQL, Params);
        Type when is_atom(Type) ->
            {ok, #{
                sql_template => SQL,
                statement_type => Type,
                values_tokens => emqx_placeholder:preproc_tmpl(SQL),
                channel_conf => Params
            }};
        {error, Reason} ->
            {error, {unrecoverable_error, {invalid_request, Reason}}}
    end;
parse_sql_template(_) ->
    {error, {unrecoverable_error, {invalid_request, missing_sql}}}.

parse_insert_template(SQL, Params) ->
    case emqx_utils_sql:split_insert(SQL) of
        {ok, {InsertPart, Values}} ->
            parse_insert_columns(SQL, Params, InsertPart, Values);
        {ok, {_InsertPart, _Values, OnClause}} ->
            {error,
                {unrecoverable_error,
                    {invalid_request, <<"ON clause is not supported: ", OnClause/binary>>}}};
        {error, Reason} ->
            {error, {unrecoverable_error, {invalid_request, Reason}}}
    end.

parse_insert_columns(SQL, Params, InsertPart, Values) ->
    case extract_columns(InsertPart) of
        {ok, Columns} ->
            build_insert_channel_state(SQL, Params, InsertPart, Values, Columns);
        {error, Reason} ->
            {error, {unrecoverable_error, {invalid_request, Reason}}}
    end.

build_insert_channel_state(SQL, Params, InsertPart, Values, Columns) ->
    {ParamSQLValues, Tokens} = emqx_placeholder:preproc_sql(Values, '?'),
    ParamSQL = <<InsertPart/binary, " values ", ParamSQLValues/binary>>,
    case count_vars(Tokens) =:= length(Columns) of
        true ->
            {ok, #{
                sql_template => SQL,
                statement_type => insert,
                insert_part => InsertPart,
                param_sql => ParamSQL,
                insert_columns => Columns,
                values_tokens => Tokens,
                channel_conf => Params
            }};
        false ->
            {error, {unrecoverable_error, {invalid_request, columns_vars_mismatch}}}
    end.

count_vars(Tokens) ->
    length([true || {var, _} <- Tokens]).

extract_columns(InsertPart) ->
    case re:run(InsertPart, "\\(([^)]*)\\)", [{capture, all_but_first, binary}]) of
        {match, [ColsBinary]} ->
            Cols = [trim_identifier(Col) || Col <- binary:split(ColsBinary, <<",">>, [global])],
            case lists:any(fun(C) -> C =:= <<>> end, Cols) of
                true ->
                    {error, insert_must_specify_columns};
                false ->
                    {ok, Cols}
            end;
        nomatch ->
            {error, insert_must_specify_columns}
    end.

%% Strip the quoting from a column identifier so that it can be looked up in the
%% names reported by `describe_table'.
trim_identifier(Col) ->
    trim_identifier_quotes(string:trim(Col)).

trim_identifier_quotes(<<$", Rest/binary>>) when byte_size(Rest) >= 1 ->
    case binary:last(Rest) of
        $" -> binary:part(Rest, 0, byte_size(Rest) - 1);
        _ -> <<$", Rest/binary>>
    end;
trim_identifier_quotes(Col) ->
    Col.

extract_table(InsertPart) ->
    %% `get_statement_type/1' and `split_insert/1' accept `INSERT INTO' in any
    %% case, so the table name must be extracted case-insensitively as well.
    case
        re:run(InsertPart, "insert\\s+into\\s+([^\\s\\(]+)", [
            caseless, {capture, all_but_first, binary}
        ])
    of
        {match, [Table]} ->
            {ok, Table};
        nomatch ->
            {error, {unrecoverable_error, {invalid_request, table_not_found}}}
    end.

%%===================
%% describe (only once at channel creation)
%%===================

resolve_column_types(_PoolName, #{statement_type := Type}, _ResourceOpts) when Type =/= insert ->
    {ok, []};
resolve_column_types(
    PoolName, #{insert_part := InsertPart, insert_columns := Columns}, ResourceOpts
) ->
    maybe
        {ok, TableName} ?= extract_table(InsertPart),
        {ok, DescribeCols} ?= describe_columns(PoolName, TableName, describe_timeout(ResourceOpts)),
        TypedColumns = typed_columns(Columns, DescribeCols),
        ok ?= validate_columns(Columns, TypedColumns),
        {ok, TypedColumns}
    else
        {error, Reason} -> {error, Reason}
    end.

%% `describe_table' is called synchronously while the channel is created, so it
%% must not be able to block the resource manager forever.
describe_timeout(ResourceOpts) ->
    case ?REQUEST_TTL(ResourceOpts) of
        infinity -> ?DEFAULT_REQUEST_TTL;
        Timeout -> Timeout
    end.

describe_columns(PoolName, TableName, Timeout) ->
    case
        ecpool:pick_and_do(
            PoolName,
            {?MODULE, worker_describe, [TableName, Timeout]},
            handover
        )
    of
        {ok, DescribeCols} ->
            {ok, DescribeCols};
        {error, Reason} ->
            %% A describe failure (e.g. table not found) is a config error, not
            %% a transient one: classify it accordingly so the channel add (and
            %% thus the action creation) fails loudly instead of being retried
            %% forever.
            {error, emqx_odbc:classify_error(Reason)}
    end.

%% Build the list of ODBC types matching the explicitly listed INSERT columns,
%% looking the names up case-insensitively.
typed_columns(Columns, DescribeCols) ->
    DTMap = maps:from_list([{norm_name(Name), Type} || {Name, Type} <- DescribeCols]),
    [maps:get(norm_name(Col), DTMap, '$undef') || Col <- Columns].

validate_columns(Columns, TypedColumns) ->
    case missing_columns(Columns, TypedColumns) of
        [] ->
            validate_param_types(Columns, TypedColumns);
        Missing ->
            {error, {unrecoverable_error, {invalid_request, {missing_column_type, Missing}}}}
    end.

missing_columns(Columns, TypedColumns) ->
    [Col || {Col, Type} <- lists:zip(Columns, TypedColumns), Type =:= '$undef'].

%% Reject column types that `odbc:param_query/4' cannot bind while the channel
%% is being created. Doing it here makes the action fail loudly on creation
%% instead of failing on every message at runtime (e.g. binary or CLOB columns,
%% which cannot be bound as parameters without corrupting the data).
validate_param_types(Columns, Types) ->
    case unsupported_param_types(Columns, Types) of
        [] ->
            ok;
        Unsupported ->
            {error, {unrecoverable_error, {invalid_request, {unsupported_odbc_types, Unsupported}}}}
    end.

unsupported_param_types(Columns, Types) ->
    [
        {Col, Type}
     || {Col, Type} <- lists:zip(Columns, Types),
        is_unsupported_param_type(Type)
    ].

is_unsupported_param_type(Type) ->
    case emqx_odbc:to_param_type(Type) of
        {error, _} -> true;
        _ -> false
    end.

worker_describe(Conn, Table, Timeout) ->
    emqx_odbc:describe_table(Conn, Table, Timeout).

norm_name(Name) ->
    string:uppercase(to_list(Name)).

%%===================
%% Query
%%===================

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
        "dameng_connector_received",
        #{query => Query, connector => ResourceId, state => State}
    ),
    ChannelId = get_channel_id(Query),
    ChannelState = maps:get(ChannelId, Channels),
    case maps:get(statement_type, ChannelState) of
        insert ->
            Msgs = get_msgs(Query),
            Result = ecpool:pick_and_do(
                PoolName,
                {?MODULE, worker_do_insert, [ChannelState, Msgs, State]},
                ApplyMode
            ),
            handle_result(Result, ResourceId, Query);
        Type when is_atom(Type) ->
            case get_msgs(Query) of
                [Msg] ->
                    Result = ecpool:pick_and_do(
                        PoolName,
                        {?MODULE, worker_do_literal, [ChannelState, Msg, State]},
                        ApplyMode
                    ),
                    handle_result(Result, ResourceId, Query);
                _ ->
                    {error, {unrecoverable_error, {invalid_request, only_insert_supports_batch}}}
            end
    end.

handle_result({error, {recoverable_error, _} = Reason} = Result, _ResourceId, _Query) ->
    %% Recoverable errors (e.g. a flapping connection) are already logged with
    %% the raw driver message by `classify_odbc_error/1'.
    ?tp(dameng_connector_query_return, #{error => Reason}),
    Result;
handle_result({error, Reason}, ResourceId, Query) ->
    ?tp(dameng_connector_query_return, #{error => Reason}),
    ?SLOG(error, #{
        msg => "dameng_connector_do_query_failed",
        connector => ResourceId,
        query => Query,
        reason => Reason
    }),
    case Reason of
        ecpool_empty -> {error, {recoverable_error, ecpool_empty}};
        _ -> {error, Reason}
    end;
handle_result(Result, _, _) ->
    ?tp(dameng_connector_query_return, #{result => Result}),
    Result.

%%===================
%% Insert via odbc:param_query
%%===================

worker_do_insert(Conn, ChannelState, Msgs, #{resource_opts := ResourceOpts} = _State) ->
    #{param_sql := ParamSQL, values_tokens := Tokens, column_types := ColTypes} = ChannelState,
    UndefinedAsNull = get_undefined_as_null(ChannelState),
    try
        case build_param_params(Tokens, Msgs, ColTypes, UndefinedAsNull) of
            {ok, Params} ->
                case emqx_odbc:param_query(Conn, ParamSQL, Params, ?REQUEST_TTL(ResourceOpts)) of
                    {updated, _N} ->
                        %% Return `ok' (like the sqlserver bridge) so the
                        %% resource/trace treats the insert as a plain success.
                        ok;
                    {selected, _Cols, Rows} ->
                        {ok, Rows};
                    {error, ErrStr} ->
                        {error, classify_odbc_error(ErrStr)}
                end;
            {error, Err1} ->
                {error, Err1}
        end
    catch
        _Type:Reason:St ->
            ?SLOG(error, #{msg => "invalid_request", reason => Reason, stacktrace => St}),
            {error, {unrecoverable_error, {invalid_request, Reason}}}
    end.

%%===================
%% Non-insert (select/update/delete) via literal SQL
%%===================

worker_do_literal(Conn, ChannelState, Msg, #{resource_opts := ResourceOpts}) ->
    #{values_tokens := Tokens} = ChannelState,
    UndefinedAsNull = get_undefined_as_null(ChannelState),
    case render_literal(Tokens, Msg, UndefinedAsNull) of
        {ok, SQL} ->
            try
                case emqx_odbc:sql_query(Conn, SQL, ?REQUEST_TTL(ResourceOpts)) of
                    {selected, _Cols, Rows} ->
                        {ok, Rows};
                    {updated, _N} ->
                        %% Consistent with the insert path: a write that affected
                        %% rows is a plain success.
                        ok;
                    {error, ErrStr} ->
                        {error, classify_odbc_error(ErrStr)}
                end
            catch
                _Type:Reason:St ->
                    ?SLOG(error, #{msg => "invalid_request", reason => Reason, stacktrace => St}),
                    {error, {unrecoverable_error, {invalid_request, Reason}}}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% The literal path interpolates values into the statement, so they must be
%% quoted for DM8: it is Oracle compatible, which escapes a single quote by
%% doubling it and gives no special meaning to the backslash. (The SQL Server
%% helper `emqx_placeholder:proc_sqlserver_param_str/2' used before this change
%% backslash-escapes quotes and backslashes, which corrupts values containing a
%% backslash and can produce a syntax error for values containing a quote.)
render_literal(Tokens, Msg, UndefinedAsNull) ->
    Vars = [Var || {var, Var} <- Tokens],
    Values = [emqx_placeholder:lookup_var(Var, Msg) || Var <- Vars],
    case {UndefinedAsNull, lists:member(undefined, Values)} of
        {false, true} ->
            {error, {unrecoverable_error, undefined_var}};
        _ ->
            {ok, iolist_to_binary(render_literal_parts(Tokens, Msg, UndefinedAsNull))}
    end.

render_literal_parts([{str, Str} | Rest], Msg, UndefinedAsNull) ->
    [Str | render_literal_parts(Rest, Msg, UndefinedAsNull)];
render_literal_parts([{var, Var} | Rest], Msg, UndefinedAsNull) ->
    Value = emqx_placeholder:lookup_var(Var, Msg),
    [quote_literal(Value, UndefinedAsNull) | render_literal_parts(Rest, Msg, UndefinedAsNull)];
render_literal_parts([], _Msg, _UndefinedAsNull) ->
    [].

quote_literal(undefined, true) ->
    <<"NULL">>;
quote_literal(null, _UndefinedAsNull) ->
    %% A JSON `null' is an explicit value, not a missing variable: write SQL
    %% NULL like the parameter path does (`emqx_odbc:to_odbc_value(null, _)').
    <<"NULL">>;
quote_literal(Value, _UndefinedAsNull) ->
    emqx_utils_sql:to_sql_string(Value, #{escaping => sql_std}).

%% Classify a driver error and make sure a recoverable (connection level)
%% failure is visible in the logs together with the raw driver message.
%% Unrecoverable errors are logged by `handle_result/3'.
classify_odbc_error(ErrStr) ->
    Classified = emqx_odbc:classify_error(ErrStr),
    case Classified of
        {recoverable_error, _} ->
            ?SLOG(info, #{
                msg => "dameng_connector_recoverable_error",
                error => ErrStr
            });
        _ ->
            ok
    end,
    Classified.

%%===================
%% Param building
%%===================

build_param_params(_Tokens, [], _ColTypes, _UndefinedAsNull) ->
    {error, {unrecoverable_error, {invalid_request, empty_batch}}};
build_param_params(Tokens, Msgs, ColTypes, UndefinedAsNull) ->
    %% Render each message to a list of raw values (one per token/column).
    RowVals = [
        emqx_placeholder:proc_tmpl(Tokens, Msg, #{return => rawlist, var_trans => fun(V) -> V end})
     || Msg <- Msgs
    ],
    PerColumn = transpose(RowVals),
    build_column_params(zip(ColTypes, PerColumn), UndefinedAsNull).

build_column_params([], _UndefinedAsNull) ->
    {ok, []};
build_column_params([{ColType, ColVals} | Rest], UndefinedAsNull) ->
    case emqx_odbc:to_param_type(ColType) of
        {error, Reason} ->
            {error, Reason};
        ParamType ->
            case convert_vals(ColVals, ColType, UndefinedAsNull, []) of
                {ok, BindVals} ->
                    %% Size the parameter buffer from the values so that `odbc'
                    %% can never write past it (see emqx_odbc:fit_param_type/2).
                    BindType = emqx_odbc:fit_param_type(ParamType, BindVals),
                    case build_column_params(Rest, UndefinedAsNull) of
                        {ok, Acc} -> {ok, [{BindType, BindVals} | Acc]};
                        {error, Reason} -> {error, Reason}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end
    end.

convert_vals([], _ColType, _UndefinedAsNull, Acc) ->
    {ok, lists:reverse(Acc)};
convert_vals([V | Rest], ColType, UndefinedAsNull, Acc) ->
    case emqx_odbc:to_odbc_value(V, ColType, UndefinedAsNull) of
        {ok, V1} ->
            convert_vals(Rest, ColType, UndefinedAsNull, [V1 | Acc]);
        {error, Reason} ->
            {error, Reason}
    end.

get_undefined_as_null(#{channel_conf := ChannelConf}) ->
    maps:get(undefined_vars_as_null, ChannelConf, false).

transpose([]) ->
    [];
transpose(Rows) ->
    Width = length(hd(Rows)),
    Acc0 = lists:duplicate(Width, []),
    Acc = lists:foldl(
        fun(Row, A) ->
            [[V | C] || {V, C} <- lists:zip(Row, A)]
        end,
        Acc0,
        Rows
    ),
    [lists:reverse(C) || C <- Acc].

zip([], []) -> [];
zip([H1 | T1], [H2 | T2]) -> [{H1, H2} | zip(T1, T2)].

%%===================
%% Helpers
%%===================

get_channel_id([{ChannelId, _Req} | _]) ->
    ChannelId;
get_channel_id({ChannelId, _Req}) ->
    ChannelId.

get_msgs([]) ->
    [];
get_msgs([{_Ch, Msg} | Rest]) ->
    [Msg | get_msgs(Rest)];
get_msgs({_Ch, Msg}) ->
    [Msg].

%% Ensure the `odbcserver' port program shipped with the OTP `odbc' application is
%% runnable (mode 755), otherwise `odbc:connect/2' may fail even though the driver
%% is present. This is a best effort repair: when it does not succeed the reason is
%% logged, because `odbc:connect/2' would otherwise fail with a much less
%% actionable error.
ensure_odbcserver_executable() ->
    case code:priv_dir(odbc) of
        PrivDir when is_list(PrivDir) ->
            ensure_odbcserver_executable(filename:join([PrivDir, "bin", "odbcserver"]));
        {error, Reason} ->
            %% The `odbc' application is not in the code path: there is no file
            %% to repair.
            log_odbcserver_unusable(#{
                what => priv_dir_not_found,
                app => odbc,
                reason => Reason
            })
    end.

ensure_odbcserver_executable(Path) ->
    case file:read_file_info(Path) of
        {ok, #file_info{mode = ?FILE_MODE_755}} ->
            ok;
        {ok, Info} ->
            case file:write_file_info(Path, Info#file_info{mode = ?FILE_MODE_755}) of
                ok ->
                    ok;
                {error, Reason} ->
                    log_odbcserver_unusable(#{
                        what => chmod_failed,
                        path => Path,
                        reason => Reason
                    })
            end;
        {error, Reason} ->
            log_odbcserver_unusable(#{
                what => not_accessible,
                path => Path,
                reason => Reason
            })
    end.

log_odbcserver_unusable(Info) ->
    ?SLOG(warning, Info#{msg => "dameng_odbcserver_unusable"}),
    ok.

parse_server(undefined, DefaultPort) ->
    #{hostname => undefined, port => DefaultPort};
parse_server(Server, DefaultPort) ->
    Parsed = emqx_schema:parse_server(Server, ?DAMENG_HOST_OPTIONS),
    #{hostname := Host, port := Port} = Parsed,
    EffectivePort =
        case Port of
            -1 -> DefaultPort;
            _ -> Port
        end,
    #{hostname => unicode:characters_to_binary(Host, utf8), port => EffectivePort}.

%%===================
%% Conversion helpers
%%===================

to_list(B) when is_binary(B) ->
    binary_to_list(B);
to_list(L) when is_list(L) ->
    L.
