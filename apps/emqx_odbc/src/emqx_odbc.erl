%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

%% @doc A thin wrapper around the Erlang `odbc' module, shared by the ODBC
%% based data integrations.
%%
%% The purpose of this module is to factor out the low level ODBC concerns
%% (connection string construction, query execution, error classification and
%% value type conversion) so that the ODBC based connectors share one
%% implementation instead of copy-pasting it.
%%
%% Connection string construction supports the two forms that ODBC drivers
%% accept:
%%   * DSN based:  `DSN=<name>[;UID=...;PWD=...]'
%%   * DSN-less:   `Driver=<name|path>;Server=<host[:port]>[;UID=...;PWD=...;Charset=...]'
%%
%% There is deliberately no `Database' attribute in the DSN-less form: DM8
%% locates the target instance by host and port and ignores it.
%%
%% The `driver' field accepts either a driver name registered in `odbcinst.ini'
%% (e.g. `DM8 ODBC DRIVER', automatically wrapped in `{}') or a path to the
%% driver library (e.g. `/opt/dmdbms/bin/libdodbc.so', used as-is).
%%
%% Character values are bound with `odbc:param_query/4', which sizes its
%% parameter buffers from the type tuple and fills them with
%% `ei_decode_binary/4' (a call without any bounds check); `fit_param_type/2'
%% makes sure the buffer always fits the values that are bound.
%% @end
-module(emqx_odbc).

-export([
    connect/1,
    disconnect/1,
    sql_query/2,
    sql_query/3,
    param_query/4,
    describe_table/2,
    describe_table/3,
    build_conn_string/1,
    to_param_type/1,
    fit_param_type/2,
    to_odbc_value/2,
    to_odbc_value/3,
    classify_error/1,
    is_connection_closed_error/1,
    is_connection_broken_error/1,
    is_timeout_error/1,
    is_table_not_found_error/1
]).

-export_type([connection/0]).

-type connection() :: term().
-type sql() :: string() | binary().
-type odbc_timeout() :: non_neg_integer() | infinity.
-type value() :: term().

%% ODBC data type as reported by `describe_table'.
-type col_type() :: odbc:odbc_data_type().

%% A description of the target data source in a form that `build_conn_string/1'
%% understands. Either `dsn' or `driver' plus `server' must be given.
-type conn_map() :: #{
    server => unicode:chardata(),
    port => integer(),
    username => unicode:chardata(),
    password => emqx_secret:t(unicode:chardata()),
    driver => unicode:chardata(),
    dsn => unicode:chardata(),
    charset => unicode:chardata()
}.

%% Sizes used to bind the types that `odbc:param_query/4' has no native binding
%% for: the driver reports them with capital letters, so their string
%% representation is bound instead.
-define(BIGINT_STR_SIZE, 24).
-define(DATE_STR_SIZE, 16).
-define(TIME_STR_SIZE, 16).

%% `odbc' binds a DECIMAL/NUMERIC parameter with a precision above 15 as a
%% string, using a 50 byte buffer (`DEC_NUM_LENGTH' in `odbcserver.c') minus
%% the 2 NUL bytes it appends.
-define(DECIMAL_STR_SIZE, 48).

%% SQLSTATEs which mean "the connection is gone"; such failures are recoverable.
-define(BROKEN_CONNECTION_SQLSTATES, [
    %% unable to connect to data source
    <<"08001">>,
    %% connection does not exist
    <<"08003">>,
    %% connection failure
    <<"08006">>,
    %% communication link failure
    <<"08S01">>,
    %% SQL Server ODBC driver internal states.
    <<"IMC01">>,
    <<"IMC02">>,
    <<"IMC03">>,
    <<"IMC04">>,
    <<"IMC05">>,
    <<"IMC06">>
]).

%% SQLSTATEs which mean "the statement timed out"; the request may succeed when
%% it is retried, so it is recoverable.
-define(TIMEOUT_SQLSTATES, [<<"HYT00">>, <<"HYT01">>]).

%%====================================================================
%% Connection management
%%====================================================================

%% @doc Open a connection built from `ConnMap'.
-spec connect(conn_map()) -> {ok, connection()} | {error, term()}.
connect(ConnMap) ->
    case validate_conn_map(ConnMap) of
        ok ->
            Opts = [
                {binary_strings, on},
                {extended_errors, on}
            ],
            odbc:connect(build_conn_string(ConnMap), Opts);
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Close an open connection.
-spec disconnect(connection()) -> ok | {error, term()}.
disconnect(Connection) ->
    odbc:disconnect(Connection).

%% A DSN carries every connection attribute; otherwise the driver and the
%% server must be spelled out. Fail fast with a config error instead of letting
%% the driver report a confusing "data source name not found".
validate_conn_map(ConnMap) ->
    case dsn(ConnMap) of
        undefined ->
            Missing = [
                Attr
             || {Attr, Value} <-
                    [
                        {driver, maps:get(driver, ConnMap, undefined)},
                        {server, maps:get(server, ConnMap, undefined)}
                    ],
                is_blank(Value)
            ],
            case Missing of
                [] -> ok;
                _ -> {error, {missing_conn_attribute, Missing}}
            end;
        _Dsn ->
            ok
    end.

is_blank(undefined) -> true;
is_blank(<<>>) -> true;
is_blank("") -> true;
is_blank(_) -> false.

dsn(ConnMap) ->
    Value = maps:get(dsn, ConnMap, undefined),
    case is_blank(Value) of
        true -> undefined;
        false -> Value
    end.

%%====================================================================
%% Query API
%%====================================================================

%% @doc Run a literal SQL statement.
-spec sql_query(connection(), sql()) -> term().
sql_query(Connection, SQL) ->
    sql_query(Connection, SQL, infinity).

-spec sql_query(connection(), sql(), odbc_timeout()) -> term().
sql_query(Connection, SQL, Timeout) ->
    odbc:sql_query(Connection, to_str(SQL), Timeout).

%% @doc Run a parameterized query using `odbc:param_query/4'.
%% Note: `odbc:param_query' requires the SQL query to be a string (list), not a
%% binary, hence the conversion via `to_str/1'.
-spec param_query(connection(), sql(), [{col_type(), [value()]}], odbc_timeout()) -> term().
param_query(Connection, SQL, Params, Timeout) ->
    odbc:param_query(Connection, to_str(SQL), Params, Timeout).

%% @doc Describe a table: returns `{ok, [{ColName, OdbcType}]}'.
-spec describe_table(connection(), unicode:chardata()) -> term().
describe_table(Connection, Table) ->
    describe_table(Connection, Table, infinity).

-spec describe_table(connection(), unicode:chardata(), odbc_timeout()) -> term().
describe_table(Connection, Table, Timeout) ->
    odbc:describe_table(Connection, to_str(Table), Timeout).

%%====================================================================
%% Connection string builder
%%====================================================================

%% @doc Build an ODBC connection string from a `conn_map()'.
-spec build_conn_string(conn_map()) -> string().
build_conn_string(ConnMap) ->
    case dsn(ConnMap) of
        undefined ->
            server_conn_string(ConnMap);
        Dsn ->
            dsn_conn_string(Dsn, ConnMap)
    end.

dsn_conn_string(Dsn, ConnMap) ->
    Parts0 = ["DSN=" ++ str(Dsn)],
    Parts1 = append_opt("UID", maps:get(username, ConnMap, undefined), Parts0),
    Parts2 = append_secret("PWD", maps:get(password, ConnMap, undefined), Parts1),
    lists:flatten(lists:join(";", Parts2)).

server_conn_string(ConnMap) ->
    Parts0 =
        case maps:get(driver, ConnMap, undefined) of
            undefined -> [];
            Driver -> ["Driver=" ++ driver_str(Driver)]
        end,
    Parts1 = Parts0 ++ ["Server=" ++ str(server_with_port(ConnMap))],
    Parts2 = append_opt("UID", maps:get(username, ConnMap, undefined), Parts1),
    Parts3 = append_secret("PWD", maps:get(password, ConnMap, undefined), Parts2),
    Parts4 = append_opt("Charset", maps:get(charset, ConnMap, undefined), Parts3),
    lists:flatten(lists:join(";", Parts4)).

%% Wrap a registered driver name in `{}' unless it is a path to the driver
%% library. Only an absolute path or a path containing a directory separator is
%% treated as a path: registered driver names may legitimately contain dots
%% (e.g. `MySQL ODBC 8.0 Driver').
driver_str(Driver) ->
    Path = str(Driver),
    case is_driver_path(Path) of
        true -> Path;
        false -> "{" ++ Path ++ "}"
    end.

is_driver_path([$/ | _]) -> true;
is_driver_path([$\\ | _]) -> true;
is_driver_path(Path) -> lists:member($/, Path) orelse lists:member($\\, Path).

%% If the configured `server' already carries an explicit port, keep it;
%% otherwise append the `port' field (if any). IPv6 addresses are bracketed
%% (`[::1]:5236').
server_with_port(ConnMap) ->
    Server = str(maps:get(server, ConnMap, "")),
    case has_explicit_port(Server) of
        true ->
            Server;
        false ->
            case maps:get(port, ConnMap, undefined) of
                undefined -> Server;
                Port -> append_port(Server, integer_to_list(Port))
            end
    end.

append_port(Server, Port) ->
    case {lists:member($:, Server), lists:prefix("[", Server)} of
        {true, true} -> Server ++ ":" ++ Port;
        {true, false} -> "[" ++ Server ++ "]:" ++ Port;
        {false, _} -> Server ++ ":" ++ Port
    end.

has_explicit_port(Server) ->
    case re:run(Server, <<"^\\[.+\\]:[0-9]+$">>, [{capture, none}]) of
        match ->
            true;
        nomatch ->
            re:run(Server, <<"^[^:]+:[0-9]+$">>, [{capture, none}]) =:= match
    end.

%% Blank attributes are omitted entirely: appending `UID=' or `PWD=' overrides
%% the value carried by a DSN entry with an empty one.
append_opt(_Key, Value, Acc) when
    Value =:= undefined; Value =:= null; Value =:= <<>>; Value =:= ""
->
    Acc;
append_opt(Key, Value, Acc) ->
    Acc ++ [Key ++ "=" ++ str(Value)].

append_secret(_Key, undefined, Acc) ->
    Acc;
append_secret(_Key, null, Acc) ->
    Acc;
append_secret(Key, Secret, Acc) ->
    case emqx_secret:unwrap(Secret) of
        Value when Value =:= undefined; Value =:= null; Value =:= <<>>; Value =:= "" ->
            Acc;
        Value ->
            Acc ++ [Key ++ "=" ++ str(Value)]
    end.

%%====================================================================
%% Type conversion (describe_table type -> param_query type/value)
%%====================================================================

%% @doc Map a `describe_table'-reported type to a type accepted by
%% `odbc:param_query'. For a handful of column types (BIGINT, DATE, TIME)
%% `param_query' has no dedicated binding, but the driver implicitly converts a
%% `sql_varchar' value, so the string representation is bound instead.
-spec to_param_type(col_type()) -> odbc:odbc_data_type() | {error, term()}.
to_param_type(T) when
    T =:= sql_integer;
    T =:= sql_smallint;
    T =:= sql_tinyint
->
    T;
to_param_type({sql_decimal, _P, _S} = T) ->
    T;
to_param_type({sql_numeric, _P, _S} = T) ->
    T;
to_param_type({sql_char, _N} = T) ->
    T;
to_param_type({sql_varchar, _N} = T) ->
    T;
to_param_type({sql_wchar, _N} = T) ->
    T;
to_param_type({sql_wvarchar, _N} = T) ->
    T;
to_param_type({sql_float, _P} = T) ->
    T;
to_param_type(T) when T =:= sql_real; T =:= sql_double -> T;
to_param_type(sql_bit) ->
    sql_bit;
to_param_type(sql_timestamp) ->
    sql_timestamp;
%% Coerced types: bind the string representation (see to_odbc_value/2).
to_param_type('SQL_BIGINT') ->
    {sql_varchar, ?BIGINT_STR_SIZE};
to_param_type('SQL_TYPE_DATE') ->
    {sql_varchar, ?DATE_STR_SIZE};
to_param_type('SQL_TYPE_TIME') ->
    {sql_varchar, ?TIME_STR_SIZE};
to_param_type('SQL_TYPE_TIMESTAMP') ->
    sql_timestamp;
%% These types are reported by the driver in capital letters, which means that
%% `odbc:param_query/4' has no native binding for them. They are rejected
%% instead of being coerced to a string binding:
%%   * character data would be sent as NUL terminated strings (`SQL_NTS'), so
%%     binary data would silently be truncated at the first NUL byte;
%%   * large object columns (CLOB/NCLOB) report an unbounded column size, so
%%     there is no safe way to bind them.
%% Connectors are expected to reject such columns when the channel is created,
%% rather than failing on every message at runtime.
to_param_type({sql_wlongvarchar, _N}) ->
    {error, {unrecoverable_error, {unsupported_odbc_type, sql_wlongvarchar}}};
to_param_type(Type) when
    Type =:= 'SQL_BINARY';
    Type =:= 'SQL_VARBINARY';
    Type =:= 'SQL_LONGVARBINARY';
    Type =:= 'SQL_LONGVARCHAR';
    Type =:= 'SQL_UNKNOWN_TYPE';
    Type =:= 'ODBC_UNSUPPORTED_TYPE'
->
    {error, {unrecoverable_error, {unsupported_odbc_type, Type}}};
to_param_type(Other) ->
    {error, {unrecoverable_error, {unsupported_odbc_type, Other}}}.

%% @doc Return the `param_query' type to bind the given (already converted)
%% values with.
%%
%% `odbc' sizes its parameter buffer from the size in the type tuple:
%%   * CHAR/VARCHAR: `Size + 1' bytes per value;
%%   * WCHAR/WVARCHAR: `(Size + 1) * 2' bytes per value;
%% and then appends a NUL terminator of its own to binary values (see
%% `odbc:string_terminate_value/1', which appends 2 bytes). The buffer is filled
%% by `ei_decode_binary/4', which does not check the destination size, so a value
%% longer than the declared column would overflow the port program's heap.
%% The declared size is kept when it already leaves room for the values (so that
%% the driver sees the real column size) and enlarged to fit them otherwise.
-spec fit_param_type(col_type(), [value()]) -> odbc:odbc_data_type().
fit_param_type({sql_char, Size}, Values) when is_integer(Size), Size >= 0 ->
    {sql_char, fit_char_size(Size, Values)};
fit_param_type({sql_varchar, Size}, Values) when is_integer(Size), Size >= 0 ->
    {sql_varchar, fit_char_size(Size, Values)};
fit_param_type({sql_wchar, Size}, Values) when is_integer(Size), Size >= 0 ->
    {sql_wchar, fit_wide_size(Size, Values)};
fit_param_type({sql_wvarchar, Size}, Values) when is_integer(Size), Size >= 0 ->
    {sql_wvarchar, fit_wide_size(Size, Values)};
fit_param_type(Type, _Values) ->
    Type.

%% The buffer must hold the longest value plus the 2 NUL bytes `odbc' appends.
fit_char_size(Size, Values) ->
    max(Size, char_buffer_size(Values)).

%% Wide values are counted in UTF-16 code units; the buffer must hold the
%% longest value plus the UTF-16 NUL `odbc' appends.
fit_wide_size(Size, Values) ->
    max(Size, wide_buffer_size(Values)).

%% The buffer must hold the longest value plus the 2 NUL bytes `odbc' appends.
char_buffer_size(Values) ->
    max_value_size(Values, fun
        (Value) when is_binary(Value) -> byte_size(Value);
        (_) -> 0
    end) + 1.

%% Wide values are counted in UTF-16 code units; the buffer must hold the
%% longest value plus the UTF-16 NUL `odbc' appends.
wide_buffer_size(Values) ->
    max_value_size(Values, fun
        (Value) when is_binary(Value) -> byte_size(Value) div 2;
        (_) -> 0
    end).

max_value_size(Values, SizeFun) ->
    lists:max([SizeFun(Value) || Value <- Values] ++ [0]).

%% @doc Convert a concrete value into the Erlang representation expected by
%% `param_query' for the given type. `undefined' handling is done by /3.
-spec to_odbc_value(value(), col_type()) -> {ok, value()} | {error, term()}.
to_odbc_value(null, _Type) ->
    {ok, null};
to_odbc_value(undefined, _Type) ->
    {error, {unrecoverable_error, undefined_var}};
to_odbc_value(Value, Type) when
    Type =:= sql_integer;
    Type =:= sql_smallint;
    Type =:= sql_tinyint
->
    to_int(Value);
to_odbc_value(Value, {sql_decimal, P, S}) ->
    to_decimal(Value, P, S);
to_odbc_value(Value, {sql_numeric, P, S}) ->
    to_decimal(Value, P, S);
to_odbc_value(Value, Type) when
    Type =:= sql_real;
    Type =:= sql_double
->
    to_float(Value);
to_odbc_value(Value, {sql_float, _P}) ->
    to_float(Value);
to_odbc_value(Value, sql_bit) ->
    to_bool(Value);
to_odbc_value(Value, sql_timestamp) ->
    to_timestamp(Value);
%% Defensive: some drivers report the TIMESTAMP column with the SQL_* name form.
to_odbc_value(Value, 'SQL_TYPE_TIMESTAMP') ->
    to_timestamp(Value);
to_odbc_value(Value, sql_char) ->
    to_char_value(Value, undefined, sql_char);
to_odbc_value(Value, {sql_char, Size}) ->
    to_char_value(Value, Size, {sql_char, Size});
to_odbc_value(Value, sql_varchar) ->
    to_char_value(Value, undefined, sql_varchar);
to_odbc_value(Value, {sql_varchar, Size}) ->
    to_char_value(Value, Size, {sql_varchar, Size});
%% Wide character columns must be sent as UTF-16LE binaries (`odbc' adds the
%% NUL terminator itself).
to_odbc_value(Value, sql_wchar) ->
    to_wide_value(Value, undefined, sql_wchar);
to_odbc_value(Value, {sql_wchar, Size}) ->
    to_wide_value(Value, Size, {sql_wchar, Size});
to_odbc_value(Value, sql_wvarchar) ->
    to_wide_value(Value, undefined, sql_wvarchar);
to_odbc_value(Value, {sql_wvarchar, Size}) ->
    to_wide_value(Value, Size, {sql_wvarchar, Size});
%% The driver coerces a string into these columns.
to_odbc_value(Value, 'SQL_BIGINT') ->
    to_char_value(Value, ?BIGINT_STR_SIZE, 'SQL_BIGINT');
to_odbc_value(Value, 'SQL_TYPE_DATE') ->
    to_char_value(Value, ?DATE_STR_SIZE, 'SQL_TYPE_DATE');
to_odbc_value(Value, 'SQL_TYPE_TIME') ->
    to_char_value(Value, ?TIME_STR_SIZE, 'SQL_TYPE_TIME');
to_odbc_value(Value, Other) ->
    {error, {unrecoverable_error, {unsupported_odbc_type, Other, Value}}}.

%% @doc Convert a value with explicit `undefined' policy.
%% `UndefinedAsNull = true' maps `undefined' to DB `null'; `false' is an error.
-spec to_odbc_value(value(), col_type(), boolean()) ->
    {ok, value()} | {error, term()}.
to_odbc_value(undefined, _Type, true) ->
    {ok, null};
to_odbc_value(undefined, _Type, false) ->
    {error, {unrecoverable_error, undefined_var}};
to_odbc_value(Value, Type, _) ->
    to_odbc_value(Value, Type).

%%====================================================================
%% Error classification
%%====================================================================

%% @doc Normalize an ODBC failure into a recoverable/unrecoverable error tuple
%% as expected by the `emqx_resource' behaviour.
-spec classify_error(term()) ->
    {recoverable_error, term()} | {unrecoverable_error, term()}.
classify_error(Reason) ->
    case is_connection_closed_error(Reason) orelse is_connection_broken_error(Reason) of
        true ->
            {recoverable_error, <<"connection_closed">>};
        false ->
            case is_timeout_error(Reason) of
                true ->
                    {recoverable_error, <<"timeout">>};
                false ->
                    case is_table_not_found_error(Reason) of
                        true ->
                            {unrecoverable_error, {invalid_request, <<"table_not_found">>}};
                        false ->
                            {unrecoverable_error, {invalid_request, Reason}}
                    end
            end
    end.

%% @doc `connection_closed' is what `odbc' returns when the port program or the
%% connection process died.
-spec is_connection_closed_error(term()) -> boolean().
is_connection_closed_error(Reason) ->
    {_SqlState, Text} = split_reason(Reason),
    binary:match(Text, <<"connection_closed">>) =/= nomatch.

%% @doc Detect a driver reported communication failure from its SQLSTATE.
%% Note that the driver reports ordinary SQL errors with `HY000' ("general
%% error") as well, so it is deliberately not matched: classifying those as
%% recoverable would make the resource retry a permanently failing request
%% forever.
-spec is_connection_broken_error(term()) -> boolean().
is_connection_broken_error(Reason) ->
    {SqlState, _Text} = split_reason(Reason),
    lists:member(SqlState, ?BROKEN_CONNECTION_SQLSTATES).

-spec is_timeout_error(term()) -> boolean().
is_timeout_error(Reason) ->
    {SqlState, Text} = split_reason(Reason),
    case SqlState of
        <<>> -> Text =:= <<"timeout">>;
        _ -> lists:member(SqlState, ?TIMEOUT_SQLSTATES)
    end.

-spec is_table_not_found_error(term()) -> boolean().
is_table_not_found_error(Reason) ->
    {SqlState, Text} = split_reason(Reason),
    SqlState =:= <<"42S02">> orelse
        re:run(Text, <<"table.*not.*exist|not.*exist.*table">>, [
            caseless, {capture, none}
        ]) =:= match.

%% `odbc' reports failures as `{error, Reason}'. With `extended_errors' enabled
%% (which we always do) the reason is `{SqlState, NativeError, Message}';
%% otherwise (and for connection level failures) it is a plain string, possibly
%% carrying the SQLSTATE in a " SQLSTATE IS: <state>" suffix.
split_reason(Reason) when
    is_tuple(Reason),
    tuple_size(Reason) =:= 3,
    (is_list(element(1, Reason)) orelse is_binary(element(1, Reason)))
->
    {to_bin(element(1, Reason)), to_bin(element(3, Reason))};
split_reason(Reason) ->
    Text = to_bin(Reason),
    {sqlstate_of(Text), Text}.

sqlstate_of(Text) ->
    case re:run(Text, <<"SQLSTATE IS: ([0-9A-Za-z]{5})">>, [{capture, all_but_first, binary}]) of
        {match, [SqlState]} -> SqlState;
        nomatch -> <<>>
    end.

%%====================================================================
%% Value helpers
%%====================================================================

to_int(Value) ->
    case maybe_int(Value) of
        {ok, I} -> {ok, I};
        error -> {error, {unrecoverable_error, {invalid_value, Value}}}
    end.

maybe_int(I) when is_integer(I) -> {ok, I};
maybe_int(B) when is_binary(B) ->
    try
        {ok, binary_to_integer(B)}
    catch
        _:_ -> error
    end;
maybe_int(S) when is_list(S) ->
    try
        {ok, list_to_integer(S)}
    catch
        _:_ -> error
    end;
%% A float is accepted only when it has no fractional part: binding `1.9' to an
%% integer column would otherwise silently store `1'. `==' compares the truncated
%% integer and the float numerically (`1' == `1.0'); going through the integer
%% also avoids converting a very large integer back to a float.
maybe_int(F) when is_float(F) ->
    I = trunc(F),
    case I == F of
        true -> {ok, I};
        false -> error
    end;
maybe_int(_) ->
    error.

to_float(Value) ->
    case maybe_float(Value) of
        {ok, F} -> {ok, F};
        error -> {error, {unrecoverable_error, {invalid_value, Value}}}
    end.

maybe_float(F) when is_float(F) ->
    {ok, F};
maybe_float(I) when is_integer(I) ->
    {ok, float(I)};
maybe_float(B) when is_binary(B); is_list(B) ->
    case try_float(B) of
        {ok, F} ->
            {ok, F};
        error ->
            case try_int(B) of
                {ok, I} -> {ok, float(I)};
                error -> error
            end
    end;
maybe_float(_) ->
    error.

try_float(B) when is_binary(B) ->
    try
        {ok, binary_to_float(B)}
    catch
        _:_ -> error
    end;
try_float(S) when is_list(S) ->
    try
        {ok, list_to_float(S)}
    catch
        _:_ -> error
    end.

try_int(B) when is_binary(B) ->
    try
        {ok, binary_to_integer(B)}
    catch
        _:_ -> error
    end;
try_int(S) when is_list(S) ->
    try
        {ok, list_to_integer(S)}
    catch
        _:_ -> error
    end.

%% `odbc' binds DECIMAL/NUMERIC parameters according to the precision and the
%% scale (see `map_dec_num_2_c_column/3' in `odbcserver.c'):
%%   * precision =< 9 with scale 0 -> SQL_C_SLONG (integer);
%%   * precision 10..15 with scale 0, or precision =< 15 with scale > 0 ->
%%     SQL_C_DOUBLE (float);
%%   * precision >= 16 -> SQL_C_CHAR (string, bound from a 50 byte buffer).
%% The value must match the binding, otherwise `odbc' decodes it into
%% uninitialized memory and the driver writes garbage into the column.
to_decimal(Value, P, S) when S =:= 0, P =< 9 ->
    to_int(Value);
to_decimal(Value, P, S) when
    (P >= 10 andalso P =< 15 andalso S =:= 0) orelse (P =< 15 andalso S > 0)
->
    to_float(Value);
to_decimal(Value, _P, _S) ->
    %% Unlike the character types, `odbc' does not NUL terminate a string bound
    %% DECIMAL/NUMERIC value (`fix_params/1' skips `string_terminate/1' for it)
    %% even though it is passed to the driver as `SQL_C_CHAR' with `SQL_NTS'.
    %% Without the terminator the driver reads whatever follows the value in the
    %% parameter buffer, which fails with a conversion error (DM8 reports
    %% `22018 Invalid convert string').
    case to_char_value(Value, ?DECIMAL_STR_SIZE, decimal) of
        {ok, Bin} -> {ok, <<Bin/binary, 0>>};
        {error, _} = Error -> Error
    end.

to_bool(true) ->
    {ok, true};
to_bool(false) ->
    {ok, false};
to_bool(B) when is_binary(B) ->
    case B of
        <<"true">> -> {ok, true};
        <<"false">> -> {ok, false};
        <<"1">> -> {ok, true};
        <<"0">> -> {ok, false};
        _ -> {error, {unrecoverable_error, {invalid_value, B}}}
    end;
to_bool(1) ->
    {ok, true};
to_bool(0) ->
    {ok, false};
to_bool(Other) ->
    {error, {unrecoverable_error, {invalid_value, Other}}}.

to_timestamp({{Y, M, D}, {H, Mi, S}} = T) when
    is_integer(Y),
    is_integer(M),
    is_integer(D),
    is_integer(H),
    is_integer(Mi),
    is_integer(S)
->
    {ok, T};
to_timestamp(B) when is_binary(B) ->
    case parse_timestamp(B) of
        {ok, T} -> {ok, T};
        error -> {error, {unrecoverable_error, {invalid_timestamp, B}}}
    end;
to_timestamp(S) when is_list(S) ->
    to_timestamp(unicode:characters_to_binary(S));
to_timestamp(Other) ->
    {error, {unrecoverable_error, {invalid_timestamp, Other}}}.

%% Accepts `YYYY-MM-DD[ HH:MM:SS[.SSS]]'.
parse_timestamp(Bin) ->
    case
        re:run(
            Bin,
            <<"^(\\d{4})-(\\d{2})-(\\d{2})(?:[ T](\\d{2}):(\\d{2}):(\\d{2})(?:\\.\\d+)?)?$">>,
            [{capture, all_but_first, binary}]
        )
    of
        {match, [Y, M, D]} ->
            {ok, {{b2i(Y), b2i(M), b2i(D)}, {0, 0, 0}}};
        {match, [Y, M, D, H, Mi, S]} ->
            {ok, {{b2i(Y), b2i(M), b2i(D)}, {b2i(H), b2i(Mi), b2i(S)}}};
        nomatch ->
            error
    end.

b2i(B) -> binary_to_integer(B).

%% Convert a value for a character column, rejecting what `odbc' cannot bind
%% faithfully: a NUL byte would silently truncate the value (character
%% parameters are bound as NUL terminated strings) and a value longer than the
%% column would overflow the parameter buffer.
to_char_value(Value, Size, Type) ->
    Bin = to_bin(Value),
    case check_string(Bin, Size, Type) of
        ok -> {ok, Bin};
        {error, _} = Error -> Error
    end.

check_string(Bin, Size, Type) ->
    case binary:match(Bin, <<0>>) of
        {_, _} ->
            {error, {unrecoverable_error, {invalid_value, {nul_byte_in_string, Type}}}};
        nomatch when is_integer(Size), Size >= 0, byte_size(Bin) > Size ->
            {error,
                {unrecoverable_error,
                    {invalid_value, {value_too_long, Type, Size, byte_size(Bin)}}}};
        nomatch ->
            ok
    end.

%% `odbc' binds wide character parameters as NUL terminated strings (`SQL_NTS'),
%% so the value must be a UTF-16LE binary; `odbc' appends the terminating NUL
%% itself (see `odbc:string_terminate_value/1').
to_wide_value(Value, Size, Type) ->
    case to_utf16le(Value) of
        {ok, Encoded} ->
            case check_wide(Value, Encoded, Size, Type) of
                ok -> {ok, Encoded};
                {error, _} = Error -> Error
            end;
        error ->
            {error, {unrecoverable_error, {invalid_value, {not_a_string, Type}}}}
    end.

check_wide(Value, Encoded, Size, Type) ->
    case binary:match(to_bin(Value), <<0>>) of
        {_, _} ->
            {error, {unrecoverable_error, {invalid_value, {nul_byte_in_string, Type}}}};
        nomatch when is_integer(Size), Size >= 0, byte_size(Encoded) div 2 > Size ->
            {error,
                {unrecoverable_error,
                    {invalid_value, {value_too_long, Type, Size, byte_size(Encoded) div 2}}}};
        nomatch ->
            ok
    end.

to_utf16le(Value) when is_binary(Value) ->
    utf16le(Value);
to_utf16le(Value) when is_list(Value) ->
    utf16le(unicode:characters_to_binary(Value));
to_utf16le(_Value) ->
    error.

utf16le(Bin) ->
    case unicode:characters_to_binary(Bin, utf8, {utf16, little}) of
        Encoded when is_binary(Encoded) -> {ok, Encoded};
        _ -> error
    end.

to_bin(B) when is_binary(B) -> B;
to_bin(L) when is_list(L) -> unicode:characters_to_binary(L);
to_bin(N) when is_integer(N) -> integer_to_binary(N);
to_bin(F) when is_float(F) -> float_to_binary(F, [{decimals, 12}, compact]);
to_bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
to_bin(Other) -> iolist_to_binary(io_lib:format("~p", [Other])).

to_str(B) when is_binary(B) -> binary_to_list(B);
to_str(S) when is_list(S) -> S;
to_str(N) when is_integer(N) -> integer_to_list(N);
to_str(A) when is_atom(A) -> atom_to_list(A).

str(B) when is_binary(B) -> binary_to_list(B);
str(S) when is_list(S) -> S;
str(N) when is_integer(N) -> integer_to_list(N);
str(F) when is_float(F) -> float_to_list(F, [{decimals, 12}, compact]);
str(A) when is_atom(A) -> atom_to_list(A);
str(Other) -> lists:flatten(io_lib:format("~p", [Other])).
