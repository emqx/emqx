%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_ee_connector_clickhouse).

-include_lib("emqx_connector/include/emqx_connector.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-behaviour(emqx_resource).

-import(hoconsc, [mk/2, enum/1, ref/2]).

%%=====================================================================
%% Exports
%%=====================================================================

%% Hocon config schema exports
-export([
    roots/0,
    fields/1,
    values/1
]).

%% callbacks for behaviour emqx_resource
-export([
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_batch_query/3,
    on_get_status/2
]).

%% callbacks for ecpool
-export([connect/1]).

%% Internal exports used to execute code with ecpool worker
-export([
    execute_sql_in_clickhouse_server_using_connection/2
]).

%%=====================================================================
%% Types
%%=====================================================================

-type url() :: emqx_http_lib:uri_map().
-reflect_type([url/0]).
-typerefl_from_string({url/0, emqx_http_lib, uri_parse}).

-type templates() ::
    #{}
    | #{
        send_message_template := term(),
        extend_send_message_template := term()
    }.

-type state() ::
    #{
        templates := templates(),
        poolname := atom()
    }.

-type clickhouse_config() :: map().

%%=====================================================================
%% Configuration and default values
%%=====================================================================

roots() ->
    [{config, #{type => hoconsc:ref(?MODULE, config)}}].

fields(config) ->
    [
        {url,
            hoconsc:mk(
                url(),
                #{
                    required => true,
                    validator => fun
                        (#{query := _Query}) ->
                            {error, "There must be no query in the url"};
                        (_) ->
                            ok
                    end,
                    desc => ?DESC("base_url")
                }
            )},
        {connect_timeout,
            hoconsc:mk(
                emqx_schema:duration_ms(),
                #{
                    default => <<"15s">>,
                    desc => ?DESC("connect_timeout")
                }
            )}
    ] ++ emqx_connector_schema_lib:relational_db_fields().

values(post) ->
    maps:merge(values(put), #{name => <<"connector">>});
values(get) ->
    values(post);
values(put) ->
    #{
        database => <<"mqtt">>,
        enable => true,
        pool_size => 8,
        type => clickhouse,
        url => <<"http://127.0.0.1:8123">>
    };
values(_) ->
    #{}.

%% ===================================================================
%% Callbacks defined in emqx_resource
%% ===================================================================

callback_mode() -> always_sync.

%% -------------------------------------------------------------------
%% on_start callback and related functions
%% -------------------------------------------------------------------

-spec on_start(resource_id(), clickhouse_config()) -> {ok, state()} | {error, _}.

on_start(
    InstanceID,
    #{
        url := URL,
        database := DB,
        pool_size := PoolSize,
        connect_timeout := ConnectTimeout
    } = Config
) ->
    ?SLOG(info, #{
        msg => "starting_clickhouse_connector",
        connector => InstanceID,
        config => emqx_misc:redact(Config)
    }),
    PoolName = emqx_plugin_libs_pool:pool_name(InstanceID),
    Options = [
        {url, URL},
        {user, maps:get(username, Config, "default")},
        {key, emqx_secret:wrap(maps:get(password, Config, "public"))},
        {database, DB},
        {auto_reconnect, ?AUTO_RECONNECT_INTERVAL},
        {pool_size, PoolSize},
        {pool, PoolName}
    ],
    InitState = #{
        poolname => PoolName,
        connect_timeout => ConnectTimeout
    },
    try
        Templates = prepare_sql_templates(Config),
        State = maps:merge(InitState, #{templates => Templates}),
        case emqx_plugin_libs_pool:start_pool(PoolName, ?MODULE, Options) of
            ok ->
                {ok, State};
            {error, Reason} ->
                log_start_error(Config, Reason, none),
                {error, Reason}
        end
    catch
        _:CatchReason:Stacktrace ->
            log_start_error(Config, CatchReason, Stacktrace),
            {error, CatchReason}
    end.

log_start_error(Config, Reason, Stacktrace) ->
    StacktraceMap =
        case Stacktrace of
            none -> #{};
            _ -> #{stacktrace => Stacktrace}
        end,
    LogMessage =
        #{
            msg => "clickhouse_connector_start_failed",
            error_reason => Reason,
            config => emqx_misc:redact(Config)
        },
    ?SLOG(info, maps:merge(LogMessage, StacktraceMap)),
    ?tp(
        clickhouse_connector_start_failed,
        #{error => Reason}
    ).

%% Helper functions to prepare SQL tempaltes

prepare_sql_templates(#{
    sql := Template,
    batch_value_separator := Separator
}) ->
    InsertTemplate =
        emqx_plugin_libs_rule:preproc_tmpl(Template),
    BulkExtendInsertTemplate =
        prepare_sql_bulk_extend_template(Template, Separator),
    #{
        send_message_template => InsertTemplate,
        extend_send_message_template => BulkExtendInsertTemplate
    };
prepare_sql_templates(_) ->
    %% We don't create any templates if this is a non-bridge connector
    #{}.

prepare_sql_bulk_extend_template(Template, Separator) ->
    ValuesTemplate = split_clickhouse_insert_sql(Template),
    %% The value part has been extracted
    %% Add separator before ValuesTemplate so that one can append it
    %% to an insert template
    ExtendParamTemplate = iolist_to_binary([Separator, ValuesTemplate]),
    emqx_plugin_libs_rule:preproc_tmpl(ExtendParamTemplate).

%% This function is similar to emqx_plugin_libs_rule:split_insert_sql/1 but can
%% also handle Clickhouse's SQL extension for INSERT statments that allows the
%% user to specify different formats:
%%
%% https://clickhouse.com/docs/en/sql-reference/statements/insert-into/
%%
split_clickhouse_insert_sql(SQL) ->
    ErrorMsg = <<"The SQL template should be an SQL INSERT statement but it is something else.">>,
    case
        re:split(SQL, "(\\s+(?i:values)|(?i:format\\s+(?:[A-Za-z0-9_])+)\\s+)", [{return, binary}])
    of
        [Part1, _, Part3] ->
            case string:trim(Part1, leading) of
                <<"insert", _/binary>> ->
                    Part3;
                <<"INSERT", _/binary>> ->
                    Part3;
                _ ->
                    erlang:error(ErrorMsg)
            end;
        _ ->
            erlang:error(ErrorMsg)
    end.

% This is a callback for ecpool which is triggered by the call to
% emqx_plugin_libs_pool:start_pool in on_start/2

connect(Options) ->
    URL = iolist_to_binary(emqx_http_lib:normalize(proplists:get_value(url, Options))),
    User = proplists:get_value(user, Options),
    Database = proplists:get_value(database, Options),
    Key = emqx_secret:unwrap(proplists:get_value(key, Options)),
    Pool = proplists:get_value(pool, Options),
    PoolSize = proplists:get_value(pool_size, Options),
    FixedOptions = [
        {url, URL},
        {database, Database},
        {user, User},
        {key, Key},
        {pool, Pool},
        {pool_size, PoolSize}
    ],
    case clickhouse:start_link(FixedOptions) of
        {ok, _Conn} = Ok ->
            Ok;
        {error, Reason} ->
            {error, Reason}
    end.

%% -------------------------------------------------------------------
%% on_stop emqx_resouce callback
%% -------------------------------------------------------------------

-spec on_stop(resource_id(), resource_state()) -> term().

on_stop(ResourceID, #{poolname := PoolName}) ->
    ?SLOG(info, #{
        msg => "stopping clickouse connector",
        connector => ResourceID
    }),
    emqx_plugin_libs_pool:stop_pool(PoolName).

%% -------------------------------------------------------------------
%% on_get_status emqx_resouce callback and related functions
%% -------------------------------------------------------------------

on_get_status(
    _InstId,
    #{
        poolname := PoolName,
        connect_timeout := Timeout
    } = State
) ->
    case do_get_status(PoolName, Timeout) of
        ok ->
            {connected, State};
        {error, Reason} ->
            {disconnected, State, Reason}
    end.

do_get_status(PoolName, Timeout) ->
    Workers = [Worker || {_WorkerName, Worker} <- ecpool:workers(PoolName)],
    DoPerWorker =
        fun(Worker) ->
            case ecpool_worker:exec(Worker, fun clickhouse:detailed_status/1, Timeout) of
                ok ->
                    ok;
                {error, Reason} = Error ->
                    ?SLOG(error, #{
                        msg => "clickhouse_connector_get_status_failed",
                        reason => Reason,
                        worker => Worker
                    }),
                    Error
            end
        end,
    try emqx_misc:pmap(DoPerWorker, Workers, Timeout) of
        Results ->
            case [E || {error, _} = E <- Results] of
                [] ->
                    ok;
                Errors ->
                    hd(Errors)
            end
    catch
        exit:timeout ->
            ?SLOG(error, #{
                msg => "clickhouse_connector_pmap_failed",
                reason => timeout
            }),
            {error, timeout}
    end.

%% -------------------------------------------------------------------
%% on_query emqx_resouce callback and related functions
%% -------------------------------------------------------------------

-spec on_query
    (resource_id(), Request, resource_state()) -> query_result() when
        Request :: {RequestType, Data},
        RequestType :: send_message,
        Data :: map();
    (resource_id(), Request, resource_state()) -> query_result() when
        Request :: {RequestType, SQL},
        RequestType :: sql | query,
        SQL :: binary().

on_query(
    ResourceID,
    {RequestType, DataOrSQL},
    #{poolname := PoolName} = State
) ->
    ?SLOG(debug, #{
        msg => "clickhouse connector received sql query",
        connector => ResourceID,
        type => RequestType,
        sql => DataOrSQL,
        state => State
    }),
    %% Have we got a query or data to fit into an SQL template?
    SimplifiedRequestType = query_type(RequestType),
    #{templates := Templates} = State,
    SQL = get_sql(SimplifiedRequestType, Templates, DataOrSQL),
    ClickhouseResult = execute_sql_in_clickhouse_server(PoolName, SQL),
    transform_and_log_clickhouse_result(ClickhouseResult, ResourceID, SQL).

get_sql(send_message, #{send_message_template := PreparedSQL}, Data) ->
    emqx_plugin_libs_rule:proc_tmpl(PreparedSQL, Data);
get_sql(_, _, SQL) ->
    SQL.

query_type(sql) ->
    query;
query_type(query) ->
    query;
%% Data that goes to bridges use the prepared template
query_type(send_message) ->
    send_message.

%% -------------------------------------------------------------------
%% on_batch_query emqx_resouce callback and related functions
%% -------------------------------------------------------------------

-spec on_batch_query(resource_id(), BatchReq, resource_state()) -> query_result() when
    BatchReq :: nonempty_list({'send_message', map()}).

on_batch_query(
    ResourceID,
    BatchReq,
    State
) ->
    %% Currently we only support batch requests with the send_message key
    {Keys, ObjectsToInsert} = lists:unzip(BatchReq),
    ensure_keys_are_of_type_send_message(Keys),
    %% Pick out the SQL template
    #{
        templates := Templates,
        poolname := PoolName
    } = State,
    %% Create batch insert SQL statement
    SQL = objects_to_sql(ObjectsToInsert, Templates),
    %% Do the actual query in the database
    ResultFromClickhouse = execute_sql_in_clickhouse_server(PoolName, SQL),
    %% Transform the result to a better format
    transform_and_log_clickhouse_result(ResultFromClickhouse, ResourceID, SQL).

ensure_keys_are_of_type_send_message(Keys) ->
    case lists:all(fun is_send_message_atom/1, Keys) of
        true ->
            ok;
        false ->
            erlang:error(
                {unrecoverable_error,
                    <<"Unexpected type for batch message (Expected send_message)">>}
            )
    end.

is_send_message_atom(send_message) ->
    true;
is_send_message_atom(_) ->
    false.

objects_to_sql(
    [FirstObject | RemainingObjects] = _ObjectsToInsert,
    #{
        send_message_template := InsertTemplate,
        extend_send_message_template := BulkExtendInsertTemplate
    }
) ->
    %% Prepare INSERT-statement and the first row after VALUES
    InsertStatementHead = emqx_plugin_libs_rule:proc_tmpl(InsertTemplate, FirstObject),
    FormatObjectDataFunction =
        fun(Object) ->
            emqx_plugin_libs_rule:proc_tmpl(BulkExtendInsertTemplate, Object)
        end,
    InsertStatementTail = lists:map(FormatObjectDataFunction, RemainingObjects),
    CompleteStatement = erlang:iolist_to_binary([InsertStatementHead, InsertStatementTail]),
    CompleteStatement;
objects_to_sql(_, _) ->
    erlang:error(<<"Templates for bulk insert missing.">>).

%% -------------------------------------------------------------------
%% Helper functions that are used by both on_query/3 and on_batch_query/3
%% -------------------------------------------------------------------

%% This function is used by on_query/3 and on_batch_query/3 to send a query to
%% the database server and receive a result
execute_sql_in_clickhouse_server(PoolName, SQL) ->
    ecpool:pick_and_do(
        PoolName,
        {?MODULE, execute_sql_in_clickhouse_server_using_connection, [SQL]},
        no_handover
    ).

execute_sql_in_clickhouse_server_using_connection(Connection, SQL) ->
    clickhouse:query(Connection, SQL, []).

%% This function transforms the result received from clickhouse to something
%% that is a little bit more readable and creates approprieate log messages
transform_and_log_clickhouse_result({ok, 200, <<"">>} = _ClickhouseResult, _, _) ->
    snabbkaffe_log_return(ok),
    ok;
transform_and_log_clickhouse_result({ok, 200, Data}, _, _) ->
    Result = {ok, Data},
    snabbkaffe_log_return(Result),
    Result;
transform_and_log_clickhouse_result(ClickhouseErrorResult, ResourceID, SQL) ->
    ?SLOG(error, #{
        msg => "clickhouse connector do sql query failed",
        connector => ResourceID,
        sql => SQL,
        reason => ClickhouseErrorResult
    }),
    {error, ClickhouseErrorResult}.

snabbkaffe_log_return(_Result) ->
    ?tp(
        clickhouse_connector_query_return,
        #{result => _Result}
    ).
