%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_tdengine_connector).

-behaviour(emqx_resource).

-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-export([roots/0, fields/1]).

%% `emqx_resource' API
-export([
    callback_mode/0,
    is_buffer_supported/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_batch_query/3,
    on_get_status/2
]).

-export([connect/1, do_get_status/1, execute/3, do_batch_insert/4]).

-import(hoconsc, [mk/2, enum/1, ref/2]).

-define(TD_HOST_OPTIONS, #{
    default_port => 6041
}).

%%=====================================================================
%% Hocon schema
roots() ->
    [{config, #{type => hoconsc:ref(?MODULE, config)}}].

fields(config) ->
    [
        {server, server()}
        | adjust_fields(emqx_connector_schema_lib:relational_db_fields())
    ].

adjust_fields(Fields) ->
    lists:map(
        fun
            ({username, OrigUsernameFn}) ->
                {username, add_default_fn(OrigUsernameFn, <<"root">>)};
            ({password, OrigPasswordFn}) ->
                {password, make_required_fn(OrigPasswordFn)};
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

make_required_fn(OrigFn) ->
    fun
        (required) -> true;
        (Field) -> OrigFn(Field)
    end.

server() ->
    Meta = #{desc => ?DESC("server")},
    emqx_schema:servers_sc(Meta, ?TD_HOST_OPTIONS).

%%========================================================================================
%% `emqx_resource' API
%%========================================================================================

callback_mode() -> always_sync.

is_buffer_supported() -> false.

on_start(
    InstanceId,
    #{
        server := Server,
        username := Username,
        password := Password,
        pool_size := PoolSize
    } = Config
) ->
    ?SLOG(info, #{
        msg => "starting_tdengine_connector",
        connector => InstanceId,
        config => emqx_utils:redact(Config)
    }),

    #{hostname := Host, port := Port} = emqx_schema:parse_server(Server, ?TD_HOST_OPTIONS),
    Options = [
        {host, to_bin(Host)},
        {port, Port},
        {username, Username},
        {password, Password},
        {pool_size, PoolSize},
        {pool, binary_to_atom(InstanceId, utf8)}
    ],

    Prepares = parse_prepare_sql(Config),
    State = Prepares#{pool_name => InstanceId, query_opts => query_opts(Config)},
    case emqx_resource_pool:start(InstanceId, ?MODULE, Options) of
        ok ->
            {ok, State};
        Error ->
            Error
    end.

on_stop(InstanceId, _State) ->
    ?SLOG(info, #{
        msg => "stopping_tdengine_connector",
        connector => InstanceId
    }),
    emqx_resource_pool:stop(InstanceId).

on_query(InstanceId, {query, SQL}, State) ->
    do_query(InstanceId, SQL, State);
on_query(InstanceId, {Key, Data}, #{insert_tokens := InsertTksMap} = State) ->
    case maps:find(Key, InsertTksMap) of
        {ok, Tokens} when is_map(Data) ->
            SQL = emqx_placeholder:proc_sql_param_str(Tokens, Data),
            do_query(InstanceId, SQL, State);
        _ ->
            {error, {unrecoverable_error, invalid_request}}
    end.

%% aggregate the batch queries to one SQL is a heavy job, we should put it in the worker process
on_batch_query(
    InstanceId,
    [{Key, _Data = #{}} | _] = BatchReq,
    #{batch_tokens := BatchTksMap, query_opts := Opts} = State
) ->
    case maps:find(Key, BatchTksMap) of
        {ok, Tokens} ->
            do_query_job(
                InstanceId,
                {?MODULE, do_batch_insert, [Tokens, BatchReq, Opts]},
                State
            );
        _ ->
            {error, {unrecoverable_error, batch_prepare_not_implemented}}
    end;
on_batch_query(InstanceId, BatchReq, State) ->
    LogMeta = #{connector => InstanceId, request => BatchReq, state => State},
    ?SLOG(error, LogMeta#{msg => "invalid request"}),
    {error, {unrecoverable_error, invalid_request}}.

on_get_status(_InstanceId, #{pool_name := PoolName}) ->
    Health = emqx_resource_pool:health_check_workers(PoolName, fun ?MODULE:do_get_status/1),
    status_result(Health).

do_get_status(Conn) ->
    case tdengine:insert(Conn, "select server_version()", []) of
        {ok, _} -> true;
        _ -> false
    end.

status_result(_Status = true) -> connected;
status_result(_Status = false) -> connecting.

%%========================================================================================
%% Helper fns
%%========================================================================================

do_query(InstanceId, Query, #{query_opts := Opts} = State) ->
    do_query_job(InstanceId, {?MODULE, execute, [Query, Opts]}, State).

do_query_job(InstanceId, Job, #{pool_name := PoolName} = State) ->
    ?TRACE(
        "QUERY",
        "tdengine_connector_received",
        #{connector => InstanceId, job => Job, state => State}
    ),
    Result = ecpool:pick_and_do(PoolName, Job, no_handover),

    case Result of
        {error, Reason} ->
            ?tp(
                tdengine_connector_query_return,
                #{error => Reason}
            ),
            ?SLOG(error, #{
                msg => "tdengine_connector_do_query_failed",
                connector => InstanceId,
                job => Job,
                reason => Reason
            }),
            case Reason of
                ecpool_empty ->
                    {error, {recoverable_error, Reason}};
                _ ->
                    Result
            end;
        _ ->
            ?tp(
                tdengine_connector_query_return,
                #{result => Result}
            ),
            Result
    end.

execute(Conn, Query, Opts) ->
    tdengine:insert(Conn, Query, Opts).

do_batch_insert(Conn, Tokens, BatchReqs, Opts) ->
    Queries = aggregate_query(Tokens, BatchReqs),
    SQL = maps:fold(
        fun(InsertPart, Values, Acc) ->
            lists:foldl(
                fun(ValuePart, IAcc) ->
                    <<IAcc/binary, " ", ValuePart/binary>>
                end,
                <<Acc/binary, " ", InsertPart/binary, " VALUES">>,
                Values
            )
        end,
        <<"INSERT INTO">>,
        Queries
    ),
    execute(Conn, SQL, Opts).

aggregate_query({InsertPartTks, ParamsPartTks}, BatchReqs) ->
    lists:foldl(
        fun({_, Data}, Acc) ->
            InsertPart = emqx_placeholder:proc_sql_param_str(InsertPartTks, Data),
            ParamsPart = emqx_placeholder:proc_sql_param_str(ParamsPartTks, Data),
            Values = maps:get(InsertPart, Acc, []),
            maps:put(InsertPart, [ParamsPart | Values], Acc)
        end,
        #{},
        BatchReqs
    ).

connect(Opts) ->
    tdengine:start_link(Opts).

query_opts(#{database := Database} = _Opts) ->
    [{db_name, Database}].

parse_prepare_sql(Config) ->
    SQL =
        case maps:get(sql, Config, undefined) of
            undefined -> #{};
            Template -> #{send_message => Template}
        end,

    parse_batch_prepare_sql(maps:to_list(SQL), #{}, #{}).

parse_batch_prepare_sql([{Key, H} | T], InsertTksMap, BatchTksMap) ->
    case emqx_utils_sql:get_statement_type(H) of
        select ->
            parse_batch_prepare_sql(T, InsertTksMap, BatchTksMap);
        insert ->
            InsertTks = emqx_placeholder:preproc_tmpl(H),
            H1 = string:trim(H, trailing, ";"),
            case split_insert_sql(H1) of
                [_InsertStr, InsertPart, _ValuesStr, ParamsPart] ->
                    InsertPartTks = emqx_placeholder:preproc_tmpl(InsertPart),
                    ParamsPartTks = emqx_placeholder:preproc_tmpl(ParamsPart),
                    parse_batch_prepare_sql(
                        T,
                        InsertTksMap#{Key => InsertTks},
                        BatchTksMap#{Key => {InsertPartTks, ParamsPartTks}}
                    );
                Result ->
                    ?SLOG(error, #{msg => "split sql failed", sql => H, result => Result}),
                    parse_batch_prepare_sql(T, InsertTksMap, BatchTksMap)
            end;
        Type when is_atom(Type) ->
            ?SLOG(error, #{msg => "detect sql type unsupported", sql => H, type => Type}),
            parse_batch_prepare_sql(T, InsertTksMap, BatchTksMap);
        {error, Reason} ->
            ?SLOG(error, #{msg => "detect sql type failed", sql => H, reason => Reason}),
            parse_batch_prepare_sql(T, InsertTksMap, BatchTksMap)
    end;
parse_batch_prepare_sql([], InsertTksMap, BatchTksMap) ->
    #{
        insert_tokens => InsertTksMap,
        batch_tokens => BatchTksMap
    }.

to_bin(List) when is_list(List) ->
    unicode:characters_to_binary(List, utf8).

split_insert_sql(SQL0) ->
    SQL = formalize_sql(SQL0),
    lists:filtermap(
        fun(E) ->
            case string:trim(E) of
                <<>> ->
                    false;
                E1 ->
                    {true, E1}
            end
        end,
        re:split(SQL, "(?i)(insert into)|(?i)(values)")
    ).

formalize_sql(Input) ->
    %% 1. replace all whitespaces like '\r' '\n' or spaces to a single space char.
    SQL = re:replace(Input, "\\s+", " ", [global, {return, binary}]),
    %% 2. trims the result
    string:trim(SQL).
