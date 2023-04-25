%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ee_connector_tdengine).

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

-export([connect/1, do_get_status/1, execute/3]).

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

on_stop(InstanceId, #{pool_name := PoolName}) ->
    ?SLOG(info, #{
        msg => "stopping_tdengine_connector",
        connector => InstanceId
    }),
    emqx_resource_pool:stop(PoolName).

on_query(InstanceId, {query, SQL}, State) ->
    do_query(InstanceId, SQL, State);
on_query(InstanceId, Request, State) ->
    %% because the `emqx-tdengine` client only supports a single SQL cmd
    %% so the `on_query` and `on_batch_query` have the same process, that is:
    %% we need to collect all data into one SQL cmd and then call the insert API
    on_batch_query(InstanceId, [Request], State).

on_batch_query(
    InstanceId,
    BatchReq,
    #{batch_inserts := Inserts, batch_params_tokens := ParamsTokens} = State
) ->
    case hd(BatchReq) of
        {Key, _} ->
            case maps:get(Key, Inserts, undefined) of
                undefined ->
                    {error, {unrecoverable_error, batch_prepare_not_implemented}};
                InsertSQL ->
                    Tokens = maps:get(Key, ParamsTokens),
                    do_batch_insert(InstanceId, BatchReq, InsertSQL, Tokens, State)
            end;
        Request ->
            LogMeta = #{connector => InstanceId, first_request => Request, state => State},
            ?SLOG(error, LogMeta#{msg => "invalid request"}),
            {error, {unrecoverable_error, invalid_request}}
    end.

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

do_batch_insert(InstanceId, BatchReqs, InsertPart, Tokens, State) ->
    SQL = emqx_plugin_libs_rule:proc_batch_sql(BatchReqs, InsertPart, Tokens),
    do_query(InstanceId, SQL, State).

do_query(InstanceId, Query, #{pool_name := PoolName, query_opts := Opts} = State) ->
    ?TRACE(
        "QUERY",
        "tdengine_connector_received",
        #{connector => InstanceId, query => Query, state => State}
    ),
    Result = ecpool:pick_and_do(PoolName, {?MODULE, execute, [Query, Opts]}, no_handover),

    case Result of
        {error, Reason} ->
            ?tp(
                tdengine_connector_query_return,
                #{error => Reason}
            ),
            ?SLOG(error, #{
                msg => "tdengine_connector_do_query_failed",
                connector => InstanceId,
                query => Query,
                reason => Reason
            }),
            Result;
        _ ->
            ?tp(
                tdengine_connector_query_return,
                #{result => Result}
            ),
            Result
    end.

execute(Conn, Query, Opts) ->
    tdengine:insert(Conn, Query, Opts).

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

parse_batch_prepare_sql([{Key, H} | T], BatchInserts, BatchTks) ->
    case emqx_plugin_libs_rule:detect_sql_type(H) of
        {ok, select} ->
            parse_batch_prepare_sql(T, BatchInserts, BatchTks);
        {ok, insert} ->
            case emqx_plugin_libs_rule:split_insert_sql(H) of
                {ok, {InsertSQL, Params}} ->
                    ParamsTks = emqx_plugin_libs_rule:preproc_tmpl(Params),
                    parse_batch_prepare_sql(
                        T,
                        BatchInserts#{Key => InsertSQL},
                        BatchTks#{Key => ParamsTks}
                    );
                {error, Reason} ->
                    ?SLOG(error, #{msg => "split sql failed", sql => H, reason => Reason}),
                    parse_batch_prepare_sql(T, BatchInserts, BatchTks)
            end;
        {error, Reason} ->
            ?SLOG(error, #{msg => "detect sql type failed", sql => H, reason => Reason}),
            parse_batch_prepare_sql(T, BatchInserts, BatchTks)
    end;
parse_batch_prepare_sql([], BatchInserts, BatchTks) ->
    #{
        batch_inserts => BatchInserts,
        batch_params_tokens => BatchTks
    }.

to_bin(List) when is_list(List) ->
    unicode:characters_to_binary(List, utf8).
