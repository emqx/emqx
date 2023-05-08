%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_opents_connector).

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

-export([connect/1]).

-import(hoconsc, [mk/2, enum/1, ref/2]).

%%=====================================================================
%% Hocon schema
roots() ->
    [{config, #{type => hoconsc:ref(?MODULE, config)}}].

fields(config) ->
    [
        {server, mk(binary(), #{required => true, desc => ?DESC("server")})},
        {pool_size, fun emqx_connector_schema_lib:pool_size/1},
        {summary, mk(boolean(), #{default => true, desc => ?DESC("summary")})},
        {details, mk(boolean(), #{default => false, desc => ?DESC("details")})},
        {auto_reconnect, fun emqx_connector_schema_lib:auto_reconnect/1}
    ].

%%========================================================================================
%% `emqx_resource' API
%%========================================================================================

callback_mode() -> always_sync.

is_buffer_supported() -> false.

on_start(
    InstanceId,
    #{
        server := Server,
        pool_size := PoolSize,
        summary := Summary,
        details := Details,
        resource_opts := #{batch_size := BatchSize}
    } = Config
) ->
    ?SLOG(info, #{
        msg => "starting_opents_connector",
        connector => InstanceId,
        config => emqx_utils:redact(Config)
    }),

    Options = [
        {server, to_str(Server)},
        {summary, Summary},
        {details, Details},
        {max_batch_size, BatchSize},
        {pool_size, PoolSize}
    ],

    State = #{pool_name => InstanceId, server => Server},
    case opentsdb_connectivity(Server) of
        ok ->
            case emqx_resource_pool:start(InstanceId, ?MODULE, Options) of
                ok ->
                    {ok, State};
                Error ->
                    Error
            end;
        {error, Reason} = Error ->
            ?SLOG(error, #{msg => "Initiate resource failed", reason => Reason}),
            Error
    end.

on_stop(InstanceId, #{pool_name := PoolName} = _State) ->
    ?SLOG(info, #{
        msg => "stopping_opents_connector",
        connector => InstanceId
    }),
    emqx_resource_pool:stop(PoolName).

on_query(InstanceId, Request, State) ->
    on_batch_query(InstanceId, [Request], State).

on_batch_query(
    InstanceId,
    BatchReq,
    State
) ->
    Datas = [format_opentsdb_msg(Msg) || {_Key, Msg} <- BatchReq],
    do_query(InstanceId, Datas, State).

on_get_status(_InstanceId, #{server := Server}) ->
    Result =
        case opentsdb_connectivity(Server) of
            ok ->
                connected;
            {error, Reason} ->
                ?SLOG(error, #{msg => "OpenTSDB lost connection", reason => Reason}),
                connecting
        end,
    Result.

%%========================================================================================
%% Helper fns
%%========================================================================================

do_query(InstanceId, Query, #{pool_name := PoolName} = State) ->
    ?TRACE(
        "QUERY",
        "opents_connector_received",
        #{connector => InstanceId, query => Query, state => State}
    ),
    Result = ecpool:pick_and_do(PoolName, {opentsdb, put, [Query]}, no_handover),

    case Result of
        {error, Reason} ->
            ?tp(
                opents_connector_query_return,
                #{error => Reason}
            ),
            ?SLOG(error, #{
                msg => "opents_connector_do_query_failed",
                connector => InstanceId,
                query => Query,
                reason => Reason
            }),
            Result;
        _ ->
            ?tp(
                opents_connector_query_return,
                #{result => Result}
            ),
            Result
    end.

connect(Opts) ->
    opentsdb:start_link(Opts).

to_str(List) when is_list(List) ->
    List;
to_str(Bin) when is_binary(Bin) ->
    erlang:binary_to_list(Bin).

opentsdb_connectivity(Server) ->
    SvrUrl =
        case Server of
            <<"http://", _/binary>> -> Server;
            <<"https://", _/binary>> -> Server;
            _ -> "http://" ++ Server
        end,
    emqx_plugin_libs_rule:http_connectivity(SvrUrl).

format_opentsdb_msg(Msg) ->
    maps:with(
        [
            timestamp,
            metric,
            tags,
            value,
            <<"timestamp">>,
            <<"metric">>,
            <<"tags">>,
            <<"value">>
        ],
        Msg
    ).
