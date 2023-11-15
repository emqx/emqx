%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_dynamo_connector).

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
    on_start/2,
    on_stop/2,
    on_query/3,
    on_batch_query/3,
    on_get_status/2
]).

-export([
    connect/1
]).

-import(hoconsc, [mk/2, enum/1, ref/2]).

%%=====================================================================
%% Hocon schema
roots() ->
    [{config, #{type => hoconsc:ref(?MODULE, config)}}].

fields(config) ->
    [
        {url, mk(binary(), #{required => true, desc => ?DESC("url")})},
        {table, mk(binary(), #{required => true, desc => ?DESC("table")})},
        {aws_access_key_id,
            mk(
                binary(),
                #{required => true, desc => ?DESC("aws_access_key_id")}
            )},
        {aws_secret_access_key,
            emqx_schema_secret:mk(
                #{
                    required => true,
                    desc => ?DESC("aws_secret_access_key")
                }
            )},
        {pool_size, fun emqx_connector_schema_lib:pool_size/1},
        {auto_reconnect, fun emqx_connector_schema_lib:auto_reconnect/1}
    ].

%%========================================================================================
%% `emqx_resource' API
%%========================================================================================

callback_mode() -> always_sync.

on_start(
    InstanceId,
    #{
        url := Url,
        aws_access_key_id := AccessKeyID,
        aws_secret_access_key := SecretAccessKey,
        table := Table,
        pool_size := PoolSize
    } = Config
) ->
    ?SLOG(info, #{
        msg => "starting_dynamo_connector",
        connector => InstanceId,
        config => redact(Config)
    }),

    {Schema, Server, DefaultPort} = get_host_info(to_str(Url)),
    #{hostname := Host, port := Port} = emqx_schema:parse_server(Server, #{
        default_port => DefaultPort
    }),

    Options = [
        {config, #{
            host => Host,
            port => Port,
            aws_access_key_id => to_str(AccessKeyID),
            aws_secret_access_key => SecretAccessKey,
            schema => Schema
        }},
        {pool_size, PoolSize}
    ],

    Templates = parse_template(Config),
    State = #{
        pool_name => InstanceId,
        table => Table,
        templates => Templates
    },
    case emqx_resource_pool:start(InstanceId, ?MODULE, Options) of
        ok ->
            {ok, State};
        Error ->
            Error
    end.

on_stop(InstanceId, _State) ->
    ?SLOG(info, #{
        msg => "stopping_dynamo_connector",
        connector => InstanceId
    }),
    emqx_resource_pool:stop(InstanceId).

on_query(InstanceId, Query, State) ->
    do_query(InstanceId, Query, State).

%% we only support batch insert
on_batch_query(InstanceId, [{send_message, _} | _] = Query, State) ->
    do_query(InstanceId, Query, State);
on_batch_query(_InstanceId, Query, _State) ->
    {error, {unrecoverable_error, {invalid_request, Query}}}.

%% we only support batch insert

on_get_status(_InstanceId, #{pool_name := Pool}) ->
    Health = emqx_resource_pool:health_check_workers(
        Pool, {emqx_bridge_dynamo_connector_client, is_connected, []}
    ),
    status_result(Health).

status_result(_Status = true) -> connected;
status_result(_Status = false) -> connecting.

%%========================================================================================
%% Helper fns
%%========================================================================================

do_query(
    InstanceId,
    Query,
    #{pool_name := PoolName, templates := Templates, table := Table} = State
) ->
    ?TRACE(
        "QUERY",
        "dynamo_connector_received",
        #{connector => InstanceId, query => Query, state => State}
    ),
    Result = ecpool:pick_and_do(
        PoolName,
        {emqx_bridge_dynamo_connector_client, query, [Table, Query, Templates]},
        no_handover
    ),

    case Result of
        {error, Reason} ->
            ?tp(
                dynamo_connector_query_return,
                #{error => Reason}
            ),
            ?SLOG(error, #{
                msg => "dynamo_connector_do_query_failed",
                connector => InstanceId,
                query => Query,
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
                dynamo_connector_query_return,
                #{result => Result}
            ),
            Result
    end.

connect(Opts) ->
    Config = proplists:get_value(config, Opts),
    {ok, _Pid} = emqx_bridge_dynamo_connector_client:start_link(Config).

parse_template(Config) ->
    Templates =
        case maps:get(template, Config, undefined) of
            undefined -> #{};
            <<>> -> #{};
            Template -> #{send_message => Template}
        end,

    parse_template(maps:to_list(Templates), #{}).

parse_template([{Key, H} | T], Templates) ->
    ParamsTks = emqx_placeholder:preproc_tmpl(H),
    parse_template(
        T,
        Templates#{Key => ParamsTks}
    );
parse_template([], Templates) ->
    Templates.

to_str(List) when is_list(List) ->
    List;
to_str(Bin) when is_binary(Bin) ->
    erlang:binary_to_list(Bin).

get_host_info("http://" ++ Server) ->
    {"http://", Server, 80};
get_host_info("https://" ++ Server) ->
    {"https://", Server, 443};
get_host_info(Server) ->
    {"http://", Server, 80}.

redact(Data) ->
    emqx_utils:redact(Data, fun(Any) -> Any =:= aws_secret_access_key end).
