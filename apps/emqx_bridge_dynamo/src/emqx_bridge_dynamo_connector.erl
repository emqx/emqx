%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_dynamo_connector).

-behaviour(emqx_resource).

-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_trace.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-export([roots/0, fields/1, namespace/0]).

%% `emqx_resource' API
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

-export([
    connect/1
]).

-import(hoconsc, [mk/2, enum/1, ref/2]).

%%=====================================================================
%% Hocon schema

namespace() -> dynamodka.

roots() ->
    [{config, #{type => hoconsc:ref(?MODULE, config)}}].

fields(config) ->
    [
        {url, mk(binary(), #{required => true, desc => ?DESC("url")})},
        {region, mk(binary(), #{required => true, desc => ?DESC("region")})},
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
        {auto_reconnect, fun emqx_connector_schema_lib:auto_reconnect/1},
        emqx_bridge_v2_schema:undefined_as_null_field()
    ].

%%========================================================================================
%% `emqx_resource' API
%%========================================================================================
resource_type() -> dynamo.

callback_mode() -> always_sync.

on_start(
    InstanceId,
    #{
        url := Url,
        aws_access_key_id := AccessKeyID,
        aws_secret_access_key := SecretAccessKey,
        pool_size := PoolSize
    } = Config
) ->
    ?SLOG(info, #{
        msg => "starting_dynamo_connector",
        connector => InstanceId,
        config => redact(Config)
    }),

    {Scheme, Server, DefaultPort} = get_host_info(to_str(Url)),
    #{hostname := Host, port := Port} = emqx_schema:parse_server(Server, #{
        default_port => DefaultPort
    }),

    Options = [
        {config, #{
            host => Host,
            port => Port,
            aws_access_key_id => to_str(AccessKeyID),
            aws_secret_access_key => SecretAccessKey,
            scheme => Scheme
        }},
        {pool_size, PoolSize}
    ],
    State = #{
        pool_name => InstanceId,
        installed_channels => #{}
    },
    case Config of
        #{region := Region} ->
            application:set_env(erlcloud, aws_region, to_str(Region));
        _ ->
            ok
    end,
    case emqx_resource_pool:start(InstanceId, ?MODULE, Options) of
        ok ->
            {ok, State};
        Error ->
            Error
    end.

on_add_channel(
    _InstId,
    #{
        installed_channels := InstalledChannels
    } = OldState,
    ChannelId,
    ChannelConfig
) ->
    {ok, ChannelState} = create_channel_state(ChannelConfig),
    NewInstalledChannels = maps:put(ChannelId, ChannelState, InstalledChannels),
    %% Update state
    NewState = OldState#{installed_channels => NewInstalledChannels},
    {ok, NewState}.

create_channel_state(
    #{parameters := Conf} = _ChannelConfig
) ->
    Base = maps:without([template], Conf),

    Templates = parse_template_from_conf(Conf),
    State = Base#{
        templates => Templates
    },
    {ok, State}.

on_remove_channel(
    _InstId,
    #{
        installed_channels := InstalledChannels
    } = OldState,
    ChannelId
) ->
    NewInstalledChannels = maps:remove(ChannelId, InstalledChannels),
    %% Update state
    NewState = OldState#{installed_channels => NewInstalledChannels},
    {ok, NewState}.

on_get_channel_status(
    _ResId,
    _ChannelId,
    _State
) ->
    ?status_connected.

on_get_channels(ResId) ->
    emqx_bridge_v2:get_channels_for_connector(ResId).

on_stop(InstanceId, _State) ->
    ?SLOG(info, #{
        msg => "stopping_dynamo_connector",
        connector => InstanceId
    }),
    ?tp(
        dynamo_connector_on_stop,
        #{instance_id => InstanceId}
    ),
    emqx_resource_pool:stop(InstanceId).

on_query(InstanceId, Query, State) ->
    do_query(InstanceId, Query, State).

%% we only support batch insert
on_batch_query(InstanceId, [{_ChannelId, _} | _] = Query, State) ->
    do_query(InstanceId, Query, State);
on_batch_query(_InstanceId, Query, _State) ->
    {error, {unrecoverable_error, {invalid_request, Query}}}.

on_format_query_result({ok, Result}) ->
    #{result => ok, info => Result};
on_format_query_result(Result) ->
    Result.

health_check_timeout() ->
    2500.

on_get_status(_InstanceId, #{pool_name := Pool}) ->
    Health = emqx_resource_pool:health_check_workers(
        Pool,
        {emqx_bridge_dynamo_connector_client, is_connected, [
            health_check_timeout()
        ]},
        health_check_timeout(),
        #{return_values => true}
    ),
    case Health of
        {error, timeout} ->
            {?status_connecting, <<"timeout_while_checking_connection">>};
        {ok, Results} ->
            status_result(Results)
    end.

status_result(Results) ->
    case lists:filter(fun(Res) -> Res =/= true end, Results) of
        [] when Results =:= [] ->
            ?status_connecting;
        [] ->
            ?status_connected;
        [{false, Error} | _] ->
            {?status_connecting, Error}
    end.

%%========================================================================================
%% Helper fns
%%========================================================================================

do_query(
    InstanceId,
    Query,
    #{
        pool_name := PoolName,
        installed_channels := Channels
    } = State
) ->
    ?TRACE(
        "QUERY",
        "dynamo_connector_received",
        #{connector => InstanceId, query => Query, state => State}
    ),
    ChannelId = get_channel_id(Query),
    QueryTuple = get_query_tuple(Query),
    ChannelState = maps:get(ChannelId, Channels),
    #{
        table := Table,
        templates := Templates
    } = ChannelState,
    TraceRenderedCTX =
        emqx_trace:make_rendered_action_template_trace_context(ChannelId),
    Result =
        case ensuare_dynamo_keys(Query, ChannelState) of
            true ->
                ecpool:pick_and_do(
                    PoolName,
                    {emqx_bridge_dynamo_connector_client, query, [
                        Table, QueryTuple, Templates, TraceRenderedCTX, ChannelState
                    ]},
                    no_handover
                );
            _ ->
                {error, missing_filter_or_range_key}
        end,

    case Result of
        {error, ?EMQX_TRACE_STOP_ACTION(_)} = Error ->
            Error;
        {error, Reason} ->
            ?tp(
                dynamo_connector_query_return,
                #{
                    error => Reason,
                    instance_id => InstanceId
                }
            ),
            ?SLOG(error, #{
                msg => "dynamo_connector_do_query_failed",
                connector => InstanceId,
                channel => ChannelId,
                query => QueryTuple,
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
                #{
                    result => Result,
                    instance_id => InstanceId
                }
            ),
            Result
    end.

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

ensuare_dynamo_keys({_, Data} = Query, State) when is_map(Data) ->
    ensuare_dynamo_keys([Query], State);
ensuare_dynamo_keys([{_, Data} | _] = Queries, State) when is_map(Data) ->
    Keys = maps:values(maps:with([hash_key, range_key], State)),
    lists:all(
        fun({_, Query}) ->
            lists:all(
                fun(Key) ->
                    is_dynamo_key_existing(Key, Query)
                end,
                Keys
            )
        end,
        Queries
    );
%% this is not a insert query
ensuare_dynamo_keys(_Query, _State) ->
    true.

connect(Opts) ->
    Config = proplists:get_value(config, Opts),
    {ok, _Pid} = emqx_bridge_dynamo_connector_client:start_link(Config).

parse_template_from_conf(Config) ->
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

is_dynamo_key_existing(Bin, Query) when is_binary(Bin) ->
    case maps:is_key(Bin, Query) of
        true ->
            true;
        _ ->
            try
                Key = erlang:binary_to_existing_atom(Bin),
                maps:is_key(Key, Query)
            catch
                _:_ ->
                    false
            end
    end.
