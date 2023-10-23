%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_syskeeper_connector).

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
    query_mode/1,
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

-define(SYSKEEPER_HOST_OPTIONS, #{
    default_port => 9092
}).

-define(EXTRA_CALL_TIMEOUT, 2000).

%% -------------------------------------------------------------------------------------------------
%% Hocon schema
roots() ->
    [{config, #{type => hoconsc:ref(?MODULE, config)}}].

fields(config) ->
    [
        {server, server()},
        {ack_mode,
            mk(
                enum([need_ack, no_ack]),
                #{desc => ?DESC(ack_mode), default => <<"no_ack">>}
            )},
        {ack_timeout,
            mk(
                emqx_schema:timeout_duration_ms(),
                #{desc => ?DESC(ack_timeout), default => <<"10s">>}
            )},
        {pool_size, fun
            (default) ->
                16;
            (Other) ->
                emqx_connector_schema_lib:pool_size(Other)
        end}
    ].

server() ->
    Meta = #{desc => ?DESC("server")},
    emqx_schema:servers_sc(Meta, ?SYSKEEPER_HOST_OPTIONS).

%% -------------------------------------------------------------------------------------------------
%% `emqx_resource' API

callback_mode() -> always_sync.

query_mode(_) -> sync.

on_start(
    InstanceId,
    #{
        server := Server,
        pool_size := PoolSize,
        ack_timeout := AckTimeout,
        target_topic := TargetTopic,
        target_qos := TargetQoS
    } = Config
) ->
    ?SLOG(info, #{
        msg => "starting_syskeeper_connector",
        connector => InstanceId,
        config => Config
    }),

    HostCfg = emqx_schema:parse_server(Server, ?SYSKEEPER_HOST_OPTIONS),

    Options = [
        {options,
            maps:merge(
                HostCfg,
                maps:with([ack_mode, ack_timeout], Config)
            )},
        {pool_size, PoolSize}
    ],

    State = #{
        pool_name => InstanceId,
        target_qos => TargetQoS,
        ack_timeout => AckTimeout,
        templates => parse_template(Config),
        target_topic_tks => emqx_placeholder:preproc_tmpl(TargetTopic)
    },
    case emqx_resource_pool:start(InstanceId, ?MODULE, Options) of
        ok ->
            {ok, State};
        Error ->
            Error
    end.

on_stop(InstanceId, _State) ->
    ?SLOG(info, #{
        msg => "stopping_syskeeper_connector",
        connector => InstanceId
    }),
    emqx_resource_pool:stop(InstanceId).

on_query(InstanceId, {send_message, _} = Query, State) ->
    do_query(InstanceId, [Query], State);
on_query(_InstanceId, Query, _State) ->
    {error, {unrecoverable_error, {invalid_request, Query}}}.

%% we only support batch insert
on_batch_query(InstanceId, [{send_message, _} | _] = Query, State) ->
    do_query(InstanceId, Query, State);
on_batch_query(_InstanceId, Query, _State) ->
    {error, {unrecoverable_error, {invalid_request, Query}}}.

on_get_status(_InstanceId, #{pool_name := Pool, ack_timeout := AckTimeout}) ->
    Health = emqx_resource_pool:health_check_workers(
        Pool, {emqx_bridge_syskeeper_client, heartbeat, [AckTimeout + ?EXTRA_CALL_TIMEOUT]}
    ),
    status_result(Health).

status_result(true) -> connected;
status_result(false) -> connecting;
status_result({error, _}) -> connecting.

%% -------------------------------------------------------------------------------------------------
%% Helper fns

do_query(
    InstanceId,
    Query,
    #{pool_name := PoolName, ack_timeout := AckTimeout} = State
) ->
    ?TRACE(
        "QUERY",
        "syskeeper_connector_received",
        #{connector => InstanceId, query => Query, state => State}
    ),

    Result =
        case try_apply_template(Query, State) of
            {ok, Msg} ->
                ecpool:pick_and_do(
                    PoolName,
                    {emqx_bridge_syskeeper_client, forward, [Msg, AckTimeout + ?EXTRA_CALL_TIMEOUT]},
                    no_handover
                );
            Error ->
                Error
        end,

    case Result of
        {error, Reason} ->
            ?tp(
                syskeeper_connector_query_return,
                #{error => Reason}
            ),
            ?SLOG(error, #{
                msg => "syskeeper_connector_do_query_failed",
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
                syskeeper_connector_query_return,
                #{result => Result}
            ),
            Result
    end.

connect(Opts) ->
    Options = proplists:get_value(options, Opts),
    emqx_bridge_syskeeper_client:start_link(Options).

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

try_apply_template([{Type, _} | _] = Datas, #{templates := Templates} = State) ->
    case maps:find(Type, Templates) of
        {ok, Template} ->
            apply_template(Datas, Template, State);
        _ ->
            {error, {unrecoverable_error, {invalid_request, Datas}}}
    end.

apply_template(Datas, Template, State) ->
    apply_template(Datas, Template, State, []).

apply_template([{_, Data} | T], Template, State, Acc) ->
    case do_apply_template(Data, Template, State) of
        {ok, Msg} ->
            apply_template(T, Template, State, [Msg | Acc]);
        Error ->
            Error
    end;
apply_template([], _Template, _State, Acc) ->
    {ok, lists:reverse(Acc)}.

do_apply_template(#{id := Id, qos := QoS, clientid := From} = Data, Template, #{
    target_qos := TargetQoS, target_topic_tks := TargetTopicTks
}) ->
    Msg = maps:with([qos, flags, topic, payload, timestamp], Data),
    Topic = emqx_placeholder:proc_tmpl(TargetTopicTks, Msg),
    {ok, Msg#{
        id => emqx_guid:from_hexstr(Id),
        qos :=
            case TargetQoS of
                -1 ->
                    QoS;
                _ ->
                    TargetQoS
            end,
        from => From,
        topic := Topic,
        payload := format_data(Template, Msg)
    }};
do_apply_template(Data, Template, State) ->
    ?SLOG(info, #{
        msg => "syskeeper_connector_apply_template_error",
        data => Data,
        template => Template,
        state => State
    }),
    {error, {unrecoverable_error, {invalid_data, Data}}}.

format_data([], Msg) ->
    emqx_utils_json:encode(Msg);
format_data(Tokens, Msg) ->
    emqx_placeholder:proc_tmpl(Tokens, Msg).
