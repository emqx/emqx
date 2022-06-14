%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_bridge_api).

-behaviour(minirest_api).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").

-import(hoconsc, [mk/2, array/1, enum/1]).

%% Swagger specs from hocon schema
-export([
    api_spec/0,
    paths/0,
    schema/1,
    namespace/0
]).

%% API callbacks
-export([
    '/bridges'/2,
    '/bridges/:id'/2,
    '/bridges/:id/operation/:operation'/2,
    '/nodes/:node/bridges/:id/operation/:operation'/2,
    '/bridges/:id/reset_metrics'/2
]).

-export([lookup_from_local_node/2]).

-define(CONN_TYPES, [mqtt]).

-define(TRY_PARSE_ID(ID, EXPR),
    try emqx_bridge_resource:parse_bridge_id(Id) of
        {BridgeType, BridgeName} ->
            EXPR
    catch
        error:{invalid_bridge_id, Id0} ->
            {400,
                error_msg(
                    'INVALID_ID',
                    <<"invalid_bridge_id: ", Id0/binary,
                        ". Bridge Ids must be of format {type}:{name}">>
                )}
    end
).

-define(METRICS(MATCH, SUCC, FAILED, RATE, RATE_5, RATE_MAX), #{
    matched => MATCH,
    success => SUCC,
    failed => FAILED,
    rate => RATE,
    rate_last5m => RATE_5,
    rate_max => RATE_MAX
}).
-define(metrics(MATCH, SUCC, FAILED, RATE, RATE_5, RATE_MAX), #{
    matched := MATCH,
    success := SUCC,
    failed := FAILED,
    rate := RATE,
    rate_last5m := RATE_5,
    rate_max := RATE_MAX
}).

namespace() -> "bridge".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => false}).

paths() ->
    [
        "/bridges",
        "/bridges/:id",
        "/bridges/:id/operation/:operation",
        "/nodes/:node/bridges/:id/operation/:operation",
        "/bridges/:id/reset_metrics"
    ].

error_schema(Code, Message) when is_atom(Code) ->
    error_schema([Code], Message);
error_schema(Codes, Message) when is_list(Message) ->
    error_schema(Codes, list_to_binary(Message));
error_schema(Codes, Message) when is_list(Codes) andalso is_binary(Message) ->
    emqx_dashboard_swagger:error_codes(Codes, Message).

get_response_body_schema() ->
    emqx_dashboard_swagger:schema_with_examples(
        emqx_bridge_schema:get_response(),
        bridge_info_examples(get)
    ).

param_path_operation_cluster() ->
    {operation,
        mk(
            enum([enable, disable, stop, restart]),
            #{
                in => path,
                required => true,
                example => <<"start">>,
                desc => ?DESC("desc_param_path_operation_cluster")
            }
        )}.

param_path_operation_on_node() ->
    {operation,
        mk(
            enum([stop, restart]),
            #{
                in => path,
                required => true,
                example => <<"start">>,
                desc => ?DESC("desc_param_path_operation_on_node")
            }
        )}.

param_path_node() ->
    {node,
        mk(
            binary(),
            #{
                in => path,
                required => true,
                example => <<"emqx@127.0.0.1">>,
                desc => ?DESC("desc_param_path_node")
            }
        )}.

param_path_id() ->
    {id,
        mk(
            binary(),
            #{
                in => path,
                required => true,
                example => <<"webhook:my_webhook">>,
                desc => ?DESC("desc_param_path_id")
            }
        )}.

bridge_info_array_example(Method) ->
    [Config || #{value := Config} <- maps:values(bridge_info_examples(Method))].

bridge_info_examples(Method) ->
    maps:merge(conn_bridge_examples(Method), #{
        <<"my_webhook">> => #{
            summary => <<"WebHook">>,
            value => info_example(webhook, awesome, Method)
        }
    }).

conn_bridge_examples(Method) ->
    lists:foldl(
        fun(Type, Acc) ->
            SType = atom_to_list(Type),
            KeyIngress = bin(SType ++ "_ingress"),
            KeyEgress = bin(SType ++ "_egress"),
            maps:merge(Acc, #{
                KeyIngress => #{
                    summary => bin(string:uppercase(SType) ++ " Ingress Bridge"),
                    value => info_example(Type, ingress, Method)
                },
                KeyEgress => #{
                    summary => bin(string:uppercase(SType) ++ " Egress Bridge"),
                    value => info_example(Type, egress, Method)
                }
            })
        end,
        #{},
        ?CONN_TYPES
    ).

info_example(Type, Direction, Method) ->
    maps:merge(
        info_example_basic(Type, Direction),
        method_example(Type, Direction, Method)
    ).

method_example(Type, Direction, Method) when Method == get; Method == post ->
    SType = atom_to_list(Type),
    SDir = atom_to_list(Direction),
    SName =
        case Type of
            webhook -> "my_" ++ SType;
            _ -> "my_" ++ SDir ++ "_" ++ SType ++ "_bridge"
        end,
    TypeNameExamp = #{
        type => bin(SType),
        name => bin(SName)
    },
    maybe_with_metrics_example(TypeNameExamp, Method);
method_example(_Type, _Direction, put) ->
    #{}.

maybe_with_metrics_example(TypeNameExamp, get) ->
    TypeNameExamp#{
        metrics => ?METRICS(0, 0, 0, 0, 0, 0),
        node_metrics => [
            #{
                node => node(),
                metrics => ?METRICS(0, 0, 0, 0, 0, 0)
            }
        ]
    };
maybe_with_metrics_example(TypeNameExamp, _) ->
    TypeNameExamp.

info_example_basic(webhook, _) ->
    #{
        enable => true,
        url => <<"http://localhost:9901/messages/${topic}">>,
        request_timeout => <<"15s">>,
        connect_timeout => <<"15s">>,
        max_retries => 3,
        retry_interval => <<"10s">>,
        pool_type => <<"random">>,
        pool_size => 4,
        enable_pipelining => 100,
        ssl => #{enable => false},
        local_topic => <<"emqx_webhook/#">>,
        method => post,
        body => <<"${payload}">>
    };
info_example_basic(mqtt, ingress) ->
    #{
        enable => true,
        connector => <<"mqtt:my_mqtt_connector">>,
        direction => ingress,
        remote_topic => <<"aws/#">>,
        remote_qos => 1,
        local_topic => <<"from_aws/${topic}">>,
        local_qos => <<"${qos}">>,
        payload => <<"${payload}">>,
        retain => <<"${retain}">>
    };
info_example_basic(mqtt, egress) ->
    #{
        enable => true,
        connector => <<"mqtt:my_mqtt_connector">>,
        direction => egress,
        local_topic => <<"emqx/#">>,
        remote_topic => <<"from_emqx/${topic}">>,
        remote_qos => <<"${qos}">>,
        payload => <<"${payload}">>,
        retain => false
    }.

schema("/bridges") ->
    #{
        'operationId' => '/bridges',
        get => #{
            tags => [<<"bridges">>],
            summary => <<"List Bridges">>,
            description => ?DESC("desc_api1"),
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_example(
                    array(emqx_bridge_schema:get_response()),
                    bridge_info_array_example(get)
                )
            }
        },
        post => #{
            tags => [<<"bridges">>],
            summary => <<"Create Bridge">>,
            description => ?DESC("desc_api2"),
            'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                emqx_bridge_schema:post_request(),
                bridge_info_examples(post)
            ),
            responses => #{
                201 => get_response_body_schema(),
                400 => error_schema('ALREADY_EXISTS', "Bridge already exists")
            }
        }
    };
schema("/bridges/:id") ->
    #{
        'operationId' => '/bridges/:id',
        get => #{
            tags => [<<"bridges">>],
            summary => <<"Get Bridge">>,
            description => ?DESC("desc_api3"),
            parameters => [param_path_id()],
            responses => #{
                200 => get_response_body_schema(),
                404 => error_schema('NOT_FOUND', "Bridge not found")
            }
        },
        put => #{
            tags => [<<"bridges">>],
            summary => <<"Update Bridge">>,
            description => ?DESC("desc_api4"),
            parameters => [param_path_id()],
            'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                emqx_bridge_schema:put_request(),
                bridge_info_examples(put)
            ),
            responses => #{
                200 => get_response_body_schema(),
                404 => error_schema('NOT_FOUND', "Bridge not found"),
                400 => error_schema(['BAD_REQUEST', 'INVALID_ID'], "Update bridge failed")
            }
        },
        delete => #{
            tags => [<<"bridges">>],
            summary => <<"Delete Bridge">>,
            description => ?DESC("desc_api5"),
            parameters => [param_path_id()],
            responses => #{
                204 => <<"Bridge deleted">>,
                400 => error_schema(['INVALID_ID'], "Update bridge failed"),
                503 => error_schema('SERVICE_UNAVAILABLE', "Service unavailable")
            }
        }
    };
schema("/bridges/:id/reset_metrics") ->
    #{
        'operationId' => '/bridges/:id/reset_metrics',
        put => #{
            tags => [<<"bridges">>],
            summary => <<"Reset Bridge Metrics">>,
            description => ?DESC("desc_api6"),
            parameters => [param_path_id()],
            responses => #{
                200 => <<"Reset success">>,
                400 => error_schema(['BAD_REQUEST'], "RPC Call Failed")
            }
        }
    };
schema("/bridges/:id/operation/:operation") ->
    #{
        'operationId' => '/bridges/:id/operation/:operation',
        post => #{
            tags => [<<"bridges">>],
            summary => <<"Enable/Disable/Stop/Restart Bridge">>,
            description => ?DESC("desc_api7"),
            parameters => [
                param_path_id(),
                param_path_operation_cluster()
            ],
            responses => #{
                200 => <<"Operation success">>,
                503 => error_schema('SERVICE_UNAVAILABLE', "Service unavailable"),
                400 => error_schema('INVALID_ID', "Bad bridge ID")
            }
        }
    };
schema("/nodes/:node/bridges/:id/operation/:operation") ->
    #{
        'operationId' => '/nodes/:node/bridges/:id/operation/:operation',
        post => #{
            tags => [<<"bridges">>],
            summary => <<"Stop/Restart Bridge">>,
            description => ?DESC("desc_api8"),
            parameters => [
                param_path_node(),
                param_path_id(),
                param_path_operation_on_node()
            ],
            responses => #{
                200 => <<"Operation success">>,
                400 => error_schema('INVALID_ID', "Bad bridge ID"),
                403 => error_schema('FORBIDDEN_REQUEST', "forbidden operation"),
                503 => error_schema('SERVICE_UNAVAILABLE', "Service unavailable")
            }
        }
    }.

'/bridges'(post, #{body := #{<<"type">> := BridgeType, <<"name">> := BridgeName} = Conf0}) ->
    Conf = filter_out_request_body(Conf0),
    case emqx_bridge:lookup(BridgeType, BridgeName) of
        {ok, _} ->
            {400, error_msg('ALREADY_EXISTS', <<"bridge already exists">>)};
        {error, not_found} ->
            case ensure_bridge_created(BridgeType, BridgeName, Conf) of
                ok -> lookup_from_all_nodes(BridgeType, BridgeName, 201);
                {error, Error} -> {400, Error}
            end
    end;
'/bridges'(get, _Params) ->
    {200,
        zip_bridges([
            [format_resp(Data, Node) || Data <- emqx_bridge_proto_v1:list_bridges(Node)]
         || Node <- mria_mnesia:running_nodes()
        ])}.

'/bridges/:id'(get, #{bindings := #{id := Id}}) ->
    ?TRY_PARSE_ID(Id, lookup_from_all_nodes(BridgeType, BridgeName, 200));
'/bridges/:id'(put, #{bindings := #{id := Id}, body := Conf0}) ->
    Conf = filter_out_request_body(Conf0),
    ?TRY_PARSE_ID(
        Id,
        case emqx_bridge:lookup(BridgeType, BridgeName) of
            {ok, _} ->
                case ensure_bridge_created(BridgeType, BridgeName, Conf) of
                    ok ->
                        lookup_from_all_nodes(BridgeType, BridgeName, 200);
                    {error, Error} ->
                        {400, Error}
                end;
            {error, not_found} ->
                {404, error_msg('NOT_FOUND', <<"bridge not found">>)}
        end
    );
'/bridges/:id'(delete, #{bindings := #{id := Id}}) ->
    ?TRY_PARSE_ID(
        Id,
        case emqx_bridge:remove(BridgeType, BridgeName) of
            {ok, _} -> {204};
            {error, timeout} -> {503, error_msg('SERVICE_UNAVAILABLE', <<"request timeout">>)};
            {error, Reason} -> {500, error_msg('INTERNAL_ERROR', Reason)}
        end
    ).

'/bridges/:id/reset_metrics'(put, #{bindings := #{id := Id}}) ->
    ?TRY_PARSE_ID(
        Id,
        case
            emqx_bridge_resource:reset_metrics(
                emqx_bridge_resource:resource_id(BridgeType, BridgeName)
            )
        of
            ok -> {200, <<"Reset success">>};
            Reason -> {400, error_msg('BAD_REQUEST', Reason)}
        end
    ).

lookup_from_all_nodes(BridgeType, BridgeName, SuccCode) ->
    Nodes = mria_mnesia:running_nodes(),
    case is_ok(emqx_bridge_proto_v1:lookup_from_all_nodes(Nodes, BridgeType, BridgeName)) of
        {ok, [{ok, _} | _] = Results} ->
            {SuccCode, format_bridge_info([R || {ok, R} <- Results])};
        {ok, [{error, not_found} | _]} ->
            {404, error_msg('NOT_FOUND', <<"not_found">>)};
        {error, ErrL} ->
            {500, error_msg('INTERNAL_ERROR', ErrL)}
    end.

lookup_from_local_node(BridgeType, BridgeName) ->
    case emqx_bridge:lookup(BridgeType, BridgeName) of
        {ok, Res} -> {ok, format_resp(Res)};
        Error -> Error
    end.

'/bridges/:id/operation/:operation'(post, #{
    bindings :=
        #{id := Id, operation := Op}
}) ->
    ?TRY_PARSE_ID(
        Id,
        case operation_func(Op) of
            invalid ->
                {400, error_msg('BAD_REQUEST', <<"invalid operation">>)};
            OperFunc when OperFunc == enable; OperFunc == disable ->
                case emqx_bridge:disable_enable(OperFunc, BridgeType, BridgeName) of
                    {ok, _} ->
                        {200};
                    {error, {pre_config_update, _, bridge_not_found}} ->
                        {404, error_msg('NOT_FOUND', <<"bridge not found">>)};
                    {error, {_, _, timeout}} ->
                        {503, error_msg('SERVICE_UNAVAILABLE', <<"request timeout">>)};
                    {error, timeout} ->
                        {503, error_msg('SERVICE_UNAVAILABLE', <<"request timeout">>)};
                    {error, Reason} ->
                        {500, error_msg('INTERNAL_ERROR', Reason)}
                end;
            OperFunc ->
                Nodes = mria_mnesia:running_nodes(),
                operation_to_all_nodes(Nodes, OperFunc, BridgeType, BridgeName)
        end
    ).

'/nodes/:node/bridges/:id/operation/:operation'(post, #{
    bindings :=
        #{id := Id, operation := Op, node := Node}
}) ->
    ?TRY_PARSE_ID(
        Id,
        case node_operation_func(Op) of
            invalid ->
                {400, error_msg('BAD_REQUEST', <<"invalid operation">>)};
            OperFunc ->
                TargetNode = binary_to_atom(Node, utf8),
                ConfMap = emqx:get_config([bridges, BridgeType, BridgeName]),
                case maps:get(enable, ConfMap, false) of
                    false ->
                        {403,
                            error_msg(
                                'FORBIDDEN_REQUEST', <<"forbidden operation: bridge disabled">>
                            )};
                    true ->
                        case emqx_bridge_proto_v1:OperFunc(TargetNode, BridgeType, BridgeName) of
                            ok ->
                                {200};
                            {error, timeout} ->
                                {503, error_msg('SERVICE_UNAVAILABLE', <<"request timeout">>)};
                            {error, Reason} ->
                                {500, error_msg('INTERNAL_ERROR', Reason)}
                        end
                end
        end
    ).

node_operation_func(<<"stop">>) -> stop_bridge_to_node;
node_operation_func(<<"restart">>) -> restart_bridge_to_node;
node_operation_func(_) -> invalid.

operation_func(<<"stop">>) -> stop;
operation_func(<<"restart">>) -> restart;
operation_func(<<"enable">>) -> enable;
operation_func(<<"disable">>) -> disable;
operation_func(_) -> invalid.

operation_to_all_nodes(Nodes, OperFunc, BridgeType, BridgeName) ->
    RpcFunc =
        case OperFunc of
            restart -> restart_bridges_to_all_nodes;
            stop -> stop_bridges_to_all_nodes
        end,
    case is_ok(emqx_bridge_proto_v1:RpcFunc(Nodes, BridgeType, BridgeName)) of
        {ok, _} ->
            {200};
        {error, [timeout | _]} ->
            {503, error_msg('SERVICE_UNAVAILABLE', <<"request timeout">>)};
        {error, ErrL} ->
            {500, error_msg('INTERNAL_ERROR', ErrL)}
    end.

ensure_bridge_created(BridgeType, BridgeName, Conf) ->
    case emqx_bridge:create(BridgeType, BridgeName, Conf) of
        {ok, _} -> ok;
        {error, Reason} -> {error, error_msg('BAD_REQUEST', Reason)}
    end.

zip_bridges([BridgesFirstNode | _] = BridgesAllNodes) ->
    lists:foldl(
        fun(#{type := Type, name := Name}, Acc) ->
            Bridges = pick_bridges_by_id(Type, Name, BridgesAllNodes),
            [format_bridge_info(Bridges) | Acc]
        end,
        [],
        BridgesFirstNode
    ).

pick_bridges_by_id(Type, Name, BridgesAllNodes) ->
    lists:foldl(
        fun(BridgesOneNode, Acc) ->
            case
                [
                    Bridge
                 || Bridge = #{type := Type0, name := Name0} <- BridgesOneNode,
                    Type0 == Type,
                    Name0 == Name
                ]
            of
                [BridgeInfo] ->
                    [BridgeInfo | Acc];
                [] ->
                    ?SLOG(warning, #{
                        msg => "bridge_inconsistent_in_cluster",
                        reason => not_found,
                        type => Type,
                        name => Name,
                        bridge => emqx_bridge_resource:bridge_id(Type, Name)
                    }),
                    Acc
            end
        end,
        [],
        BridgesAllNodes
    ).

format_bridge_info([FirstBridge | _] = Bridges) ->
    Res = maps:remove(node, FirstBridge),
    NodeStatus = collect_status(Bridges),
    NodeMetrics = collect_metrics(Bridges),
    Res#{
        status => aggregate_status(NodeStatus),
        node_status => NodeStatus,
        metrics => aggregate_metrics(NodeMetrics),
        node_metrics => NodeMetrics
    }.

collect_status(Bridges) ->
    [maps:with([node, status], B) || B <- Bridges].

aggregate_status(AllStatus) ->
    Head = fun([A | _]) -> A end,
    HeadVal = maps:get(status, Head(AllStatus), connecting),
    AllRes = lists:all(fun(#{status := Val}) -> Val == HeadVal end, AllStatus),
    case AllRes of
        true -> HeadVal;
        false -> inconsistent
    end.

collect_metrics(Bridges) ->
    [maps:with([node, metrics], B) || B <- Bridges].

aggregate_metrics(AllMetrics) ->
    InitMetrics = ?METRICS(0, 0, 0, 0, 0, 0),
    lists:foldl(
        fun(
            #{metrics := ?metrics(Match1, Succ1, Failed1, Rate1, Rate5m1, RateMax1)},
            ?metrics(Match0, Succ0, Failed0, Rate0, Rate5m0, RateMax0)
        ) ->
            ?METRICS(
                Match1 + Match0,
                Succ1 + Succ0,
                Failed1 + Failed0,
                Rate1 + Rate0,
                Rate5m1 + Rate5m0,
                RateMax1 + RateMax0
            )
        end,
        InitMetrics,
        AllMetrics
    ).

format_resp(Data) ->
    format_resp(Data, node()).

format_resp(
    #{
        type := Type,
        name := BridgeName,
        raw_config := RawConf,
        resource_data := #{status := Status, metrics := Metrics}
    },
    Node
) ->
    RawConfFull = fill_defaults(Type, RawConf),
    RawConfFull#{
        type => Type,
        name => maps:get(<<"name">>, RawConf, BridgeName),
        node => Node,
        status => Status,
        metrics => format_metrics(Metrics)
    }.

format_metrics(#{
    counters := #{failed := Failed, exception := Ex, matched := Match, success := Succ},
    rate := #{
        matched := #{current := Rate, last5m := Rate5m, max := RateMax}
    }
}) ->
    ?METRICS(Match, Succ, Failed + Ex, Rate, Rate5m, RateMax).

fill_defaults(Type, RawConf) ->
    PackedConf = pack_bridge_conf(Type, RawConf),
    FullConf = emqx_config:fill_defaults(emqx_bridge_schema, PackedConf, #{}),
    unpack_bridge_conf(Type, FullConf).

pack_bridge_conf(Type, RawConf) ->
    #{<<"bridges">> => #{bin(Type) => #{<<"foo">> => RawConf}}}.

unpack_bridge_conf(Type, PackedConf) ->
    #{<<"bridges">> := Bridges} = PackedConf,
    #{<<"foo">> := RawConf} = maps:get(bin(Type), Bridges),
    RawConf.

is_ok(ResL) ->
    case
        lists:filter(
            fun
                ({ok, _}) -> false;
                (ok) -> false;
                (_) -> true
            end,
            ResL
        )
    of
        [] -> {ok, [Res || {ok, Res} <- ResL]};
        ErrL -> {error, ErrL}
    end.

filter_out_request_body(Conf) ->
    ExtraConfs = [
        <<"id">>,
        <<"type">>,
        <<"name">>,
        <<"status">>,
        <<"node_status">>,
        <<"node_metrics">>,
        <<"metrics">>,
        <<"node">>
    ],
    maps:without(ExtraConfs, Conf).

error_msg(Code, Msg) ->
    #{code => Code, message => emqx_misc:readable_error_msg(Msg)}.

bin(S) when is_list(S) ->
    list_to_binary(S);
bin(S) when is_atom(S) ->
    atom_to_binary(S, utf8);
bin(S) when is_binary(S) ->
    S.
