%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_connector_api).

-behaviour(minirest_api).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_utils/include/emqx_utils_api.hrl").

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
    '/connectors'/2,
    '/connectors/:id'/2,
    '/connectors/:id/enable/:enable'/2,
    '/connectors/:id/:operation'/2,
    '/nodes/:node/connectors/:id/:operation'/2,
    '/connectors_probe'/2
]).

-export([lookup_from_local_node/2]).

-define(CONNECTOR_NOT_ENABLED,
    ?BAD_REQUEST(<<"Forbidden operation, connector not enabled">>)
).

-define(CONNECTOR_NOT_FOUND(CONNECTOR_TYPE, CONNECTOR_NAME),
    ?NOT_FOUND(
        <<"Connector lookup failed: connector named '", (bin(CONNECTOR_NAME))/binary, "' of type ",
            (bin(CONNECTOR_TYPE))/binary, " does not exist.">>
    )
).

%% Don't turn connector_name to atom, it's maybe not a existing atom.
-define(TRY_PARSE_ID(ID, EXPR),
    try emqx_connector_resource:parse_connector_id(Id, #{atom_name => false}) of
        {ConnectorType, ConnectorName} ->
            EXPR
    catch
        throw:#{reason := Reason} ->
            ?NOT_FOUND(<<"Invalid connector ID, ", Reason/binary>>)
    end
).

namespace() -> "connector".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [
        "/connectors",
        "/connectors/:id",
        "/connectors/:id/enable/:enable",
        "/connectors/:id/:operation",
        "/nodes/:node/connectors/:id/:operation",
        "/connectors_probe"
    ].

error_schema(Code, Message) when is_atom(Code) ->
    error_schema([Code], Message);
error_schema(Codes, Message) when is_list(Message) ->
    error_schema(Codes, list_to_binary(Message));
error_schema(Codes, Message) when is_list(Codes) andalso is_binary(Message) ->
    emqx_dashboard_swagger:error_codes(Codes, Message).

get_response_body_schema() ->
    emqx_dashboard_swagger:schema_with_examples(
        emqx_connector_schema:get_response(),
        connector_info_examples(get)
    ).

param_path_operation_cluster() ->
    {operation,
        mk(
            enum([start]),
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
            enum([start]),
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
                example => <<"http:my_http_connector">>,
                desc => ?DESC("desc_param_path_id")
            }
        )}.

param_path_enable() ->
    {enable,
        mk(
            boolean(),
            #{
                in => path,
                required => true,
                desc => ?DESC("desc_param_path_enable"),
                example => true
            }
        )}.

connector_info_array_example(Method) ->
    lists:map(fun(#{value := Config}) -> Config end, maps:values(connector_info_examples(Method))).

connector_info_examples(Method) ->
    emqx_connector_schema:examples(Method).

schema("/connectors") ->
    #{
        'operationId' => '/connectors',
        get => #{
            tags => [<<"connectors">>],
            summary => <<"List connectors">>,
            description => ?DESC("desc_api1"),
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_example(
                    array(emqx_connector_schema:get_response()),
                    connector_info_array_example(get)
                )
            }
        },
        post => #{
            tags => [<<"connectors">>],
            summary => <<"Create connector">>,
            description => ?DESC("desc_api2"),
            'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                emqx_connector_schema:post_request(),
                connector_info_examples(post)
            ),
            responses => #{
                201 => get_response_body_schema(),
                400 => error_schema('ALREADY_EXISTS', "Connector already exists")
            }
        }
    };
schema("/connectors/:id") ->
    #{
        'operationId' => '/connectors/:id',
        get => #{
            tags => [<<"connectors">>],
            summary => <<"Get connector">>,
            description => ?DESC("desc_api3"),
            parameters => [param_path_id()],
            responses => #{
                200 => get_response_body_schema(),
                404 => error_schema('NOT_FOUND', "Connector not found")
            }
        },
        put => #{
            tags => [<<"connectors">>],
            summary => <<"Update connector">>,
            description => ?DESC("desc_api4"),
            parameters => [param_path_id()],
            'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                emqx_connector_schema:put_request(),
                connector_info_examples(put)
            ),
            responses => #{
                200 => get_response_body_schema(),
                404 => error_schema('NOT_FOUND', "Connector not found"),
                400 => error_schema('BAD_REQUEST', "Update connector failed")
            }
        },
        delete => #{
            tags => [<<"connectors">>],
            summary => <<"Delete connector">>,
            description => ?DESC("desc_api5"),
            parameters => [param_path_id()],
            responses => #{
                204 => <<"Connector deleted">>,
                400 => error_schema(
                    'BAD_REQUEST',
                    "Cannot delete connector while active rules are defined for this connector"
                ),
                404 => error_schema('NOT_FOUND', "Connector not found"),
                503 => error_schema('SERVICE_UNAVAILABLE', "Service unavailable")
            }
        }
    };
schema("/connectors/:id/enable/:enable") ->
    #{
        'operationId' => '/connectors/:id/enable/:enable',
        put =>
            #{
                tags => [<<"connectors">>],
                summary => <<"Enable or disable connector">>,
                desc => ?DESC("desc_enable_connector"),
                parameters => [param_path_id(), param_path_enable()],
                responses =>
                    #{
                        204 => <<"Success">>,
                        404 => error_schema(
                            'NOT_FOUND', "Connector not found or invalid operation"
                        ),
                        503 => error_schema('SERVICE_UNAVAILABLE', "Service unavailable")
                    }
            }
    };
schema("/connectors/:id/:operation") ->
    #{
        'operationId' => '/connectors/:id/:operation',
        post => #{
            tags => [<<"connectors">>],
            summary => <<"Manually start a connector">>,
            description => ?DESC("desc_api7"),
            parameters => [
                param_path_id(),
                param_path_operation_cluster()
            ],
            responses => #{
                204 => <<"Operation success">>,
                400 => error_schema(
                    'BAD_REQUEST', "Problem with configuration of external service"
                ),
                404 => error_schema('NOT_FOUND', "Connector not found or invalid operation"),
                501 => error_schema('NOT_IMPLEMENTED', "Not Implemented"),
                503 => error_schema('SERVICE_UNAVAILABLE', "Service unavailable")
            }
        }
    };
schema("/nodes/:node/connectors/:id/:operation") ->
    #{
        'operationId' => '/nodes/:node/connectors/:id/:operation',
        post => #{
            tags => [<<"connectors">>],
            summary => <<"Manually start a connector on a given node">>,
            description => ?DESC("desc_api8"),
            parameters => [
                param_path_node(),
                param_path_id(),
                param_path_operation_on_node()
            ],
            responses => #{
                204 => <<"Operation success">>,
                400 => error_schema(
                    'BAD_REQUEST',
                    "Problem with configuration of external service or connector not enabled"
                ),
                404 => error_schema(
                    'NOT_FOUND', "Connector or node not found or invalid operation"
                ),
                501 => error_schema('NOT_IMPLEMENTED', "Not Implemented"),
                503 => error_schema('SERVICE_UNAVAILABLE', "Service unavailable")
            }
        }
    };
schema("/connectors_probe") ->
    #{
        'operationId' => '/connectors_probe',
        post => #{
            tags => [<<"connectors">>],
            desc => ?DESC("desc_api9"),
            summary => <<"Test creating connector">>,
            'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                emqx_connector_schema:post_request(),
                connector_info_examples(post)
            ),
            responses => #{
                204 => <<"Test connector OK">>,
                400 => error_schema(['TEST_FAILED'], "connector test failed")
            }
        }
    }.

'/connectors'(post, #{body := #{<<"type">> := ConnectorType, <<"name">> := ConnectorName} = Conf0}) ->
    case emqx_connector:is_exist(ConnectorType, ConnectorName) of
        true ->
            ?BAD_REQUEST('ALREADY_EXISTS', <<"connector already exists">>);
        false ->
            Conf = filter_out_request_body(Conf0),
            create_connector(ConnectorType, ConnectorName, Conf)
    end;
'/connectors'(get, _Params) ->
    Nodes = emqx:running_nodes(),
    NodeReplies = emqx_connector_proto_v1:list_connectors_on_nodes(Nodes),
    case is_ok(NodeReplies) of
        {ok, NodeConnectors} ->
            AllConnectors = [
                [format_resource(Data, Node) || Data <- Connectors]
             || {Node, Connectors} <- lists:zip(Nodes, NodeConnectors)
            ],
            ?OK(zip_connectors(AllConnectors));
        {error, Reason} ->
            ?INTERNAL_ERROR(Reason)
    end.

'/connectors/:id'(get, #{bindings := #{id := Id}}) ->
    ?TRY_PARSE_ID(Id, lookup_from_all_nodes(ConnectorType, ConnectorName, 200));
'/connectors/:id'(put, #{bindings := #{id := Id}, body := Conf0}) ->
    Conf1 = filter_out_request_body(Conf0),
    ?TRY_PARSE_ID(
        Id,
        case emqx_connector:is_exist(ConnectorType, ConnectorName) of
            true ->
                RawConf = emqx:get_raw_config([connectors, ConnectorType, ConnectorName], #{}),
                Conf = emqx_utils:deobfuscate(Conf1, RawConf),
                update_connector(ConnectorType, ConnectorName, Conf);
            false ->
                ?CONNECTOR_NOT_FOUND(ConnectorType, ConnectorName)
        end
    );
'/connectors/:id'(delete, #{bindings := #{id := Id}}) ->
    ?TRY_PARSE_ID(
        Id,
        case emqx_connector:is_exist(ConnectorType, ConnectorName) of
            true ->
                case emqx_connector:remove(ConnectorType, ConnectorName) of
                    ok ->
                        ?NO_CONTENT;
                    {error, {post_config_update, _HandlerMod, {active_channels, Channels}}} ->
                        ?BAD_REQUEST(
                            {<<"Cannot delete connector while there are active channels defined for this connector">>,
                                Channels}
                        );
                    {error, timeout} ->
                        ?SERVICE_UNAVAILABLE(<<"request timeout">>);
                    {error, Reason} ->
                        ?INTERNAL_ERROR(Reason)
                end;
            false ->
                ?CONNECTOR_NOT_FOUND(ConnectorType, ConnectorName)
        end
    ).

'/connectors_probe'(post, Request) ->
    RequestMeta = #{module => ?MODULE, method => post, path => "/connectors_probe"},
    case emqx_dashboard_swagger:filter_check_request_and_translate_body(Request, RequestMeta) of
        {ok, #{body := #{<<"type">> := ConnType} = Params}} ->
            Params1 = maybe_deobfuscate_connector_probe(Params),
            case
                emqx_connector_resource:create_dry_run(ConnType, maps:remove(<<"type">>, Params1))
            of
                ok ->
                    ?NO_CONTENT;
                {error, #{kind := validation_error} = Reason0} ->
                    Reason = redact(Reason0),
                    ?BAD_REQUEST('TEST_FAILED', emqx_utils_api:to_json(Reason));
                {error, Reason0} when not is_tuple(Reason0); element(1, Reason0) =/= 'exit' ->
                    Reason1 =
                        case Reason0 of
                            {unhealthy_target, Message} -> Message;
                            _ -> Reason0
                        end,
                    Reason = redact(Reason1),
                    ?BAD_REQUEST('TEST_FAILED', Reason)
            end;
        BadRequest ->
            redact(BadRequest)
    end.

maybe_deobfuscate_connector_probe(
    #{<<"type">> := ConnectorType, <<"name">> := ConnectorName} = Params
) ->
    case emqx_connector:is_exist(ConnectorType, ConnectorName) of
        true ->
            RawConf = emqx:get_raw_config([connectors, ConnectorType, ConnectorName], #{}),
            emqx_utils:deobfuscate(Params, RawConf);
        false ->
            %% A connector may be probed before it's created, so not finding it here is fine
            Params
    end;
maybe_deobfuscate_connector_probe(Params) ->
    Params.

lookup_from_all_nodes(ConnectorType, ConnectorName, SuccCode) ->
    Nodes = mria:running_nodes(),
    case
        is_ok(emqx_connector_proto_v1:lookup_from_all_nodes(Nodes, ConnectorType, ConnectorName))
    of
        {ok, [{ok, _} | _] = Results} ->
            {SuccCode, format_connector_info([R || {ok, R} <- Results])};
        {ok, [{error, not_found} | _]} ->
            ?CONNECTOR_NOT_FOUND(ConnectorType, ConnectorName);
        {error, Reason} ->
            ?INTERNAL_ERROR(Reason)
    end.

lookup_from_local_node(ConnectorType, ConnectorName) ->
    case emqx_connector:lookup(ConnectorType, ConnectorName) of
        {ok, Res} -> {ok, format_resource(Res, node())};
        Error -> Error
    end.

create_connector(ConnectorType, ConnectorName, Conf) ->
    create_or_update_connector(ConnectorType, ConnectorName, Conf, 201).

update_connector(ConnectorType, ConnectorName, Conf) ->
    create_or_update_connector(ConnectorType, ConnectorName, Conf, 200).

create_or_update_connector(ConnectorType, ConnectorName, Conf, HttpStatusCode) ->
    Check =
        try
            is_binary(ConnectorType) andalso emqx_resource:validate_type(ConnectorType),
            ok = emqx_resource:validate_name(ConnectorName)
        catch
            throw:Error ->
                ?BAD_REQUEST(emqx_utils_api:to_json(Error))
        end,
    case Check of
        ok ->
            do_create_or_update_connector(ConnectorType, ConnectorName, Conf, HttpStatusCode);
        BadRequest ->
            BadRequest
    end.

do_create_or_update_connector(ConnectorType, ConnectorName, Conf, HttpStatusCode) ->
    case emqx_connector:create(ConnectorType, ConnectorName, Conf) of
        {ok, _} ->
            lookup_from_all_nodes(ConnectorType, ConnectorName, HttpStatusCode);
        {error, {PreOrPostConfigUpdate, _HandlerMod, Reason}} when
            PreOrPostConfigUpdate =:= pre_config_update;
            PreOrPostConfigUpdate =:= post_config_update
        ->
            ?BAD_REQUEST(emqx_utils_api:to_json(redact(Reason)));
        {error, Reason} when is_map(Reason) ->
            ?BAD_REQUEST(emqx_utils_api:to_json(redact(Reason)))
    end.

'/connectors/:id/enable/:enable'(put, #{bindings := #{id := Id, enable := Enable}}) ->
    ?TRY_PARSE_ID(
        Id,
        case emqx_connector:disable_enable(enable_func(Enable), ConnectorType, ConnectorName) of
            {ok, _} ->
                ?NO_CONTENT;
            {error, {pre_config_update, _, connector_not_found}} ->
                ?CONNECTOR_NOT_FOUND(ConnectorType, ConnectorName);
            {error, {_, _, timeout}} ->
                ?SERVICE_UNAVAILABLE(<<"request timeout">>);
            {error, timeout} ->
                ?SERVICE_UNAVAILABLE(<<"request timeout">>);
            {error, Reason} ->
                ?INTERNAL_ERROR(Reason)
        end
    ).

'/connectors/:id/:operation'(post, #{
    bindings :=
        #{id := Id, operation := Op}
}) ->
    ?TRY_PARSE_ID(
        Id,
        begin
            OperFunc = operation_func(all, Op),
            Nodes = mria:running_nodes(),
            call_operation_if_enabled(all, OperFunc, [Nodes, ConnectorType, ConnectorName])
        end
    ).

'/nodes/:node/connectors/:id/:operation'(post, #{
    bindings :=
        #{id := Id, operation := Op, node := Node}
}) ->
    ?TRY_PARSE_ID(
        Id,
        case emqx_utils:safe_to_existing_atom(Node, utf8) of
            {ok, TargetNode} ->
                OperFunc = operation_func(TargetNode, Op),
                call_operation_if_enabled(TargetNode, OperFunc, [
                    TargetNode, ConnectorType, ConnectorName
                ]);
            {error, _} ->
                ?NOT_FOUND(<<"Invalid node name: ", Node/binary>>)
        end
    ).

call_operation_if_enabled(NodeOrAll, OperFunc, [Nodes, BridgeType, BridgeName]) ->
    try is_enabled_connector(BridgeType, BridgeName) of
        false ->
            ?CONNECTOR_NOT_ENABLED;
        true ->
            call_operation(NodeOrAll, OperFunc, [Nodes, BridgeType, BridgeName])
    catch
        throw:not_found ->
            ?CONNECTOR_NOT_FOUND(BridgeType, BridgeName)
    end.

is_enabled_connector(ConnectorType, ConnectorName) ->
    try emqx:get_config([connectors, ConnectorType, binary_to_existing_atom(ConnectorName)]) of
        ConfMap ->
            maps:get(enable, ConfMap, true)
    catch
        error:{config_not_found, _} ->
            throw(not_found);
        error:badarg ->
            %% catch non-existing atom,
            %% none-existing atom means it is not available in config PT storage.
            throw(not_found)
    end.

operation_func(all, start) -> start_connectors_to_all_nodes;
operation_func(_Node, start) -> start_connector_to_node.

enable_func(true) -> enable;
enable_func(false) -> disable.

zip_connectors([ConnectorsFirstNode | _] = ConnectorsAllNodes) ->
    lists:foldl(
        fun(#{type := Type, name := Name}, Acc) ->
            Connectors = pick_connectors_by_id(Type, Name, ConnectorsAllNodes),
            [format_connector_info(Connectors) | Acc]
        end,
        [],
        ConnectorsFirstNode
    ).

pick_connectors_by_id(Type, Name, ConnectorsAllNodes) ->
    lists:foldl(
        fun(ConnectorsOneNode, Acc) ->
            case
                [
                    Connector
                 || Connector = #{type := Type0, name := Name0} <- ConnectorsOneNode,
                    Type0 == Type,
                    Name0 == Name
                ]
            of
                [ConnectorInfo] ->
                    [ConnectorInfo | Acc];
                [] ->
                    ?SLOG(warning, #{
                        msg => "connector_inconsistent_in_cluster",
                        reason => not_found,
                        type => Type,
                        name => Name,
                        connector => emqx_connector_resource:connector_id(Type, Name)
                    }),
                    Acc
            end
        end,
        [],
        ConnectorsAllNodes
    ).

format_connector_info([FirstConnector | _] = Connectors) ->
    Res = maps:remove(node, FirstConnector),
    NodeStatus = node_status(Connectors),
    StatusReason = first_status_reason(Connectors),
    Info0 = Res#{
        status => aggregate_status(NodeStatus),
        node_status => NodeStatus
    },
    Info = emqx_utils_maps:put_if(Info0, status_reason, StatusReason, StatusReason =/= undefined),
    redact(Info).

node_status(Connectors) ->
    [maps:with([node, status, status_reason], B) || B <- Connectors].

first_status_reason(Connectors) ->
    StatusReasons = [Reason || #{status_reason := Reason} <- Connectors, Reason =/= undefined],
    case StatusReasons of
        [Reason | _] -> Reason;
        _ -> undefined
    end.

aggregate_status(AllStatus) ->
    Head = fun([A | _]) -> A end,
    HeadVal = maps:get(status, Head(AllStatus), connecting),
    AllRes = lists:all(fun(#{status := Val}) -> Val == HeadVal end, AllStatus),
    case AllRes of
        true -> HeadVal;
        false -> inconsistent
    end.

format_resource(
    #{
        type := Type,
        name := ConnectorName,
        raw_config := RawConf0,
        resource_data := ResourceData0
    },
    Node
) ->
    ResourceData = lookup_channels(Type, ConnectorName, ResourceData0),
    RawConf = fill_defaults(Type, RawConf0),
    redact(
        maps:merge(
            RawConf#{
                type => Type,
                name => maps:get(<<"name">>, RawConf, ConnectorName),
                node => Node
            },
            format_resource_data(ResourceData)
        )
    ).

lookup_channels(Type, Name, ResourceData0) ->
    ConnectorResId = emqx_connector_resource:resource_id(Type, Name),
    case emqx_resource:get_channels(ConnectorResId) of
        {ok, Channels} ->
            ResourceData0#{channels => maps:from_list(Channels)};
        {error, not_found} ->
            ResourceData0#{channels => #{}}
    end.

format_resource_data(ResData) ->
    maps:fold(fun format_resource_data/3, #{}, maps:with([status, error, channels], ResData)).

format_resource_data(error, undefined, Result) ->
    Result;
format_resource_data(error, Error, Result) ->
    Result#{status_reason => emqx_utils:readable_error_msg(Error)};
format_resource_data(channels, Channels, Result) ->
    #{
        actions := Actions,
        sources := Sources
    } = lists:foldl(
        fun(Id, Acc) ->
            case emqx_bridge_v2:parse_id(Id) of
                #{kind := source, name := Name} ->
                    maps:update_with(sources, fun(Ss) -> [Name | Ss] end, Acc);
                #{name := Name} ->
                    maps:update_with(actions, fun(As) -> [Name | As] end, Acc)
            end
        end,
        #{actions => [], sources => []},
        maps:keys(Channels)
    ),
    Result#{actions => lists:sort(Actions), sources => lists:sort(Sources)};
format_resource_data(K, V, Result) ->
    Result#{K => V}.

fill_defaults(Type, RawConf) ->
    PackedConf = pack_connector_conf(Type, RawConf),
    FullConf = emqx_config:fill_defaults(emqx_connector_schema, PackedConf, #{}),
    unpack_connector_conf(Type, FullConf).

pack_connector_conf(Type, RawConf) ->
    #{<<"connectors">> => #{bin(Type) => #{<<"foo">> => RawConf}}}.

unpack_connector_conf(Type, PackedConf) ->
    TypeBin = bin(Type),
    #{<<"connectors">> := Bridges} = PackedConf,
    #{<<"foo">> := RawConf} = maps:get(TypeBin, Bridges),
    RawConf.

is_ok(ok) ->
    ok;
is_ok(OkResult = {ok, _}) ->
    OkResult;
is_ok(Error = {error, _}) ->
    Error;
is_ok(timeout) ->
    %% Returned by `emqx_resource_manager:start' when the connector fails to reach either
    %% `?status_connected' or `?status_disconnected' within `start_timeout'.
    timeout;
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
        ErrL -> hd(ErrL)
    end.

filter_out_request_body(Conf) ->
    ExtraConfs = [
        <<"id">>,
        <<"type">>,
        <<"name">>,
        <<"status">>,
        <<"status_reason">>,
        <<"node_status">>,
        <<"node">>
    ],
    maps:without(ExtraConfs, Conf).

bin(S) when is_list(S) ->
    list_to_binary(S);
bin(S) when is_atom(S) ->
    atom_to_binary(S, utf8);
bin(S) when is_binary(S) ->
    S.

call_operation(NodeOrAll, OperFunc, Args = [_Nodes, ConnectorType, ConnectorName]) ->
    case is_ok(do_bpapi_call(NodeOrAll, OperFunc, Args)) of
        Ok when Ok =:= ok; is_tuple(Ok), element(1, Ok) =:= ok ->
            ?NO_CONTENT;
        timeout ->
            %% Returned by `emqx_resource_manager:start' when the connector fails to reach
            %% either `?status_connected' or `?status_disconnected' within
            %% `start_timeout'.
            ?BAD_REQUEST(<<
                "Timeout while waiting for connector to reach connected status."
                " Please try again."
            >>);
        {error, not_implemented} ->
            ?NOT_IMPLEMENTED;
        {error, timeout} ->
            ?BAD_REQUEST(<<"Request timeout">>);
        {error, {start_pool_failed, Name, Reason}} ->
            Msg = bin(
                io_lib:format("Failed to start ~p pool for reason ~p", [Name, redact(Reason)])
            ),
            ?BAD_REQUEST(Msg);
        {error, not_found} ->
            ConnectorId = emqx_connector_resource:connector_id(ConnectorType, ConnectorName),
            ?SLOG(warning, #{
                msg => "connector_inconsistent_in_cluster_for_call_operation",
                reason => not_found,
                type => ConnectorType,
                name => ConnectorName,
                connector => ConnectorId
            }),
            ?SERVICE_UNAVAILABLE(<<"Connector not found on remote node: ", ConnectorId/binary>>);
        {error, {node_not_found, Node}} ->
            ?NOT_FOUND(<<"Node not found: ", (atom_to_binary(Node))/binary>>);
        {error, {unhealthy_target, Message}} ->
            ?BAD_REQUEST(Message);
        {error, Reason} when not is_tuple(Reason); element(1, Reason) =/= 'exit' ->
            ?BAD_REQUEST(redact(Reason))
    end.

do_bpapi_call(all, Call, Args) ->
    maybe_unwrap(
        do_bpapi_call_vsn(emqx_bpapi:supported_version(emqx_connector), Call, Args)
    );
do_bpapi_call(Node, Call, Args) ->
    case lists:member(Node, mria:running_nodes()) of
        true ->
            do_bpapi_call_vsn(emqx_bpapi:supported_version(Node, emqx_connector), Call, Args);
        false ->
            {error, {node_not_found, Node}}
    end.

do_bpapi_call_vsn(Version, Call, Args) ->
    case is_supported_version(Version, Call) of
        true ->
            apply(emqx_connector_proto_v1, Call, Args);
        false ->
            {error, not_implemented}
    end.

is_supported_version(Version, Call) ->
    lists:member(Version, supported_versions(Call)).

supported_versions(_Call) -> [1].

maybe_unwrap({error, not_implemented}) ->
    {error, not_implemented};
maybe_unwrap(RpcMulticallResult) ->
    emqx_rpc:unwrap_erpc(RpcMulticallResult).

redact(Term) ->
    emqx_utils:redact(Term).
