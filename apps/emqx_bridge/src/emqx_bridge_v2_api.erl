%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_bridge_v2_api).

-behaviour(minirest_api).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_utils/include/emqx_utils_api.hrl").
-include_lib("emqx_bridge/include/emqx_bridge.hrl").

-import(hoconsc, [mk/2, array/1, enum/1]).
-import(emqx_utils, [redact/1]).

%% Swagger specs from hocon schema
-export([
    api_spec/0,
    paths/0,
    schema/1,
    namespace/0
]).

%% API callbacks
-export([
    '/actions'/2,
    '/actions/:id'/2,
    '/actions/:id/metrics'/2,
    '/actions/:id/metrics/reset'/2,
    '/actions/:id/enable/:enable'/2,
    '/actions/:id/:operation'/2,
    '/nodes/:node/actions/:id/:operation'/2,
    '/actions_probe'/2,
    '/action_types'/2
]).

%% BpAPI / RPC Targets
-export([lookup_from_local_node/2, get_metrics_from_local_node/2]).

-define(BRIDGE_NOT_FOUND(BRIDGE_TYPE, BRIDGE_NAME),
    ?NOT_FOUND(
        <<"Bridge lookup failed: bridge named '", (bin(BRIDGE_NAME))/binary, "' of type ",
            (bin(BRIDGE_TYPE))/binary, " does not exist.">>
    )
).

-define(BRIDGE_NOT_ENABLED,
    ?BAD_REQUEST(<<"Forbidden operation, bridge not enabled">>)
).

-define(TRY_PARSE_ID(ID, EXPR),
    try emqx_bridge_resource:parse_bridge_id(Id, #{atom_name => false}) of
        {BridgeType, BridgeName} ->
            EXPR
    catch
        throw:#{reason := Reason} ->
            ?NOT_FOUND(<<"Invalid bridge ID, ", Reason/binary>>)
    end
).

namespace() -> "actions".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [
        "/actions",
        "/actions/:id",
        "/actions/:id/enable/:enable",
        "/actions/:id/:operation",
        "/nodes/:node/actions/:id/:operation",
        %% Caveat: metrics paths must come *after* `/:operation', otherwise minirest will
        %% try to match the latter first, trying to interpret `metrics' as an operation...
        "/actions/:id/metrics",
        "/actions/:id/metrics/reset",
        "/actions_probe",
        "/action_types"
    ].

error_schema(Code, Message) when is_atom(Code) ->
    error_schema([Code], Message);
error_schema(Codes, Message) when is_list(Message) ->
    error_schema(Codes, list_to_binary(Message));
error_schema(Codes, Message) when is_list(Codes) andalso is_binary(Message) ->
    emqx_dashboard_swagger:error_codes(Codes, Message).

get_response_body_schema() ->
    emqx_dashboard_swagger:schema_with_examples(
        emqx_bridge_v2_schema:get_response(),
        bridge_info_examples(get)
    ).

bridge_info_examples(Method) ->
    emqx_bridge_v2_schema:examples(Method).

bridge_info_array_example(Method) ->
    lists:map(fun(#{value := Config}) -> Config end, maps:values(bridge_info_examples(Method))).

param_path_id() ->
    {id,
        mk(
            binary(),
            #{
                in => path,
                required => true,
                example => <<"http:my_http_action">>,
                desc => ?DESC("desc_param_path_id")
            }
        )}.

param_qs_delete_cascade() ->
    {also_delete_dep_actions,
        mk(
            boolean(),
            #{
                in => query,
                required => false,
                default => false,
                desc => ?DESC("desc_qs_also_delete_dep_actions")
            }
        )}.

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

schema("/actions") ->
    #{
        'operationId' => '/actions',
        get => #{
            tags => [<<"actions">>],
            summary => <<"List bridges">>,
            description => ?DESC("desc_api1"),
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_example(
                    array(emqx_bridge_v2_schema:get_response()),
                    bridge_info_array_example(get)
                )
            }
        },
        post => #{
            tags => [<<"actions">>],
            summary => <<"Create bridge">>,
            description => ?DESC("desc_api2"),
            'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                emqx_bridge_v2_schema:post_request(),
                bridge_info_examples(post)
            ),
            responses => #{
                201 => get_response_body_schema(),
                400 => error_schema('ALREADY_EXISTS', "Bridge already exists")
            }
        }
    };
schema("/actions/:id") ->
    #{
        'operationId' => '/actions/:id',
        get => #{
            tags => [<<"actions">>],
            summary => <<"Get bridge">>,
            description => ?DESC("desc_api3"),
            parameters => [param_path_id()],
            responses => #{
                200 => get_response_body_schema(),
                404 => error_schema('NOT_FOUND', "Bridge not found")
            }
        },
        put => #{
            tags => [<<"actions">>],
            summary => <<"Update bridge">>,
            description => ?DESC("desc_api4"),
            parameters => [param_path_id()],
            'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                emqx_bridge_v2_schema:put_request(),
                bridge_info_examples(put)
            ),
            responses => #{
                200 => get_response_body_schema(),
                404 => error_schema('NOT_FOUND', "Bridge not found"),
                400 => error_schema('BAD_REQUEST', "Update bridge failed")
            }
        },
        delete => #{
            tags => [<<"actions">>],
            summary => <<"Delete bridge">>,
            description => ?DESC("desc_api5"),
            parameters => [param_path_id(), param_qs_delete_cascade()],
            responses => #{
                204 => <<"Bridge deleted">>,
                400 => error_schema(
                    'BAD_REQUEST',
                    "Cannot delete bridge while active rules are defined for this bridge"
                ),
                404 => error_schema('NOT_FOUND', "Bridge not found"),
                503 => error_schema('SERVICE_UNAVAILABLE', "Service unavailable")
            }
        }
    };
schema("/actions/:id/metrics") ->
    #{
        'operationId' => '/actions/:id/metrics',
        get => #{
            tags => [<<"actions">>],
            summary => <<"Get action metrics">>,
            description => ?DESC("desc_bridge_metrics"),
            parameters => [param_path_id()],
            responses => #{
                200 => emqx_bridge_schema:metrics_fields(),
                404 => error_schema('NOT_FOUND', "Action not found")
            }
        }
    };
schema("/actions/:id/metrics/reset") ->
    #{
        'operationId' => '/actions/:id/metrics/reset',
        put => #{
            tags => [<<"actions">>],
            summary => <<"Reset action metrics">>,
            description => ?DESC("desc_api6"),
            parameters => [param_path_id()],
            responses => #{
                204 => <<"Reset success">>,
                404 => error_schema('NOT_FOUND', "Action not found")
            }
        }
    };
schema("/actions/:id/enable/:enable") ->
    #{
        'operationId' => '/actions/:id/enable/:enable',
        put =>
            #{
                tags => [<<"actions">>],
                summary => <<"Enable or disable bridge">>,
                desc => ?DESC("desc_enable_bridge"),
                parameters => [param_path_id(), param_path_enable()],
                responses =>
                    #{
                        204 => <<"Success">>,
                        404 => error_schema(
                            'NOT_FOUND', "Bridge not found or invalid operation"
                        ),
                        503 => error_schema('SERVICE_UNAVAILABLE', "Service unavailable")
                    }
            }
    };
schema("/actions/:id/:operation") ->
    #{
        'operationId' => '/actions/:id/:operation',
        post => #{
            tags => [<<"actions">>],
            summary => <<"Manually start a bridge">>,
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
                404 => error_schema('NOT_FOUND', "Bridge not found or invalid operation"),
                501 => error_schema('NOT_IMPLEMENTED', "Not Implemented"),
                503 => error_schema('SERVICE_UNAVAILABLE', "Service unavailable")
            }
        }
    };
schema("/nodes/:node/actions/:id/:operation") ->
    #{
        'operationId' => '/nodes/:node/actions/:id/:operation',
        post => #{
            tags => [<<"actions">>],
            summary => <<"Manually start a bridge on a given node">>,
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
                    "Problem with configuration of external service or bridge not enabled"
                ),
                404 => error_schema(
                    'NOT_FOUND', "Bridge or node not found or invalid operation"
                ),
                501 => error_schema('NOT_IMPLEMENTED', "Not Implemented"),
                503 => error_schema('SERVICE_UNAVAILABLE', "Service unavailable")
            }
        }
    };
schema("/actions_probe") ->
    #{
        'operationId' => '/actions_probe',
        post => #{
            tags => [<<"actions">>],
            desc => ?DESC("desc_api9"),
            summary => <<"Test creating bridge">>,
            'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                emqx_bridge_v2_schema:post_request(),
                bridge_info_examples(post)
            ),
            responses => #{
                204 => <<"Test bridge OK">>,
                400 => error_schema(['TEST_FAILED'], "bridge test failed")
            }
        }
    };
schema("/action_types") ->
    #{
        'operationId' => '/action_types',
        get => #{
            tags => [<<"actions">>],
            desc => ?DESC("desc_api10"),
            summary => <<"List available action types">>,
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_examples(
                    array(emqx_bridge_v2_schema:types_sc()),
                    #{
                        <<"types">> =>
                            #{
                                summary => <<"Action types">>,
                                value => emqx_bridge_v2_schema:types()
                            }
                    }
                )
            }
        }
    }.

'/actions'(post, #{body := #{<<"type">> := BridgeType, <<"name">> := BridgeName} = Conf0}) ->
    case emqx_bridge_v2:lookup(BridgeType, BridgeName) of
        {ok, _} ->
            ?BAD_REQUEST('ALREADY_EXISTS', <<"bridge already exists">>);
        {error, not_found} ->
            Conf = filter_out_request_body(Conf0),
            create_bridge(BridgeType, BridgeName, Conf)
    end;
'/actions'(get, _Params) ->
    Nodes = mria:running_nodes(),
    NodeReplies = emqx_bridge_proto_v5:v2_list_bridges_on_nodes(Nodes),
    case is_ok(NodeReplies) of
        {ok, NodeBridges} ->
            AllBridges = [
                [format_resource(Data, Node) || Data <- Bridges]
             || {Node, Bridges} <- lists:zip(Nodes, NodeBridges)
            ],
            ?OK(zip_bridges(AllBridges));
        {error, Reason} ->
            ?INTERNAL_ERROR(Reason)
    end.

'/actions/:id'(get, #{bindings := #{id := Id}}) ->
    ?TRY_PARSE_ID(Id, lookup_from_all_nodes(BridgeType, BridgeName, 200));
'/actions/:id'(put, #{bindings := #{id := Id}, body := Conf0}) ->
    Conf1 = filter_out_request_body(Conf0),
    ?TRY_PARSE_ID(
        Id,
        case emqx_bridge_v2:lookup(BridgeType, BridgeName) of
            {ok, _} ->
                RawConf = emqx:get_raw_config([bridges, BridgeType, BridgeName], #{}),
                Conf = deobfuscate(Conf1, RawConf),
                update_bridge(BridgeType, BridgeName, Conf);
            {error, not_found} ->
                ?BRIDGE_NOT_FOUND(BridgeType, BridgeName)
        end
    );
'/actions/:id'(delete, #{bindings := #{id := Id}, query_string := Qs}) ->
    ?TRY_PARSE_ID(
        Id,
        case emqx_bridge_v2:lookup(BridgeType, BridgeName) of
            {ok, _} ->
                AlsoDeleteActions =
                    case maps:get(<<"also_delete_dep_actions">>, Qs, <<"false">>) of
                        <<"true">> -> true;
                        true -> true;
                        _ -> false
                    end,
                case
                    emqx_bridge_v2:check_deps_and_remove(BridgeType, BridgeName, AlsoDeleteActions)
                of
                    ok ->
                        ?NO_CONTENT;
                    {error, #{
                        reason := rules_depending_on_this_bridge,
                        rule_ids := RuleIds
                    }} ->
                        RuleIdLists = [binary_to_list(iolist_to_binary(X)) || X <- RuleIds],
                        RulesStr = string:join(RuleIdLists, ", "),
                        Msg = io_lib:format(
                            "Cannot delete bridge while active rules are depending on it: ~s\n"
                            "Append ?also_delete_dep_actions=true to the request URL to delete "
                            "rule actions that depend on this bridge as well.",
                            [RulesStr]
                        ),
                        ?BAD_REQUEST(iolist_to_binary(Msg));
                    {error, timeout} ->
                        ?SERVICE_UNAVAILABLE(<<"request timeout">>);
                    {error, Reason} ->
                        ?INTERNAL_ERROR(Reason)
                end;
            {error, not_found} ->
                ?BRIDGE_NOT_FOUND(BridgeType, BridgeName)
        end
    ).

'/actions/:id/metrics'(get, #{bindings := #{id := Id}}) ->
    ?TRY_PARSE_ID(Id, get_metrics_from_all_nodes(BridgeType, BridgeName)).

'/actions/:id/metrics/reset'(put, #{bindings := #{id := Id}}) ->
    ?TRY_PARSE_ID(
        Id,
        begin
            ActionType = emqx_bridge_v2:bridge_v2_type_to_connector_type(BridgeType),
            ok = emqx_bridge_v2:reset_metrics(ActionType, BridgeName),
            ?NO_CONTENT
        end
    ).

'/actions/:id/enable/:enable'(put, #{bindings := #{id := Id, enable := Enable}}) ->
    ?TRY_PARSE_ID(
        Id,
        case emqx_bridge_v2:disable_enable(enable_func(Enable), BridgeType, BridgeName) of
            {ok, _} ->
                ?NO_CONTENT;
            {error, {pre_config_update, _, bridge_not_found}} ->
                ?BRIDGE_NOT_FOUND(BridgeType, BridgeName);
            {error, {_, _, timeout}} ->
                ?SERVICE_UNAVAILABLE(<<"request timeout">>);
            {error, timeout} ->
                ?SERVICE_UNAVAILABLE(<<"request timeout">>);
            {error, Reason} ->
                ?INTERNAL_ERROR(Reason)
        end
    ).

'/actions/:id/:operation'(post, #{
    bindings :=
        #{id := Id, operation := Op}
}) ->
    ?TRY_PARSE_ID(
        Id,
        begin
            OperFunc = operation_func(all, Op),
            Nodes = mria:running_nodes(),
            call_operation_if_enabled(all, OperFunc, [Nodes, BridgeType, BridgeName])
        end
    ).

'/nodes/:node/actions/:id/:operation'(post, #{
    bindings :=
        #{id := Id, operation := Op, node := Node}
}) ->
    ?TRY_PARSE_ID(
        Id,
        case emqx_utils:safe_to_existing_atom(Node, utf8) of
            {ok, TargetNode} ->
                OperFunc = operation_func(TargetNode, Op),
                call_operation_if_enabled(TargetNode, OperFunc, [TargetNode, BridgeType, BridgeName]);
            {error, _} ->
                ?NOT_FOUND(<<"Invalid node name: ", Node/binary>>)
        end
    ).

'/actions_probe'(post, Request) ->
    RequestMeta = #{module => ?MODULE, method => post, path => "/actions_probe"},
    case emqx_dashboard_swagger:filter_check_request_and_translate_body(Request, RequestMeta) of
        {ok, #{body := #{<<"type">> := ConnType} = Params}} ->
            Params1 = maybe_deobfuscate_bridge_probe(Params),
            Params2 = maps:remove(<<"type">>, Params1),
            case emqx_bridge_v2:create_dry_run(ConnType, Params2) of
                ok ->
                    ?NO_CONTENT;
                {error, #{kind := validation_error} = Reason0} ->
                    Reason = redact(Reason0),
                    ?BAD_REQUEST('TEST_FAILED', map_to_json(Reason));
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

'/action_types'(get, _Request) ->
    ?OK(emqx_bridge_v2_schema:types()).

maybe_deobfuscate_bridge_probe(#{<<"type">> := BridgeType, <<"name">> := BridgeName} = Params) ->
    case emqx_bridge:lookup(BridgeType, BridgeName) of
        {ok, #{raw_config := RawConf}} ->
            %% TODO check if RawConf optained above is compatible with the commented out code below
            %% RawConf = emqx:get_raw_config([bridges, BridgeType, BridgeName], #{}),
            deobfuscate(Params, RawConf);
        _ ->
            %% A bridge may be probed before it's created, so not finding it here is fine
            Params
    end;
maybe_deobfuscate_bridge_probe(Params) ->
    Params.

%%% API helpers
is_ok(ok) ->
    ok;
is_ok(OkResult = {ok, _}) ->
    OkResult;
is_ok(Error = {error, _}) ->
    Error;
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

deobfuscate(NewConf, OldConf) ->
    maps:fold(
        fun(K, V, Acc) ->
            case maps:find(K, OldConf) of
                error ->
                    Acc#{K => V};
                {ok, OldV} when is_map(V), is_map(OldV) ->
                    Acc#{K => deobfuscate(V, OldV)};
                {ok, OldV} ->
                    case emqx_utils:is_redacted(K, V) of
                        true ->
                            Acc#{K => OldV};
                        _ ->
                            Acc#{K => V}
                    end
            end
        end,
        #{},
        NewConf
    ).

%% bridge helpers
lookup_from_all_nodes(BridgeType, BridgeName, SuccCode) ->
    Nodes = mria:running_nodes(),
    case is_ok(emqx_bridge_proto_v5:v2_lookup_from_all_nodes(Nodes, BridgeType, BridgeName)) of
        {ok, [{ok, _} | _] = Results} ->
            {SuccCode, format_bridge_info([R || {ok, R} <- Results])};
        {ok, [{error, not_found} | _]} ->
            ?BRIDGE_NOT_FOUND(BridgeType, BridgeName);
        {error, Reason} ->
            ?INTERNAL_ERROR(Reason)
    end.

get_metrics_from_all_nodes(ActionType, ActionName) ->
    Nodes = emqx:running_nodes(),
    Result = maybe_unwrap(
        emqx_bridge_proto_v5:v2_get_metrics_from_all_nodes(Nodes, ActionType, ActionName)
    ),
    case Result of
        Metrics when is_list(Metrics) ->
            {200, format_bridge_metrics(lists:zip(Nodes, Metrics))};
        {error, Reason} ->
            ?INTERNAL_ERROR(Reason)
    end.

operation_func(all, start) -> v2_start_bridge_to_all_nodes;
operation_func(_Node, start) -> v2_start_bridge_to_node.

call_operation_if_enabled(NodeOrAll, OperFunc, [Nodes, BridgeType, BridgeName]) ->
    try is_enabled_bridge(BridgeType, BridgeName) of
        false ->
            ?BRIDGE_NOT_ENABLED;
        true ->
            call_operation(NodeOrAll, OperFunc, [Nodes, BridgeType, BridgeName])
    catch
        throw:not_found ->
            ?BRIDGE_NOT_FOUND(BridgeType, BridgeName)
    end.

is_enabled_bridge(BridgeType, BridgeName) ->
    try emqx_bridge_v2:lookup(BridgeType, binary_to_existing_atom(BridgeName)) of
        {ok, #{raw_config := ConfMap}} ->
            maps:get(<<"enable">>, ConfMap, false);
        {error, not_found} ->
            throw(not_found)
    catch
        error:badarg ->
            %% catch non-existing atom,
            %% none-existing atom means it is not available in config PT storage.
            throw(not_found)
    end.

call_operation(NodeOrAll, OperFunc, Args = [_Nodes, BridgeType, BridgeName]) ->
    case is_ok(do_bpapi_call(NodeOrAll, OperFunc, Args)) of
        Ok when Ok =:= ok; is_tuple(Ok), element(1, Ok) =:= ok ->
            ?NO_CONTENT;
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
            BridgeId = emqx_bridge_resource:bridge_id(BridgeType, BridgeName),
            ?SLOG(warning, #{
                msg => "bridge_inconsistent_in_cluster_for_call_operation",
                reason => not_found,
                type => BridgeType,
                name => BridgeName,
                bridge => BridgeId
            }),
            ?SERVICE_UNAVAILABLE(<<"Bridge not found on remote node: ", BridgeId/binary>>);
        {error, {node_not_found, Node}} ->
            ?NOT_FOUND(<<"Node not found: ", (atom_to_binary(Node))/binary>>);
        {error, Reason} ->
            ?BAD_REQUEST(redact(Reason))
    end.

do_bpapi_call(all, Call, Args) ->
    maybe_unwrap(
        do_bpapi_call_vsn(emqx_bpapi:supported_version(emqx_bridge), Call, Args)
    );
do_bpapi_call(Node, Call, Args) ->
    case lists:member(Node, mria:running_nodes()) of
        true ->
            do_bpapi_call_vsn(emqx_bpapi:supported_version(Node, emqx_bridge), Call, Args);
        false ->
            {error, {node_not_found, Node}}
    end.

do_bpapi_call_vsn(Version, Call, Args) ->
    case is_supported_version(Version, Call) of
        true ->
            apply(emqx_bridge_proto_v5, Call, Args);
        false ->
            {error, not_implemented}
    end.

is_supported_version(Version, Call) ->
    lists:member(Version, supported_versions(Call)).

supported_versions(_Call) -> [5].

maybe_unwrap({error, not_implemented}) ->
    {error, not_implemented};
maybe_unwrap(RpcMulticallResult) ->
    emqx_rpc:unwrap_erpc(RpcMulticallResult).

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
    NodeStatus = node_status(Bridges),
    redact(Res#{
        status => aggregate_status(NodeStatus),
        node_status => NodeStatus
    }).

node_status(Bridges) ->
    [maps:with([node, status, status_reason], B) || B <- Bridges].

aggregate_status(AllStatus) ->
    Head = fun([A | _]) -> A end,
    HeadVal = maps:get(status, Head(AllStatus), connecting),
    AllRes = lists:all(
        fun
            (#{status := Val}) -> Val == HeadVal;
            (_) -> false
        end,
        AllStatus
    ),
    case AllRes of
        true -> HeadVal;
        false -> inconsistent
    end.

%% RPC Target
lookup_from_local_node(BridgeType, BridgeName) ->
    case emqx_bridge_v2:lookup(BridgeType, BridgeName) of
        {ok, Res} -> {ok, format_resource(Res, node())};
        Error -> Error
    end.

%% RPC Target
get_metrics_from_local_node(ActionType, ActionName) ->
    format_metrics(emqx_bridge_v2:get_metrics(ActionType, ActionName)).

%% resource
format_resource(
    #{
        type := Type,
        name := Name,
        status := Status,
        error := Error,
        raw_config := RawConf,
        resource_data := _ResourceData
    },
    Node
) ->
    redact(
        maps:merge(
            RawConf#{
                type => Type,
                name => maps:get(<<"name">>, RawConf, Name),
                node => Node,
                status => Status,
                error => Error
            },
            format_bridge_status_and_error(#{status => Status, error => Error})
        )
    ).

format_metrics(#{
    counters := #{
        'dropped' := Dropped,
        'dropped.other' := DroppedOther,
        'dropped.expired' := DroppedExpired,
        'dropped.queue_full' := DroppedQueueFull,
        'dropped.resource_not_found' := DroppedResourceNotFound,
        'dropped.resource_stopped' := DroppedResourceStopped,
        'matched' := Matched,
        'retried' := Retried,
        'late_reply' := LateReply,
        'failed' := SentFailed,
        'success' := SentSucc,
        'received' := Rcvd
    },
    gauges := Gauges,
    rate := #{
        matched := #{current := Rate, last5m := Rate5m, max := RateMax}
    }
}) ->
    Queued = maps:get('queuing', Gauges, 0),
    SentInflight = maps:get('inflight', Gauges, 0),
    ?METRICS(
        Dropped,
        DroppedOther,
        DroppedExpired,
        DroppedQueueFull,
        DroppedResourceNotFound,
        DroppedResourceStopped,
        Matched,
        Queued,
        Retried,
        LateReply,
        SentFailed,
        SentInflight,
        SentSucc,
        Rate,
        Rate5m,
        RateMax,
        Rcvd
    );
format_metrics(_Metrics) ->
    %% Empty metrics: can happen when a node joins another and a
    %% bridge is not yet replicated to it, so the counters map is
    %% empty.
    empty_metrics().

empty_metrics() ->
    ?METRICS(
        _Dropped = 0,
        _DroppedOther = 0,
        _DroppedExpired = 0,
        _DroppedQueueFull = 0,
        _DroppedResourceNotFound = 0,
        _DroppedResourceStopped = 0,
        _Matched = 0,
        _Queued = 0,
        _Retried = 0,
        _LateReply = 0,
        _SentFailed = 0,
        _SentInflight = 0,
        _SentSucc = 0,
        _Rate = 0,
        _Rate5m = 0,
        _RateMax = 0,
        _Rcvd = 0
    ).

format_bridge_metrics(Bridges) ->
    NodeMetrics = lists:filtermap(
        fun
            ({Node, Metrics}) when is_map(Metrics) ->
                {true, #{node => Node, metrics => Metrics}};
            ({Node, _}) ->
                {true, #{node => Node, metrics => empty_metrics()}}
        end,
        Bridges
    ),
    #{
        metrics => aggregate_metrics(NodeMetrics),
        node_metrics => NodeMetrics
    }.

aggregate_metrics(AllMetrics) ->
    InitMetrics = ?EMPTY_METRICS,
    lists:foldl(fun aggregate_metrics/2, InitMetrics, AllMetrics).

aggregate_metrics(
    #{
        metrics := ?metrics(
            M1, M2, M3, M4, M5, M6, M7, M8, M9, M10, M11, M12, M13, M14, M15, M16, M17
        )
    },
    ?metrics(
        N1, N2, N3, N4, N5, N6, N7, N8, N9, N10, N11, N12, N13, N14, N15, N16, N17
    )
) ->
    ?METRICS(
        M1 + N1,
        M2 + N2,
        M3 + N3,
        M4 + N4,
        M5 + N5,
        M6 + N6,
        M7 + N7,
        M8 + N8,
        M9 + N9,
        M10 + N10,
        M11 + N11,
        M12 + N12,
        M13 + N13,
        M14 + N14,
        M15 + N15,
        M16 + N16,
        M17 + N17
    ).

format_bridge_status_and_error(Data) ->
    maps:fold(fun format_resource_data/3, #{}, maps:with([status, error], Data)).

format_resource_data(error, undefined, Result) ->
    Result;
format_resource_data(error, Error, Result) ->
    Result#{status_reason => emqx_utils:readable_error_msg(Error)};
format_resource_data(K, V, Result) ->
    Result#{K => V}.

create_bridge(BridgeType, BridgeName, Conf) ->
    create_or_update_bridge(BridgeType, BridgeName, Conf, 201).

update_bridge(BridgeType, BridgeName, Conf) ->
    create_or_update_bridge(BridgeType, BridgeName, Conf, 200).

create_or_update_bridge(BridgeType, BridgeName, Conf, HttpStatusCode) ->
    Check =
        try
            is_binary(BridgeType) andalso emqx_resource:validate_type(BridgeType),
            ok = emqx_resource:validate_name(BridgeName)
        catch
            throw:Error ->
                ?BAD_REQUEST(map_to_json(Error))
        end,
    case Check of
        ok ->
            do_create_or_update_bridge(BridgeType, BridgeName, Conf, HttpStatusCode);
        BadRequest ->
            BadRequest
    end.

do_create_or_update_bridge(BridgeType, BridgeName, Conf, HttpStatusCode) ->
    case emqx_bridge_v2:create(BridgeType, BridgeName, Conf) of
        {ok, _} ->
            lookup_from_all_nodes(BridgeType, BridgeName, HttpStatusCode);
        {error, {PreOrPostConfigUpdate, _HandlerMod, Reason}} when
            PreOrPostConfigUpdate =:= pre_config_update;
            PreOrPostConfigUpdate =:= post_config_update
        ->
            ?BAD_REQUEST(map_to_json(redact(Reason)));
        {error, Reason} when is_map(Reason) ->
            ?BAD_REQUEST(map_to_json(redact(Reason)))
    end.

enable_func(true) -> enable;
enable_func(false) -> disable.

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

%% general helpers
bin(S) when is_list(S) ->
    list_to_binary(S);
bin(S) when is_atom(S) ->
    atom_to_binary(S, utf8);
bin(S) when is_binary(S) ->
    S.

map_to_json(M0) ->
    %% When dealing with Hocon validation errors, `value' might contain non-serializable
    %% values (e.g.: user_lookup_fun), so we try again without that key if serialization
    %% fails as a best effort.
    M1 = emqx_utils_maps:jsonable_map(M0, fun(K, V) -> {K, emqx_utils_maps:binary_string(V)} end),
    try
        emqx_utils_json:encode(M1)
    catch
        error:_ ->
            M2 = maps:without([value, <<"value">>], M1),
            emqx_utils_json:encode(M2)
    end.
