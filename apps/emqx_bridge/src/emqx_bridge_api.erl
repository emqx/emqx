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
-module(emqx_bridge_api).

-behaviour(minirest_api).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_utils/include/emqx_utils_api.hrl").
-include_lib("emqx_bridge/include/emqx_bridge.hrl").
-include_lib("emqx_bridge/include/emqx_bridge_proto.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

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
    '/bridges/:id/enable/:enable'/2,
    '/bridges/:id/:operation'/2,
    '/nodes/:node/bridges/:id/:operation'/2,
    '/bridges/:id/metrics'/2,
    '/bridges/:id/metrics/reset'/2,
    '/bridges_probe'/2
]).

-export([lookup_from_local_node/2]).
-export([get_metrics_from_local_node/2]).

%% used by actions/sources schema
-export([mqtt_v1_example/1]).

%% only for testing/mocking
-export([supported_versions/1]).

-export([format_bridge_metrics/1, format_metrics/1]).

-define(BPAPI_NAME, emqx_bridge).

-define(BRIDGE_NOT_ENABLED,
    ?BAD_REQUEST(<<"Forbidden operation, bridge not enabled">>)
).

-define(BRIDGE_NOT_FOUND(BRIDGE_TYPE, BRIDGE_NAME),
    ?NOT_FOUND(
        <<"Bridge lookup failed: bridge named '", (bin(BRIDGE_NAME))/binary, "' of type ",
            (bin(BRIDGE_TYPE))/binary, " does not exist.">>
    )
).

%% Don't turn bridge_name to atom, it's maybe not a existing atom.
-define(TRY_PARSE_ID(ID, EXPR),
    try emqx_bridge_resource:parse_bridge_id(Id, #{atom_name => false}) of
        {BridgeType, BridgeName} ->
            EXPR
    catch
        throw:#{reason := Reason} ->
            ?NOT_FOUND(<<"Invalid bridge ID, ", Reason/binary>>)
    end
).

namespace() -> "bridge".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{
        check_schema => fun emqx_dashboard_swagger:validate_content_type_json/2
    }).

paths() ->
    [
        "/bridges",
        "/bridges/:id",
        "/bridges/:id/enable/:enable",
        "/bridges/:id/:operation",
        "/nodes/:node/bridges/:id/:operation",
        "/bridges/:id/metrics",
        "/bridges/:id/metrics/reset",
        "/bridges_probe"
    ].

error_schema(Code, Message) ->
    error_schema(Code, Message, _ExtraFields = []).

error_schema(Code, Message, ExtraFields) when is_atom(Code) ->
    error_schema([Code], Message, ExtraFields);
error_schema(Codes, Message, ExtraFields) when is_list(Message) ->
    error_schema(Codes, list_to_binary(Message), ExtraFields);
error_schema(Codes, Message, ExtraFields) when is_list(Codes) andalso is_binary(Message) ->
    ExtraFields ++ emqx_dashboard_swagger:error_codes(Codes, Message).

get_response_body_schema() ->
    emqx_dashboard_swagger:schema_with_examples(
        emqx_bridge_schema:get_response(),
        bridge_info_examples(get)
    ).

param_path_operation_cluster() ->
    {operation,
        mk(
            enum([start, stop, restart]),
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
            enum([start, stop, restart]),
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
                example => <<"http:http_example">>,
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

bridge_info_array_example(Method) ->
    lists:map(fun(#{value := Config}) -> Config end, maps:values(bridge_info_examples(Method))).

bridge_info_examples(Method) ->
    maps:merge(
        #{
            <<"http_example">> => #{
                summary => <<"HTTP">>,
                value => info_example(http, Method)
            },
            <<"mqtt_example">> => #{
                summary => <<"MQTT Bridge">>,
                value => mqtt_v1_example(Method)
            }
        },
        emqx_enterprise_bridge_examples(Method)
    ).

-if(?EMQX_RELEASE_EDITION == ee).
emqx_enterprise_bridge_examples(Method) ->
    emqx_bridge_enterprise:examples(Method).
-else.
emqx_enterprise_bridge_examples(_Method) -> #{}.
-endif.

mqtt_v1_example(Method) ->
    info_example(mqtt, Method).

info_example(Type, Method) ->
    maps:merge(
        info_example_basic(Type),
        method_example(Type, Method)
    ).

method_example(Type, Method) when Method == get; Method == post ->
    SType = atom_to_list(Type),
    SName = SType ++ "_example",
    #{
        type => bin(SType),
        name => bin(SName)
    };
method_example(_Type, put) ->
    #{}.

info_example_basic(http) ->
    #{
        enable => true,
        url => <<"http://localhost:9901/messages/${topic}">>,
        request_timeout => <<"15s">>,
        connect_timeout => <<"15s">>,
        max_retries => 3,
        pool_type => <<"random">>,
        pool_size => 4,
        enable_pipelining => 100,
        ssl => #{enable => false},
        local_topic => <<"emqx_http/#">>,
        method => post,
        body => <<"${payload}">>,
        resource_opts => #{
            worker_pool_size => 1,
            health_check_interval => 15000,
            query_mode => async,
            inflight_window => 100,
            max_buffer_bytes => 100 * 1024 * 1024
        }
    };
info_example_basic(mqtt) ->
    (mqtt_main_example())#{
        egress => mqtt_egress_example(),
        ingress => mqtt_ingress_example()
    }.

mqtt_main_example() ->
    #{
        enable => true,
        server => <<"127.0.0.1:1883">>,
        proto_ver => <<"v4">>,
        username => <<"foo">>,
        password => <<"******">>,
        clean_start => true,
        keepalive => <<"300s">>,
        retry_interval => <<"15s">>,
        max_inflight => 100,
        resource_opts => #{
            health_check_interval => <<"15s">>,
            query_mode => sync,
            max_buffer_bytes => 100 * 1024 * 1024
        },
        ssl => #{
            enable => false
        }
    }.
mqtt_egress_example() ->
    #{
        local => #{
            topic => <<"emqx/#">>
        },
        remote => #{
            topic => <<"from_emqx/${topic}">>,
            qos => <<"${qos}">>,
            payload => <<"${payload}">>,
            retain => false
        }
    }.
mqtt_ingress_example() ->
    #{
        remote => #{
            topic => <<"aws/#">>,
            qos => 1
        },
        local => #{
            topic => <<"from_aws/${topic}">>,
            qos => <<"${qos}">>,
            payload => <<"${payload}">>,
            retain => <<"${retain}">>
        }
    }.

schema("/bridges") ->
    #{
        'operationId' => '/bridges',
        get => #{
            tags => [<<"bridges">>],
            summary => <<"List bridges">>,
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
            summary => <<"Create bridge">>,
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
            summary => <<"Get bridge">>,
            description => ?DESC("desc_api3"),
            parameters => [param_path_id()],
            responses => #{
                200 => get_response_body_schema(),
                404 => error_schema('NOT_FOUND', "Bridge not found")
            }
        },
        put => #{
            tags => [<<"bridges">>],
            summary => <<"Update bridge">>,
            description => ?DESC("desc_api4"),
            parameters => [param_path_id()],
            'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                emqx_bridge_schema:put_request(),
                bridge_info_examples(put)
            ),
            responses => #{
                200 => get_response_body_schema(),
                404 => error_schema('NOT_FOUND', "Bridge not found"),
                400 => error_schema('BAD_REQUEST', "Update bridge failed")
            }
        },
        delete => #{
            tags => [<<"bridges">>],
            summary => <<"Delete bridge">>,
            description => ?DESC("desc_api5"),
            parameters => [param_path_id()],
            responses => #{
                204 => <<"Bridge deleted">>,
                400 => error_schema(
                    'BAD_REQUEST',
                    "Cannot delete bridge while active rules are defined for this bridge",
                    [{rules, mk(array(string()), #{desc => "Dependent Rule IDs"})}]
                ),
                404 => error_schema('NOT_FOUND', "Bridge not found"),
                503 => error_schema('SERVICE_UNAVAILABLE', "Service unavailable")
            }
        }
    };
schema("/bridges/:id/metrics") ->
    #{
        'operationId' => '/bridges/:id/metrics',
        get => #{
            tags => [<<"bridges">>],
            summary => <<"Get bridge metrics">>,
            description => ?DESC("desc_bridge_metrics"),
            parameters => [param_path_id()],
            responses => #{
                200 => emqx_bridge_schema:metrics_fields(),
                404 => error_schema('NOT_FOUND', "Bridge not found")
            }
        }
    };
schema("/bridges/:id/metrics/reset") ->
    #{
        'operationId' => '/bridges/:id/metrics/reset',
        put => #{
            tags => [<<"bridges">>],
            summary => <<"Reset bridge metrics">>,
            description => ?DESC("desc_api6"),
            parameters => [param_path_id()],
            responses => #{
                204 => <<"Reset success">>,
                404 => error_schema('NOT_FOUND', "Bridge not found")
            }
        }
    };
schema("/bridges/:id/enable/:enable") ->
    #{
        'operationId' => '/bridges/:id/enable/:enable',
        put =>
            #{
                tags => [<<"bridges">>],
                summary => <<"Enable or disable bridge">>,
                desc => ?DESC("desc_enable_bridge"),
                parameters => [param_path_id(), param_path_enable()],
                responses =>
                    #{
                        204 => <<"Success">>,
                        400 => error_schema('BAD_REQUEST', non_compat_bridge_msg()),
                        404 => error_schema('NOT_FOUND', "Bridge not found or invalid operation"),
                        503 => error_schema('SERVICE_UNAVAILABLE', "Service unavailable")
                    }
            }
    };
schema("/bridges/:id/:operation") ->
    #{
        'operationId' => '/bridges/:id/:operation',
        post => #{
            tags => [<<"bridges">>],
            summary => <<"Stop or restart bridge">>,
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
schema("/nodes/:node/bridges/:id/:operation") ->
    #{
        'operationId' => '/nodes/:node/bridges/:id/:operation',
        post => #{
            tags => [<<"bridges">>],
            summary => <<"Stop/restart bridge">>,
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
                404 => error_schema('NOT_FOUND', "Bridge or node not found or invalid operation"),
                501 => error_schema('NOT_IMPLEMENTED', "Not Implemented"),
                503 => error_schema('SERVICE_UNAVAILABLE', "Service unavailable")
            }
        }
    };
schema("/bridges_probe") ->
    #{
        'operationId' => '/bridges_probe',
        post => #{
            tags => [<<"bridges">>],
            desc => ?DESC("desc_api9"),
            summary => <<"Test creating bridge">>,
            'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                emqx_bridge_schema:post_request(),
                bridge_info_examples(post)
            ),
            responses => #{
                204 => <<"Test bridge OK">>,
                400 => error_schema(['TEST_FAILED'], "bridge test failed")
            }
        }
    }.

'/bridges'(post, #{body := #{<<"type">> := BridgeType0, <<"name">> := BridgeName} = Conf0}) ->
    BridgeType = upgrade_type(BridgeType0),
    case emqx_bridge:lookup(BridgeType, BridgeName) of
        {ok, _} ->
            ?BAD_REQUEST('ALREADY_EXISTS', <<"bridge already exists">>);
        {error, not_bridge_v1_compatible} ->
            ?BAD_REQUEST('ALREADY_EXISTS', non_compat_bridge_msg());
        {error, not_found} ->
            Conf = filter_out_request_body(Conf0),
            create_bridge(BridgeType, BridgeName, Conf)
    end;
'/bridges'(get, _Params) ->
    Nodes = mria:running_nodes(),
    NodeReplies = emqx_bridge_proto_v4:list_bridges_on_nodes(Nodes),
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

'/bridges/:id'(get, #{bindings := #{id := Id}}) ->
    ?TRY_PARSE_ID(
        Id,
        begin
            CompatErrorMsg = non_compat_bridge_msg(),
            case lookup_from_all_nodes(BridgeType, BridgeName, 200) of
                {400, #{code := 'BAD_REQUEST', message := CompatErrorMsg}} ->
                    ?BRIDGE_NOT_FOUND(BridgeType, BridgeName);
                Res ->
                    Res
            end
        end
    );
'/bridges/:id'(put, #{bindings := #{id := Id}, body := Conf0}) ->
    Conf1 = filter_out_request_body(Conf0),
    ?TRY_PARSE_ID(
        Id,
        case emqx_bridge:lookup(BridgeType, BridgeName) of
            {ok, #{raw_config := RawConf}} ->
                %% TODO will the maybe_upgrade step done by emqx_bridge:lookup cause any problems
                Conf = emqx_utils:deobfuscate(Conf1, RawConf),
                update_bridge(BridgeType, BridgeName, Conf);
            {error, not_found} ->
                ?BRIDGE_NOT_FOUND(BridgeType, BridgeName);
            {error, not_bridge_v1_compatible} ->
                ?BAD_REQUEST('ALREADY_EXISTS', non_compat_bridge_msg())
        end
    );
'/bridges/:id'(delete, #{bindings := #{id := Id}, query_string := Qs}) ->
    ?TRY_PARSE_ID(
        Id,
        case emqx_bridge:lookup(BridgeType, BridgeName) of
            {ok, _} ->
                AlsoDelete =
                    case maps:get(<<"also_delete_dep_actions">>, Qs, <<"false">>) of
                        <<"true">> -> [rule_actions, connector];
                        true -> [rule_actions, connector];
                        _ -> [connector]
                    end,
                case emqx_bridge:check_deps_and_remove(BridgeType, BridgeName, AlsoDelete) of
                    ok ->
                        ?NO_CONTENT;
                    {error, #{
                        reason := rules_depending_on_this_bridge,
                        rule_ids := RuleIds
                    }} ->
                        Msg0 = ?ERROR_MSG(
                            'BAD_REQUEST',
                            bin("Cannot delete bridge while active rules are depending on it")
                        ),
                        Msg = Msg0#{rules => RuleIds},
                        {400, Msg};
                    {error, timeout} ->
                        ?SERVICE_UNAVAILABLE(<<"request timeout">>);
                    {error, Reason} ->
                        ?INTERNAL_ERROR(Reason)
                end;
            {error, not_found} ->
                ?BRIDGE_NOT_FOUND(BridgeType, BridgeName);
            {error, not_bridge_v1_compatible} ->
                ?BAD_REQUEST(non_compat_bridge_msg())
        end
    ).

'/bridges/:id/metrics'(get, #{bindings := #{id := Id}}) ->
    ?TRY_PARSE_ID(Id, get_metrics_from_all_nodes(BridgeType, BridgeName)).

'/bridges/:id/metrics/reset'(put, #{bindings := #{id := Id}}) ->
    ?TRY_PARSE_ID(
        Id,
        case emqx_bridge_v2:is_bridge_v2_type(BridgeType) of
            true ->
                try
                    ok = emqx_bridge_v2:bridge_v1_reset_metrics(BridgeType, BridgeName),
                    ?NO_CONTENT
                catch
                    error:Reason ->
                        ?BAD_REQUEST(Reason)
                end;
            false ->
                ok = emqx_bridge_resource:reset_metrics(
                    emqx_bridge_resource:resource_id(BridgeType, BridgeName)
                ),
                ?NO_CONTENT
        end
    ).

'/bridges_probe'(post, Request) ->
    RequestMeta = #{module => ?MODULE, method => post, path => "/bridges_probe"},
    case emqx_dashboard_swagger:filter_check_request_and_translate_body(Request, RequestMeta) of
        {ok, #{body := #{<<"type">> := BridgeType} = Params}} ->
            Params1 = maybe_deobfuscate_bridge_probe(Params),
            Params2 = maps:remove(<<"type">>, Params1),
            ?tp(bridge_v1_api_dry_run, #{params => Params2}),
            case emqx_bridge_resource:create_dry_run(BridgeType, Params2) of
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

maybe_deobfuscate_bridge_probe(#{<<"type">> := BridgeType0, <<"name">> := BridgeName} = Params) ->
    BridgeType = upgrade_type(BridgeType0),
    case emqx_bridge:lookup(BridgeType, BridgeName) of
        {ok, #{raw_config := RawConf}} ->
            %% TODO check if RawConf obtained above is compatible with the commented out code below
            %% RawConf = emqx:get_raw_config([bridges, BridgeType, BridgeName], #{}),
            emqx_utils:deobfuscate(Params, RawConf);
        _ ->
            %% A bridge may be probed before it's created, so not finding it here is fine
            Params
    end;
maybe_deobfuscate_bridge_probe(Params) ->
    Params.

get_metrics_from_all_nodes(BridgeType, BridgeName) ->
    Nodes = mria:running_nodes(),
    Result = do_bpapi_call(all, get_metrics_from_all_nodes, [Nodes, BridgeType, BridgeName]),
    case Result of
        Metrics when is_list(Metrics) ->
            {200, format_bridge_metrics(lists:zip(Nodes, Metrics))};
        {error, Reason} ->
            ?INTERNAL_ERROR(Reason)
    end.

lookup_from_all_nodes(BridgeType, BridgeName, SuccCode) ->
    Nodes = mria:running_nodes(),
    case is_ok(emqx_bridge_proto_v4:lookup_from_all_nodes(Nodes, BridgeType, BridgeName)) of
        {ok, [{ok, _} | _] = Results} ->
            {SuccCode, format_bridge_info([R || {ok, R} <- Results])};
        {ok, [{error, not_found} | _]} ->
            ?BRIDGE_NOT_FOUND(BridgeType, BridgeName);
        {ok, [{error, not_bridge_v1_compatible} | _]} ->
            ?BAD_REQUEST(non_compat_bridge_msg());
        {error, Reason} ->
            ?INTERNAL_ERROR(Reason)
    end.

lookup_from_local_node(ActionType, ActionName) ->
    %% TODO: BUG: shouldn't accept an action type here, only V1 types....
    case emqx_bridge:lookup(ActionType, ActionName) of
        {ok, Res} -> {ok, format_resource(Res, node())};
        Error -> Error
    end.

create_bridge(BridgeType, BridgeName, Conf) ->
    create_or_update_bridge(BridgeType, BridgeName, Conf, 201).

update_bridge(BridgeType, BridgeName, Conf) ->
    case emqx_bridge_v2:is_bridge_v2_type(BridgeType) of
        true ->
            case emqx_bridge_v2:bridge_v1_is_valid(BridgeType, BridgeName) of
                true ->
                    create_or_update_bridge(BridgeType, BridgeName, Conf, 200);
                false ->
                    ?NOT_FOUND(non_compat_bridge_msg())
            end;
        false ->
            create_or_update_bridge(BridgeType, BridgeName, Conf, 200)
    end.

create_or_update_bridge(BridgeType0, BridgeName, Conf, HttpStatusCode) ->
    BridgeType = upgrade_type(BridgeType0),
    case emqx_bridge:create(BridgeType, BridgeName, Conf) of
        {ok, _} ->
            lookup_from_all_nodes(BridgeType, BridgeName, HttpStatusCode);
        {error, {pre_config_update, _HandlerMod, Reason}} when is_map(Reason) ->
            ?BAD_REQUEST(emqx_utils_api:to_json(redact(Reason)));
        {error, Reason} when is_map(Reason) ->
            ?BAD_REQUEST(emqx_utils_api:to_json(redact(Reason)))
    end.

get_metrics_from_local_node(BridgeType0, BridgeName) ->
    BridgeType = upgrade_type(BridgeType0),
    MetricsResult = emqx_bridge:get_metrics(BridgeType, BridgeName),
    format_metrics(MetricsResult).

'/bridges/:id/enable/:enable'(put, #{bindings := #{id := Id, enable := Enable}}) ->
    ?TRY_PARSE_ID(
        Id,
        case enable_func(Enable) of
            invalid ->
                ?NOT_FOUND(<<"Invalid operation">>);
            OperFunc ->
                case emqx_bridge:disable_enable(OperFunc, BridgeType, BridgeName) of
                    {ok, _} ->
                        ?NO_CONTENT;
                    {error, {pre_config_update, _, bridge_not_found}} ->
                        ?BRIDGE_NOT_FOUND(BridgeType, BridgeName);
                    {error, {_, _, timeout}} ->
                        ?SERVICE_UNAVAILABLE(<<"request timeout">>);
                    {error, timeout} ->
                        ?SERVICE_UNAVAILABLE(<<"request timeout">>);
                    {error, not_bridge_v1_compatible} ->
                        ?BAD_REQUEST(non_compat_bridge_msg());
                    {error, bridge_not_found} ->
                        ?BRIDGE_NOT_FOUND(BridgeType, BridgeName);
                    {error, Reason} ->
                        ?INTERNAL_ERROR(Reason)
                end
        end
    ).

'/bridges/:id/:operation'(post, #{
    bindings :=
        #{id := Id, operation := Op}
}) ->
    ?TRY_PARSE_ID(
        Id,
        case operation_to_all_func(Op) of
            invalid ->
                ?NOT_FOUND(<<"Invalid operation: ", Op/binary>>);
            OperFunc ->
                try is_bridge_enabled(BridgeType, BridgeName) of
                    false ->
                        ?BRIDGE_NOT_ENABLED;
                    true ->
                        Nodes = mria:running_nodes(),
                        call_operation(all, OperFunc, [Nodes, BridgeType, BridgeName])
                catch
                    throw:not_found ->
                        ?BRIDGE_NOT_FOUND(BridgeType, BridgeName)
                end
        end
    ).

'/nodes/:node/bridges/:id/:operation'(post, #{
    bindings :=
        #{id := Id, operation := Op, node := Node}
}) ->
    ?TRY_PARSE_ID(
        Id,
        case node_operation_func(Op) of
            invalid ->
                ?NOT_FOUND(<<"Invalid operation: ", Op/binary>>);
            OperFunc ->
                try is_bridge_enabled(BridgeType, BridgeName) of
                    false ->
                        ?BRIDGE_NOT_ENABLED;
                    true ->
                        case emqx_utils:safe_to_existing_atom(Node, utf8) of
                            {ok, TargetNode} ->
                                call_operation(TargetNode, OperFunc, [
                                    TargetNode, BridgeType, BridgeName
                                ]);
                            {error, _} ->
                                ?NOT_FOUND(<<"Invalid node name: ", Node/binary>>)
                        end
                catch
                    throw:not_found ->
                        ?BRIDGE_NOT_FOUND(BridgeType, BridgeName)
                end
        end
    ).

is_bridge_enabled(BridgeType, BridgeName) ->
    case emqx_bridge_v2:is_bridge_v2_type(BridgeType) of
        true -> is_bridge_enabled_v2(BridgeType, BridgeName);
        false -> is_bridge_enabled_v1(BridgeType, BridgeName)
    end.

is_bridge_enabled_v1(BridgeType, BridgeName) ->
    %% we read from the translated config because the defaults are populated here.
    try emqx:get_config([bridges, BridgeType, binary_to_existing_atom(BridgeName)]) of
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

is_bridge_enabled_v2(BridgeV1Type, BridgeName) ->
    BridgeV2Type = emqx_bridge_v2:bridge_v1_type_to_bridge_v2_type(BridgeV1Type),
    try emqx:get_config([actions, BridgeV2Type, binary_to_existing_atom(BridgeName)]) of
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

node_operation_func(<<"restart">>) -> restart_bridge_to_node;
node_operation_func(<<"start">>) -> start_bridge_to_node;
node_operation_func(<<"stop">>) -> stop_bridge_to_node;
node_operation_func(_) -> invalid.

operation_to_all_func(<<"restart">>) -> restart_bridges_to_all_nodes;
operation_to_all_func(<<"start">>) -> start_bridges_to_all_nodes;
operation_to_all_func(<<"stop">>) -> stop_bridges_to_all_nodes;
operation_to_all_func(_) -> invalid.

enable_func(<<"true">>) -> enable;
enable_func(<<"false">>) -> disable;
enable_func(_) -> invalid.

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

format_bridge_metrics(Bridges) ->
    FilteredBridges = lists:filter(
        fun
            ({_Node, Metric}) when is_map(Metric) -> true;
            (_) -> false
        end,
        Bridges
    ),
    NodeMetrics = collect_metrics(FilteredBridges),
    #{
        metrics => aggregate_metrics(NodeMetrics),
        node_metrics => NodeMetrics
    }.

node_status(Bridges) ->
    [maps:with([node, status, status_reason], B) || B <- Bridges].

aggregate_status(AllStatus) ->
    Head = fun([A | _]) -> A end,
    HeadVal = maps:get(status, Head(AllStatus), connecting),
    AllRes = lists:all(fun(#{status := Val}) -> Val == HeadVal end, AllStatus),
    case AllRes of
        true -> HeadVal;
        false -> inconsistent
    end.

collect_metrics(Bridges) ->
    [#{node => Node, metrics => Metrics} || {Node, Metrics} <- Bridges].

aggregate_metrics(AllMetrics) ->
    InitMetrics = ?EMPTY_METRICS_V1,
    lists:foldl(fun aggregate_metrics/2, InitMetrics, AllMetrics).

aggregate_metrics(
    #{
        metrics := ?metrics_v1(
            M1, M2, M3, M4, M5, M6, M7, M8, M9, M10, M11, M12, M13, M14, M15, M16, M17
        )
    },
    ?metrics_v1(
        N1, N2, N3, N4, N5, N6, N7, N8, N9, N10, N11, N12, N13, N14, N15, N16, N17
    )
) ->
    ?METRICS_V1(
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

format_resource(
    #{
        type := ActionType,
        name := BridgeName,
        raw_config := RawConf,
        resource_data := ResourceData
    },
    Node
) ->
    BridgeV1Type = downgrade_type(ActionType, emqx_bridge_lib:get_conf(ActionType, BridgeName)),
    RawConfFull = fill_defaults(BridgeV1Type, RawConf),
    redact(
        maps:merge(
            RawConfFull#{
                type => BridgeV1Type,
                name => maps:get(<<"name">>, RawConf, BridgeName),
                node => Node
            },
            format_resource_data(ResourceData)
        )
    ).

format_resource_data(ResData) ->
    maps:fold(fun format_resource_data/3, #{}, maps:with([status, error], ResData)).

format_resource_data(error, undefined, Result) ->
    Result;
format_resource_data(error, Error, Result) ->
    Result#{status_reason => emqx_utils:readable_error_msg(Error)};
format_resource_data(K, V, Result) ->
    Result#{K => V}.

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
    ?METRICS_V1(
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
    ?METRICS_V1(
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

fill_defaults(Type, RawConf) ->
    PackedConf = pack_bridge_conf(Type, RawConf),
    FullConf = emqx_config:fill_defaults(emqx_bridge_schema, PackedConf, #{}),
    unpack_bridge_conf(Type, FullConf).

pack_bridge_conf(Type, RawConf) ->
    #{<<"bridges">> => #{bin(Type) => #{<<"foo">> => RawConf}}}.

filter_raw_conf(_TypeBin, RawConf) ->
    RawConf.

unpack_bridge_conf(Type, PackedConf) ->
    TypeBin = bin(Type),
    #{<<"bridges">> := Bridges} = PackedConf,
    #{<<"foo">> := RawConf} = maps:get(TypeBin, Bridges),
    filter_raw_conf(TypeBin, RawConf).

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

filter_out_request_body(Conf) ->
    ExtraConfs = [
        <<"id">>,
        <<"type">>,
        <<"name">>,
        <<"status">>,
        <<"status_reason">>,
        <<"node_status">>,
        <<"node_metrics">>,
        <<"metrics">>,
        <<"node">>
    ],
    maps:without(ExtraConfs, Conf).

bin(S) when is_list(S) ->
    list_to_binary(S);
bin(S) when is_atom(S) ->
    atom_to_binary(S, utf8);
bin(S) when is_binary(S) ->
    S.

call_operation(NodeOrAll, OperFunc, Args = [_Nodes, BridgeType, BridgeName]) ->
    case is_ok(do_bpapi_call(NodeOrAll, OperFunc, Args)) of
        Ok when Ok =:= ok; is_tuple(Ok), element(1, Ok) =:= ok ->
            ?NO_CONTENT;
        {error, not_implemented} ->
            %% Should only happen if we call `start` on a node that is
            %% still on an older bpapi version that doesn't support it.
            maybe_try_restart(NodeOrAll, OperFunc, Args);
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
        {error, {unhealthy_target, Message}} ->
            ?BAD_REQUEST(Message);
        {error, Reason} ->
            ?BAD_REQUEST(redact(Reason))
    end.

maybe_try_restart(all, start_bridges_to_all_nodes, Args) ->
    call_operation(all, restart_bridges_to_all_nodes, Args);
maybe_try_restart(Node, start_bridge_to_node, Args) ->
    call_operation(Node, restart_bridge_to_node, Args);
maybe_try_restart(_, _, _) ->
    ?NOT_IMPLEMENTED.

do_bpapi_call(all, Call, Args) ->
    maybe_unwrap(
        do_bpapi_call_vsn(emqx_bpapi:supported_version(?BPAPI_NAME), Call, Args)
    );
do_bpapi_call(Node, Call, Args) ->
    case lists:member(Node, mria:running_nodes()) of
        true ->
            do_bpapi_call_vsn(emqx_bpapi:supported_version(Node, ?BPAPI_NAME), Call, Args);
        false ->
            {error, {node_not_found, Node}}
    end.

do_bpapi_call_vsn(SupportedVersion, Call, Args) ->
    case lists:member(SupportedVersion, ?MODULE:supported_versions(Call)) of
        true ->
            apply(emqx_bridge_proto_v4, Call, Args);
        false ->
            {error, not_implemented}
    end.

maybe_unwrap({error, not_implemented}) ->
    {error, not_implemented};
maybe_unwrap(RpcMulticallResult) ->
    emqx_rpc:unwrap_erpc(RpcMulticallResult).

supported_versions(start_bridge_to_node) ->
    bpapi_version_range(2, ?MAX_SUPPORTED_PROTO_VERSION);
supported_versions(start_bridges_to_all_nodes) ->
    bpapi_version_range(2, ?MAX_SUPPORTED_PROTO_VERSION);
supported_versions(get_metrics_from_all_nodes) ->
    bpapi_version_range(4, ?MAX_SUPPORTED_PROTO_VERSION);
supported_versions(_Call) ->
    bpapi_version_range(1, ?MAX_SUPPORTED_PROTO_VERSION).

%% [From, To] (inclusive on both ends)
bpapi_version_range(From, To) ->
    lists:seq(From, To).

redact(Term) ->
    emqx_utils:redact(Term).

non_compat_bridge_msg() ->
    <<"bridge already exists as non Bridge V1 compatible action">>.

upgrade_type(Type) ->
    emqx_bridge_lib:upgrade_type(Type).

downgrade_type(Type, Conf) ->
    emqx_bridge_lib:downgrade_type(Type, Conf).
