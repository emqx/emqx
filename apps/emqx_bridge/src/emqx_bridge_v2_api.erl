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
    '/bridges_v2'/2,
    '/bridges_v2/:id'/2,
    '/bridges_v2/:id/enable/:enable'/2
    %%,
    %% '/bridges_v2/:id/:operation'/2,
    %% '/nodes/:node/bridges_v2/:id/:operation'/2,
    %% '/bridges_v2_probe'/2
]).

%% BpAPI
-export([lookup_from_local_node/2]).

-define(BRIDGE_NOT_FOUND(BRIDGE_TYPE, BRIDGE_NAME),
    ?NOT_FOUND(
        <<"Bridge lookup failed: bridge named '", (bin(BRIDGE_NAME))/binary, "' of type ",
            (bin(BRIDGE_TYPE))/binary, " does not exist.">>
    )
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

namespace() -> "bridge_v2".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => false}).

paths() ->
    [
        "/bridges_v2",
        "/bridges_v2/:id",
        "/bridges_v2/:id/enable/:enable"
        %%,
        %% "/bridges_v2/:id/:operation",
        %% "/nodes/:node/bridges_v2/:id/:operation",
        %% "/bridges_v2_probe"
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
    maps:merge(
        #{},
        emqx_enterprise_bridge_examples(Method)
    ).

bridge_info_array_example(Method) ->
    lists:map(fun(#{value := Config}) -> Config end, maps:values(bridge_info_examples(Method))).

-if(?EMQX_RELEASE_EDITION == ee).
emqx_enterprise_bridge_examples(Method) ->
    emqx_bridge_v2_enterprise:examples(Method).
-else.
emqx_enterprise_bridgex_examples(_Method) -> #{}.
-endif.

param_path_id() ->
    {id,
        mk(
            binary(),
            #{
                in => path,
                required => true,
                example => <<"webhook:webhook_example">>,
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

schema("/bridges_v2") ->
    #{
        'operationId' => '/bridges_v2',
        get => #{
            tags => [<<"bridges_v2">>],
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
            tags => [<<"bridges_v2">>],
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
schema("/bridges_v2/:id") ->
    #{
        'operationId' => '/bridges_v2/:id',
        get => #{
            tags => [<<"bridges_v2">>],
            summary => <<"Get bridge">>,
            description => ?DESC("desc_api3"),
            parameters => [param_path_id()],
            responses => #{
                200 => get_response_body_schema(),
                404 => error_schema('NOT_FOUND', "Bridge not found")
            }
        },
        put => #{
            tags => [<<"bridges_v2">>],
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
            tags => [<<"bridges_v2">>],
            summary => <<"Delete bridge">>,
            description => ?DESC("desc_api5"),
            parameters => [param_path_id()],
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
schema("/bridges_v2/:id/enable/:enable") ->
    #{
        'operationId' => '/bridges_v2/:id/enable/:enable',
        put =>
            #{
                tags => [<<"bridges_v2">>],
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
    }.
%% schema("/bridges/:id/:operation") ->
%%     #{
%%         'operationId' => '/bridges/:id/:operation',
%%         post => #{
%%             tags => [<<"bridges">>],
%%             summary => <<"Stop, start or restart bridge">>,
%%             description => ?DESC("desc_api7"),
%%             parameters => [
%%                 param_path_id(),
%%                 param_path_operation_cluster()
%%             ],
%%             responses => #{
%%                 204 => <<"Operation success">>,
%%                 400 => error_schema(
%%                     'BAD_REQUEST', "Problem with configuration of external service"
%%                 ),
%%                 404 => error_schema('NOT_FOUND', "Bridge not found or invalid operation"),
%%                 501 => error_schema('NOT_IMPLEMENTED', "Not Implemented"),
%%                 503 => error_schema('SERVICE_UNAVAILABLE', "Service unavailable")
%%             }
%%         }
%%     };
%% schema("/nodes/:node/bridges/:id/:operation") ->
%%     #{
%%         'operationId' => '/nodes/:node/bridges/:id/:operation',
%%         post => #{
%%             tags => [<<"bridges">>],
%%             summary => <<"Stop, start or restart bridge">>,
%%             description => ?DESC("desc_api8"),
%%             parameters => [
%%                 param_path_node(),
%%                 param_path_id(),
%%                 param_path_operation_on_node()
%%             ],
%%             responses => #{
%%                 204 => <<"Operation success">>,
%%                 400 => error_schema(
%%                     'BAD_REQUEST',
%%                     "Problem with configuration of external service or bridge not enabled"
%%                 ),
%%                 404 => error_schema(
%%                     'NOT_FOUND', "Bridge or node not found or invalid operation"
%%                 ),
%%                 501 => error_schema('NOT_IMPLEMENTED', "Not Implemented"),
%%                 503 => error_schema('SERVICE_UNAVAILABLE', "Service unavailable")
%%             }
%%         }
%%     };
%% schema("/bridges_probe") ->
%%     #{
%%         'operationId' => '/bridges_probe',
%%         post => #{
%%             tags => [<<"bridges">>],
%%             desc => ?DESC("desc_api9"),
%%             summary => <<"Test creating bridge">>,
%%             'requestBody' => emqx_dashboard_swagger:schema_with_examples(
%%                 emqx_bridge_schema:post_request(),
%%                 bridge_info_examples(post)
%%             ),
%%             responses => #{
%%                 204 => <<"Test bridge OK">>,
%%                 400 => error_schema(['TEST_FAILED'], "bridge test failed")
%%             }
%%         }
%%     }.

'/bridges_v2'(post, #{body := #{<<"type">> := BridgeType, <<"name">> := BridgeName} = Conf0}) ->
    case emqx_bridge_v2:lookup(BridgeType, BridgeName) of
        {ok, _} ->
            ?BAD_REQUEST('ALREADY_EXISTS', <<"bridge already exists">>);
        {error, bridge_not_found} ->
            Conf = filter_out_request_body(Conf0),
            create_bridge(BridgeType, BridgeName, Conf)
    end;
'/bridges_v2'(get, _Params) ->
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
    end;
'/bridges_v2'(post, _Params) ->
    ?BAD_REQUEST("Bad Request");
'/bridges_v2'(_, _) ->
    ?METHOD_NOT_ALLOWED.

'/bridges_v2/:id'(get, #{bindings := #{id := Id}}) ->
    ?TRY_PARSE_ID(Id, lookup_from_all_nodes(BridgeType, BridgeName, 200));
'/bridges_v2/:id'(put, #{bindings := #{id := Id}, body := Conf0}) ->
    Conf1 = filter_out_request_body(Conf0),
    ?TRY_PARSE_ID(
        Id,
        case emqx_bridge_v2:lookup(BridgeType, BridgeName) of
            {ok, _} ->
                RawConf = emqx:get_raw_config([bridges, BridgeType, BridgeName], #{}),
                Conf = deobfuscate(Conf1, RawConf),
                update_bridge(BridgeType, BridgeName, Conf);
            {error, bridge_not_found} ->
                ?BRIDGE_NOT_FOUND(BridgeType, BridgeName)
        end
    );
'/bridges_v2/:id'(delete, #{bindings := #{id := Id}}) ->
    ?TRY_PARSE_ID(
        Id,
        case emqx_bridge_v2:lookup(BridgeType, BridgeName) of
            {ok, _} ->
                case emqx_bridge_v2:remove(BridgeType, BridgeName) of
                    {ok, _} ->
                        ?NO_CONTENT;
                    {error, {active_channels, Channels}} ->
                        ?BAD_REQUEST(
                            {<<"Cannot delete bridge while there are active channels defined for this bridge">>,
                                Channels}
                        );
                    {error, timeout} ->
                        ?SERVICE_UNAVAILABLE(<<"request timeout">>);
                    {error, Reason} ->
                        ?INTERNAL_ERROR(Reason)
                end;
            {error, bridge_not_found} ->
                ?BRIDGE_NOT_FOUND(BridgeType, BridgeName)
        end
    );
'/bridges_v2/:id'(_, _) ->
    ?METHOD_NOT_ALLOWED.

'/bridges_v2/:id/enable/:enable'(put, #{bindings := #{id := Id, enable := Enable}}) ->
    ?TRY_PARSE_ID(
        Id,
        case enable_func(Enable) of
            invalid ->
                ?NOT_FOUND(<<"Invalid operation">>);
            OperFunc ->
                case emqx_bridge_v2:disable_enable(OperFunc, BridgeType, BridgeName) of
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
        end
    );
'/bridges_v2/:id/enable/:enable'(_, _) ->
    ?METHOD_NOT_ALLOWED.

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
        {ok, [{error, bridge_not_found} | _]} ->
            ?BRIDGE_NOT_FOUND(BridgeType, BridgeName);
        {error, Reason} ->
            ?INTERNAL_ERROR(Reason)
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
    AllRes = lists:all(fun(#{status := Val}) -> Val == HeadVal end, AllStatus),
    case AllRes of
        true -> HeadVal;
        false -> inconsistent
    end.

lookup_from_local_node(BridgeType, BridgeName) ->
    case emqx_bridge_v2:lookup(BridgeType, BridgeName) of
        {ok, Res} -> {ok, format_resource(Res, node())};
        Error -> Error
    end.

%% resource
format_resource(
    #{
        type := Type,
        name := Name,
        raw_config := RawConf,
        resource_data := ResourceData
    },
    Node
) ->
    redact(
        maps:merge(
            RawConf#{
                type => Type,
                name => maps:get(<<"name">>, RawConf, Name),
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

create_bridge(BridgeType, BridgeName, Conf) ->
    create_or_update_bridge(BridgeType, BridgeName, Conf, 201).

update_bridge(BridgeType, BridgeName, Conf) ->
    create_or_update_bridge(BridgeType, BridgeName, Conf, 200).

create_or_update_bridge(BridgeType, BridgeName, Conf, HttpStatusCode) ->
    case emqx_bridge_v2:create(BridgeType, BridgeName, Conf) of
        {ok, _} ->
            lookup_from_all_nodes(BridgeType, BridgeName, HttpStatusCode);
        {error, Reason} when is_map(Reason) ->
            ?BAD_REQUEST(map_to_json(redact(Reason)))
    end.

enable_func(<<"true">>) -> enable;
enable_func(<<"false">>) -> disable;
enable_func(_) -> invalid.

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
