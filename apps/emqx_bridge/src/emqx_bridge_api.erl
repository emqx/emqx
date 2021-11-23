%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-export([api_spec/0]).

-export([ list_create_bridges_in_cluster/2
        , list_local_bridges/1
        , crud_bridges_in_cluster/2
        , manage_bridges/2
        , lookup_from_local_node/2
        ]).

-define(TYPES, [mqtt, http]).
-define(TRY_PARSE_ID(ID, EXPR),
    try emqx_bridge:parse_bridge_id(Id) of
        {BridgeType, BridgeName} -> EXPR
    catch
        error:{invalid_bridge_id, Id0} ->
            {400, #{code => 'INVALID_ID', message => <<"invalid_bridge_id: ", Id0/binary,
                ". Bridge Ids must be of format <bridge_type>:<name>">>}}
    end).

-define(METRICS(MATCH, SUCC, FAILED, RATE, RATE_5, RATE_MAX),
    #{  matched => MATCH,
        success => SUCC,
        failed => FAILED,
        speed => RATE,
        speed_last5m => RATE_5,
        speed_max => RATE_MAX
    }).
-define(metrics(MATCH, SUCC, FAILED, RATE, RATE_5, RATE_MAX),
    #{  matched := MATCH,
        success := SUCC,
        failed := FAILED,
        speed := RATE,
        speed_last5m := RATE_5,
        speed_max := RATE_MAX
    }).

req_schema() ->
    Schema = [
        case maps:to_list(emqx:get_raw_config([bridges, T], #{})) of
            %% the bridge is not configured, so we have no method to get the schema
            [] -> #{};
            [{_K, Conf} | _] ->
                emqx_mgmt_api_configs:gen_schema(Conf)
        end
     || T <- ?TYPES],
    #{'oneOf' => Schema}.

node_schema() ->
    #{type => string, example => "emqx@127.0.0.1"}.

status_schema() ->
    #{type => string, enum => [connected, disconnected]}.

metrics_schema() ->
    #{ type => object
     , properties => #{
           matched => #{type => integer, example => "0"},
           success => #{type => integer, example => "0"},
           failed => #{type => integer, example => "0"},
           speed => #{type => number, format => float, example => "0.0"},
           speed_last5m => #{type => number, format => float, example => "0.0"},
           speed_max => #{type => number, format => float, example => "0.0"}
       }
    }.

per_node_schema(Key, Schema) ->
    #{
        type => array,
        items => #{
            type => object,
            properties => #{
                node => node_schema(),
                Key => Schema
            }
        }
    }.

resp_schema() ->
    AddMetadata = fun(Prop) ->
        Prop#{status => status_schema(),
              node_status => per_node_schema(status, status_schema()),
              metrics => metrics_schema(),
              node_metrics => per_node_schema(metrics, metrics_schema()),
              id => #{type => string, example => "http:my_http_bridge"},
              bridge_type => #{type => string, enum => ?TYPES},
              node => node_schema()
            }
    end,
    more_props_resp_schema(AddMetadata).

more_props_resp_schema(AddMetadata) ->
    #{'oneOf' := Schema} = req_schema(),
    Schema1 = [S#{properties => AddMetadata(Prop)}
               || S = #{properties := Prop} <- Schema],
    #{'oneOf' => Schema1}.

api_spec() ->
    {bridge_apis(), []}.

bridge_apis() ->
    [list_all_bridges_api(), crud_bridges_apis(), operation_apis()].

list_all_bridges_api() ->
    ReqSchema = more_props_resp_schema(fun(Prop) ->
        Prop#{id => #{type => string, required => true}}
    end),
    RespSchema = resp_schema(),
    Metadata = #{
        get => #{
            description => <<"List all created bridges">>,
            responses => #{
                <<"200">> => emqx_mgmt_util:array_schema(resp_schema(),
                    <<"A list of the bridges">>)
            }
        },
        post => #{
            description => <<"Create a new bridge">>,
            'requestBody' => emqx_mgmt_util:schema(ReqSchema),
            responses => #{
                <<"201">> => emqx_mgmt_util:schema(RespSchema, <<"Bridge created">>),
                <<"400">> => emqx_mgmt_util:error_schema(<<"Create bridge failed">>,
                    ['UPDATE_FAILED'])
            }
        }
    },
    {"/bridges/", Metadata, list_create_bridges_in_cluster}.

crud_bridges_apis() ->
    ReqSchema = req_schema(),
    RespSchema = resp_schema(),
    Metadata = #{
        get => #{
            description => <<"Get a bridge by Id">>,
            parameters => [param_path_id()],
            responses => #{
                <<"200">> => emqx_mgmt_util:array_schema(RespSchema,
                    <<"The details of the bridge">>),
                <<"404">> => emqx_mgmt_util:error_schema(<<"Bridge not found">>, ['NOT_FOUND'])
            }
        },
        put => #{
            description => <<"Update a bridge">>,
            parameters => [param_path_id()],
            'requestBody' => emqx_mgmt_util:schema(ReqSchema),
            responses => #{
                <<"200">> => emqx_mgmt_util:array_schema(RespSchema, <<"Bridge updated">>),
                <<"400">> => emqx_mgmt_util:error_schema(<<"Update bridge failed">>,
                    ['UPDATE_FAILED'])
            }
        },
        delete => #{
            description => <<"Delete a bridge">>,
            parameters => [param_path_id()],
            responses => #{
                <<"204">> => emqx_mgmt_util:schema(<<"Bridge deleted">>),
                <<"404">> => emqx_mgmt_util:error_schema(<<"Bridge not found">>, ['NOT_FOUND'])
            }
        }
    },
    {"/bridges/:id", Metadata, crud_bridges_in_cluster}.

operation_apis() ->
    Metadata = #{
        post => #{
            description => <<"Start/Stop/Restart bridges on a specific node">>,
            parameters => [
                param_path_node(),
                param_path_id(),
                param_path_operation()],
            responses => #{
                <<"500">> => emqx_mgmt_util:error_schema(<<"Operation Failed">>,
                                                         ['INTERNAL_ERROR']),
                <<"200">> => emqx_mgmt_util:schema(<<"Operation success">>)}}},
    {"/nodes/:node/bridges/:id/operation/:operation", Metadata, manage_bridges}.

param_path_node() ->
    #{
        name => node,
        in => path,
        schema => #{type => string},
        required => true,
        example => node()
    }.

param_path_id() ->
    #{
        name => id,
        in => path,
        schema => #{type => string},
        required => true
    }.

param_path_operation()->
    #{
        name => operation,
        in => path,
        required => true,
        schema => #{
            type => string,
            enum => [start, stop, restart]},
        example => restart
    }.

list_create_bridges_in_cluster(post, #{body := #{<<"id">> := Id} = Conf}) ->
    ?TRY_PARSE_ID(Id,
        case emqx_bridge:lookup(BridgeType, BridgeName) of
            {ok, _} -> {400, #{code => 'ALREADY_EXISTS', message => <<"bridge already exists">>}};
            {error, not_found} ->
                case ensure_bridge(BridgeType, BridgeName, maps:remove(<<"id">>, Conf)) of
                    ok -> lookup_from_all_nodes(Id, BridgeType, BridgeName, 201);
                    {error, Error} -> {400, Error}
                end
        end);
list_create_bridges_in_cluster(get, _Params) ->
    {200, zip_bridges([list_local_bridges(Node) || Node <- mria_mnesia:running_nodes()])}.

list_local_bridges(Node) when Node =:= node() ->
    [format_resp(Data) || Data <- emqx_bridge:list()];
list_local_bridges(Node) ->
    rpc_call(Node, list_local_bridges, [Node]).

crud_bridges_in_cluster(get, #{bindings := #{id := Id}}) ->
    ?TRY_PARSE_ID(Id, lookup_from_all_nodes(Id, BridgeType, BridgeName, 200));

crud_bridges_in_cluster(put, #{bindings := #{id := Id}, body := Conf}) ->
    ?TRY_PARSE_ID(Id,
        case emqx_bridge:lookup(BridgeType, BridgeName) of
            {ok, _} ->
                case ensure_bridge(BridgeType, BridgeName, Conf) of
                    ok -> lookup_from_all_nodes(Id, BridgeType, BridgeName, 200);
                    {error, Error} -> {400, Error}
                end;
            {error, not_found} ->
                {404, #{code => 'NOT_FOUND', message => <<"bridge not found">>}}
        end);

crud_bridges_in_cluster(delete, #{bindings := #{id := Id}}) ->
    ?TRY_PARSE_ID(Id,
        case emqx_conf:remove(emqx_bridge:config_key_path() ++ [BridgeType, BridgeName],
                #{override_to => cluster}) of
            {ok, _} -> {204};
            {error, Reason} ->
                {500, #{code => 102, message => emqx_resource_api:stringify(Reason)}}
        end).

lookup_from_all_nodes(Id, BridgeType, BridgeName, SuccCode) ->
    case rpc_multicall(lookup_from_local_node, [BridgeType, BridgeName]) of
        {ok, [{ok, _} | _] = Results} ->
            {SuccCode, format_bridge_info([R || {ok, R} <- Results])};
        {ok, [{error, not_found} | _]} ->
            {404, error_msg('NOT_FOUND', <<"not_found: ", Id/binary>>)};
        {error, ErrL} ->
            {500, error_msg('UNKNOWN_ERROR', ErrL)}
    end.

lookup_from_local_node(BridgeType, BridgeName) ->
    case emqx_bridge:lookup(BridgeType, BridgeName) of
        {ok, Res} -> {ok, format_resp(Res)};
        Error -> Error
    end.

manage_bridges(post, #{bindings := #{node := Node, id := Id, operation := Op}}) ->
    OperFun =
        fun (<<"start">>) -> start;
            (<<"stop">>) -> stop;
            (<<"restart">>) -> restart
        end,
    ?TRY_PARSE_ID(Id,
        case rpc_call(binary_to_atom(Node, latin1), emqx_bridge, OperFun(Op),
                [BridgeType, BridgeName]) of
            ok -> {200};
            {error, Reason} ->
                {500, #{code => 102, message => emqx_resource_api:stringify(Reason)}}
        end).

ensure_bridge(BridgeType, BridgeName, Conf) ->
    case emqx_conf:update(emqx_bridge:config_key_path() ++ [BridgeType, BridgeName], Conf,
            #{override_to => cluster}) of
        {ok, _} -> ok;
        {error, Reason} ->
            {error, error_msg('BAD_ARG', Reason)}
    end.

zip_bridges([BridgesFirstNode | _] = BridgesAllNodes) ->
    lists:foldl(fun(#{id := Id}, Acc) ->
            Bridges = pick_bridges_by_id(Id, BridgesAllNodes),
            [format_bridge_info(Bridges) | Acc]
        end, [], BridgesFirstNode).

pick_bridges_by_id(Id, BridgesAllNodes) ->
    lists:foldl(fun(BridgesOneNode, Acc) ->
            [BridgeInfo] = [Bridge || Bridge = #{id := Id0} <- BridgesOneNode, Id0 == Id],
            [BridgeInfo | Acc]
        end, [], BridgesAllNodes).

format_bridge_info([FirstBridge | _] = Bridges) ->
    Res = maps:remove(node, FirstBridge),
    NodeStatus = collect_status(Bridges),
    NodeMetrics = collect_metrics(Bridges),
    Res#{ status => aggregate_status(NodeStatus)
        , node_status => NodeStatus
        , metrics => aggregate_metrics(NodeMetrics)
        , node_metrics => NodeMetrics
        }.

collect_status(Bridges) ->
    [maps:with([node, status], B) || B <- Bridges].

aggregate_status(AllStatus) ->
    AllConnected = lists:all(fun (#{status := connected}) -> true;
                                 (_) -> false
                             end, AllStatus),
    case AllConnected of
        true -> connected;
        false -> disconnected
    end.

collect_metrics(Bridges) ->
    [maps:with([node, metrics], B) || B <- Bridges].

aggregate_metrics(AllMetrics) ->
    InitMetrics = ?METRICS(0,0,0,0,0,0),
    lists:foldl(fun(#{metrics := ?metrics(Match1, Succ1, Failed1, Rate1, Rate5m1, RateMax1)},
                    ?metrics(Match0, Succ0, Failed0, Rate0, Rate5m0, RateMax0)) ->
            ?METRICS(Match1 + Match0, Succ1 + Succ0, Failed1 + Failed0,
                     Rate1 + Rate0, Rate5m1 + Rate5m0, RateMax1 + RateMax0)
        end, InitMetrics, AllMetrics).

format_resp(#{id := Id, raw_config := RawConf,
              resource_data := #{mod := Mod, status := Status, metrics := Metrics}}) ->
    IsConnected = fun(started) -> connected; (_) -> disconnected end,
    RawConf#{
        id => Id,
        node => node(),
        bridge_type => emqx_bridge:bridge_type(Mod),
        status => IsConnected(Status),
        metrics => Metrics
    }.

rpc_multicall(Func, Args) ->
    Nodes = mria_mnesia:running_nodes(),
    ResL = erpc:multicall(Nodes, ?MODULE, Func, Args, 15000),
    case lists:filter(fun({ok, _}) -> false; (_) -> true end, ResL) of
        [] -> {ok, [Res || {ok, Res} <- ResL]};
        ErrL -> {error, ErrL}
    end.

rpc_call(Node, Fun, Args) ->
    rpc_call(Node, ?MODULE, Fun, Args).

rpc_call(Node, Mod, Fun, Args) when Node =:= node() ->
    apply(Mod, Fun, Args);
rpc_call(Node, Mod, Fun, Args) ->
    case rpc:call(Node, Mod, Fun, Args) of
        {badrpc, Reason} -> {error, Reason};
        Res -> Res
    end.

error_msg(Code, Msg) when is_binary(Msg) ->
    #{code => Code, message => Msg};
error_msg(Code, Msg) ->
    #{code => Code, message => list_to_binary(io_lib:format("~p", [Msg]))}.
