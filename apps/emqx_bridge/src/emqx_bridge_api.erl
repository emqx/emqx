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

-export([ list_bridges/2
        , list_local_bridges/1
        , crud_bridges_cluster/2
        , crud_bridges/3
        , manage_bridges/2
        ]).

-define(TYPES, [mqtt]).
-define(BRIDGE(N, T, C), #{<<"id">> => N, <<"type">> => T, <<"config">> => C}).
-define(TRY_PARSE_ID(ID, EXPR),
    try emqx_bridge:parse_bridge_id(Id) of
        {BridgeType, BridgeName} -> EXPR
    catch
        error:{invalid_bridge_id, Id0} ->
            {400, #{code => 102, message => <<"invalid_bridge_id: ", Id0/binary>>}}
    end).

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

resp_schema() ->
    #{'oneOf' := Schema} = req_schema(),
    AddMetadata = fun(Prop) ->
        Prop#{is_connected => #{type => boolean},
              id => #{type => string},
              bridge_type => #{type => string, enum => ?TYPES},
              node => #{type => string}}
    end,
    Schema1 = [S#{properties => AddMetadata(Prop)}
               || S = #{properties := Prop} <- Schema],
    #{'oneOf' => Schema1}.

api_spec() ->
    {bridge_apis(), []}.

bridge_apis() ->
    [list_all_bridges_api(), crud_bridges_apis(), operation_apis()].

list_all_bridges_api() ->
    Metadata = #{
        get => #{
            description => <<"List all created bridges">>,
            responses => #{
                <<"200">> => emqx_mgmt_util:array_schema(resp_schema(),
                    <<"A list of the bridges">>)
            }
        }
    },
    {"/bridges/", Metadata, list_bridges}.

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
            description => <<"Create or update a bridge">>,
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
    {"/bridges/:id", Metadata, crud_bridges_cluster}.

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

list_bridges(get, _Params) ->
    {200, lists:append([list_local_bridges(Node) || Node <- mria_mnesia:running_nodes()])}.

list_local_bridges(Node) when Node =:= node() ->
    [format_resp(Data) || Data <- emqx_bridge:list_bridges()];
list_local_bridges(Node) ->
    rpc_call(Node, list_local_bridges, [Node]).

crud_bridges_cluster(Method, Params) ->
    Results = [crud_bridges(Node, Method, Params) || Node <- mria_mnesia:running_nodes()],
    case lists:filter(fun({200}) -> false; ({200, _}) -> false; (_) -> true end, Results) of
        [] ->
            case Results of
                [{200} | _] -> {200};
                _ -> {200, [Res || {200, Res} <- Results]}
            end;
        Errors ->
            hd(Errors)
    end.

crud_bridges(Node, Method, Params) when Node =/= node() ->
    rpc_call(Node, crud_bridges, [Node, Method, Params]);

crud_bridges(_, get, #{bindings := #{id := Id}}) ->
    ?TRY_PARSE_ID(Id, case emqx_bridge:get_bridge(BridgeType, BridgeName) of
        {ok, Data} -> {200, format_resp(Data)};
        {error, not_found} ->
            {404, #{code => 102, message => <<"not_found: ", Id/binary>>}}
    end);

crud_bridges(_, put, #{bindings := #{id := Id}, body := Conf}) ->
    ?TRY_PARSE_ID(Id,
        case emqx:update_config(emqx_bridge:config_key_path() ++ [BridgeType, BridgeName], Conf,
                #{rawconf_with_defaults => true}) of
            {ok, #{raw_config := RawConf, post_config_update := #{emqx_bridge := Data}}} ->
                {200, format_resp(#{id => Id, raw_config => RawConf, resource_data => Data})};
            {ok, _} -> %% the bridge already exits
                {ok, Data} = emqx_bridge:get_bridge(BridgeType, BridgeName),
                {200, format_resp(Data)};
            {error, Reason} ->
                {500, #{code => 102, message => emqx_resource_api:stringnify(Reason)}}
        end);

crud_bridges(_, delete, #{bindings := #{id := Id}}) ->
    ?TRY_PARSE_ID(Id,
        case emqx:remove_config(emqx_bridge:config_key_path() ++ [BridgeType, BridgeName]) of
            {ok, _} -> {200};
            {error, Reason} ->
                {500, #{code => 102, message => emqx_resource_api:stringnify(Reason)}}
        end).

manage_bridges(post, #{bindings := #{node := Node, id := Id, operation := Op}}) ->
    OperFun =
        fun (<<"start">>) -> start_bridge;
            (<<"stop">>) -> stop_bridge;
            (<<"restart">>) -> restart_bridge
        end,
    ?TRY_PARSE_ID(Id,
        case rpc_call(binary_to_atom(Node, latin1), emqx_bridge, OperFun(Op),
                [BridgeType, BridgeName]) of
            ok -> {200};
            {error, Reason} ->
                {500, #{code => 102, message => emqx_resource_api:stringnify(Reason)}}
        end).

format_resp(#{id := Id, raw_config := RawConf, resource_data := #{mod := Mod, status := Status}}) ->
    IsConnected = fun(started) -> true; (_) -> false end,
    RawConf#{
        id => Id,
        node => node(),
        bridge_type => emqx_bridge:bridge_type(Mod),
        is_connected => IsConnected(Status)
    }.

rpc_call(Node, Fun, Args) ->
    rpc_call(Node, ?MODULE, Fun, Args).

rpc_call(Node, Mod, Fun, Args) when Node =:= node() ->
    apply(Mod, Fun, Args);
rpc_call(Node, Mod, Fun, Args) ->
    case rpc:call(Node, Mod, Fun, Args) of
        {badrpc, Reason} -> {error, Reason};
        Res -> Res
    end.
