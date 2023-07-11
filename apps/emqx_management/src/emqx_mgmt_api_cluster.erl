%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_mgmt_api_cluster).

-behaviour(minirest_api).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").

-export([api_spec/0, fields/1, paths/0, schema/1, namespace/0]).
-export([
    cluster_info/2,
    cluster_topology/2,
    invite_node/2,
    force_leave/2,
    join/1,
    connected_replicants/0
]).

namespace() -> "cluster".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [
        "/cluster",
        "/cluster/topology",
        "/cluster/:node/invite",
        "/cluster/:node/force_leave"
    ].

schema("/cluster") ->
    #{
        'operationId' => cluster_info,
        get => #{
            desc => ?DESC(get_cluster_info),
            tags => [<<"Cluster">>],
            responses => #{
                200 => [
                    {name, ?HOCON(string(), #{desc => "Cluster name"})},
                    {nodes, ?HOCON(?ARRAY(string()), #{desc => "Node name"})},
                    {self, ?HOCON(string(), #{desc => "Self node name"})}
                ]
            }
        }
    };
schema("/cluster/topology") ->
    #{
        'operationId' => cluster_topology,
        get => #{
            desc => ?DESC(get_cluster_topology),
            tags => [<<"Cluster">>],
            responses => #{
                200 => ?HOCON(?ARRAY(?REF(core_replicants)), #{desc => <<"Cluster topology">>})
            }
        }
    };
schema("/cluster/:node/invite") ->
    #{
        'operationId' => invite_node,
        put => #{
            desc => ?DESC(invite_node),
            tags => [<<"Cluster">>],
            parameters => [hoconsc:ref(node)],
            responses => #{
                200 => <<"ok">>,
                400 => emqx_dashboard_swagger:error_codes(['BAD_REQUEST'])
            }
        }
    };
schema("/cluster/:node/force_leave") ->
    #{
        'operationId' => force_leave,
        delete => #{
            desc => ?DESC(force_remove_node),
            tags => [<<"Cluster">>],
            parameters => [hoconsc:ref(node)],
            responses => #{
                204 => <<"Delete successfully">>,
                404 => emqx_dashboard_swagger:error_codes(['NOT_FOUND'])
            }
        }
    }.

fields(node) ->
    [
        {node,
            hoconsc:mk(
                binary(),
                #{
                    desc => <<"node name">>,
                    example => <<"emqx2@127.0.0.1">>,
                    in => path,
                    validator => fun validate_node/1
                }
            )}
    ];
fields(replicant_info) ->
    [
        {node,
            ?HOCON(
                atom(),
                #{desc => <<"Replicant node name">>, example => <<"emqx-replicant@127.0.0.2">>}
            )},
        {streams,
            ?HOCON(
                non_neg_integer(),
                #{desc => <<"The number of RLOG (replicated log) streams">>, example => <<"10">>}
            )}
    ];
fields(core_replicants) ->
    [
        {core_node,
            ?HOCON(
                atom(),
                #{desc => <<"Core node name">>, example => <<"emqx-core@127.0.0.1">>}
            )},
        {replicant_nodes, ?HOCON(?ARRAY(?REF(replicant_info)))}
    ].

validate_node(Node) ->
    case string:split(Node, "@", all) of
        [_, _] -> ok;
        _ -> {error, "Bad node name"}
    end.

cluster_info(get, _) ->
    ClusterName = application:get_env(ekka, cluster_name, emqxcl),
    Info = #{
        name => ClusterName,
        nodes => emqx:running_nodes(),
        self => node()
    },
    {200, Info}.

cluster_topology(get, _) ->
    RunningCores = running_cores(),
    {Replicants, BadNodes} = emqx_mgmt_cluster_proto_v2:connected_replicants(RunningCores),
    CoreReplicants = lists:zip(
        lists:filter(
            fun(N) -> not lists:member(N, BadNodes) end,
            RunningCores
        ),
        Replicants
    ),
    Topology = lists:map(
        fun
            ({Core, {badrpc, Reason}}) ->
                ?SLOG(error, #{
                    msg => "failed_to_get_replicant_nodes",
                    core_node => Core,
                    reason => Reason
                }),
                #{core_node => Core, replicant_nodes => []};
            ({Core, Repls}) ->
                #{core_node => Core, replicant_nodes => format_replicants(Repls)}
        end,
        CoreReplicants
    ),
    BadNodes =/= [] andalso ?SLOG(error, #{msg => "rpc_call_failed", bad_nodes => BadNodes}),
    {200, Topology}.

format_replicants(Replicants) ->
    maps:fold(
        fun(K, V, Acc) ->
            [#{node => K, streams => length(V)} | Acc]
        end,
        [],
        maps:groups_from_list(fun({_, N, _}) -> N end, Replicants)
    ).

running_cores() ->
    Running = emqx:running_nodes(),
    lists:filter(fun(C) -> lists:member(C, Running) end, emqx:cluster_nodes(cores)).

invite_node(put, #{bindings := #{node := Node0}}) ->
    Node = ekka_node:parse_name(binary_to_list(Node0)),
    case emqx_mgmt_cluster_proto_v1:invite_node(Node, node()) of
        ok ->
            {200};
        ignore ->
            {400, #{code => 'BAD_REQUEST', message => <<"Can't invite self">>}};
        {badrpc, Error} ->
            {400, #{code => 'BAD_REQUEST', message => error_message(Error)}};
        {error, Error} ->
            {400, #{code => 'BAD_REQUEST', message => error_message(Error)}}
    end.

force_leave(delete, #{bindings := #{node := Node0}}) ->
    Node = ekka_node:parse_name(binary_to_list(Node0)),
    case ekka:force_leave(Node) of
        ok ->
            {204};
        ignore ->
            {400, #{code => 'BAD_REQUEST', message => <<"Can't leave self">>}};
        {error, Error} ->
            {400, #{code => 'BAD_REQUEST', message => error_message(Error)}}
    end.

-spec join(node()) -> ok | ignore | {error, term()}.
join(Node) ->
    ekka:join(Node).

-spec connected_replicants() -> [{atom(), node(), pid()}].
connected_replicants() ->
    mria_status:agents().

error_message(Msg) ->
    iolist_to_binary(io_lib:format("~p", [Msg])).
