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
-module(emqx_mgmt_api_cluster).

-behaviour(minirest_api).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-export([api_spec/0, fields/1, paths/0, schema/1, namespace/0]).
-export([cluster_info/2, invite_node/2, force_leave/2, join/1]).

namespace() -> "cluster".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [
        "/cluster",
        "/cluster/:node/invite",
        "/cluster/:node/force_leave"
    ].

schema("/cluster") ->
    #{
        'operationId' => cluster_info,
        get => #{
            description => "Get cluster info",
            responses => #{
                200 => [
                    {name, ?HOCON(string(), #{desc => "Cluster name"})},
                    {nodes, ?HOCON(?ARRAY(string()), #{desc => "Node name"})},
                    {self, ?HOCON(string(), #{desc => "Self node name"})}
                ]
            }
        }
    };
schema("/cluster/:node/invite") ->
    #{
        'operationId' => invite_node,
        put => #{
            description => "Invite node to cluster",
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
            description => "Force leave node from cluster",
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
        nodes => mria_mnesia:running_nodes(),
        self => node()
    },
    {200, Info}.

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

error_message(Msg) ->
    iolist_to_binary(io_lib:format("~p", [Msg])).
