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

-module(emqx_mgmt_api_listeners).

-behaviour(minirest_api).

-export([namespace/0, api_spec/0, paths/0, schema/1, fields/1]).
-import(emqx_dashboard_swagger, [error_codes/2, error_codes/1]).

-export([
    listener_type_status/2,
    list_listeners/2,
    crud_listeners_by_id/2,
    action_listeners_by_id/2
]).

%% for rpc call
-export([
    do_list_listeners/0,
    do_update_listener/2,
    do_remove_listener/1
]).

-include_lib("emqx/include/emqx.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-define(NODE_LISTENER_NOT_FOUND, <<"Node name or listener id not found">>).
-define(NODE_NOT_FOUND_OR_DOWN, <<"Node not found or Down">>).
-define(LISTENER_NOT_FOUND, <<"Listener id not found">>).
-define(LISTENER_ID_INCONSISTENT, <<"Path and body's listener id not match">>).
-define(ADDR_PORT_INUSE, <<"Addr port in use">>).

-define(OPTS(_OverrideTo_), #{rawconf_with_defaults => true, override_to => _OverrideTo_}).

namespace() -> "listeners".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [
        "/listeners_status",
        "/listeners",
        "/listeners/:id",
        "/listeners/:id/:action"
    ].

schema("/listeners_status") ->
    #{
        'operationId' => listener_type_status,
        get => #{
            tags => [<<"listeners">>],
            desc => <<"List all running node's listeners live status. group by listener type">>,
            responses => #{200 => ?HOCON(?ARRAY(?R_REF(listener_type_status)))}
        }
    };
schema("/listeners") ->
    #{
        'operationId' => list_listeners,
        get => #{
            tags => [<<"listeners">>],
            desc => <<"List all running node's listeners for the specified type.">>,
            parameters => [
                {type,
                    ?HOCON(
                        ?ENUM(listeners_type()),
                        #{desc => "Listener type", in => query, required => false}
                    )}
            ],
            responses => #{200 => ?HOCON(?ARRAY(?R_REF(listener_id_status)))}
        }
    };
schema("/listeners/:id") ->
    #{
        'operationId' => crud_listeners_by_id,
        get => #{
            tags => [<<"listeners">>],
            desc => <<"List all running node's listeners for the specified id.">>,
            parameters => [?R_REF(listener_id)],
            responses => #{
                200 => ?HOCON(listener_schema(#{bind => true})),
                404 => error_codes(['BAD_LISTENER_ID', 'BAD_REQUEST'], ?LISTENER_NOT_FOUND)
            }
        },
        put => #{
            tags => [<<"listeners">>],
            desc => <<"Update the specified listener on all nodes.">>,
            parameters => [?R_REF(listener_id)],
            'requestBody' => ?HOCON(listener_schema(#{bind => false}), #{}),
            responses => #{
                200 => ?HOCON(listener_schema(#{bind => true}), #{}),
                400 => error_codes(['BAD_REQUEST']),
                404 => error_codes(['BAD_LISTENER_ID', 'BAD_REQUEST'], ?LISTENER_NOT_FOUND)
            }
        },
        post => #{
            tags => [<<"listeners">>],
            desc => <<"Create the specified listener on all nodes.">>,
            parameters => [?R_REF(listener_id)],
            'requestBody' => ?HOCON(listener_schema(#{bind => true}), #{}),
            responses => #{
                200 => ?HOCON(listener_schema(#{bind => true}), #{}),
                400 => error_codes(['BAD_LISTENER_ID', 'BAD_REQUEST'])
            }
        },
        delete => #{
            tags => [<<"listeners">>],
            desc => <<"Delete the specified listener on all nodes.">>,
            parameters => [?R_REF(listener_id)],
            responses => #{
                204 => <<"Listener deleted">>,
                400 => error_codes(['BAD_REQUEST'])
            }
        }
    };
schema("/listeners/:id/:action") ->
    #{
        'operationId' => action_listeners_by_id,
        post => #{
            tags => [<<"listeners">>],
            desc => <<"Start/stop/restart listeners on all nodes.">>,
            parameters => [
                ?R_REF(listener_id),
                ?R_REF(action)
            ],
            responses => #{
                200 => <<"Updated">>,
                400 => error_codes(['BAD_REQUEST'])
            }
        }
    }.

fields(listener_id) ->
    [
        {id,
            ?HOCON(atom(), #{
                desc => "Listener id",
                example => 'tcp:default',
                validator => fun validate_id/1,
                in => path
            })}
    ];
fields(action) ->
    [
        {action,
            ?HOCON(?ENUM([start, stop, restart]), #{
                desc => "listener action",
                example => start,
                in => path
            })}
    ];
fields(node) ->
    [
        {"node",
            ?HOCON(atom(), #{
                desc => "Node name",
                example => "emqx@127.0.0.1",
                in => path
            })}
    ];
fields(listener_type_status) ->
    [
        {type, ?HOCON(?ENUM(listeners_type()), #{desc => "Listener type", required => true})},
        {enable, ?HOCON(boolean(), #{desc => "Listener enable", required => true})},
        {ids, ?HOCON(?ARRAY(string()), #{desc => "Listener Ids", required => true})},
        {status, ?HOCON(?R_REF(status))},
        {node_status, ?HOCON(?ARRAY(?R_REF(node_status)))}
    ];
fields(listener_id_status) ->
    fields(listener_id) ++
        [
            {enable, ?HOCON(boolean(), #{desc => "Listener enable", required => true})},
            {number, ?HOCON(typerefl:pos_integer(), #{desc => "ListenerId number"})},
            {status, ?HOCON(?R_REF(status))},
            {node_status, ?HOCON(?ARRAY(?R_REF(node_status)))}
        ];
fields(status) ->
    [
        {max_connections,
            ?HOCON(hoconsc:union([infinity, integer()]), #{desc => "Max connections"})},
        {current_connections, ?HOCON(non_neg_integer(), #{desc => "Current connections"})}
    ];
fields(node_status) ->
    fields(node) ++ fields(status);
fields(Type) ->
    Listeners = listeners_info(#{bind => true}) ++ listeners_info(#{bind => false}),
    [Schema] = [S || #{ref := ?R_REF(_, T), schema := S} <- Listeners, T =:= Type],
    Schema.

listener_schema(Opts) ->
    ?UNION(lists:map(fun(#{ref := Ref}) -> Ref end, listeners_info(Opts))).

listeners_type() ->
    lists:map(
        fun({Type, _}) -> list_to_existing_atom(Type) end,
        hocon_schema:fields(emqx_schema, "listeners")
    ).

listeners_info(Opts) ->
    Listeners = hocon_schema:fields(emqx_schema, "listeners"),
    lists:map(
        fun({Type, #{type := ?MAP(_Name, ?R_REF(Mod, Field))}}) ->
            Fields0 = hocon_schema:fields(Mod, Field),
            Fields1 = lists:keydelete("authentication", 1, Fields0),
            Fields2 = lists:keydelete("limiter", 1, Fields1),
            Fields3 = required_bind(Fields2, Opts),
            Ref = listeners_ref(Type, Opts),
            TypeAtom = list_to_existing_atom(Type),
            #{
                ref => ?R_REF(Ref),
                schema => [
                    {type, ?HOCON(?ENUM([TypeAtom]), #{desc => "Listener type", required => true})},
                    {running, ?HOCON(boolean(), #{desc => "Listener status", required => false})},
                    {id,
                        ?HOCON(atom(), #{
                            desc => "Listener id",
                            required => true,
                            validator => fun validate_id/1
                        })},
                    {current_connections,
                        ?HOCON(
                            non_neg_integer(),
                            #{desc => "Current connections", required => false}
                        )}
                    | Fields3
                ]
            }
        end,
        Listeners
    ).

required_bind(Fields, #{bind := true}) ->
    Fields;
required_bind(Fields, #{bind := false}) ->
    {value, {_, Hocon}, Fields1} = lists:keytake("bind", 1, Fields),
    [{"bind", Hocon#{required => false}} | Fields1].

listeners_ref(Type, #{bind := Bind}) ->
    Suffix =
        case Bind of
            true -> "_required_bind";
            false -> "_not_required_bind"
        end,
    Type ++ Suffix.

validate_id(Id) ->
    case emqx_listeners:parse_listener_id(Id) of
        {error, Reason} -> {error, Reason};
        {ok, _} -> ok
    end.

%% api
listener_type_status(get, _Request) ->
    Listeners = maps:to_list(listener_status_by_type(list_listeners(), #{})),
    List = lists:map(fun({Type, L}) -> L#{type => Type} end, Listeners),
    {200, List}.

list_listeners(get, #{query_string := Query}) ->
    Listeners = list_listeners(),
    NodeL =
        case maps:find(<<"type">>, Query) of
            {ok, Type} -> listener_type_filter(atom_to_binary(Type), Listeners);
            error -> Listeners
        end,
    {200, listener_status_by_id(NodeL)}.

crud_listeners_by_id(get, #{bindings := #{id := Id0}}) ->
    Listeners = [
        Conf#{<<"id">> => Id, <<"type">> => Type}
     || {Id, Type, Conf} <- emqx_listeners:list_raw(),
        Id =:= Id0
    ],
    case Listeners of
        [] -> {404, #{code => 'BAD_LISTENER_ID', message => ?LISTENER_NOT_FOUND}};
        [L] -> {200, L}
    end;
crud_listeners_by_id(put, #{bindings := #{id := Id}, body := Body0}) ->
    case parse_listener_conf(Body0) of
        {Id, Type, Name, Conf} ->
            Key = [listeners, Type, Name],
            case emqx_conf:get_raw(Key, undefined) of
                undefined ->
                    {404, #{code => 'BAD_LISTENER_ID', message => ?LISTENER_NOT_FOUND}};
                PrevConf ->
                    MergeConf = emqx_map_lib:deep_merge(PrevConf, Conf),
                    case emqx_conf:update(Key, MergeConf, ?OPTS(cluster)) of
                        {ok, #{raw_config := _RawConf}} ->
                            crud_listeners_by_id(get, #{bindings => #{id => Id}});
                        {error, Reason} ->
                            {400, #{code => 'BAD_REQUEST', message => err_msg(Reason)}}
                    end
            end;
        {error, Reason} ->
            {400, #{code => 'BAD_REQUEST', message => err_msg(Reason)}};
        _ ->
            {400, #{code => 'BAD_LISTENER_ID', message => ?LISTENER_ID_INCONSISTENT}}
    end;
crud_listeners_by_id(post, #{bindings := #{id := Id}, body := Body0}) ->
    case parse_listener_conf(Body0) of
        {Id, Type, Name, Conf} ->
            Key = [listeners, Type, Name],
            case emqx_conf:get(Key, undefined) of
                undefined ->
                    case emqx_conf:update([listeners, Type, Name], Conf, ?OPTS(cluster)) of
                        {ok, #{raw_config := _RawConf}} ->
                            crud_listeners_by_id(get, #{bindings => #{id => Id}});
                        {error, Reason} ->
                            {400, #{code => 'BAD_REQUEST', message => err_msg(Reason)}}
                    end;
                _ ->
                    {400, #{code => 'BAD_LISTENER_ID', message => <<"Already Exist">>}}
            end;
        {error, Reason} ->
            {400, #{code => 'BAD_REQUEST', message => err_msg(Reason)}};
        _ ->
            {400, #{code => 'BAD_LISTENER_ID', message => ?LISTENER_ID_INCONSISTENT}}
    end;
crud_listeners_by_id(delete, #{bindings := #{id := Id}}) ->
    {ok, #{type := Type, name := Name}} = emqx_listeners:parse_listener_id(Id),
    case emqx_conf:remove([listeners, Type, Name], ?OPTS(cluster)) of
        {ok, _} -> {204};
        {error, Reason} -> {400, #{code => 'BAD_REQUEST', message => err_msg(Reason)}}
    end.

parse_listener_conf(Conf0) ->
    Conf1 = maps:remove(<<"running">>, Conf0),
    Conf2 = maps:remove(<<"current_connections">>, Conf1),
    {IdBin, Conf3} = maps:take(<<"id">>, Conf2),
    {TypeBin, Conf4} = maps:take(<<"type">>, Conf3),
    {ok, #{type := Type, name := Name}} = emqx_listeners:parse_listener_id(IdBin),
    TypeAtom = binary_to_existing_atom(TypeBin),
    case Type =:= TypeAtom of
        true -> {binary_to_existing_atom(IdBin), TypeAtom, Name, Conf4};
        false -> {error, listener_type_inconsistent}
    end.

action_listeners_by_id(post, #{bindings := #{id := Id, action := Action}}) ->
    Results = [action_listeners(Node, Id, Action) || Node <- mria_mnesia:running_nodes()],
    case
        lists:filter(
            fun
                ({_, {200}}) -> false;
                (_) -> true
            end,
            Results
        )
    of
        [] -> {200};
        Errors -> {400, #{code => 'BAD_REQUEST', message => action_listeners_err(Errors)}}
    end.

%%%==============================================================================================

action_listeners(Node, Id, Action) ->
    {Node, do_action_listeners(Action, Node, Id)}.

do_action_listeners(start, Node, Id) ->
    case wrap_rpc(emqx_broker_proto_v1:start_listener(Node, Id)) of
        ok -> {200};
        {error, {already_started, _}} -> {200};
        {error, Reason} -> {400, #{code => 'BAD_REQUEST', message => err_msg(Reason)}}
    end;
do_action_listeners(stop, Node, Id) ->
    case wrap_rpc(emqx_broker_proto_v1:stop_listener(Node, Id)) of
        ok -> {200};
        {error, not_found} -> {200};
        {error, Reason} -> {400, #{code => 'BAD_REQUEST', message => err_msg(Reason)}}
    end;
do_action_listeners(restart, Node, Id) ->
    case wrap_rpc(emqx_broker_proto_v1:restart_listener(Node, Id)) of
        ok -> {200};
        {error, not_found} -> do_action_listeners(start, Node, Id);
        {error, Reason} -> {400, #{code => 'BAD_REQUEST', message => err_msg(Reason)}}
    end.

action_listeners_err(Errors) ->
    list_to_binary(
        lists:foldl(
            fun({Node, Err}, Str) ->
                err_msg_str(#{node => Node, error => Err}) ++ "; " ++ Str
            end,
            "",
            Errors
        )
    ).

err_msg(Atom) when is_atom(Atom) -> atom_to_binary(Atom);
err_msg(Reason) -> list_to_binary(err_msg_str(Reason)).

err_msg_str(Reason) ->
    io_lib:format("~p", [Reason]).

list_listeners() ->
    [list_listeners(Node) || Node <- mria_mnesia:running_nodes()].

list_listeners(Node) ->
    wrap_rpc(emqx_management_proto_v1:list_listeners(Node)).

listener_status_by_id(NodeL) ->
    Listeners = maps:to_list(listener_status_by_id(NodeL, #{})),
    lists:map(
        fun({Id, L}) ->
            L1 = maps:remove(ids, L),
            #{node_status := Nodes} = L1,
            L1#{number => maps:size(Nodes), id => Id}
        end,
        Listeners
    ).

listener_status_by_type([], Acc) ->
    Acc;
listener_status_by_type([NodeL | Rest], Acc) ->
    #{<<"node">> := Node, <<"listeners">> := Listeners} = NodeL,
    Acc1 = lists:foldl(
        fun(L, Acc0) -> format_status(<<"type">>, Node, L, Acc0) end,
        Acc,
        Listeners
    ),
    listener_status_by_type(Rest, Acc1).

listener_status_by_id([], Acc) ->
    Acc;
listener_status_by_id([NodeL | Rest], Acc) ->
    #{<<"node">> := Node, <<"listeners">> := Listeners} = NodeL,
    Acc1 = lists:foldl(
        fun(L, Acc0) -> format_status(<<"id">>, Node, L, Acc0) end,
        Acc,
        Listeners
    ),
    listener_status_by_id(Rest, Acc1).

listener_type_filter(Type0, Listeners) ->
    lists:map(
        fun(Conf = #{<<"listeners">> := Listeners0}) ->
            Conf#{
                <<"listeners">> =>
                    [C || C = #{<<"type">> := Type} <- Listeners0, Type =:= Type0]
            }
        end,
        Listeners
    ).

-spec do_list_listeners() -> map().
do_list_listeners() ->
    Listeners = [
        Conf#{<<"id">> => Id, <<"type">> => Type}
     || {Id, Type, Conf} <- emqx_listeners:list_raw()
    ],
    #{
        <<"node">> => node(),
        <<"listeners">> => Listeners
    }.

-spec do_update_listener(string(), emqx_config:update_request()) ->
    {ok, map()} | {error, _}.
do_update_listener(Id, Config) ->
    {ok, #{type := Type, name := Name}} = emqx_listeners:parse_listener_id(Id),
    case emqx:update_config([listeners, Type, Name], Config, ?OPTS(local)) of
        {ok, #{raw_config := RawConf}} -> {ok, RawConf};
        {error, Reason} -> {error, Reason}
    end.

-spec do_remove_listener(string()) -> ok.
do_remove_listener(Id) ->
    {ok, #{type := Type, name := Name}} = emqx_listeners:parse_listener_id(Id),
    case emqx:remove_config([listeners, Type, Name], ?OPTS(local)) of
        {ok, _} -> ok;
        {error, Reason} -> error(Reason)
    end.

wrap_rpc({badrpc, Reason}) ->
    {error, Reason};
wrap_rpc(Res) ->
    Res.

format_status(Key, Node, Listener, Acc) ->
    #{
        <<"id">> := Id,
        <<"running">> := Running,
        <<"max_connections">> := MaxConnections,
        <<"current_connections">> := CurrentConnections
    } = Listener,
    GroupKey = maps:get(Key, Listener),
    case maps:find(GroupKey, Acc) of
        error ->
            Acc#{
                GroupKey => #{
                    enable => Running,
                    ids => [Id],
                    status => #{
                        max_connections => MaxConnections,
                        current_connections => CurrentConnections
                    },
                    node_status => #{
                        Node => #{
                            max_connections => MaxConnections,
                            current_connections => CurrentConnections
                        }
                    }
                }
            };
        {ok, GroupValue} ->
            #{
                ids := Ids,
                status := #{
                    max_connections := MaxConnections0,
                    current_connections := CurrentConnections0
                },
                node_status := NodeStatus0
            } = GroupValue,
            NodeStatus =
                case maps:find(Node, NodeStatus0) of
                    error ->
                        #{
                            Node => #{
                                max_connections => MaxConnections,
                                current_connections => CurrentConnections
                            }
                        };
                    {ok, #{
                        max_connections := PrevMax,
                        current_connections := PrevCurr
                    }} ->
                        NodeStatus0#{
                            Node => #{
                                max_connections => max_conn(MaxConnections, PrevMax),
                                current_connections => CurrentConnections + PrevCurr
                            }
                        }
                end,
            Acc#{
                GroupKey =>
                    GroupValue#{
                        ids => lists:usort([Id | Ids]),
                        status => #{
                            max_connections => max_conn(MaxConnections0, MaxConnections),
                            current_connections => CurrentConnections0 + CurrentConnections
                        },
                        node_status => NodeStatus
                    }
            }
    end.

max_conn(_Int1, infinity) -> infinity;
max_conn(infinity, _Int) -> infinity;
max_conn(Int1, Int2) -> Int1 + Int2.
