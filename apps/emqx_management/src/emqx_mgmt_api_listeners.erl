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
    stop_listeners_by_id/2,
    start_listeners_by_id/2,
    restart_listeners_by_id/2,
    action_listeners_by_id/2
]).

%% for rpc call
-export([
    do_list_listeners/0
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
        "/listeners/:id/stop",
        "/listeners/:id/start",
        "/listeners/:id/restart"
    ].

schema("/listeners_status") ->
    #{
        'operationId' => listener_type_status,
        get => #{
            tags => [<<"listeners">>],
            desc => <<"List all running node's listeners live status. group by listener type">>,
            responses => #{
                200 =>
                    emqx_dashboard_swagger:schema_with_example(
                        ?ARRAY(?R_REF(listener_type_status)),
                        listener_type_status_example()
                    )
            }
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
                        #{desc => "Listener type", in => query, required => false, example => tcp}
                    )}
            ],
            responses => #{
                200 =>
                    emqx_dashboard_swagger:schema_with_example(
                        ?ARRAY(?R_REF(listener_id_status)),
                        listener_id_status_example()
                    )
            }
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
                200 => listener_schema(#{bind => true}),
                404 => error_codes(['BAD_LISTENER_ID', 'BAD_REQUEST'], ?LISTENER_NOT_FOUND)
            }
        },
        put => #{
            tags => [<<"listeners">>],
            desc => <<"Update the specified listener on all nodes.">>,
            parameters => [?R_REF(listener_id)],
            'requestBody' => listener_schema(#{bind => false}),
            responses => #{
                200 => listener_schema(#{bind => true}),
                400 => error_codes(['BAD_REQUEST']),
                404 => error_codes(['BAD_LISTENER_ID', 'BAD_REQUEST'], ?LISTENER_NOT_FOUND)
            }
        },
        post => #{
            tags => [<<"listeners">>],
            desc => <<"Create the specified listener on all nodes.">>,
            parameters => [?R_REF(listener_id)],
            'requestBody' => listener_schema(#{bind => true}),
            responses => #{
                200 => listener_schema(#{bind => true}),
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
schema("/listeners/:id/start") ->
    #{
        'operationId' => start_listeners_by_id,
        post => #{
            tags => [<<"listeners">>],
            desc => <<"Start the listener on all nodes.">>,
            parameters => [
                ?R_REF(listener_id)
            ],
            responses => #{
                200 => <<"Updated">>,
                400 => error_codes(['BAD_REQUEST', 'BAD_LISTENER_ID'])
            }
        }
    };
schema("/listeners/:id/stop") ->
    #{
        'operationId' => stop_listeners_by_id,
        post => #{
            tags => [<<"listeners">>],
            desc => <<"Stop the listener on all nodes.">>,
            parameters => [
                ?R_REF(listener_id)
            ],
            responses => #{
                200 => <<"Updated">>,
                400 => error_codes(['BAD_REQUEST', 'BAD_LISTENER_ID'])
            }
        }
    };
schema("/listeners/:id/restart") ->
    #{
        'operationId' => restart_listeners_by_id,
        post => #{
            tags => [<<"listeners">>],
            desc => <<"Restart listeners on all nodes.">>,
            parameters => [
                ?R_REF(listener_id)
            ],
            responses => #{
                200 => <<"Updated">>,
                400 => error_codes(['BAD_REQUEST', 'BAD_LISTENER_ID'])
            }
        }
    }.

fields(listener_id) ->
    [
        {id,
            ?HOCON(atom(), #{
                desc => "Listener id",
                example => 'tcp:demo',
                validator => fun validate_id/1,
                required => true,
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
            {number, ?HOCON(typerefl:pos_integer(), #{desc => "ListenerId counter"})},
            {bind,
                ?HOCON(
                    hoconsc:union([emqx_schema:ip_port(), integer()]),
                    #{desc => "Listener bind addr", required => true}
                )},
            {acceptors, ?HOCON(typerefl:pos_integer(), #{desc => "ListenerId acceptors"})},
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
    emqx_dashboard_swagger:schema_with_example(
        ?UNION(lists:map(fun(#{ref := Ref}) -> Ref end, listeners_info(Opts))),
        tcp_schema_example()
    ).

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
            Fields3 = required_bind(Fields1, Opts),
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
    List = lists:map(
        fun({Type, L}) ->
            L1 = maps:without([bind, acceptors], L),
            L1#{type => Type}
        end,
        Listeners
    ),
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
            Path = [listeners, Type, Name],
            case emqx_conf:get_raw(Path, undefined) of
                undefined ->
                    {404, #{code => 'BAD_LISTENER_ID', message => ?LISTENER_NOT_FOUND}};
                PrevConf ->
                    MergeConfT = emqx_map_lib:deep_merge(PrevConf, Conf),
                    MergeConf = emqx_listeners:ensure_override_limiter_conf(MergeConfT, Conf),
                    case update(Path, MergeConf) of
                        {ok, #{raw_config := _RawConf}} ->
                            crud_listeners_by_id(get, #{bindings => #{id => Id}});
                        {error, not_found} ->
                            {404, #{code => 'BAD_LISTENER_ID', message => ?LISTENER_NOT_FOUND}};
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
            Path = [listeners, Type, Name],
            case create(Path, Conf) of
                {ok, #{raw_config := _RawConf}} ->
                    crud_listeners_by_id(get, #{bindings => #{id => Id}});
                {error, already_exist} ->
                    {400, #{code => 'BAD_LISTENER_ID', message => <<"Already Exist">>}};
                {error, Reason} ->
                    {400, #{code => 'BAD_REQUEST', message => err_msg(Reason)}}
            end;
        {error, Reason} ->
            {400, #{code => 'BAD_REQUEST', message => err_msg(Reason)}};
        _ ->
            {400, #{code => 'BAD_LISTENER_ID', message => ?LISTENER_ID_INCONSISTENT}}
    end;
crud_listeners_by_id(delete, #{bindings := #{id := Id}}) ->
    {ok, #{type := Type, name := Name}} = emqx_listeners:parse_listener_id(Id),
    case ensure_remove([listeners, Type, Name]) of
        {ok, _} -> {204};
        {error, Reason} -> {400, #{code => 'BAD_REQUEST', message => err_msg(Reason)}}
    end.

parse_listener_conf(Conf0) ->
    Conf1 = maps:without([<<"running">>, <<"current_connections">>], Conf0),
    {IdBin, Conf2} = maps:take(<<"id">>, Conf1),
    {TypeBin, Conf3} = maps:take(<<"type">>, Conf2),
    {ok, #{type := Type, name := Name}} = emqx_listeners:parse_listener_id(IdBin),
    TypeAtom = binary_to_existing_atom(TypeBin),
    case Type =:= TypeAtom of
        true -> {binary_to_existing_atom(IdBin), TypeAtom, Name, Conf3};
        false -> {error, listener_type_inconsistent}
    end.

stop_listeners_by_id(Method, Body = #{bindings := Bindings}) ->
    action_listeners_by_id(
        Method,
        Body#{bindings := maps:put(action, stop, Bindings)}
    ).
start_listeners_by_id(Method, Body = #{bindings := Bindings}) ->
    action_listeners_by_id(
        Method,
        Body#{bindings := maps:put(action, start, Bindings)}
    ).
restart_listeners_by_id(Method, Body = #{bindings := Bindings}) ->
    action_listeners_by_id(
        Method,
        Body#{bindings := maps:put(action, restart, Bindings)}
    ).

action_listeners_by_id(post, #{bindings := #{id := Id, action := Action}}) ->
    {ok, #{type := Type, name := Name}} = emqx_listeners:parse_listener_id(Id),
    Path = [listeners, Type, Name],
    case emqx_conf:get_raw(Path, undefined) of
        undefined ->
            {404, #{code => 'BAD_LISTENER_ID', message => ?LISTENER_NOT_FOUND}};
        _PrevConf ->
            case action(Path, Action, enabled(Action)) of
                {ok, #{raw_config := _RawConf}} ->
                    {200};
                {error, not_found} ->
                    {404, #{code => 'BAD_LISTENER_ID', message => ?LISTENER_NOT_FOUND}};
                {error, Reason} ->
                    {400, #{code => 'BAD_REQUEST', message => err_msg(Reason)}}
            end
    end.

%%%==============================================================================================

enabled(start) -> #{<<"enabled">> => true};
enabled(stop) -> #{<<"enabled">> => false};
enabled(restart) -> #{<<"enabled">> => true}.

err_msg(Atom) when is_atom(Atom) -> atom_to_binary(Atom);
err_msg(Reason) -> list_to_binary(err_msg_str(Reason)).

err_msg_str(Reason) ->
    io_lib:format("~p", [Reason]).

list_listeners() ->
    [list_listeners(Node) || Node <- mria_mnesia:running_nodes()].

list_listeners(Node) ->
    wrap_rpc(emqx_management_proto_v2:list_listeners(Node)).

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

wrap_rpc({badrpc, Reason}) ->
    {error, Reason};
wrap_rpc(Res) ->
    Res.

format_status(Key, Node, Listener, Acc) ->
    #{
        <<"id">> := Id,
        <<"running">> := Running,
        <<"max_connections">> := MaxConnections,
        <<"current_connections">> := CurrentConnections,
        <<"acceptors">> := Acceptors,
        <<"bind">> := Bind
    } = Listener,
    GroupKey = maps:get(Key, Listener),
    case maps:find(GroupKey, Acc) of
        error ->
            Acc#{
                GroupKey => #{
                    enable => Running,
                    ids => [Id],
                    acceptors => Acceptors,
                    bind => Bind,
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

max_conn(_Int1, <<"infinity">>) -> <<"infinity">>;
max_conn(<<"infinity">>, _Int) -> <<"infinity">>;
max_conn(Int1, Int2) -> Int1 + Int2.

update(Path, Conf) ->
    wrap(emqx_conf:update(Path, {update, Conf}, ?OPTS(cluster))).

action(Path, Action, Conf) ->
    wrap(emqx_conf:update(Path, {action, Action, Conf}, ?OPTS(cluster))).

create(Path, Conf) ->
    wrap(emqx_conf:update(Path, {create, Conf}, ?OPTS(cluster))).

ensure_remove(Path) ->
    wrap(emqx_conf:remove(Path, ?OPTS(cluster))).

wrap({error, {post_config_update, emqx_listeners, Reason}}) -> {error, Reason};
wrap({error, {pre_config_update, emqx_listeners, Reason}}) -> {error, Reason};
wrap({error, Reason}) -> {error, Reason};
wrap(Ok) -> Ok.

listener_type_status_example() ->
    [
        #{
            enable => false,
            ids => ["tcp:demo"],
            node_status => #{
                'emqx@127.0.0.1' => #{
                    current_connections => 11,
                    max_connections => 1024000
                },
                'emqx@127.0.0.2' => #{
                    current_connections => 10,
                    max_connections => 1024000
                }
            },
            status => #{
                current_connections => 21,
                max_connections => 2048000
            },
            type => tcp
        },
        #{
            enable => false,
            ids => ["ssl:default"],
            node_status => #{
                'emqx@127.0.0.1' => #{
                    current_connections => 31,
                    max_connections => infinity
                },
                'emqx@127.0.0.2' => #{
                    current_connections => 40,
                    max_connections => infinity
                }
            },
            status => #{
                current_connections => 71,
                max_connections => infinity
            },
            type => ssl
        }
    ].

listener_id_status_example() ->
    [
        #{
            acceptors => 16,
            bind => <<"0.0.0.0:1884">>,
            enable => true,
            id => <<"tcp:demo">>,
            node_status => #{
                'emqx@127.0.0.1' => #{
                    current_connections => 100,
                    max_connections => 1024000
                },
                'emqx@127.0.0.2' => #{
                    current_connections => 101,
                    max_connections => 1024000
                }
            },
            number => 2,
            status => #{
                current_connections => 201,
                max_connections => 2048000
            }
        },
        #{
            acceptors => 32,
            bind => <<"0.0.0.0:1883">>,
            enable => true,
            id => <<"tcp:default">>,
            node_status => #{
                'emqx@127.0.0.1' => #{
                    current_connections => 300,
                    max_connections => infinity
                },
                'emqx@127.0.0.2' => #{
                    current_connections => 201,
                    max_connections => infinity
                }
            },
            number => 2,
            status => #{
                current_connections => 501,
                max_connections => infinity
            }
        }
    ].

tcp_schema_example() ->
    #{
        acceptors => 16,
        access_rules => ["allow all"],
        bind => <<"0.0.0.0:1884">>,
        current_connections => 10240,
        id => <<"tcp:demo">>,
        max_connections => 204800,
        mountpoint => <<"/">>,
        proxy_protocol => false,
        proxy_protocol_timeout => <<"3s">>,
        running => true,
        tcp_options => #{
            active_n => 100,
            backlog => 1024,
            buffer => <<"4KB">>,
            high_watermark => <<"1MB">>,
            nodelay => false,
            reuseaddr => true,
            send_timeout => <<"15s">>,
            send_timeout_close => true
        },
        type => tcp,
        zone => default
    }.
