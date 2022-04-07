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
    list_listeners/2,
    crud_listeners_by_id/2,
    list_listeners_on_node/2,
    crud_listener_by_id_on_node/2,
    action_listeners_by_id/2,
    action_listeners_by_id_on_node/2
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
        "/listeners",
        "/listeners/:id",
        "/listeners/:id/:action",
        "/nodes/:node/listeners",
        "/nodes/:node/listeners/:id",
        "/nodes/:node/listeners/:id/:action"
    ].

schema("/listeners") ->
    #{
        'operationId' => list_listeners,
        get => #{
            tags => [<<"listeners">>],
            desc => <<"List all running node's listeners.">>,
            responses => #{200 => ?HOCON(?ARRAY(?R_REF(listeners)))}
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
                200 => ?HOCON(?ARRAY(?R_REF(listeners)))
            }
        },
        put => #{
            tags => [<<"listeners">>],
            desc => <<"Create or update the specified listener on all nodes.">>,
            parameters => [?R_REF(listener_id)],
            'requestBody' => ?HOCON(listener_schema(), #{}),
            responses => #{
                200 => ?HOCON(listener_schema(), #{}),
                400 => error_codes(['BAD_LISTENER_ID', 'BAD_REQUEST'], ?LISTENER_NOT_FOUND)
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
    };
schema("/nodes/:node/listeners") ->
    #{
        'operationId' => list_listeners_on_node,
        get => #{
            tags => [<<"listeners">>],
            desc => <<"List all listeners on the specified node.">>,
            parameters => [?R_REF(node)],
            responses => #{
                200 => ?HOCON(?ARRAY(listener_schema())),
                400 => error_codes(['BAD_NODE', 'BAD_REQUEST'], ?NODE_NOT_FOUND_OR_DOWN)
            }
        }
    };
schema("/nodes/:node/listeners/:id") ->
    #{
        'operationId' => crud_listener_by_id_on_node,
        get => #{
            tags => [<<"listeners">>],
            desc => <<"Get the specified listener on the specified node.">>,
            parameters => [
                ?R_REF(listener_id),
                ?R_REF(node)
            ],
            responses => #{
                200 => ?HOCON(listener_schema()),
                400 => error_codes(['BAD_REQUEST']),
                404 => error_codes(['BAD_LISTEN_ID'], ?NODE_LISTENER_NOT_FOUND)
            }
        },
        put => #{
            tags => [<<"listeners">>],
            desc => <<"Create or update the specified listener on the specified node.">>,
            parameters => [
                ?R_REF(listener_id),
                ?R_REF(node)
            ],
            'requestBody' => ?HOCON(listener_schema()),
            responses => #{
                200 => ?HOCON(listener_schema()),
                400 => error_codes(['BAD_REQUEST'])
            }
        },
        delete => #{
            tags => [<<"listeners">>],
            desc => <<"Delete the specified listener on the specified node.">>,
            parameters => [
                ?R_REF(listener_id),
                ?R_REF(node)
            ],
            responses => #{
                204 => <<"Listener deleted">>,
                400 => error_codes(['BAD_REQUEST'])
            }
        }
    };
schema("/nodes/:node/listeners/:id/:action") ->
    #{
        'operationId' => action_listeners_by_id_on_node,
        post => #{
            tags => [<<"listeners">>],
            desc => <<"Start/stop/restart listeners on a specified node.">>,
            parameters => [
                ?R_REF(node),
                ?R_REF(listener_id),
                ?R_REF(action)
            ],
            responses => #{
                200 => <<"Updated">>,
                400 => error_codes(['BAD_REQUEST'])
            }
        }
    }.

fields(listeners) ->
    [
        {"node",
            ?HOCON(atom(), #{
                desc => "Node name",
                example => "emqx@127.0.0.1",
                required => true
            })},
        {"listeners", ?ARRAY(listener_schema())}
    ];
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
fields(Type) ->
    Listeners = listeners_info(),
    [Schema] = [S || #{ref := ?R_REF(_, T), schema := S} <- Listeners, T =:= Type],
    Schema.

listener_schema() ->
    ?UNION(lists:map(fun(#{ref := Ref}) -> Ref end, listeners_info())).

listeners_info() ->
    Listeners = hocon_schema:fields(emqx_schema, "listeners"),
    lists:map(
        fun({Type, #{type := ?MAP(_Name, ?R_REF(Mod, Field))}}) ->
            Fields0 = hocon_schema:fields(Mod, Field),
            Fields1 = lists:keydelete("authentication", 1, Fields0),
            TypeAtom = list_to_existing_atom(Type),
            #{
                ref => ?R_REF(TypeAtom),
                schema => [
                    {type, ?HOCON(?ENUM([TypeAtom]), #{desc => "Listener type", required => true})},
                    {running, ?HOCON(boolean(), #{desc => "Listener status", required => false})},
                    {id,
                        ?HOCON(atom(), #{
                            desc => "Listener id",
                            required => true,
                            validator => fun validate_id/1
                        })}
                    | Fields1
                ]
            }
        end,
        Listeners
    ).

validate_id(Id) ->
    case emqx_listeners:parse_listener_id(Id) of
        {error, Reason} -> {error, Reason};
        {ok, _} -> ok
    end.

%% api
list_listeners(get, _Request) ->
    {200, list_listeners()}.

crud_listeners_by_id(get, #{bindings := #{id := Id}}) ->
    {200, list_listeners_by_id(Id)};
crud_listeners_by_id(put, #{bindings := #{id := Id}, body := Body0}) ->
    case parse_listener_conf(Body0) of
        {Id, Type, Name, Conf} ->
            case emqx_conf:update([listeners, Type, Name], Conf, ?OPTS(cluster)) of
                {ok, #{raw_config := _RawConf}} ->
                    crud_listeners_by_id(get, #{bindings => #{id => Id}});
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
    case emqx_conf:remove([listeners, Type, Name], ?OPTS(cluster)) of
        {ok, _} -> {204};
        {error, Reason} -> {400, #{code => 'BAD_REQUEST', message => err_msg(Reason)}}
    end.

parse_listener_conf(Conf0) ->
    Conf1 = maps:remove(<<"running">>, Conf0),
    {IdBin, Conf2} = maps:take(<<"id">>, Conf1),
    {TypeBin, Conf3} = maps:take(<<"type">>, Conf2),
    {ok, #{type := Type, name := Name}} = emqx_listeners:parse_listener_id(IdBin),
    TypeAtom = binary_to_existing_atom(TypeBin),
    case Type =:= TypeAtom of
        true -> {binary_to_existing_atom(IdBin), TypeAtom, Name, Conf3};
        false -> {error, listener_type_inconsistent}
    end.

list_listeners_on_node(get, #{bindings := #{node := Node}}) ->
    case list_listeners(Node) of
        {error, nodedown} ->
            {400, #{code => 'BAD_NODE', message => ?NODE_NOT_FOUND_OR_DOWN}};
        {error, Reason} ->
            {400, #{code => 'BAD_REQUEST', message => err_msg(Reason)}};
        #{<<"listeners">> := Listener} ->
            {200, Listener}
    end.

crud_listener_by_id_on_node(get, #{bindings := #{id := Id, node := Node}}) ->
    case get_listener(Node, Id) of
        {error, not_found} ->
            {404, #{code => 'BAD_LISTEN_ID', message => ?NODE_LISTENER_NOT_FOUND}};
        {error, Reason} ->
            {400, #{code => 'BAD_REQUEST', message => err_msg(Reason)}};
        Listener ->
            {200, Listener}
    end;
crud_listener_by_id_on_node(put, #{bindings := #{id := Id, node := Node}, body := Body}) ->
    case parse_listener_conf(Body) of
        {error, Reason} ->
            {400, #{code => 'BAD_REQUEST', message => err_msg(Reason)}};
        {Id, Type, _Name, Conf} ->
            case update_listener(Node, Id, Conf) of
                {error, nodedown} ->
                    {400, #{code => 'BAD_REQUEST', message => ?NODE_NOT_FOUND_OR_DOWN}};
                %% TODO
                {error, {eaddrinuse, _}} ->
                    {400, #{code => 'BAD_REQUEST', message => ?ADDR_PORT_INUSE}};
                {error, Reason} ->
                    {400, #{code => 'BAD_REQUEST', message => err_msg(Reason)}};
                {ok, Listener} ->
                    {200, Listener#{<<"id">> => Id, <<"type">> => Type, <<"running">> => true}}
            end;
        _ ->
            {400, #{code => 'BAD_REQUEST', message => ?LISTENER_ID_INCONSISTENT}}
    end;
crud_listener_by_id_on_node(delete, #{bindings := #{id := Id, node := Node}}) ->
    case remove_listener(Node, Id) of
        ok -> {204};
        {error, Reason} -> {400, #{code => 'BAD_REQUEST', message => err_msg(Reason)}}
    end.

action_listeners_by_id_on_node(post,
    #{bindings := #{id := Id, action := Action, node := Node}}) ->
    {_, Result} = action_listeners(Node, Id, Action),
    Result.

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

list_listeners_by_id(Id) ->
    listener_id_filter(Id, list_listeners()).

get_listener(Node, Id) ->
    case listener_id_filter(Id, [list_listeners(Node)]) of
        [#{<<"listeners">> := []}] -> {error, not_found};
        [#{<<"listeners">> := [Listener]}] -> Listener
    end.

listener_id_filter(Id, Listeners) ->
    lists:map(
        fun(Conf = #{<<"listeners">> := Listeners0}) ->
            Conf#{
                <<"listeners">> =>
                    [C || C = #{<<"id">> := Id0} <- Listeners0, Id =:= Id0]
            }
        end,
        Listeners
    ).

update_listener(Node, Id, Config) ->
    wrap_rpc(emqx_management_proto_v1:update_listener(Node, Id, Config)).

remove_listener(Node, Id) ->
    wrap_rpc(emqx_management_proto_v1:remove_listener(Node, Id)).

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
