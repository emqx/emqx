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

-module(emqx_mgmt_api_listeners).

-behaviour(minirest_api).

-export([api_spec/0]).

-export([ list_listeners/2
        , crud_listeners_by_id/2
        , list_listeners_on_node/2
        , crud_listener_by_id_on_node/2
        , manage_listeners/2
        , jsonable_resp/2
        ]).

-export([format/1]).

-include_lib("emqx/include/emqx.hrl").

-define(NODE_LISTENER_NOT_FOUND, <<"Node name or listener id not found">>).
-define(NODE_NOT_FOUND_OR_DOWN, <<"Node not found or Down">>).
-define(LISTENER_NOT_FOUND, <<"Listener id not found">>).
-define(ADDR_PORT_INUSE, <<"Addr port in use">>).
-define(CONFIG_SCHEMA_ERROR, <<"Config schema error">>).
-define(INVALID_LISTENER_PROTOCOL, <<"Invalid listener type">>).
-define(UPDATE_CONFIG_FAILED, <<"Update configuration failed">>).
-define(OPERATION_FAILED, <<"Operation failed">>).

api_spec() ->
    {
        [
            api_list_listeners(),
            api_list_update_listeners_by_id(),
            api_manage_listeners(),
            api_list_listeners_on_node(),
            api_get_update_listener_by_id_on_node(),
            api_manage_listeners_on_node()
        ],
        []
    }.

-define(TYPES_ATOM, [tcp, ssl, ws, wss, quic]).
req_schema() ->
    Schema = [emqx_mgmt_api_configs:gen_schema(
        emqx:get_raw_config([listeners, T, default], #{}))
     || T <- ?TYPES_ATOM],
    #{'oneOf' => Schema}.

resp_schema() ->
    #{'oneOf' := Schema} = req_schema(),
    AddMetadata = fun(Prop) ->
        Prop#{running => #{type => boolean},
              id => #{type => string},
              node => #{type => string}}
    end,
    Schema1 = [S#{properties => AddMetadata(Prop)}
               || S = #{properties := Prop} <- Schema],
    #{'oneOf' => Schema1}.

api_list_listeners() ->
    Metadata = #{
        get => #{
            description => <<"List listeners from all nodes in the cluster">>,
            responses => #{
                <<"200">> =>
                    emqx_mgmt_util:array_schema(resp_schema(),
                                                <<"List listeners successfully">>)}}},
    {"/listeners", Metadata, list_listeners}.

api_list_update_listeners_by_id() ->
    Metadata = #{
        get => #{
            description => <<"List listeners by a given Id from all nodes in the cluster">>,
            parameters => [param_path_id()],
            responses => #{
                <<"404">> =>
                    emqx_mgmt_util:error_schema(?LISTENER_NOT_FOUND, ['BAD_LISTENER_ID']),
                <<"200">> =>
                    emqx_mgmt_util:array_schema(resp_schema(), <<"List listeners successfully">>)}},
        put => #{
            description =>
                <<"Create or update a listener by a given Id to all nodes in the cluster">>,
            parameters => [param_path_id()],
            'requestBody' => emqx_mgmt_util:schema(req_schema(), <<"Listener Config">>),
            responses => #{
                <<"400">> =>
                    emqx_mgmt_util:error_schema(?UPDATE_CONFIG_FAILED,
                                                ['BAD_LISTENER_ID', 'BAD_CONFIG_SCHEMA']),
                <<"404">> =>
                    emqx_mgmt_util:error_schema(?LISTENER_NOT_FOUND, ['BAD_LISTENER_ID']),
                <<"500">> =>
                    emqx_mgmt_util:error_schema(?OPERATION_FAILED, ['INTERNAL_ERROR']),
                <<"200">> =>
                    emqx_mgmt_util:array_schema(resp_schema(),
                                                <<"Create or update listener successfully">>)}},
        delete => #{
            description => <<"Delete a listener by a given Id to all nodes in the cluster">>,
            parameters => [param_path_id()],
            responses => #{
                <<"404">> =>
                    emqx_mgmt_util:error_schema(?LISTENER_NOT_FOUND, ['BAD_LISTENER_ID']),
                <<"204">> =>
                    emqx_mgmt_util:schema(<<"Delete listener successfully">>)}}
    },
    {"/listeners/:id", Metadata, crud_listeners_by_id}.

api_list_listeners_on_node() ->
    Metadata = #{
        get => #{
            description => <<"List listeners in one node">>,
            parameters => [param_path_node()],
            responses => #{
                <<"404">> =>
                    emqx_mgmt_util:error_schema(?NODE_NOT_FOUND_OR_DOWN, ['RESOURCE_NOT_FOUND']),
                <<"500">> =>
                    emqx_mgmt_util:error_schema(?OPERATION_FAILED, ['INTERNAL_ERROR']),
                <<"200">> =>
                    emqx_mgmt_util:schema(resp_schema(), <<"List listeners successfully">>)}}},
    {"/nodes/:node/listeners", Metadata, list_listeners_on_node}.

api_get_update_listener_by_id_on_node() ->
    Metadata = #{
        get => #{
            description => <<"Get a listener by a given Id on a specific node">>,
            parameters => [param_path_node(), param_path_id()],
            responses => #{
                <<"404">> =>
                    emqx_mgmt_util:error_schema(?NODE_LISTENER_NOT_FOUND,
                        ['BAD_NODE_NAME', 'BAD_LISTENER_ID']),
                <<"200">> =>
                    emqx_mgmt_util:schema(resp_schema(), <<"Get listener successfully">>)}},
        put => #{
            description => <<"Create or update a listener by a given Id on a specific node">>,
            parameters => [param_path_node(), param_path_id()],
            'requestBody' => emqx_mgmt_util:schema(req_schema(), <<"Listener Config">>),
            responses => #{
                <<"400">> =>
                    emqx_mgmt_util:error_schema(?UPDATE_CONFIG_FAILED,
                                                ['BAD_LISTENER_ID', 'BAD_CONFIG_SCHEMA']),
                <<"404">> =>
                    emqx_mgmt_util:error_schema(?NODE_LISTENER_NOT_FOUND,
                        ['BAD_NODE_NAME', 'BAD_LISTENER_ID']),
                <<"500">> =>
                    emqx_mgmt_util:error_schema(?OPERATION_FAILED, ['INTERNAL_ERROR']),
                <<"200">> =>
                    emqx_mgmt_util:schema(resp_schema(), <<"Get listener successfully">>)}},
        delete => #{
            description => <<"Delete a listener by a given Id to all nodes in the cluster">>,
            parameters => [param_path_node(), param_path_id()],
            responses => #{
                <<"404">> =>
                    emqx_mgmt_util:error_schema(?LISTENER_NOT_FOUND, ['BAD_LISTENER_ID']),
                <<"204">> =>
                    emqx_mgmt_util:schema(<<"Delete listener successfully">>)}}
    },
    {"/nodes/:node/listeners/:id", Metadata, crud_listener_by_id_on_node}.

api_manage_listeners() ->
    Metadata = #{
        post => #{
            description => <<"Restart listeners on all nodes in the cluster">>,
            parameters => [
                param_path_id(),
                param_path_operation()],
            responses => #{
                <<"500">> => emqx_mgmt_util:error_schema(?OPERATION_FAILED, ['INTERNAL_ERROR']),
                <<"200">> => emqx_mgmt_util:schema(<<"Operation success">>)}}},
    {"/listeners/:id/operation/:operation", Metadata, manage_listeners}.

api_manage_listeners_on_node() ->
    Metadata = #{
        put => #{
            description => <<"Restart listeners on all nodes in the cluster">>,
            parameters => [
                param_path_node(),
                param_path_id(),
                param_path_operation()],
            responses => #{
                <<"500">> => emqx_mgmt_util:error_schema(?OPERATION_FAILED, ['INTERNAL_ERROR']),
                <<"200">> => emqx_mgmt_util:schema(<<"Operation success">>)}}},
    {"/nodes/:node/listeners/:id/operation/:operation", Metadata, manage_listeners}.

%%%==============================================================================================
%% parameters
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
        schema => #{type => string, example => emqx_listeners:id_example()},
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

%%%==============================================================================================
%% api
list_listeners(get, _Request) ->
    {200, format(emqx_mgmt:list_listeners())}.

crud_listeners_by_id(get, #{bindings := #{id := Id}}) ->
    case [L || L = #{id := Id0} <- emqx_mgmt:list_listeners(),
            atom_to_binary(Id0, latin1) =:= Id] of
        [] ->
            {400, #{code => 'RESOURCE_NOT_FOUND', message => ?LISTENER_NOT_FOUND}};
        Listeners ->
            {200, format(Listeners)}
    end;
crud_listeners_by_id(put, #{bindings := #{id := Id}, body := Conf}) ->
    Results = format(emqx_mgmt:update_listener(Id, Conf)),
    case lists:filter(fun filter_errors/1, Results) of
        [{error, {invalid_listener_id, Id}} | _] ->
            {400, #{code => 'BAD_REQUEST', message => ?INVALID_LISTENER_PROTOCOL}};
        [{error, {emqx_conf_schema, _}} | _] ->
            {400, #{code => 'BAD_REQUEST', message => ?CONFIG_SCHEMA_ERROR}};
        [{error, {eaddrinuse, _}} | _] ->
            {400, #{code => 'BAD_REQUEST', message => ?ADDR_PORT_INUSE}};
        [{error, Reason} | _] ->
            {500, #{code => 'UNKNOWN_ERROR', message => err_msg(Reason)}};
        [] ->
            {200, Results}
    end;

crud_listeners_by_id(delete, #{bindings := #{id := Id}}) ->
    Results = emqx_mgmt:remove_listener(Id),
    case lists:filter(fun filter_errors/1, Results) of
        [] -> {204};
        Errors -> {500, #{code => 'UNKNOW_ERROR', message => err_msg(Errors)}}
    end.

list_listeners_on_node(get, #{bindings := #{node := Node}}) ->
    case emqx_mgmt:list_listeners(atom(Node)) of
        {error, nodedown} ->
            {404, #{code => 'RESOURCE_NOT_FOUND', message => ?NODE_NOT_FOUND_OR_DOWN}};
        {error, Reason} ->
            {500, #{code => 'UNKNOW_ERROR', message => err_msg(Reason)}};
        Listener ->
            {200, format(Listener)}
    end.

crud_listener_by_id_on_node(get, #{bindings := #{id := Id, node := Node}}) ->
    case emqx_mgmt:get_listener(atom(Node), atom(Id)) of
        {error, not_found} ->
            {404, #{code => 'RESOURCE_NOT_FOUND', message => ?NODE_LISTENER_NOT_FOUND}};
        {error, Reason} ->
            {500, #{code => 'UNKNOW_ERROR', message => err_msg(Reason)}};
        Listener ->
            {200, format(Listener)}
    end;
crud_listener_by_id_on_node(put, #{bindings := #{id := Id, node := Node}, body := Conf}) ->
    case emqx_mgmt:update_listener(atom(Node), Id, Conf) of
        {error, nodedown} ->
            {404, #{code => 'RESOURCE_NOT_FOUND', message => ?NODE_NOT_FOUND_OR_DOWN}};
        {error, {invalid_listener_id, _}} ->
            {400, #{code => 'BAD_REQUEST', message => ?INVALID_LISTENER_PROTOCOL}};
        {error, {emqx_conf_schema, _}} ->
            {400, #{code => 'BAD_REQUEST', message => ?CONFIG_SCHEMA_ERROR}};
        {error, {eaddrinuse, _}} ->
            {400, #{code => 'BAD_REQUEST', message => ?ADDR_PORT_INUSE}};
        {error, Reason} ->
            {500, #{code => 'UNKNOW_ERROR', message => err_msg(Reason)}};
        Listener ->
            {200, format(Listener)}
    end;
crud_listener_by_id_on_node(delete, #{bindings := #{id := Id, node := Node}}) ->
    case emqx_mgmt:remove_listener(atom(Node), Id) of
        ok -> {204};
        {error, Reason} -> {500, #{code => 'UNKNOW_ERROR', message => err_msg(Reason)}}
    end.

manage_listeners(_, #{bindings := #{id := Id, operation := Oper, node := Node}}) ->
    {_, Result} = do_manage_listeners(Node, Id, Oper),
    Result;

manage_listeners(_, #{bindings := #{id := Id, operation := Oper}}) ->
    Results = [do_manage_listeners(Node, Id, Oper) || Node <- mria_mnesia:running_nodes()],
    case lists:filter(fun({_, {200}}) -> false; (_) -> true end, Results) of
        [] -> {200};
        Errors -> {500, #{code => 'UNKNOW_ERROR', message => manage_listeners_err(Errors)}}
    end.

%%%==============================================================================================
%% util functions

do_manage_listeners(Node, Id, Oper) ->
    Param = #{node => atom(Node), id => atom(Id)},
    {Node, do_manage_listeners2(Oper, Param)}.

do_manage_listeners2(<<"start">>, Param) ->
    case emqx_mgmt:manage_listener(start_listener, Param) of
        ok -> {200};
        {error, {already_started, _}} -> {200};
        {error, Reason} ->
            {500, #{code => 'UNKNOW_ERROR', message => err_msg(Reason)}}
    end;
do_manage_listeners2(<<"stop">>, Param) ->
    case emqx_mgmt:manage_listener(stop_listener, Param) of
        ok -> {200};
        {error, not_found} -> {200};
        {error, Reason} ->
            {500, #{code => 'UNKNOW_ERROR', message => err_msg(Reason)}}
    end;
do_manage_listeners2(<<"restart">>, Param) ->
    case emqx_mgmt:manage_listener(restart_listener, Param) of
        ok -> {200};
        {error, not_found} -> do_manage_listeners2(<<"start">>, Param);
        {error, Reason} ->
            {500, #{code => 'UNKNOW_ERROR', message => err_msg(Reason)}}
    end.

manage_listeners_err(Errors) ->
    list_to_binary(lists:foldl(fun({Node, Err}, Str) ->
            err_msg_str(#{node => Node, error => Err}) ++ "; " ++ Str
        end, "", Errors)).

format(Listeners) when is_list(Listeners) ->
    [format(Listener) || Listener <- Listeners];

format({error, Reason}) ->
    {error, Reason};

format(#{node := _Node, id := _Id} = Conf) when is_map(Conf) ->
    emqx_map_lib:jsonable_map(Conf#{
            running => trans_running(Conf)
        }, fun ?MODULE:jsonable_resp/2).

trans_running(Conf) ->
    case maps:get(running, Conf) of
        {error, _} ->
            false;
        Running ->
            Running
    end.

filter_errors({error, _}) ->
    true;
filter_errors(_) ->
    false.

jsonable_resp(bind, Port) when is_integer(Port) ->
    {bind, Port};
jsonable_resp(bind, {Addr, Port}) when is_tuple(Addr); is_integer(Port)->
    {bind, inet:ntoa(Addr) ++ ":" ++ integer_to_list(Port)};
jsonable_resp(user_lookup_fun, _) ->
    drop;
jsonable_resp(K, V) ->
    {K, V}.

atom(B) when is_binary(B) -> binary_to_atom(B, utf8);
atom(S) when is_list(S) -> list_to_atom(S);
atom(A) when is_atom(A) -> A.

err_msg(Reason) ->
    list_to_binary(err_msg_str(Reason)).

err_msg_str(Reason) ->
    io_lib:format("~p", [Reason]).
