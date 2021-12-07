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

-module(emqx_connector_api).

-behaviour(minirest_api).

-include("emqx_connector.hrl").

-include_lib("typerefl/include/types.hrl").

-import(hoconsc, [mk/2, ref/2, array/1, enum/1]).

%% Swagger specs from hocon schema
-export([api_spec/0, paths/0, schema/1, namespace/0]).

%% API callbacks
-export(['/connectors_test'/2, '/connectors'/2, '/connectors/:id'/2]).

-define(CONN_TYPES, [mqtt]).

-define(TRY_PARSE_ID(ID, EXPR),
    try emqx_connector:parse_connector_id(Id) of
        {ConnType, ConnName} ->
            _ = ConnName,
            EXPR
    catch
        error:{invalid_bridge_id, Id0} ->
            {400, #{code => 'INVALID_ID', message => <<"invalid_bridge_id: ", Id0/binary,
                ". Bridge Ids must be of format {type}:{name}">>}}
    end).

namespace() -> "connector".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => false}).

paths() -> ["/connectors_test", "/connectors", "/connectors/:id"].

error_schema(Code, Message) ->
    [ {code, mk(string(), #{example => Code})}
    , {message, mk(string(), #{example => Message})}
    ].

put_request_body_schema() ->
    emqx_dashboard_swagger:schema_with_examples(
        emqx_connector_schema:put_request(), connector_info_examples(put)).

post_request_body_schema() ->
    emqx_dashboard_swagger:schema_with_examples(
        emqx_connector_schema:post_request(), connector_info_examples(post)).

get_response_body_schema() ->
    emqx_dashboard_swagger:schema_with_examples(
        emqx_connector_schema:get_response(), connector_info_examples(get)).

connector_info_array_example(Method) ->
    [Config || #{value := Config} <- maps:values(connector_info_examples(Method))].

connector_info_examples(Method) ->
    lists:foldl(fun(Type, Acc) ->
            SType = atom_to_list(Type),
            maps:merge(Acc, #{
                Type => #{
                    summary => bin(string:uppercase(SType) ++ " Connector"),
                    value => info_example(Type, Method)
                }
            })
        end, #{}, ?CONN_TYPES).

info_example(Type, Method) ->
    maps:merge(info_example_basic(Type),
               method_example(Type, Method)).

method_example(Type, get) ->
    SType = atom_to_list(Type),
    SName = "my_" ++ SType ++ "_connector",
    #{
        id => bin(SType ++ ":" ++ SName),
        type => bin(SType),
        name => bin(SName)
    };
method_example(Type, post) ->
    SType = atom_to_list(Type),
    SName = "my_" ++ SType ++ "_connector",
    #{
        type => bin(SType),
        name => bin(SName)
    };
method_example(_Type, put) ->
    #{}.

info_example_basic(mqtt) ->
    #{
        mode => cluster_shareload,
        server => <<"127.0.0.1:1883">>,
        reconnect_interval => <<"30s">>,
        proto_ver => <<"v4">>,
        username => <<"foo">>,
        password => <<"bar">>,
        clientid => <<"foo">>,
        clean_start => true,
        keepalive => <<"300s">>,
        retry_interval => <<"30s">>,
        max_inflight => 100,
        ssl => #{
            enable => false
        }
    }.

param_path_id() ->
    [{id, mk(binary(), #{in => path, example => <<"mqtt:my_mqtt_connector">>})}].

schema("/connectors_test") ->
    #{
        operationId => '/connectors_test',
        post => #{
            tags => [<<"connectors">>],
            description => <<"Test creating a new connector by given Id <br>"
                             "The ID must be of format '{type}:{name}'">>,
            summary => <<"Test creating connector">>,
            requestBody => post_request_body_schema(),
            responses => #{
                200 => <<"Test connector OK">>,
                400 => error_schema('TEST_FAILED', "connector test failed")
            }
        }
    };

schema("/connectors") ->
    #{
        operationId => '/connectors',
        get => #{
            tags => [<<"connectors">>],
            description => <<"List all connectors">>,
            summary => <<"List connectors">>,
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_example(
                            array(emqx_connector_schema:get_response()),
                            connector_info_array_example(get))
            }
        },
        post => #{
            tags => [<<"connectors">>],
            description => <<"Create a new connector by given Id <br>"
                             "The ID must be of format '{type}:{name}'">>,
            summary => <<"Create connector">>,
            requestBody => post_request_body_schema(),
            responses => #{
                201 => get_response_body_schema(),
                400 => error_schema('ALREADY_EXISTS', "connector already exists")
            }
        }
    };

schema("/connectors/:id") ->
    #{
        operationId => '/connectors/:id',
        get => #{
            tags => [<<"connectors">>],
            description => <<"Get the connector by Id">>,
            summary => <<"Get connector">>,
            parameters => param_path_id(),
            responses => #{
                200 => get_response_body_schema(),
                404 => error_schema('NOT_FOUND', "Connector not found")
            }
        },
        put => #{
            tags => [<<"connectors">>],
            description => <<"Update an existing connector by Id">>,
            summary => <<"Update connector">>,
            parameters => param_path_id(),
            requestBody => put_request_body_schema(),
            responses => #{
                200 => get_response_body_schema(),
                400 => error_schema('UPDATE_FAIL', "Update failed"),
                404 => error_schema('NOT_FOUND', "Connector not found")
            }},
        delete => #{
            tags => [<<"connectors">>],
            description => <<"Delete a connector by Id">>,
            summary => <<"Delete connector">>,
            parameters => param_path_id(),
            responses => #{
                204 => <<"Delete connector successfully">>,
                400 => error_schema('DELETE_FAIL', "Delete failed")
            }}
    }.

'/connectors_test'(post, #{body := #{<<"type">> := ConnType} = Params}) ->
    case emqx_connector:create_dry_run(ConnType, maps:remove(<<"type">>, Params)) of
        ok -> {200};
        {error, Error} ->
            {400, error_msg('BAD_ARG', Error)}
    end.

'/connectors'(get, _Request) ->
    {200, emqx_connector:list()};

'/connectors'(post, #{body := #{<<"type">> := ConnType} = Params}) ->
    ConnName = maps:get(<<"name">>, Params, emqx_misc:gen_id()),
    case emqx_connector:lookup(ConnType, ConnName) of
        {ok, _} ->
            {400, error_msg('ALREADY_EXISTS', <<"connector already exists">>)};
        {error, not_found} ->
            case emqx_connector:update(ConnType, ConnName,
                    maps:without([<<"type">>, <<"name">>], Params)) of
                {ok, #{raw_config := RawConf}} ->
                    {201, RawConf#{<<"id">> =>
                        emqx_connector:connector_id(ConnType, ConnName)}};
                {error, Error} -> {400, error_msg('BAD_ARG', Error)}
            end
    end.

'/connectors/:id'(get, #{bindings := #{id := Id}}) ->
    ?TRY_PARSE_ID(Id,
        case emqx_connector:lookup(ConnType, ConnName) of
            {ok, Conf} -> {200, Conf#{<<"id">> => Id}};
            {error, not_found} ->
                {404, error_msg('NOT_FOUND', <<"connector not found">>)}
        end);

'/connectors/:id'(put, #{bindings := #{id := Id}, body := Params}) ->
    ?TRY_PARSE_ID(Id,
        case emqx_connector:lookup(ConnType, ConnName) of
            {ok, _} ->
                case emqx_connector:update(ConnType, ConnName, Params) of
                    {ok, #{raw_config := RawConf}} -> {200, RawConf#{<<"id">> => Id}};
                    {error, Error} -> {400, error_msg('BAD_ARG', Error)}
                end;
            {error, not_found} ->
                {404, error_msg('NOT_FOUND', <<"connector not found">>)}
        end);

'/connectors/:id'(delete, #{bindings := #{id := Id}}) ->
    ?TRY_PARSE_ID(Id,
        case emqx_connector:lookup(ConnType, ConnName) of
            {ok, _} ->
                case emqx_connector:delete(ConnType, ConnName) of
                    {ok, _} -> {204};
                    {error, Error} -> {400, error_msg('BAD_ARG', Error)}
                end;
            {error, not_found} ->
                {404, error_msg('NOT_FOUND', <<"connector not found">>)}
        end).

error_msg(Code, Msg) when is_binary(Msg) ->
    #{code => Code, message => Msg};
error_msg(Code, Msg) ->
    #{code => Code, message => bin(io_lib:format("~p", [Msg]))}.

bin(S) when is_list(S) ->
    list_to_binary(S).
