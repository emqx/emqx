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

-module(emqx_connector_api).

-behaviour(minirest_api).

-include("emqx_connector.hrl").

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

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
        error:{invalid_connector_id, Id0} ->
            {400, #{
                code => 'INVALID_ID',
                message =>
                    <<"invalid_connector_id: ", Id0/binary,
                        ". Connector Ids must be of format {type}:{name}">>
            }}
    end
).

namespace() -> "connector".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => false}).

paths() -> ["/connectors_test", "/connectors", "/connectors/:id"].

error_schema(Codes, Message) when is_list(Message) ->
    error_schema(Codes, list_to_binary(Message));
error_schema(Codes, Message) when is_binary(Message) ->
    emqx_dashboard_swagger:error_codes(Codes, Message).

put_request_body_schema() ->
    emqx_dashboard_swagger:schema_with_examples(
        emqx_connector_schema:put_request(), connector_info_examples(put)
    ).

post_request_body_schema() ->
    emqx_dashboard_swagger:schema_with_examples(
        emqx_connector_schema:post_request(), connector_info_examples(post)
    ).

get_response_body_schema() ->
    emqx_dashboard_swagger:schema_with_examples(
        emqx_connector_schema:get_response(), connector_info_examples(get)
    ).

connector_info_array_example(Method) ->
    [Config || #{value := Config} <- maps:values(connector_info_examples(Method))].

connector_info_examples(Method) ->
    lists:foldl(
        fun(Type, Acc) ->
            SType = atom_to_list(Type),
            maps:merge(Acc, #{
                Type => #{
                    summary => bin(string:uppercase(SType) ++ " Connector"),
                    value => info_example(Type, Method)
                }
            })
        end,
        #{},
        ?CONN_TYPES
    ).

info_example(Type, Method) ->
    maps:merge(
        info_example_basic(Type),
        method_example(Type, Method)
    ).

method_example(Type, Method) when Method == get; Method == post ->
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
        reconnect_interval => <<"15s">>,
        proto_ver => <<"v4">>,
        username => <<"foo">>,
        password => <<"bar">>,
        clientid => <<"foo">>,
        clean_start => true,
        keepalive => <<"300s">>,
        retry_interval => <<"15s">>,
        max_inflight => 100,
        ssl => #{
            enable => false
        }
    }.

param_path_id() ->
    [
        {id,
            mk(
                binary(),
                #{
                    in => path,
                    example => <<"mqtt:my_mqtt_connector">>,
                    desc => ?DESC("id")
                }
            )}
    ].

schema("/connectors_test") ->
    #{
        'operationId' => '/connectors_test',
        post => #{
            tags => [<<"connectors">>],
            desc => ?DESC("conn_test_post"),
            summary => <<"Test creating connector">>,
            'requestBody' => post_request_body_schema(),
            responses => #{
                204 => <<"Test connector OK">>,
                400 => error_schema(['TEST_FAILED'], "connector test failed")
            }
        }
    };
schema("/connectors") ->
    #{
        'operationId' => '/connectors',
        get => #{
            tags => [<<"connectors">>],
            desc => ?DESC("conn_get"),
            summary => <<"List connectors">>,
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_example(
                    array(emqx_connector_schema:get_response()),
                    connector_info_array_example(get)
                )
            }
        },
        post => #{
            tags => [<<"connectors">>],
            desc => ?DESC("conn_post"),
            summary => <<"Create connector">>,
            'requestBody' => post_request_body_schema(),
            responses => #{
                201 => get_response_body_schema(),
                400 => error_schema(['ALREADY_EXISTS'], "connector already exists")
            }
        }
    };
schema("/connectors/:id") ->
    #{
        'operationId' => '/connectors/:id',
        get => #{
            tags => [<<"connectors">>],
            desc => ?DESC("conn_id_get"),
            summary => <<"Get connector">>,
            parameters => param_path_id(),
            responses => #{
                200 => get_response_body_schema(),
                404 => error_schema(['NOT_FOUND'], "Connector not found"),
                400 => error_schema(['INVALID_ID'], "Bad connector ID")
            }
        },
        put => #{
            tags => [<<"connectors">>],
            desc => ?DESC("conn_id_put"),
            summary => <<"Update connector">>,
            parameters => param_path_id(),
            'requestBody' => put_request_body_schema(),
            responses => #{
                200 => get_response_body_schema(),
                404 => error_schema(['NOT_FOUND'], "Connector not found"),
                400 => error_schema(['INVALID_ID'], "Bad connector ID")
            }
        },
        delete => #{
            tags => [<<"connectors">>],
            desc => ?DESC("conn_id_delete"),
            summary => <<"Delete connector">>,
            parameters => param_path_id(),
            responses => #{
                204 => <<"Delete connector successfully">>,
                403 => error_schema(['DEPENDENCY_EXISTS'], "Cannot remove dependent connector"),
                404 => error_schema(['NOT_FOUND'], "Delete failed, not found"),
                400 => error_schema(['INVALID_ID'], "Bad connector ID")
            }
        }
    }.

'/connectors_test'(post, #{body := #{<<"type">> := ConnType} = Params}) ->
    case emqx_connector:create_dry_run(ConnType, maps:remove(<<"type">>, Params)) of
        ok ->
            {204};
        {error, Error} ->
            {400, error_msg(['TEST_FAILED'], Error)}
    end.

'/connectors'(get, _Request) ->
    {200, [format_resp(Conn) || Conn <- emqx_connector:list_raw()]};
'/connectors'(post, #{body := #{<<"type">> := ConnType, <<"name">> := ConnName} = Params}) ->
    case emqx_connector:lookup_raw(ConnType, ConnName) of
        {ok, _} ->
            {400, error_msg('ALREADY_EXISTS', <<"connector already exists">>)};
        {error, not_found} ->
            case
                emqx_connector:update(
                    ConnType,
                    ConnName,
                    filter_out_request_body(Params)
                )
            of
                {ok, #{raw_config := RawConf}} ->
                    {201,
                        format_resp(RawConf#{
                            <<"type">> => ConnType,
                            <<"name">> => ConnName
                        })};
                {error, Error} ->
                    {400, error_msg('BAD_REQUEST', Error)}
            end
    end;
'/connectors'(post, _) ->
    {400, error_msg('BAD_REQUEST', <<"missing some required fields: [name, type]">>)}.

'/connectors/:id'(get, #{bindings := #{id := Id}}) ->
    ?TRY_PARSE_ID(
        Id,
        case emqx_connector:lookup_raw(ConnType, ConnName) of
            {ok, Conf} ->
                {200, format_resp(Conf)};
            {error, not_found} ->
                {404, error_msg('NOT_FOUND', <<"connector not found">>)}
        end
    );
'/connectors/:id'(put, #{bindings := #{id := Id}, body := Params0}) ->
    Params = filter_out_request_body(Params0),
    ?TRY_PARSE_ID(
        Id,
        case emqx_connector:lookup_raw(ConnType, ConnName) of
            {ok, _} ->
                case emqx_connector:update(ConnType, ConnName, Params) of
                    {ok, #{raw_config := RawConf}} ->
                        {200,
                            format_resp(RawConf#{
                                <<"type">> => ConnType,
                                <<"name">> => ConnName
                            })};
                    {error, Error} ->
                        {500, error_msg('INTERNAL_ERROR', Error)}
                end;
            {error, not_found} ->
                {404, error_msg('NOT_FOUND', <<"connector not found">>)}
        end
    );
'/connectors/:id'(delete, #{bindings := #{id := Id}}) ->
    ?TRY_PARSE_ID(
        Id,
        case emqx_connector:lookup_raw(ConnType, ConnName) of
            {ok, _} ->
                case emqx_connector:delete(ConnType, ConnName) of
                    {ok, _} ->
                        {204};
                    {error, {post_config_update, _, {dependency_bridges_exist, BridgeID}}} ->
                        {403,
                            error_msg(
                                'DEPENDENCY_EXISTS',
                                <<"Cannot remove the connector as it's in use by a bridge: ",
                                    BridgeID/binary>>
                            )};
                    {error, Error} ->
                        {500, error_msg('INTERNAL_ERROR', Error)}
                end;
            {error, not_found} ->
                {404, error_msg('NOT_FOUND', <<"connector not found">>)}
        end
    ).

error_msg(Code, Msg) ->
    #{code => Code, message => emqx_misc:readable_error_msg(Msg)}.

format_resp(#{<<"type">> := ConnType, <<"name">> := ConnName} = RawConf) ->
    NumOfBridges = length(
        emqx_bridge:list_bridges_by_connector(
            emqx_connector:connector_id(ConnType, ConnName)
        )
    ),
    RawConf#{
        <<"type">> => ConnType,
        <<"name">> => ConnName,
        <<"num_of_bridges">> => NumOfBridges
    }.

filter_out_request_body(Conf) ->
    ExtraConfs = [<<"clientid">>, <<"num_of_bridges">>, <<"type">>, <<"name">>],
    maps:without(ExtraConfs, Conf).

bin(S) when is_list(S) ->
    list_to_binary(S).
