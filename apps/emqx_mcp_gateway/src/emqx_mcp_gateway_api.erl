%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mcp_gateway_api).

-behaviour(minirest_api).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx_utils/include/emqx_utils_api.hrl").
-include("emqx_mcp_gateway.hrl").

-export([
    namespace/0,
    api_spec/0,
    paths/0,
    schema/1,
    fields/1
]).

-export([
    '/mcp/servers'/2,
    '/mcp/servers/:id'/2,
    '/mcp/server_names'/2,
    '/mcp/server_names/:username'/2
]).

-export([qs2ms/2]).

-define(MCP_TAGS, [<<"MCP">>]).
-define(BAD_REQUEST, 'BAD_REQUEST').
-define(SERVER_NAME_QSCHEMA, [
    {<<"like_username">>, binary}
]).

namespace() -> "mcp_gateway_api".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{
        check_schema => fun emqx_dashboard_swagger:validate_content_type_json/2
    }).

paths() ->
    [
        "/mcp/servers",
        "/mcp/servers/:id",
        "/mcp/server_names",
        "/mcp/server_names/:username"
    ].

schema("/mcp/servers") ->
    #{
        'operationId' => '/mcp/servers',
        get => #{
            tags => ?MCP_TAGS,
            summary => <<"List MCP servers">>,
            description => ?DESC("mcp_server_list_api"),
            responses => #{
                200 =>
                    ?HOCON(?ARRAY(mcp_server_get_t()), #{
                        desc => ?DESC("mcp_server_list_api_response")
                    }),
                400 => error_schema('BAD_REQUEST', "Invalid Parameters")
            }
        },
        post => #{
            tags => ?MCP_TAGS,
            summary => <<"Create an MCP server">>,
            description => ?DESC("mcp_server_post_api"),
            'requestBody' => mcp_server_post_schema(),
            responses => #{
                400 => error_schema('BAD_REQUEST', "Invalid Parameters"),
                201 => mcp_server_get_schema()
            }
        }
    };
schema("/mcp/servers/:id") ->
    #{
        'operationId' => '/mcp/servers/:id',
        get => #{
            tags => ?MCP_TAGS,
            summary => <<"Get an MCP server">>,
            description => ?DESC("mcp_server_get_api"),
            parameters => param_path_id(),
            responses => #{
                404 => error_schema('NOT_FOUND', "Not found"),
                200 => mcp_server_get_schema()
            }
        },
        put => #{
            tags => ?MCP_TAGS,
            summary => <<"Update an MCP server">>,
            description => ?DESC("mcp_server_put_api"),
            parameters => param_path_id(),
            'requestBody' => mcp_server_put_schema(),
            responses => #{
                400 => error_schema('BAD_REQUEST', "Invalid Parameters"),
                404 => error_schema('NOT_FOUND', "Not found"),
                200 => mcp_server_get_schema()
            }
        },
        delete => #{
            tags => ?MCP_TAGS,
            summary => <<"Delete an MCP server">>,
            description => ?DESC("mcp_server_delete_api"),
            parameters => param_path_id(),
            responses => #{
                404 => error_schema('NOT_FOUND', "Not found"),
                204 => <<"Deleted">>
            }
        }
    };
schema("/mcp/server_names") ->
    #{
        'operationId' => '/mcp/server_names',
        get => #{
            tags => ?MCP_TAGS,
            summary => <<"List MCP server names">>,
            description => ?DESC("mcp_server_name_list_api"),
            parameters => [
                ?R_REF(emqx_dashboard_swagger, page),
                ?R_REF(emqx_dashboard_swagger, limit),
                {<<"like_username">>,
                    ?HOCON(
                        binary(),
                        #{
                            in => query,
                            desc => ?DESC("qs_like_username"),
                            required => false,
                            example => <<"user1">>
                        }
                    )}
            ],
            responses => #{
                200 =>
                    [
                        {data,
                            ?HOCON(?ARRAY(mcp_server_name_get_t()), #{
                                desc => ?DESC("mcp_server_name_list_api_response")
                            })},
                        {meta, ?HOCON(?R_REF(emqx_dashboard_swagger, meta), #{})}
                    ],
                400 => error_schema('BAD_REQUEST', "Invalid Parameters")
            }
        },
        post => #{
            tags => ?MCP_TAGS,
            summary => <<"Add an MCP server name">>,
            description => ?DESC("mcp_server_name_post_api"),
            'requestBody' => mcp_server_name_post_schema(),
            responses => #{
                400 => error_schema('BAD_REQUEST', "Invalid Parameters"),
                201 => mcp_server_name_get_schema()
            }
        }
    };
schema("/mcp/server_names/:username") ->
    #{
        'operationId' => '/mcp/server_names/:username',
        get => #{
            tags => ?MCP_TAGS,
            summary => <<"Get an MCP server name">>,
            description => ?DESC("mcp_server_name_get_api"),
            parameters => param_path_username(),
            responses => #{
                404 => error_schema('NOT_FOUND', "Not found"),
                200 => mcp_server_name_get_schema()
            }
        },
        put => #{
            tags => ?MCP_TAGS,
            summary => <<"Update an MCP server name">>,
            description => ?DESC("mcp_server_name_put_api"),
            parameters => param_path_username(),
            'requestBody' => mcp_server_name_put_schema(),
            responses => #{
                400 => error_schema('BAD_REQUEST', "Invalid Parameters"),
                404 => error_schema('NOT_FOUND', "Not found"),
                200 => mcp_server_name_get_schema()
            }
        },
        delete => #{
            tags => ?MCP_TAGS,
            summary => <<"Delete an MCP server name">>,
            description => ?DESC("mcp_server_name_delete_api"),
            parameters => param_path_username(),
            responses => #{
                404 => error_schema('NOT_FOUND', "Not found"),
                204 => <<"Deleted">>
            }
        }
    }.

fields(stdio_server_with_id) ->
    [
        {id,
            ?HOCON(
                binary(),
                #{
                    desc => ?DESC(mcp_server_conf_id),
                    required => true,
                    example => <<"weather">>
                }
            )}
    ] ++ fields(stdio_server);
fields(stdio_server) ->
    emqx_mcp_gateway_schema:fields(stdio_server);
fields(mcp_server_name_with_id) ->
    [
        {username,
            ?HOCON(
                binary(),
                #{
                    desc => ?DESC(mqtt_username),
                    required => true,
                    example => <<"user1">>
                }
            )}
    ] ++ fields(mcp_server_name);
fields(mcp_server_name) ->
    [
        {server_name,
            ?HOCON(
                binary(),
                #{
                    desc => ?DESC(mcp_server_name),
                    required => true,
                    example => <<"simple_mcp_server/stdio/user1">>
                }
            )}
    ].

mcp_server_get_t() ->
    ?UNION([?REF(stdio_server_with_id)]).

mcp_server_get_schema() ->
    ?HOCON(
        mcp_server_get_t(),
        #{
            example => mcp_server_conf_example()
        }
    ).

mcp_server_post_schema() ->
    mcp_server_get_schema().

mcp_server_put_schema() ->
    ?HOCON(
        ?UNION([?REF(stdio_server)]),
        #{
            example => maps:remove(id, mcp_server_conf_example())
        }
    ).

mcp_server_name_get_t() ->
    ?UNION([?REF(mcp_server_name_with_id)]).

mcp_server_name_get_schema() ->
    ?HOCON(
        mcp_server_name_get_t(),
        #{
            example => mcp_server_name_example()
        }
    ).

mcp_server_name_post_schema() ->
    mcp_server_name_get_schema().

mcp_server_name_put_schema() ->
    ?HOCON(
        ?UNION([?REF(mcp_server_name)]),
        #{
            example => maps:remove(id, mcp_server_name_example())
        }
    ).

mcp_server_conf_example() ->
    #{
        id => <<"weather">>,
        enable => true,
        args => [
            <<"/tmp/weather.py">>
        ],
        env => #{<<"A">> => <<"1">>},
        command => <<"/tmp/venv-mcp/bin/python3">>,
        server_name => <<"simple_mcp_server/stdio/weather">>,
        server_type => stdio
    }.

mcp_server_name_example() ->
    #{
        username => <<"user1">>,
        server_name => <<"simple_mcp_server/stdio/user1">>
    }.

error_schema(Code, Message) when is_atom(Code) ->
    emqx_dashboard_swagger:error_codes([Code], list_to_binary(Message)).

param_path_id() ->
    [{id, ?HOCON(binary(), #{in => path, example => <<"weather">>})}].

param_path_username() ->
    [{username, ?HOCON(binary(), #{in => path, example => <<"user1">>})}].

%%--------------------------------------------------------------------
%% API handlers
%%--------------------------------------------------------------------
'/mcp/servers'(get, _Params) ->
    ?OK(tagged_map_to_list(<<"id">>, emqx:get_raw_config([mcp, servers], #{})));
'/mcp/servers'(post, #{body := #{<<"id">> := Id} = Params0}) ->
    Params = maps:remove(<<"id">>, Params0),
    case emqx:get_config([mcp, servers, Id], undefined) of
        undefined ->
            case emqx_conf:update([mcp, servers, Id], Params, #{override_to => cluster}) of
                {ok, _} ->
                    ?CREATED(Params0);
                {error, Reason} ->
                    ?SLOG(
                        info,
                        #{
                            msg => "create_mcp_server_failed",
                            server_id => Id,
                            reason => Reason
                        },
                        #{tag => ?TAG}
                    ),
                    ?BAD_REQUEST(Reason)
            end;
        _ ->
            ?BAD_REQUEST(<<"Server already exists">>)
    end;
'/mcp/servers'(post, _Params) ->
    ?BAD_REQUEST(<<"No MCP server configuration Id provided">>).

'/mcp/servers/:id'(get, #{bindings := #{id := Id}}) ->
    case emqx:get_config([mcp, servers, Id], undefined) of
        undefined ->
            ?NOT_FOUND(<<"Not found">>);
        ServerConf ->
            ?OK(ServerConf#{<<"id">> => Id})
    end;
'/mcp/servers/:id'(put, #{bindings := #{id := Id}, body := Params0}) ->
    case emqx:get_config([mcp, servers, Id], undefined) of
        undefined ->
            ?NOT_FOUND(<<"Not found">>);
        _ ->
            case emqx_conf:update([mcp, servers, Id], Params0, #{override_to => cluster}) of
                {ok, _} ->
                    ?OK(Params0#{<<"id">> => Id});
                {error, Reason} ->
                    ?SLOG(
                        info,
                        #{
                            msg => "update_mcp_server_failed",
                            server_id => Id,
                            reason => Reason
                        },
                        #{tag => ?TAG}
                    ),
                    ?BAD_REQUEST(Reason)
            end
    end;
'/mcp/servers/:id'(delete, #{bindings := #{id := Id}}) ->
    case emqx:get_config([mcp, servers, Id], undefined) of
        undefined ->
            ?NOT_FOUND(<<"Not found">>);
        _ ->
            case emqx_conf:remove([mcp, servers, Id], #{override_to => cluster}) of
                {ok, _} ->
                    ?NO_CONTENT;
                {error, Reason} ->
                    ?SLOG(
                        info,
                        #{
                            msg => "delete_mcp_server_failed",
                            server_id => Id,
                            reason => Reason
                        },
                        #{tag => ?TAG}
                    ),
                    ?BAD_REQUEST(Reason)
            end
    end.
'/mcp/server_names'(get, #{query_string := QueryString}) ->
    ServerNames =
        emqx_mgmt_api:node_query(
            node(),
            emqx_mcp_server_name_manager:table_name(),
            QueryString,
            ?SERVER_NAME_QSCHEMA,
            fun ?MODULE:qs2ms/2,
            fun emqx_mcp_server_name_manager:format_server_name/1
        ),
    ?OK(ServerNames);
'/mcp/server_names'(post, #{body := #{<<"username">> := Username, <<"server_name">> := ServerName}}) ->
    case emqx_mcp_server_name_manager:get_server_name(Username) of
        {error, not_found} ->
            ok = emqx_mcp_server_name_manager:add_server_name(Username, ServerName),
            ?CREATED(#{username => Username, server_name => ServerName});
        _ ->
            ?BAD_REQUEST(<<"Server name already exists">>)
    end;
'/mcp/server_names'(post, _Params) ->
    ?BAD_REQUEST(<<"Invalid Parameters">>).
'/mcp/server_names/:username'(get, #{bindings := #{username := Username}}) ->
    case emqx_mcp_server_name_manager:get_server_name(Username) of
        {error, not_found} ->
            ?NOT_FOUND(<<"Not found">>);
        {ok, ServerName} ->
            ?OK(#{username => Username, server_name => ServerName})
    end;
'/mcp/server_names/:username'(put, #{
    bindings := #{username := Username}, body := #{<<"server_name">> := ServerName}
}) ->
    case emqx_mcp_server_name_manager:get_server_name(Username) of
        {error, not_found} ->
            ?NOT_FOUND(<<"Not found">>);
        _ ->
            ok = emqx_mcp_server_name_manager:add_server_name(Username, ServerName),
            ?OK(#{username => Username, server_name => ServerName})
    end;
'/mcp/server_names/:username'(put, _) ->
    ?BAD_REQUEST(<<"Invalid Parameters">>);
'/mcp/server_names/:username'(delete, #{bindings := #{username := Username}}) ->
    case emqx_mcp_server_name_manager:get_server_name(Username) of
        {error, not_found} ->
            ?NOT_FOUND(<<"Not found">>);
        _ ->
            ok = emqx_mcp_server_name_manager:delete_server_name(Username),
            ?NO_CONTENT
    end.

%%------------------------------------------------------------------------------
%% QueryString to MatchSpec
%%------------------------------------------------------------------------------
-spec qs2ms(atom(), {list(), list()}) -> emqx_mgmt_api:match_spec_and_filter().
qs2ms(_Tab, {_QString, FuzzyQString}) ->
    #{
        match_spec => emqx_mcp_server_name_manager:match_all_spec(),
        fuzzy_fun => emqx_mcp_server_name_manager:fuzzy_filter_fun(FuzzyQString)
    }.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
tagged_map_to_list(TagKey, Map) ->
    [Map1#{TagKey => Tag} || {Tag, Map1} <- maps:to_list(Map)].
