-module(emqx_mcp_gateway_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-export([roots/0, fields/1, desc/1, namespace/0]).
-export([validate_cmd/1, validate_env/1]).

namespace() -> mcp.

roots() ->
    [{mcp, ?HOCON(?R_REF(mcp), #{})}].

fields(mcp) ->
    [
        {enable,
            ?HOCON(
                boolean(),
                #{
                    desc => ?DESC(enable),
                    default => true
                }
            )},
        {broker_suggested_server_name,
            ?HOCON(
                ?REF(broker_suggested_server_name),
                #{
                    desc => ?DESC(broker_suggested_server_name)
                }
            )},
        {servers,
            ?HOCON(
                ?MAP(name, ?UNION([?REF(stdio_server), ?REF(http_server), ?REF(internal_server)])),
                #{
                    desc => ?DESC(servers),
                    default => #{}
                }
            )}
    ];
fields(broker_suggested_server_name) ->
    [
        {enable,
            ?HOCON(
                boolean(),
                #{
                    desc => ?DESC(enable),
                    default => true
                }
            )},
        {bootstrap_file,
            ?HOCON(
                binary(),
                #{
                    desc => ?DESC(bootstrap_file),
                    required => false
                }
            )}
    ];
fields(stdio_server) ->
    common_server_confs(stdio) ++
        [
            {command,
                ?HOCON(
                    binary(),
                    #{
                        desc => ?DESC(command),
                        validator => fun ?MODULE:validate_cmd/1,
                        required => true
                    }
                )},
            {args,
                ?HOCON(
                    ?ARRAY(binary()),
                    #{
                        desc => ?DESC(args),
                        default => []
                    }
                )},
            {env,
                ?HOCON(
                    map(),
                    #{
                        desc => ?DESC(env),
                        validator => fun ?MODULE:validate_env/1,
                        default => #{}
                    }
                )}
        ];
fields(http_server) ->
    common_server_confs(http) ++
        [
            {url,
                ?HOCON(
                    binary(),
                    #{
                        desc => ?DESC(emqx_authn_http_schema, url),
                        required => true
                    }
                )},
            {request_timeout,
                ?HOCON(
                    emqx_schema:duration_ms(),
                    #{
                        desc => ?DESC(emqx_authn_http_schema, request_timeout),
                        default => <<"5s">>
                    }
                )}
        ] ++
        maps:to_list(
            maps:without(
                [
                    pool_type
                ],
                maps:from_list(emqx_bridge_http_connector:fields(config))
            )
        );
fields(internal_server) ->
    common_server_confs(internal) ++
        [
            {module,
                ?HOCON(
                    binary(),
                    #{
                        desc => ?DESC(module),
                        required => true
                    }
                )}
        ].

common_server_confs(Type) ->
    [
        {enable,
            ?HOCON(
                boolean(),
                #{
                    desc => ?DESC(enable),
                    default => true
                }
            )},
        {server_type,
            ?HOCON(
                Type,
                #{
                    desc => ?DESC(server_type),
                    default => true
                }
            )},
        {server_name,
            ?HOCON(
                binary(),
                #{
                    desc => ?DESC(server_name),
                    required => true
                }
            )},
        {server_desc,
            ?HOCON(
                binary(),
                #{
                    desc => ?DESC(server_desc),
                    default => <<>>
                }
            )}
    ].

validate_cmd(Cmd) ->
    %% must be a absolute path
    case filename:pathtype(Cmd) of
        absolute -> ok;
        _ -> throw({only_absolute_path_is_allowed, Cmd})
    end.

validate_env(Env) when is_map(Env) ->
    maps:foreach(
        fun
            (<<>>, _V) ->
                throw(empty_env_key_not_allowed);
            (K, V) when is_binary(K) andalso (is_binary(V) orelse V =:= false) ->
                ok;
            (K, V) ->
                throw({invalid_env_key_or_value, {K, V}})
        end,
        Env
    ).

desc(mcp) ->
    "MCP Gateway configuration";
desc(_) ->
    undefined.
