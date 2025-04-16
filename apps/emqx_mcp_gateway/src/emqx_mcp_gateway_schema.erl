-module(emqx_mcp_gateway_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-export([roots/0, fields/1, desc/1, namespace/0]).

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
                stdio,
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
        {command,
            ?HOCON(
                binary(),
                #{
                    desc => ?DESC(command),
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
                    default => #{}
                }
            )}
    ];
fields(http_server) ->
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
                http,
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
                internal,
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
        {module,
            ?HOCON(
                binary(),
                #{
                    desc => ?DESC(module),
                    required => true
                }
            )}
    ].

desc(mcp) ->
    "MCP Gateway configuration";
desc(_) ->
    undefined.
