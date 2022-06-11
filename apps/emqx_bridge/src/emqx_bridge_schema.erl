-module(emqx_bridge_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-import(hoconsc, [mk/2, ref/2]).

-export([roots/0, fields/1, desc/1, namespace/0]).

-export([
    get_response/0,
    put_request/0,
    post_request/0
]).

-export([
    common_bridge_fields/1,
    metrics_status_fields/0,
    direction_field/2
]).

%%======================================================================================
%% Hocon Schema Definitions

-define(CONN_TYPES, [mqtt]).

%%======================================================================================
%% For HTTP APIs
get_response() ->
    http_schema("get").

put_request() ->
    http_schema("put").

post_request() ->
    http_schema("post").

http_schema(Method) ->
    Schemas = lists:flatmap(
        fun(Type) ->
            [
                ref(schema_mod(Type), Method ++ "_ingress"),
                ref(schema_mod(Type), Method ++ "_egress")
            ]
        end,
        ?CONN_TYPES
    ),
    hoconsc:union([
        ref(emqx_bridge_webhook_schema, Method)
        | Schemas
    ]).

common_bridge_fields(ConnectorRef) ->
    [
        {enable,
            mk(
                boolean(),
                #{
                    desc => ?DESC("desc_enable"),
                    default => true
                }
            )},
        {connector,
            mk(
                hoconsc:union([binary(), ConnectorRef]),
                #{
                    required => true,
                    example => <<"mqtt:my_mqtt_connector">>,
                    desc => ?DESC("desc_connector")
                }
            )}
    ].

metrics_status_fields() ->
    [
        {"metrics", mk(ref(?MODULE, "metrics"), #{desc => ?DESC("desc_metrics")})},
        {"node_metrics",
            mk(
                hoconsc:array(ref(?MODULE, "node_metrics")),
                #{desc => ?DESC("desc_node_metrics")}
            )},
        {"status", mk(status(), #{desc => ?DESC("desc_status")})},
        {"node_status",
            mk(
                hoconsc:array(ref(?MODULE, "node_status")),
                #{desc => ?DESC("desc_node_status")}
            )}
    ].

direction_field(Dir, Desc) ->
    {direction,
        mk(
            Dir,
            #{
                required => true,
                default => egress,
                desc => "The direction of the bridge. Can be one of 'ingress' or 'egress'.</br>" ++
                    Desc
            }
        )}.

%%======================================================================================
%% For config files

namespace() -> "bridge".

roots() -> [bridges].

fields(bridges) ->
    [
        {webhook,
            mk(
                hoconsc:map(name, ref(emqx_bridge_webhook_schema, "config")),
                #{desc => ?DESC("bridges_webhook")}
            )}
    ] ++
        [
            {T,
                mk(
                    hoconsc:map(
                        name,
                        hoconsc:union([
                            ref(schema_mod(T), "ingress"),
                            ref(schema_mod(T), "egress")
                        ])
                    ),
                    #{desc => ?DESC("bridges_name")}
                )}
         || T <- ?CONN_TYPES
        ];
fields("metrics") ->
    [
        {"matched", mk(integer(), #{desc => ?DESC("metric_matched")})},
        {"success", mk(integer(), #{desc => ?DESC("metric_success")})},
        {"failed", mk(integer(), #{desc => ?DESC("metric_failed")})},
        {"rate", mk(float(), #{desc => ?DESC("metric_rate")})},
        {"rate_max", mk(float(), #{desc => ?DESC("metric_rate_max")})},
        {"rate_last5m",
            mk(
                float(),
                #{desc => ?DESC("metric_rate_last5m")}
            )}
    ];
fields("node_metrics") ->
    [
        node_name(),
        {"metrics", mk(ref(?MODULE, "metrics"), #{})}
    ];
fields("node_status") ->
    [
        node_name(),
        {"status", mk(status(), #{})}
    ].

desc(bridges) ->
    ?DESC("desc_bridges");
desc("metrics") ->
    ?DESC("desc_metrics");
desc("node_metrics") ->
    ?DESC("desc_node_metrics");
desc("node_status") ->
    ?DESC("desc_node_status");
desc(_) ->
    undefined.

status() ->
    hoconsc:enum([connected, disconnected, connecting]).

node_name() ->
    {"node", mk(binary(), #{desc => ?DESC("desc_node_name"), example => "emqx@127.0.0.1"})}.

schema_mod(Type) ->
    list_to_atom(lists:concat(["emqx_bridge_", Type, "_schema"])).
