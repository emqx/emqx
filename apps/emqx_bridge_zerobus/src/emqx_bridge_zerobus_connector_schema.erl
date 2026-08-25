%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_zerobus_connector_schema).

-behaviour(hocon_schema).
-behaviour(emqx_connector_examples).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include("emqx_bridge_zerobus.hrl").

%% `hocon_schema' API
-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

%% `emqx_connector_examples' API
-export([
    connector_examples/1
]).

%% API
-export([]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% `hocon_schema' API
%%------------------------------------------------------------------------------

namespace() ->
    "connector_zerobus".

roots() ->
    [].

fields(Field) when
    Field == "get_connector";
    Field == "put_connector";
    Field == "post_connector"
->
    emqx_connector_schema:api_fields(Field, ?CONNECTOR_TYPE, fields(connector_config));
fields("config_connector") ->
    emqx_connector_schema:common_fields() ++ fields(connector_config);
fields(connector_config) ->
    [
        {zerobus_endpoint,
            emqx_schema:servers_sc(
                #{
                    required => true,
                    desc => ?DESC("zerobus_endpoint")
                },
                ?PARSE_SERVER_OPTS
            )},
        {authentication,
            mk(ref(authentication), #{required => true, desc => ?DESC("authentication")})},
        {transport,
            mk(
                emqx_schema:mkunion(type, #{
                    <<"rest">> => ref(transport_rest),
                    <<"grpc">> => ref(transport_grpc)
                }),
                #{required => true, desc => ?DESC("transport_type")}
            )}
    ] ++
        emqx_connector_schema_lib:ssl_fields(#{enable_by_default => true}) ++
        emqx_connector_schema:resource_opts();
fields(authentication) ->
    Fields0 = emqx_connector_oauth2_schema:fields(client_credentials),
    Fields = lists:filtermap(
        fun
            ({scope, _}) ->
                %% this will be injected during runtime
                false;
            ({token_endpoint, _}) ->
                %% this will be injected during runtime, constructed from workspace
                %% url
                false;
            ({enable = K, Sc0}) ->
                Sc = hocon_schema:override(Sc0, #{default => true}),
                {true, {K, Sc}};
            (_) ->
                true
        end,
        Fields0
    ),
    [
        {workspace_id, mk(binary(), #{required => true, desc => ?DESC("workspace_id")})},
        {workspace_url, mk(emqx_schema:url(), #{required => true, desc => ?DESC("workspace_url")})}
        | Fields
    ];
fields(transport_rest) ->
    [
        {type, mk(rest, #{default => rest, required => true, desc => ?DESC("transport_type")})},
        {connect_timeout,
            mk(emqx_schema:timeout_duration_ms(), #{
                default => <<"10s">>, desc => ?DESC("connect_timeout")
            })},
        {pool_size, mk(pos_integer(), #{default => 8, desc => ?DESC("pool_size")})},
        {pipelining,
            mk(pos_integer(), #{
                default => 100, desc => ?DESC(emqx_bridge_http_connector, "enable_pipelining")
            })},
        emqx_connector_schema:ehttpc_max_inactive_sc()
    ];
fields(transport_grpc) ->
    [
        {type, mk(grpc, #{default => grpc, required => true, desc => ?DESC("transport_type")})},
        {connect_timeout,
            mk(emqx_schema:timeout_duration_ms(), #{
                default => <<"10s">>, desc => ?DESC("connect_timeout")
            })},
        {pool_size, mk(pos_integer(), #{default => 8, desc => ?DESC("pool_size")})}
    ].

desc("config_connector") ->
    ?DESC("config_connector");
desc(authentication) ->
    ?DESC("authentication");
desc(transport_grpc) ->
    ?DESC("transport_type");
desc(transport_rest) ->
    ?DESC("transport_type");
desc(_Name) ->
    undefined.

%%------------------------------------------------------------------------------
%% `emqx_connector_examples' API
%%------------------------------------------------------------------------------

connector_examples(Method) ->
    [
        #{
            <<"zerobus">> => #{
                summary => <<"Zerobus Connector">>,
                value => connector_example(Method)
            }
        }
    ].

connector_example(get) ->
    maps:merge(
        connector_example(put),
        #{
            status => <<"connected">>,
            node_status => [
                #{
                    node => <<"emqx@localhost">>,
                    status => <<"connected">>
                }
            ]
        }
    );
connector_example(post) ->
    maps:merge(
        connector_example(put),
        #{
            type => atom_to_binary(?CONNECTOR_TYPE),
            name => <<"my_connector">>
        }
    );
connector_example(put) ->
    #{
        enable => true,
        description => <<"My connector">>,
        authentication => #{
            client_id => ~"xxx",
            client_secret => ~"******",
            workspace_id => ~"8923749827438",
            workspace_url => ~"https://xxxx.cloud.databricks.com"
        },
        transport => #{
            type => ~"grpc",
            connect_timeout => ~"10s",
            pool_size => 8
        },
        zerobus_endpoint => ~"https://xxxx.zerobus.sa-east-1.cloud.databricks.com:443",
        resource_opts => #{
            health_check_interval => <<"45s">>,
            start_after_created => true,
            start_timeout => <<"5s">>
        }
    }.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

mk(Type, Meta) -> hoconsc:mk(Type, Meta).
ref(Struct) -> hoconsc:ref(?MODULE, Struct).
