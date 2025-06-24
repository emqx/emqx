%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_bigquery_connector_schema).

-feature(maybe_expr, enable).

-behaviour(hocon_schema).
-behaviour(emqx_connector_examples).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include("emqx_bridge_bigquery.hrl").

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
    "connector_bigquery".

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
        {connect_timeout,
            mk(
                emqx_schema:timeout_duration_ms(),
                #{
                    default => <<"15s">>,
                    desc => ?DESC(emqx_bridge_gcp_pubsub, "connect_timeout")
                }
            )},
        {pool_size,
            mk(
                pos_integer(),
                #{
                    default => 8,
                    desc => ?DESC(emqx_bridge_gcp_pubsub, "pool_size")
                }
            )},
        {pipelining,
            mk(
                pos_integer(),
                #{
                    default => 100,
                    desc => ?DESC(emqx_bridge_gcp_pubsub, "pipelining")
                }
            )},
        {max_retries,
            mk(
                non_neg_integer(),
                #{
                    required => false,
                    default => 2,
                    desc => ?DESC(emqx_bridge_gcp_pubsub, "max_retries")
                }
            )},
        emqx_connector_schema:ehttpc_max_inactive_sc(),
        {service_account_json,
            mk(
                binary(),
                #{
                    required => true,
                    validator => fun emqx_bridge_gcp_pubsub:service_account_json_validator/1,
                    sensitive => true,
                    desc => ?DESC(emqx_bridge_gcp_pubsub, "service_account_json")
                }
            )}
    ] ++
        emqx_connector_schema:resource_opts().

desc("config_connector") ->
    ?DESC("config_connector");
desc(_Name) ->
    undefined.

%%------------------------------------------------------------------------------
%% `emqx_connector_examples' API
%%------------------------------------------------------------------------------

connector_examples(Method) ->
    [
        #{
            <<"bigquery">> => #{
                summary => <<"BigQuery Connector">>,
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
        connect_timeout => <<"15s">>,
        max_inactive => <<"10s">>,
        max_retries => 2,
        pipelining => 100,
        pool_size => 8,
        service_account_json =>
            <<"{\"client_email\": \"test@myproject.iam.gserviceaccount.com\", ...}">>,
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
