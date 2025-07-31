%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_snowflake_streaming_action_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include("emqx_bridge_snowflake.hrl").

%% `hocon_schema' API
-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

%% `emqx_bridge_v2_schema' "unofficial" API
-export([
    bridge_v2_examples/1
]).

%% API
-export([]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(AGGREG_CONN_SCHEMA_MOD, emqx_bridge_snowflake_aggregated_connector_schema).
-define(AGGREG_ACTION_SCHEMA_MOD, emqx_bridge_snowflake_aggregated_action_schema).

%%-------------------------------------------------------------------------------------------------
%% `hocon_schema' API
%%-------------------------------------------------------------------------------------------------

namespace() ->
    "action_snowflake_streaming".

roots() ->
    [].

fields(Field) when
    Field == "get_bridge_v2";
    Field == "put_bridge_v2";
    Field == "post_bridge_v2"
->
    emqx_bridge_v2_schema:api_fields(Field, ?ACTION_TYPE_STREAM, fields(?ACTION_TYPE_STREAM));
fields(action) ->
    {?ACTION_TYPE_STREAM,
        mk(
            hoconsc:map(name, hoconsc:ref(?MODULE, ?ACTION_TYPE_STREAM)),
            #{
                desc => <<"Snowflake Streaming Action Config">>,
                required => false
            }
        )};
fields(?ACTION_TYPE_STREAM) ->
    emqx_bridge_v2_schema:make_producer_action_schema(
        mk(
            ref(parameters),
            #{
                required => true,
                desc => ?DESC(?AGGREG_ACTION_SCHEMA_MOD, "parameters")
            }
        ),
        #{resource_opts_ref => ref(action_resource_opts)}
    );
fields(parameters) ->
    [
        {database,
            mk(binary(), #{required => true, desc => ?DESC(?AGGREG_ACTION_SCHEMA_MOD, "database")})},
        {schema,
            mk(binary(), #{required => true, desc => ?DESC(?AGGREG_ACTION_SCHEMA_MOD, "schema")})},
        {pipe, mk(binary(), #{required => true, desc => ?DESC(?AGGREG_ACTION_SCHEMA_MOD, "pipe")})},
        {connect_timeout,
            mk(emqx_schema:timeout_duration_ms(), #{
                default => <<"15s">>, desc => ?DESC(?AGGREG_ACTION_SCHEMA_MOD, "connect_timeout")
            })},
        {pipelining,
            mk(pos_integer(), #{
                default => 100, desc => ?DESC(?AGGREG_ACTION_SCHEMA_MOD, "pipelining")
            })},
        {pool_size,
            mk(pos_integer(), #{default => 8, desc => ?DESC(?AGGREG_ACTION_SCHEMA_MOD, "pool_size")})},
        {max_retries,
            mk(non_neg_integer(), #{
                default => 3, desc => ?DESC(?AGGREG_ACTION_SCHEMA_MOD, "max_retries")
            })},
        emqx_connector_schema:ehttpc_max_inactive_sc()
    ];
fields(action_resource_opts) ->
    emqx_bridge_v2_schema:action_resource_opts_fields([
        {batch_size, #{default => 100}},
        {batch_time, #{default => <<"10ms">>}}
    ]).

desc(Name) when
    Name =:= ?ACTION_TYPE_STREAM
->
    ?DESC(Name);
desc(parameters) ->
    ?DESC(?AGGREG_ACTION_SCHEMA_MOD, parameters);
desc(action_resource_opts) ->
    ?DESC(emqx_resource_schema, "creation_opts");
desc(_Name) ->
    undefined.

%%-------------------------------------------------------------------------------------------------
%% `emqx_bridge_v2_schema' "unofficial" API
%%-------------------------------------------------------------------------------------------------

bridge_v2_examples(Method) ->
    [
        #{
            ?ACTION_TYPE_STREAM_BIN => #{
                summary => <<"Snowflake Streaming Action">>,
                value => action_example(Method)
            }
        }
    ].

action_example(post) ->
    maps:merge(
        action_example(put),
        #{
            type => ?ACTION_TYPE_STREAM_BIN,
            name => <<"my_action">>
        }
    );
action_example(get) ->
    maps:merge(
        action_example(put),
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
action_example(put) ->
    #{
        enable => true,
        description => <<"my action">>,
        connector => <<"my_connector">>,
        parameters =>
            #{
                connect_timeout => <<"15s">>,
                database => <<"testdatabase">>,
                pipe => <<"testpipe">>,
                pipe_user => <<"pipeuser">>,
                schema => <<"public">>,
                max_retries => 3,
                pipelining => 100,
                pool_size => 16
            },
        resource_opts =>
            #{
                batch_time => <<"60s">>,
                batch_size => 10_000,
                health_check_interval => <<"30s">>,
                inflight_window => 100,
                query_mode => <<"sync">>,
                request_ttl => <<"45s">>,
                worker_pool_size => 16
            }
    }.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

ref(Name) -> hoconsc:ref(?MODULE, Name).
mk(Type, Meta) -> hoconsc:mk(Type, Meta).
