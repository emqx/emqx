%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_zerobus_action_schema).

-behaviour(hocon_schema).

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

%% `emqx_bridge_v2_schema' "unofficial" API
-export([
    bridge_v2_examples/1
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
    "action_zerobus".

roots() ->
    [].

fields(Field) when
    Field == "get_bridge_v2";
    Field == "put_bridge_v2";
    Field == "post_bridge_v2"
->
    emqx_bridge_v2_schema:api_fields(Field, ?ACTION_TYPE, fields(?ACTION_TYPE));
fields(action) ->
    {?ACTION_TYPE,
        mk(
            hoconsc:map(name, ref(?ACTION_TYPE)),
            #{
                desc => <<"Zerobus Action Config">>,
                required => false
            }
        )};
fields(?ACTION_TYPE) ->
    emqx_bridge_v2_schema:make_producer_action_schema(
        mk(
            ref(action_parameters),
            #{
                required => true,
                desc => ?DESC("action_parameters")
            }
        ),
        #{resource_opts_ref => ref(action_resource_opts)}
    );
fields(action_resource_opts) ->
    emqx_bridge_v2_schema:action_resource_opts_fields([
        {batch_size, #{default => 1_000}},
        {batch_time, #{default => <<"500ms">>}}
    ]);
fields(action_parameters) ->
    [
        {catalog, mk(binary(), #{required => true, desc => ?DESC("catalog")})},
        {schema, mk(binary(), #{required => true, desc => ?DESC("schema")})},
        {table, mk(binary(), #{required => true, desc => ?DESC("table")})},
        {record,
            mk(
                emqx_schema:mkunion(type, #{
                    <<"json">> => ref(record_json),
                    <<"proto">> => ref(record_proto)
                }),
                #{required => true, desc => ?DESC("record_type")}
            )},
        {grpc_open_stream_retry_interval,
            mk(emqx_schema:timeout_duration_ms(), #{
                default => ~"5s",
                importance => ?IMPORTANCE_HIDDEN
            })}
    ];
fields(record_json) ->
    [
        {type, mk(json, #{default => json, required => true, desc => ?DESC("record_type")})}
    ];
fields(record_proto) ->
    [
        {type, mk(proto, #{default => proto, required => true, desc => ?DESC("record_type")})},
        {schema_name, mk(binary(), #{required => true, desc => ?DESC("record_proto_schema_name")})},
        {message_type,
            mk(binary(), #{required => true, desc => ?DESC("record_proto_message_type")})}
    ].

desc(Name) when
    Name =:= ?ACTION_TYPE;
    Name =:= action_parameters
->
    ?DESC(Name);
desc(record_json) ->
    ?DESC("record_type");
desc(record_proto) ->
    ?DESC("record_type");
desc(action_resource_opts) ->
    ?DESC(emqx_resource_schema, "creation_opts");
desc(_Name) ->
    undefined.

%%------------------------------------------------------------------------------
%% `emqx_bridge_v2_schema' "unofficial" API
%%------------------------------------------------------------------------------

bridge_v2_examples(Method) ->
    [
        #{
            ?ACTION_TYPE_BIN => #{
                summary => <<"Zerobus Action">>,
                value => action_example(Method)
            }
        }
    ].

action_example(post) ->
    maps:merge(
        action_example(put),
        #{
            type => ?ACTION_TYPE_BIN,
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
                catalog => ~"test0",
                schema => ~"default",
                table => ~"air_quality",
                record => #{~"type" => ~"json"}
            },
        resource_opts =>
            #{
                batch_time => <<"100ms">>,
                batch_size => 1_000,
                health_check_interval => <<"30s">>,
                inflight_window => 100,
                query_mode => <<"sync">>,
                request_ttl => <<"45s">>,
                worker_pool_size => 16
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
