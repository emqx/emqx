%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_azure_event_grid_hybrid_schema).

-behaviour(hocon_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include("emqx_bridge_azure_event_grid.hrl").

%% `hocon_schema' API
-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

%% `emqx_bridge_v2_schema' "unofficial" API
-export([
    bridge_v2_examples/1,
    source_examples/1
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
    "action_source_azure_event_grid".

roots() ->
    [].

%% Action
fields(Field) when
    Field == "get_bridge_v2";
    Field == "put_bridge_v2";
    Field == "post_bridge_v2"
->
    emqx_bridge_v2_schema:api_fields(Field, ?ACTION_TYPE, fields(action_config));
fields(action) ->
    {?ACTION_TYPE,
        mk(
            hoconsc:map(name, ref(action_config)),
            #{
                desc => <<"Azure Event Grid Action Config">>,
                required => false
            }
        )};
fields(action_config) ->
    emqx_bridge_v2_schema:make_producer_action_schema(
        mk(
            hoconsc:ref(emqx_bridge_mqtt_pubsub_schema, action_parameters),
            #{
                required => true,
                desc => ?DESC(emqx_bridge_mqtt_pubsub_schema, "action_parameters")
            }
        ),
        #{resource_opts_ref => hoconsc:ref(emqx_bridge_mqtt_pubsub_schema, action_resource_opts)}
    );
%% Source
fields(Field) when
    Field == "get_source";
    Field == "put_source";
    Field == "post_source"
->
    emqx_bridge_v2_schema:api_fields(Field, ?SOURCE_TYPE, fields(source_config));
fields(source) ->
    {?SOURCE_TYPE,
        mk(
            hoconsc:map(name, ref(source_config)),
            #{
                desc => <<"Azure Event Grid Source Config">>,
                required => false
            }
        )};
fields(source_config) ->
    emqx_bridge_v2_schema:make_consumer_action_schema(
        mk(
            hoconsc:ref(emqx_bridge_mqtt_pubsub_schema, ingress_parameters),
            #{
                required => true,
                desc => ?DESC(emqx_bridge_mqtt_pubsub_schema, "source_parameters")
            }
        )
    ).

desc(Name) when
    Name =:= action_config;
    Name =:= source_config
->
    ?DESC(Name);
desc(_Name) ->
    undefined.

%%------------------------------------------------------------------------------
%% `emqx_bridge_v2_schema' "unofficial" API
%%------------------------------------------------------------------------------

bridge_v2_examples(Method) ->
    [
        #{
            ?ACTION_TYPE_BIN => #{
                summary => <<"Azure Event Grid Action">>,
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
                topic => <<"remote/topic">>,
                qos => 2,
                retain => false,
                payload => <<"${.payload}">>
            },
        resource_opts =>
            #{
                batch_time => <<"0s">>,
                batch_size => 1,
                health_check_interval => <<"30s">>,
                inflight_window => 100,
                query_mode => <<"sync">>,
                request_ttl => <<"45s">>,
                worker_pool_size => 16
            }
    }.

source_examples(Method) ->
    [
        #{
            ?SOURCE_TYPE_BIN => #{
                summary => <<"Azure Event Grid Source">>,
                value => source_example(Method)
            }
        }
    ].

source_example(post) ->
    maps:merge(
        source_example(put),
        #{
            type => ?SOURCE_TYPE_BIN,
            name => <<"my_source">>
        }
    );
source_example(get) ->
    maps:merge(
        source_example(put),
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
source_example(put) ->
    #{
        enable => true,
        description => <<"my source">>,
        connector => <<"my_connector">>,
        parameters =>
            #{
                topic => <<"remote/topic">>,
                qos => 1
            },
        resource_opts =>
            #{
                health_check_interval => <<"30s">>
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
