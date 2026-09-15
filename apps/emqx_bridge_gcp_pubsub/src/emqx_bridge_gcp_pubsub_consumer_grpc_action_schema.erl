%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_gcp_pubsub_consumer_grpc_action_schema).

-behaviour(hocon_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include("emqx_bridge_gcp_pubsub_consumer_grpc.hrl").

%% `hocon_schema' API
-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

%% `emqx_bridge_v2_schema' "unofficial" API
-export([
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
    "source_gcp_pubsub_consumer_grpc".

roots() ->
    [].

fields(Field) when
    Field == "get_source";
    Field == "put_source";
    Field == "post_source"
->
    emqx_bridge_v2_schema:api_fields(Field, ?SOURCE_TYPE, fields(?SOURCE_TYPE));
fields(source) ->
    {?SOURCE_TYPE,
        mk(
            hoconsc:map(name, ref(?SOURCE_TYPE)),
            #{
                desc => <<"GCP PubSub Consumer (gRPC) Source Config">>,
                required => false
            }
        )};
fields(?SOURCE_TYPE) ->
    emqx_bridge_v2_schema:make_consumer_action_schema(
        mk(
            ref(source_parameters),
            #{
                required => true,
                desc => ?DESC("source_parameters")
            }
        ),
        #{resource_opts_ref => ref(resource_opts)}
    );
fields(resource_opts) ->
    [
        {request_ttl, fun emqx_resource_schema:request_ttl/1}
        | emqx_bridge_v2_schema:source_resource_opts_fields()
    ];
fields(source_parameters) ->
    [
        {topic,
            mk(
                binary(),
                #{
                    required => true,
                    validator => fun emqx_bridge_gcp_pubsub:pubsub_topic_validator/1,
                    desc => ?DESC(emqx_bridge_gcp_pubsub, "pubsub_topic")
                }
            )},
        %% Note: The minimum deadline pubsub does is 10 s.
        {ack_deadline,
            mk(
                emqx_schema:timeout_duration_s(),
                #{
                    default => <<"60s">>,
                    desc => ?DESC(emqx_bridge_gcp_pubsub, "consumer_ack_deadline"),
                    validator => fun(X) ->
                        case X > 600 orelse X < 10 of
                            true ->
                                {error, <<"Value must be between 10 s and 600 s">>};
                            false ->
                                ok
                        end
                    end
                }
            )}
    ].

desc(Name) when
    Name =:= ?SOURCE_TYPE;
    Name =:= source_parameters
->
    ?DESC(Name);
desc(resource_opts) ->
    ?DESC(emqx_resource_schema, "resource_opts");
desc(_Name) ->
    undefined.

%%------------------------------------------------------------------------------
%% `emqx_bridge_v2_schema' "unofficial" API
%%------------------------------------------------------------------------------

source_examples(Method) ->
    [
        #{
            ?SOURCE_TYPE_BIN => #{
                summary => <<"GCP PubSub Consumer (gRPC) Source">>,
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
                ack_deadline => ~"10s",
                topic => ~"my-topic"
            },
        resource_opts =>
            #{
                request_ttl => <<"45s">>,
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
