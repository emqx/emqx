%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_s3tables_action_schema).

-behaviour(hocon_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include("emqx_bridge_s3tables.hrl").

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
    "action_s3tables".

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
            hoconsc:map(name, hoconsc:ref(?MODULE, ?ACTION_TYPE)),
            #{
                desc => <<"S3Tables Action Config">>,
                required => false
            }
        )};
fields(?ACTION_TYPE) ->
    emqx_bridge_v2_schema:make_producer_action_schema(
        mk(
            hoconsc:ref(?MODULE, action_parameters),
            #{
                required => true,
                desc => ?DESC("parameters")
            }
        ),
        #{resource_opts_ref => ref(action_resource_opts)}
    );
fields(action_parameters) ->
    [
        {aggregation, mk(ref(aggregation), #{required => true, desc => ?DESC("aggregation")})},
        {namespace, mk(binary(), #{required => true, desc => ?DESC("namespace")})},
        {table, mk(binary(), #{required => true, desc => ?DESC("table")})},
        %% How can we handle different location providers other than S3 in the future?
        %% Specially since S3 http pool options live in the connector schema...
        {s3,
            mk(ref(s3_upload), #{
                desc => ?DESC("s3_upload"),
                validator => emqx_s3_schema:validators(s3_uploader),
                default => #{
                    <<"min_part_size">> => <<"5mb">>,
                    <<"max_part_size">> => <<"5gb">>
                }
            })}
    ];
fields(s3_upload) ->
    emqx_s3_schema:fields(s3_uploader);
fields(aggregation) ->
    [
        {time_interval,
            mk(
                emqx_schema:duration_s(),
                #{
                    required => false,
                    default => <<"120s">>,
                    desc => ?DESC("aggregation_interval")
                }
            )},
        {max_records,
            mk(
                pos_integer(),
                #{
                    required => false,
                    default => 100_000,
                    desc => ?DESC("aggregation_max_records")
                }
            )},
        {container,
            mk(
                emqx_schema:mkunion(
                    type,
                    #{
                        <<"avro">> => ref(container_avro),
                        <<"parquet">> => ref(container_parquet)
                    },
                    avro
                ),
                #{
                    default => #{<<"type">> => <<"avro">>},
                    desc => ?DESC("aggregation_container")
                }
            )}
    ];
fields(container_avro) ->
    [{type, mk(avro, #{desc => ?DESC("container_type_avro")})}];
fields(container_parquet) ->
    [
        {type, mk(parquet, #{desc => ?DESC("container_type_parquet")})},
        {write_old_list_structure,
            mk(boolean(), #{default => false, importance => ?IMPORTANCE_HIDDEN})},
        {enable_dictionary, mk(boolean(), #{default => false, importance => ?IMPORTANCE_HIDDEN})},
        {default_compression,
            mk(
                hoconsc:enum([none, zstd, snappy]),
                #{default => zstd, importance => ?IMPORTANCE_HIDDEN}
            )},
        {data_page_header_version,
            mk(
                hoconsc:enum([1, 2]),
                #{default => 2, importance => ?IMPORTANCE_HIDDEN}
            )},
        {max_row_group_bytes,
            mk(
                emqx_schema:bytesize(),
                #{default => <<"1MB">>, desc => ?DESC("container_parquet_max_row_group_bytes")}
            )}
    ];
fields(action_resource_opts) ->
    %% NOTE: This action benefits from generous batching defaults.
    emqx_bridge_v2_schema:action_resource_opts_fields([
        {batch_size, #{default => 10_000}},
        {batch_time, #{default => <<"60s">>}}
    ]).

desc(Name) when
    Name =:= ?ACTION_TYPE;
    Name =:= action_parameters;
    Name =:= aggregation;
    Name =:= s3_upload;
    Name =:= container_avro;
    Name =:= container_parquet;
    Name =:= parameters
->
    ?DESC(Name);
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
                summary => <<"S3Tables Action">>,
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
                aggregation => #{
                    time_interval => <<"120s">>,
                    max_records => 100_000
                },
                namespace => <<"my.ns">>,
                table => <<"my_table">>
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
%% API
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

mk(Type, Meta) -> hoconsc:mk(Type, Meta).
ref(Struct) -> hoconsc:ref(?MODULE, Struct).
