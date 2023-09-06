%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_kinesis).
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

%% hocon_schema API
-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

-export([
    conn_bridge_examples/1
]).

%%-------------------------------------------------------------------------------------------------
%% `hocon_schema' API
%%-------------------------------------------------------------------------------------------------

namespace() ->
    "bridge_kinesis".

roots() ->
    [].

fields("config_producer") ->
    emqx_bridge_schema:common_bridge_fields() ++
        fields("resource_opts") ++
        fields(connector_config) ++
        fields(producer);
fields("resource_opts") ->
    [
        {resource_opts,
            mk(
                ref(?MODULE, "creation_opts"),
                #{
                    required => false,
                    default => #{},
                    desc => ?DESC(emqx_resource_schema, "creation_opts")
                }
            )}
    ];
fields("creation_opts") ->
    emqx_resource_schema:create_opts([
        {batch_size, #{
            validator => emqx_resource_validator:max(int, 500)
        }}
    ]);
fields(connector_config) ->
    [
        {aws_access_key_id,
            mk(
                binary(),
                #{
                    required => true,
                    desc => ?DESC("aws_access_key_id")
                }
            )},
        {aws_secret_access_key,
            mk(
                binary(),
                #{
                    required => true,
                    desc => ?DESC("aws_secret_access_key"),
                    sensitive => true
                }
            )},
        {endpoint,
            mk(
                binary(),
                #{
                    required => true,
                    example => <<"https://kinesis.us-east-1.amazonaws.com">>,
                    desc => ?DESC("endpoint")
                }
            )},
        {max_retries,
            mk(
                non_neg_integer(),
                #{
                    required => false,
                    default => 2,
                    desc => ?DESC("max_retries")
                }
            )},
        {pool_size,
            sc(
                pos_integer(),
                #{
                    default => 8,
                    desc => ?DESC("pool_size")
                }
            )}
    ];
fields(producer) ->
    [
        {payload_template,
            sc(
                binary(),
                #{
                    default => <<"${.}">>,
                    desc => ?DESC("payload_template")
                }
            )},
        {local_topic,
            sc(
                binary(),
                #{
                    desc => ?DESC("local_topic")
                }
            )},
        {stream_name,
            sc(
                binary(),
                #{
                    required => true,
                    desc => ?DESC("stream_name")
                }
            )},
        {partition_key,
            sc(
                binary(),
                #{
                    required => true,
                    desc => ?DESC("partition_key")
                }
            )}
    ];
fields("get_producer") ->
    emqx_bridge_schema:status_fields() ++ fields("post_producer");
fields("post_producer") ->
    [type_field_producer(), name_field() | fields("config_producer")];
fields("put_producer") ->
    fields("config_producer").

desc("config_producer") ->
    ?DESC("desc_config");
desc("creation_opts") ->
    ?DESC(emqx_resource_schema, "creation_opts");
desc(_) ->
    undefined.

conn_bridge_examples(Method) ->
    [
        #{
            <<"kinesis_producer">> => #{
                summary => <<"Amazon Kinesis Producer Bridge">>,
                value => values(producer, Method)
            }
        }
    ].

values(producer, _Method) ->
    #{
        aws_access_key_id => <<"aws_access_key_id">>,
        aws_secret_access_key => <<"******">>,
        endpoint => <<"https://kinesis.us-east-1.amazonaws.com">>,
        max_retries => 3,
        stream_name => <<"stream_name">>,
        partition_key => <<"key">>,
        resource_opts => #{
            worker_pool_size => 1,
            health_check_interval => 15000,
            query_mode => async,
            inflight_window => 100,
            max_buffer_bytes => 100 * 1024 * 1024
        }
    }.

%%-------------------------------------------------------------------------------------------------
%% Helper fns
%%-------------------------------------------------------------------------------------------------

sc(Type, Meta) -> hoconsc:mk(Type, Meta).

mk(Type, Meta) -> hoconsc:mk(Type, Meta).

enum(OfSymbols) -> hoconsc:enum(OfSymbols).

ref(Module, Name) -> hoconsc:ref(Module, Name).

type_field_producer() ->
    {type, mk(enum([kinesis_producer]), #{required => true, desc => ?DESC("desc_type")})}.

name_field() ->
    {name, mk(binary(), #{required => true, desc => ?DESC("desc_name")})}.
