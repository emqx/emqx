%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_azure_blob_storage_action_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include("emqx_bridge_azure_blob_storage.hrl").

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

%%-------------------------------------------------------------------------------------------------
%% `hocon_schema' API
%%-------------------------------------------------------------------------------------------------

namespace() ->
    "action_azure_blob_storage".

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
                desc => <<"Azure Blob Storage Action Config">>,
                required => false
            }
        )};
fields(?ACTION_TYPE) ->
    emqx_bridge_v2_schema:make_producer_action_schema(
        mk(
            emqx_schema:mkunion(mode, #{
                <<"direct">> => ref(direct_parameters),
                <<"aggregated">> => ref(aggreg_parameters)
            }),
            #{
                required => true,
                desc => ?DESC("parameters")
            }
        ),
        #{
            resource_opts_ref => ref(action_resource_opts)
        }
    );
fields(direct_parameters) ->
    [
        {mode, mk(direct, #{required => true, desc => ?DESC("direct_mode")})},
        %% Container in the Azure Blob Storage domain, not aggregation.
        {container,
            mk(
                emqx_schema:template_str(),
                #{
                    desc => ?DESC("direct_container_template"),
                    required => true
                }
            )},
        {blob,
            mk(
                emqx_schema:template_str(),
                #{
                    desc => ?DESC("direct_blob_template"),
                    required => true
                }
            )},
        {content,
            mk(
                emqx_schema:template(),
                #{
                    required => false,
                    default => <<"${.}">>,
                    desc => ?DESC("direct_content_template")
                }
            )}
        | fields(common_action_parameters)
    ];
fields(aggreg_parameters) ->
    [
        {mode, mk(aggregated, #{required => true, desc => ?DESC("aggregated_mode")})},
        {aggregation, mk(ref(aggregation), #{required => true, desc => ?DESC("aggregation")})},
        %% Container in the Azure Blob Storage domain, not aggregation.
        {container,
            mk(
                string(),
                #{
                    desc => ?DESC("aggregated_container_name"),
                    required => true
                }
            )},
        {blob,
            mk(
                emqx_schema:template_str(),
                #{
                    desc => ?DESC("aggregated_blob_template"),
                    required => true
                }
            )},
        {min_block_size,
            mk(
                emqx_schema:bytesize(),
                #{
                    default => <<"10mb">>,
                    importance => ?IMPORTANCE_HIDDEN,
                    required => true,
                    validator => fun block_size_validator/1
                }
            )}
        | fields(common_action_parameters)
    ];
fields(aggregation) ->
    [
        emqx_connector_aggregator_schema:container(),
        %% TODO: Needs bucketing? (e.g. messages falling in this 1h interval)
        {time_interval,
            hoconsc:mk(
                emqx_schema:duration_s(),
                #{
                    required => false,
                    default => <<"1h">>,
                    desc => ?DESC("aggregation_interval")
                }
            )},
        {max_records,
            hoconsc:mk(
                pos_integer(),
                #{
                    required => false,
                    default => 1_000_000,
                    desc => ?DESC("aggregation_max_records")
                }
            )}
    ];
fields(common_action_parameters) ->
    [
        {max_block_size,
            mk(
                emqx_schema:bytesize(),
                #{
                    default => <<"4000mb">>,
                    importance => ?IMPORTANCE_HIDDEN,
                    desc => ?DESC("max_block_size"),
                    required => true,
                    validator => fun block_size_validator/1
                }
            )}
    ];
fields(action_resource_opts) ->
    %% NOTE: This action should benefit from generous batching defaults.
    emqx_bridge_v2_schema:action_resource_opts_fields([
        {batch_size, #{default => 100}},
        {batch_time, #{default => <<"10ms">>}}
    ]).

desc(Name) when
    Name =:= ?ACTION_TYPE;
    Name =:= aggregation;
    Name =:= aggreg_parameters;
    Name =:= direct_parameters
->
    ?DESC(Name);
desc(action_resource_opts) ->
    ?DESC(emqx_resource_schema, "resource_opts");
desc(_Name) ->
    undefined.

%%-------------------------------------------------------------------------------------------------
%% `emqx_bridge_v2_schema' "unofficial" API
%%-------------------------------------------------------------------------------------------------

bridge_v2_examples(Method) ->
    [
        #{
            <<"aggregated_", ?ACTION_TYPE_BIN/binary>> => #{
                summary => <<"Azure Blob Storage Aggregated Upload Action">>,
                value => action_example(Method, aggregated)
            },
            <<"direct_", ?ACTION_TYPE_BIN/binary>> => #{
                summary => <<"Azure Blob Storage Direct Upload Action">>,
                value => action_example(Method, direct)
            }
        }
    ].

action_example(post, Mode) ->
    maps:merge(
        action_example(put, Mode),
        #{
            type => ?ACTION_TYPE_BIN,
            name => <<"my_action">>
        }
    );
action_example(get, Mode) ->
    maps:merge(
        action_example(put, Mode),
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
action_example(put, aggregated) ->
    #{
        enable => true,
        description => <<"my action">>,
        connector => <<"my_connector">>,
        parameters =>
            #{
                mode => <<"aggregated">>,
                aggregation => #{
                    container => #{
                        type => <<"csv">>,
                        column_order => [<<"a">>, <<"b">>]
                    },
                    time_interval => <<"4s">>,
                    max_records => 10_000
                },
                container => <<"mycontainer">>,
                blob => <<"${action}/${node}/${datetime.rfc3339}/${sequence}">>
            },
        resource_opts =>
            #{
                batch_time => <<"10ms">>,
                batch_size => 100,
                health_check_interval => <<"30s">>,
                inflight_window => 100,
                query_mode => <<"sync">>,
                request_ttl => <<"45s">>,
                worker_pool_size => 16
            }
    };
action_example(put, direct) ->
    #{
        enable => true,
        description => <<"my action">>,
        connector => <<"my_connector">>,
        parameters =>
            #{
                mode => <<"direct">>,
                container => <<"${.payload.container}">>,
                blob => <<"${.payload.blob}">>,
                content => <<"${.payload}">>
            },
        resource_opts =>
            #{
                batch_time => <<"0ms">>,
                batch_size => 1,
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

block_size_validator(SizeLimit) ->
    case SizeLimit =< 4_000 * 1024 * 1024 of
        true -> ok;
        false -> {error, "must be less than 4000 MiB"}
    end.
