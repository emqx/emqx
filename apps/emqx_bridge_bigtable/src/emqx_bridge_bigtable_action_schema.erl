%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_bigtable_action_schema).

-behaviour(hocon_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include("emqx_bridge_bigtable.hrl").

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
    "action_bigtable".

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
                desc => <<"Bigtable Action Config">>,
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
        {instance_id, mk(binary(), #{desc => ?DESC("instance_id")})},
        {table_id, mk(binary(), #{desc => ?DESC("table_id")})},
        {row_key, mk(binary(), #{desc => ?DESC("row_key")})},
        {mutations,
            mk(
                hoconsc:array(
                    emqx_schema:mkunion(type, #{
                        <<"set_cell">> => ref(set_cell_parameters)
                    })
                ),
                #{
                    desc => ?DESC("mutations"),
                    validator => fun emqx_schema:non_empty_array/1
                }
            )}
    ];
fields(set_cell_parameters) ->
    [
        {type, mk(set_cell, #{desc => ?DESC("set_cell_type")})},
        {family_name, mk(binary(), #{required => true, desc => ?DESC("set_cell_family_name")})},
        {column_qualifier,
            mk(binary(), #{required => true, desc => ?DESC("set_cell_column_qualifier")})},
        {timestamp_micros,
            mk(binary(), #{required => true, desc => ?DESC("set_cell_timestamp_micros")})},
        {value, mk(binary(), #{required => true, desc => ?DESC("set_cell_value")})}
    ].

desc(Name) when
    Name =:= ?ACTION_TYPE;
    Name =:= action_parameters
->
    ?DESC(Name);
desc(set_cell_parameters) ->
    ?DESC("set_cell_type");
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
                summary => <<"Bigtable Action">>,
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
                instance_id => <<"myinst">>,
                table_id => <<"mytable">>,
                row_key => <<"clientid">>,
                mutations => [
                    #{
                        type => <<"set_cell">>,
                        family_name => <<"colfam">>,
                        column_qualifier => <<"mycol">>,
                        timestamp_micros => <<"-1">>,
                        value => <<"payload">>
                    }
                ]
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

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

mk(Type, Meta) -> hoconsc:mk(Type, Meta).
ref(Struct) -> hoconsc:ref(?MODULE, Struct).
