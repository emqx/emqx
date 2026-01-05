%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_emqx_tables_action_schema).

-behaviour(hocon_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include("emqx_bridge_emqx_tables.hrl").

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
    "action_emqx_tables".

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
                desc => <<"EMQX Tables Action Config">>,
                required => false
            }
        )};
fields(?ACTION_TYPE) ->
    emqx_bridge_v2_schema:make_producer_action_schema(
        mk(hoconsc:ref(emqx_bridge_greptimedb, action_parameters), #{
            required => true, desc => ?DESC("parameters")
        }),
        #{resource_opts_ref => ref(action_resource_opts)}
    );
fields(action_resource_opts) ->
    emqx_bridge_v2_schema:action_resource_opts_fields([
        {batch_size, #{default => 1000}},
        {batch_time, #{default => <<"100ms">>}}
    ]).

desc(Name) when
    Name =:= ?ACTION_TYPE;
    Name =:= action_parameters
->
    ?DESC(Name);
desc(action_resource_opts) ->
    emqx_bridge_v2_schema:desc(action_resource_opts);
desc(_Name) ->
    undefined.

%%------------------------------------------------------------------------------
%% `emqx_bridge_v2_schema' "unofficial" API
%%------------------------------------------------------------------------------

bridge_v2_examples(Method) ->
    [
        #{
            ?ACTION_TYPE_BIN => #{
                summary => <<"EMQX Tables Action">>,
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
                write_syntax => write_syntax_value(),
                precision => ms
            },
        resource_opts =>
            #{
                batch_time => <<"100ms">>,
                batch_size => 1000,
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

write_syntax_value() ->
    <<"${topic},clientid=${clientid}", " ", "payload=${payload},",
        "${clientid}_int_value=${payload.int_key}i,",
        "uint_value=${payload.uint_key}u,"
        "bool=${payload.bool}">>.
