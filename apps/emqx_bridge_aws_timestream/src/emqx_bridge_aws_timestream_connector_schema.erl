%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_aws_timestream_connector_schema).

-behaviour(hocon_schema).
-behaviour(emqx_connector_examples).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include("emqx_bridge_aws_timestream.hrl").

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
    "connector_aws_timestream".

roots() ->
    [].

fields(Field) when
    Field == "get_connector";
    Field == "put_connector";
    Field == "post_connector"
->
    emqx_connector_schema:api_fields(Field, ?CONNECTOR_TYPE, fields(connector_config));
fields("config_connector") ->
    override_parameters(emqx_bridge_influxdb:fields("config_connector"));
fields(connector_config) ->
    override_parameters(emqx_bridge_influxdb:fields(connector_config)).

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
            <<"aws_timestream">> => #{
                summary => <<"AWS Timestream Connector">>,
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
        server => <<"my.timestream.endpoint:8086">>,
        pool_size => 8,
        ssl => #{enable => true},
        parameters => #{
            influxdb_type => influxdb_api_v2,
            bucket => <<"example_bucket">>,
            org => <<"examlpe_org">>,
            token => <<"example_token">>
        },
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

override_parameters(Fields) ->
    lists:map(
        fun
            ({parameters = Key, Sc}) ->
                Overrides = #{
                    type => hoconsc:union([
                        hoconsc:ref(
                            emqx_bridge_influxdb_connector,
                            "connector_influxdb_api_v2"
                        ),
                        hoconsc:ref(
                            emqx_bridge_influxdb_connector,
                            "connector_influxdb_api_v3"
                        )
                    ])
                },
                {Key, hocon_schema:override(Sc, Overrides)};
            (Field) ->
                Field
        end,
        Fields
    ).
