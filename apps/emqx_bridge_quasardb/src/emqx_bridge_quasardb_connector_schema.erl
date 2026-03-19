%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_quasardb_connector_schema).

-behaviour(hocon_schema).
-behaviour(emqx_connector_examples).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include("emqx_bridge_quasardb.hrl").

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
    "connector_quasardb".

roots() ->
    [].

fields(Field) when
    Field == "get_connector";
    Field == "put_connector";
    Field == "post_connector"
->
    emqx_connector_schema:api_fields(Field, ?CONNECTOR_TYPE, fields(connector_config));
fields("config_connector") ->
    emqx_connector_schema:common_fields() ++ fields(connector_config);
fields(connector_config) ->
    [
        {dsn, mk(binary(), #{required => true, desc => ?DESC("dsn")})},
        {uri, mk(binary(), #{required => true, desc => ?DESC("uri")})},
        {username, mk(binary(), #{required => false, desc => ?DESC("username")})},
        {password, emqx_schema_secret:mk(#{required => false, desc => ?DESC("password")})},
        {cluster_public_key,
            mk(binary(), #{required => false, desc => ?DESC("cluster_public_key")})},
        {pool_size, mk(pos_integer(), #{default => 8, desc => ?DESC("pool_size")})},
        {connect_timeout,
            mk(emqx_schema:timeout_duration_ms(), #{
                default => <<"5s">>, desc => ?DESC("connect_timeout")
            })}
    ] ++
        emqx_connector_schema:resource_opts().

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
            <<"quasardb">> => #{
                summary => <<"QuasarDB Connector">>,
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
            name => <<"pmy_connector">>
        }
    );
connector_example(put) ->
    #{
        enable => true,
        description => <<"My connector">>,
        dsb => <<"qdb">>,
        uri => <<"qdb://127.0.0.1:2836">>,
        cluster_public_key => <<"cpk">>,
        username => <<"root">>,
        password => <<"******">>,
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

mk(Type, Meta) -> hoconsc:mk(Type, Meta).
%% ref(Struct) -> hoconsc:ref(?MODULE, Struct).
