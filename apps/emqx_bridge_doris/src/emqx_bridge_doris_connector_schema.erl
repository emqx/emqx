%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_doris_connector_schema).

-feature(maybe_expr, enable).

-behaviour(hocon_schema).
-behaviour(emqx_connector_examples).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include("emqx_bridge_doris.hrl").

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
    "connector_doris".

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
    Fields =
        lists:map(
            fun
                ({server = K, Sc}) ->
                    Override = #{
                        %% to please dialyzer...
                        type => hocon_schema:field_schema(Sc, type),
                        desc => ?DESC("server")
                    },
                    {K, hocon_schema:override(Sc, Override)};
                ({ssl = K, Sc}) ->
                    Override = #{type => hoconsc:ref(?MODULE, ssl)},
                    {K, hocon_schema:override(Sc, Override)};
                (Field) ->
                    Field
            end,
            emqx_mysql:fields(config)
        ),
    Fields ++ emqx_connector_schema:resource_opts();
fields(ssl) ->
    Fields0 = emqx_schema:fields("ssl_client_opts"),
    lists:map(
        fun
            ({"middlebox_comp_mode" = K, Sc}) ->
                Override = #{
                    %% to please dialyzer...
                    type => hocon_schema:field_schema(Sc, type),
                    default => false
                },
                {K, hocon_schema:override(Sc, Override)};
            (Field) ->
                Field
        end,
        Fields0
    ).

desc("config_connector") ->
    ?DESC("config_connector");
desc(ssl) ->
    ?DESC(emqx_connector_schema_lib, "ssl");
desc(_Name) ->
    undefined.

%%------------------------------------------------------------------------------
%% `emqx_connector_examples' API
%%------------------------------------------------------------------------------

connector_examples(Method) ->
    [
        #{
            <<"doris">> => #{
                summary => <<"Doris Connector">>,
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
        server => <<"doris-fe:9030">>,
        database => <<"test">>,
        pool_size => 8,
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
