%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_snowflake_connector_schema).

-behaviour(hocon_schema).
-behaviour(emqx_connector_examples).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include("emqx_bridge_snowflake.hrl").

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

%%-------------------------------------------------------------------------------------------------
%% `hocon_schema' API
%%-------------------------------------------------------------------------------------------------

namespace() ->
    "connector_snowflake".

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
    Fields0 = emqx_connector_schema_lib:relational_db_fields(),
    Fields1 = proplists:delete(database, Fields0),
    Fields = lists:map(
        fun
            ({Field, Sc}) when Field =:= username; Field =:= password ->
                Override = #{type => hocon_schema:field_schema(Sc, type), required => true},
                {Field, hocon_schema:override(Sc, Override)};
            ({Field, Sc}) ->
                {Field, Sc}
        end,
        Fields1
    ),
    [
        {server,
            emqx_schema:servers_sc(
                #{required => true, desc => ?DESC("server")},
                ?SERVER_OPTS
            )},
        {account,
            mk(binary(), #{
                required => true,
                desc => ?DESC("account"),
                validator => fun account_id_validator/1
            })},
        {dsn, mk(binary(), #{required => true, desc => ?DESC("dsn")})},
        {proxy,
            mk(
                hoconsc:union([none, hoconsc:ref(?MODULE, proxy_config)]),
                #{default => none, desc => ?DESC("proxy_config")}
            )}
        | Fields
    ] ++
        emqx_connector_schema:resource_opts() ++
        emqx_connector_schema_lib:ssl_fields();
fields(proxy_config) ->
    [
        {host, mk(binary(), #{required => true, desc => ?DESC("proxy_config_host")})},
        {port,
            mk(emqx_schema:port_number(), #{required => true, desc => ?DESC("proxy_config_port")})}
    ].

desc("config_connector") ->
    ?DESC("config_connector");
desc(resource_opts) ->
    ?DESC(emqx_resource_schema, resource_opts);
desc(proxy_config) ->
    ?DESC("proxy_config");
desc(_Name) ->
    undefined.

%%-------------------------------------------------------------------------------------------------
%% `emqx_connector_examples' API
%%-------------------------------------------------------------------------------------------------

connector_examples(Method) ->
    [
        #{
            <<"snowflake">> => #{
                summary => <<"Snowflake Connector">>,
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
        server => <<"myorg-myaccount.snowflakecomputing.com">>,
        account => <<"myorg-myaccount">>,
        username => <<"admin">>,
        password => <<"******">>,
        dsn => <<"snowflake">>,
        pool_size => 8,
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

account_id_validator(AccountId) ->
    case binary:split(AccountId, <<"-">>) of
        [_, _] ->
            ok;
        _ ->
            {error, <<"Account identifier must be of form ORGID-ACCOUNTNAME">>}
    end.
