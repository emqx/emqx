%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_postgresql_connector_schema).

-behaviour(emqx_connector_examples).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx_postgresql/include/emqx_postgresql.hrl").

-define(PGSQL_HOST_OPTIONS, #{
    default_port => ?PGSQL_DEFAULT_PORT
}).

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

%% Examples
-export([
    connector_examples/1,
    values/1
]).

-define(CONNECTOR_TYPE, pgsql).

namespace() ->
    "connector_postgres".

roots() ->
    [].

fields("connection_fields") ->
    [
        {server, server()},
        emqx_postgresql:disable_prepared_statements()
    ] ++
        adjust_fields(emqx_connector_schema_lib:relational_db_fields()) ++
        emqx_connector_schema_lib:ssl_fields();
fields("config_connector") ->
    fields("connection_fields") ++
        emqx_connector_schema:common_fields() ++
        emqx_connector_schema:resource_opts_ref(?MODULE, resource_opts);
fields(resource_opts) ->
    emqx_connector_schema:resource_opts_fields();
fields(config) ->
    fields("config_connector") ++
        fields(action);
fields(action) ->
    {pgsql,
        hoconsc:mk(
            hoconsc:map(name, hoconsc:ref(emqx_bridge_pgsql, pgsql_action)),
            #{
                desc => <<"PostgreSQL Action Config">>,
                required => false
            }
        )};
fields(pgsql_action) ->
    emqx_bridge_v2_schema:make_producer_action_schema(hoconsc:ref(?MODULE, action_parameters));
fields("put_bridge_v2") ->
    fields(pgsql_action);
fields("get_bridge_v2") ->
    fields(pgsql_action);
fields("post_bridge_v2") ->
    fields(pgsql_action);
fields(Field) when
    Field == "get_connector";
    Field == "put_connector";
    Field == "post_connector"
->
    fields({Field, ?CONNECTOR_TYPE});
fields({Field, Type}) when
    Field == "get_connector";
    Field == "put_connector";
    Field == "post_connector"
->
    Fields =
        fields("connection_fields") ++
            emqx_connector_schema:resource_opts_ref(?MODULE, resource_opts),
    emqx_connector_schema:api_fields(Field, Type, Fields).

server() ->
    Meta = #{desc => ?DESC("server")},
    emqx_schema:servers_sc(Meta, ?PGSQL_HOST_OPTIONS).

adjust_fields(Fields) ->
    lists:map(
        fun
            ({username, Sc}) ->
                %% to please dialyzer...
                Override = #{type => hocon_schema:field_schema(Sc, type), required => true},
                {username, hocon_schema:override(Sc, Override)};
            (Field) ->
                Field
        end,
        Fields
    ).

%% Examples
connector_examples(Method) ->
    [
        #{
            <<"pgsql">> => #{
                summary => <<"PostgreSQL Connector">>,
                value => values({Method, <<"pgsql">>})
            }
        }
    ].

%% TODO: All of these needs to be adjusted from Kafka to PostgreSQL
values({get, PostgreSQLType}) ->
    maps:merge(
        #{
            status => <<"connected">>,
            node_status => [
                #{
                    node => <<"emqx@localhost">>,
                    status => <<"connected">>
                }
            ],
            actions => [<<"my_action">>]
        },
        values({post, PostgreSQLType})
    );
values({post, PostgreSQLType}) ->
    maps:merge(
        #{
            name => <<"my_", PostgreSQLType/binary, "_connector">>,
            type => PostgreSQLType
        },
        values(common)
    );
values({put, _PostgreSQLType}) ->
    values(common);
values(common) ->
    #{
        <<"database">> => <<"emqx_data">>,
        <<"enable">> => true,
        <<"password">> => <<"public">>,
        <<"pool_size">> => 8,
        <<"server">> => <<"127.0.0.1:5432">>,
        <<"ssl">> => #{
            <<"ciphers">> => [],
            <<"depth">> => 10,
            <<"enable">> => false,
            <<"hibernate_after">> => <<"5s">>,
            <<"log_level">> => <<"notice">>,
            <<"reuse_sessions">> => true,
            <<"secure_renegotiate">> => true,
            <<"verify">> => <<"verify_peer">>,
            <<"versions">> => [<<"tlsv1.3">>, <<"tlsv1.2">>]
        },
        <<"username">> => <<"postgres">>
    }.

desc("config_connector") ->
    ?DESC("config_connector");
desc(resource_opts) ->
    ?DESC(emqx_resource_schema, "resource_opts");
desc(_) ->
    undefined.
