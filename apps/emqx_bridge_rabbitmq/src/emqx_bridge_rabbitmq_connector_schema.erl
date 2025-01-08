%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_bridge_rabbitmq_connector_schema).

-behaviour(emqx_connector_examples).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-define(TYPE, rabbitmq).

-export([roots/0, fields/1, desc/1, namespace/0]).
-export([connector_examples/1, connector_example_values/0]).

%%======================================================================================
%% Hocon Schema Definitions
namespace() -> ?TYPE.

roots() -> [].

fields("config_connector") ->
    emqx_bridge_schema:common_bridge_fields() ++
        fields(connector) ++
        emqx_connector_schema:resource_opts_ref(?MODULE, connector_resource_opts);
fields(connector) ->
    [
        {server,
            ?HOCON(
                string(),
                #{
                    default => <<"localhost">>,
                    desc => ?DESC("server")
                }
            )},
        {port,
            ?HOCON(
                emqx_schema:port_number(),
                #{
                    default => 5672,
                    desc => ?DESC("server")
                }
            )},
        {username,
            ?HOCON(
                binary(),
                #{
                    required => true,
                    desc => ?DESC("username")
                }
            )},
        {password, emqx_connector_schema_lib:password_field(#{required => true})},
        {pool_size,
            ?HOCON(
                pos_integer(),
                #{
                    default => 8,
                    desc => ?DESC("pool_size")
                }
            )},
        {timeout,
            ?HOCON(
                emqx_schema:timeout_duration_ms(),
                #{
                    default => <<"5s">>,
                    desc => ?DESC("timeout")
                }
            )},
        {virtual_host,
            ?HOCON(
                binary(),
                #{
                    default => <<"/">>,
                    desc => ?DESC("virtual_host")
                }
            )},
        {heartbeat,
            ?HOCON(
                emqx_schema:timeout_duration_ms(),
                #{
                    default => <<"30s">>,
                    desc => ?DESC("heartbeat")
                }
            )}
    ] ++
        emqx_connector_schema_lib:ssl_fields();
fields(connector_resource_opts) ->
    emqx_connector_schema:resource_opts_fields();
fields("post") ->
    emqx_connector_schema:type_and_name_fields(?TYPE) ++ fields("config_connector");
fields("put") ->
    fields("config_connector");
fields("get") ->
    emqx_bridge_schema:status_fields() ++ fields("config_connector").

desc("config_connector") ->
    ?DESC("config_connector");
desc(connector_resource_opts) ->
    ?DESC(connector_resource_opts);
desc(_) ->
    undefined.

connector_examples(Method) ->
    [
        #{
            <<"rabbitmq">> =>
                #{
                    summary => <<"Rabbitmq Connector">>,
                    value => emqx_connector_schema:connector_values(
                        Method, ?TYPE, connector_example_values()
                    )
                }
        }
    ].

connector_example_values() ->
    #{
        name => <<"rabbitmq_connector">>,
        type => rabbitmq,
        enable => true,
        server => <<"127.0.0.1">>,
        port => 5672,
        username => <<"guest">>,
        password => <<"******">>,
        pool_size => 8,
        timeout => <<"5s">>,
        virtual_host => <<"/">>,
        heartbeat => <<"30s">>,
        ssl => #{enable => false}
    }.
