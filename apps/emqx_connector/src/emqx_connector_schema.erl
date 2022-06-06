%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_connector_schema).

-behaviour(hocon_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-export([namespace/0, roots/0, fields/1, desc/1]).

-export([
    get_response/0,
    put_request/0,
    post_request/0
]).

%% the config for webhook bridges do not need connectors
-define(CONN_TYPES, [mqtt]).

%%======================================================================================
%% For HTTP APIs

get_response() ->
    http_schema("get").

put_request() ->
    http_schema("put").

post_request() ->
    http_schema("post").

http_schema(Method) ->
    Schemas = [?R_REF(schema_mod(Type), Method) || Type <- ?CONN_TYPES],
    ?UNION(Schemas).

%%======================================================================================
%% Hocon Schema Definitions

namespace() -> connector.

roots() -> ["connectors"].

fields(connectors) ->
    fields("connectors");
fields("connectors") ->
    [
        {mqtt,
            ?HOCON(
                ?MAP(name, ?R_REF(emqx_connector_mqtt_schema, "connector")),
                #{desc => ?DESC("mqtt")}
            )}
    ].

desc(Record) when
    Record =:= connectors;
    Record =:= "connectors"
->
    ?DESC("desc_connector");
desc(_) ->
    undefined.

schema_mod(Type) ->
    list_to_atom(lists:concat(["emqx_connector_", Type])).
