%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_bridge_mqtt_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-import(hoconsc, [mk/2, ref/2]).

-export([roots/0, fields/1, desc/1, namespace/0]).

%%======================================================================================
%% Hocon Schema Definitions
namespace() -> "bridge_mqtt".

roots() -> [].

fields("config") ->
    %% enable
    emqx_bridge_schema:common_bridge_fields() ++
        [
            {resource_opts,
                mk(
                    ref(?MODULE, "creation_opts"),
                    #{
                        required => false,
                        default => #{},
                        desc => ?DESC(emqx_resource_schema, <<"resource_opts">>)
                    }
                )}
        ] ++
        emqx_bridge_mqtt_connector_schema:fields("config");
fields("creation_opts") ->
    Opts = emqx_resource_schema:fields("creation_opts"),
    [O || {Field, _} = O <- Opts, not is_hidden_opts(Field)];
fields("post") ->
    [type_field(), name_field() | fields("config")];
fields("put") ->
    fields("config");
fields("get") ->
    emqx_bridge_schema:status_fields() ++ fields("config").

desc("config") ->
    ?DESC("config");
desc("creation_opts" = Name) ->
    emqx_resource_schema:desc(Name);
desc(_) ->
    undefined.

%%======================================================================================
%% internal
is_hidden_opts(Field) ->
    lists:member(Field, [enable_batch, batch_size, batch_time]).

type_field() ->
    {type, mk(mqtt, #{required => true, desc => ?DESC("desc_type")})}.

name_field() ->
    {name, mk(binary(), #{required => true, desc => ?DESC("desc_name")})}.
