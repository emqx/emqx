%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_conf_others_section).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-behaviour(hocon_schema).

%% `hocon_schema' API
-export([
    namespace/0,
    roots/0,
    fields/1,
    translations/0,
    translation/1
]).

namespace() ->
    undefined.

roots() ->
    OtherSchemas = emqx_conf_schema:other_schemas() -- special_section_modules(),
    lists:flatmap(fun roots/1, OtherSchemas).

roots(Module) ->
    lists:map(fun({_BinName, Root}) -> Root end, hocon_schema:roots(Module)).

fields(Name) ->
    emqx_conf_schema:fields(Name).

translations() ->
    emqx_conf_schema:translations().

translation(Name) ->
    emqx_conf_schema:translation(Name).

%% modules from `emqx_conf_schema:other_schemas/0' that already have
%% their special sections.
special_section_modules() ->
    [
        emqx_authn_schema,
        emqx_authz_schema,
        emqx_bridge_schema,
        emqx_rule_engine_schema
    ].
