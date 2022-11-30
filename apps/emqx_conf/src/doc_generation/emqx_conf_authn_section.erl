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

-module(emqx_conf_authn_section).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/emqx_authentication.hrl").

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
    %% this is needed to correctly load the authn modules and their
    %% fields
    PtKey = ?EMQX_AUTHENTICATION_SCHEMA_MODULE_PT_KEY,
    case persistent_term:get(PtKey, undefined) of
        undefined -> persistent_term:put(PtKey, emqx_authn_schema);
        _ -> ok
    end,
    Roots = [
        Entry
     || Entry <- emqx_schema:roots(high),
        element(1, Entry) =:= ?EMQX_AUTHENTICATION_CONFIG_ROOT_NAME
    ],
    Roots.

fields(Name) ->
    emqx_conf_schema:fields(Name).

translations() ->
    emqx_conf_schema:translations().

translation(Name) ->
    emqx_conf_schema:translation(Name).
