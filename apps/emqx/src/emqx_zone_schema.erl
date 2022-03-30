%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_zone_schema).

-export([namespace/0, roots/0, fields/1, desc/1]).

namespace() -> zone.

%% this schema module is not used at root level.
%% roots are added only for document generation.
roots() ->
    [
        "mqtt",
        "stats",
        "flapping_detect",
        "force_shutdown",
        "conn_congestion",
        "force_gc",
        "overload_protection"
    ].

%% zone schemas are clones from the same name from root level
%% only not allowed to have default values.
fields(Name) ->
    [{N, no_default(Sc)} || {N, Sc} <- emqx_schema:fields(Name)].

desc(Name) ->
    emqx_schema:desc(Name).

%% no default values for zone settings
no_default(Sc) ->
    fun
        (default) -> undefined;
        (Other) -> hocon_schema:field_schema(Sc, Other)
    end.
