%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-export([namespace/0, roots/0, fields/1, desc/1]).
-export([zones_without_default/0, global_zone_with_default/0]).

namespace() -> zone.

%% Zone values are never checked as root level.
%% We need roots defined here because it's used to generate config API schema.
roots() ->
    [
        mqtt,
        stats,
        flapping_detect,
        force_shutdown,
        conn_congestion,
        force_gc,
        overload_protection,
        durable_sessions
    ].

zones_without_default() ->
    Fields = roots(),
    Hidden = hidden(),
    lists:map(
        fun(F) ->
            case lists:member(F, Hidden) of
                true ->
                    {F,
                        ?HOCON(?R_REF(?MODULE, atom_to_list(F)), #{importance => ?IMPORTANCE_HIDDEN})};
                false ->
                    {F, ?HOCON(?R_REF(?MODULE, atom_to_list(F)), #{})}
            end
        end,
        Fields
    ).

global_zone_with_default() ->
    lists:map(
        fun(F) -> {F, ?HOCON(?R_REF(emqx_schema, atom_to_list(F)), #{})} end, roots() -- hidden()
    ).

hidden() ->
    [
        stats,
        overload_protection,
        conn_congestion
    ].

%% zone schemas are clones from the same name from root level
%% only not allowed to have default values.
fields(Name) ->
    [{N, no_default(Sc)} || {N, Sc} <- emqx_schema:fields(Name)].

desc(Name) ->
    emqx_schema:desc(Name).

%% no default values for zone settings, don't required either.
no_default(Sc) ->
    fun
        (default) -> undefined;
        (required) -> false;
        (Other) -> hocon_schema:field_schema(Sc, Other)
    end.
