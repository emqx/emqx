%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_bridge_v2_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").

-import(hoconsc, [mk/2, ref/2]).

-export([roots/0, fields/1, desc/1, namespace/0, tags/0]).

-export([
    get_response/0,
    put_request/0,
    post_request/0
]).

-if(?EMQX_RELEASE_EDITION == ee).
enterprise_api_schemas(Method) ->
    %% We *must* do this to ensure the module is really loaded, especially when we use
    %% `call_hocon' from `nodetool' to generate initial configurations.
    _ = emqx_bridge_v2_enterprise:module_info(),
    case erlang:function_exported(emqx_bridge_v2_enterprise, api_schemas, 1) of
        true -> emqx_bridge_v2_enterprise:api_schemas(Method);
        false -> []
    end.

enterprise_fields_actions() ->
    %% We *must* do this to ensure the module is really loaded, especially when we use
    %% `call_hocon' from `nodetool' to generate initial configurations.
    _ = emqx_bridge_v2_enterprise:module_info(),
    case erlang:function_exported(emqx_bridge_v2_enterprise, fields, 1) of
        true ->
            emqx_bridge_v2_enterprise:fields(bridges_v2);
        false ->
            []
    end.

-else.

enterprise_api_schemas(_Method) -> [].

enterprise_fields_actions() -> [].

-endif.

%%======================================================================================
%% For HTTP APIs
get_response() ->
    api_schema("get").

put_request() ->
    api_schema("put").

post_request() ->
    api_schema("post").

api_schema(Method) ->
    EE = enterprise_api_schemas(Method),
    hoconsc:union(bridge_api_union(EE)).

bridge_api_union(Refs) ->
    Index = maps:from_list(Refs),
    fun
        (all_union_members) ->
            maps:values(Index);
        ({value, V}) ->
            case V of
                #{<<"type">> := T} ->
                    case maps:get(T, Index, undefined) of
                        undefined ->
                            throw(#{
                                field_name => type,
                                value => T,
                                reason => <<"unknown bridge type">>
                            });
                        Ref ->
                            [Ref]
                    end;
                _ ->
                    maps:values(Index)
            end
    end.

%%======================================================================================
%% HOCON Schema Callbacks
%%======================================================================================

namespace() -> "bridges_v2".

tags() ->
    [<<"Bridge V2">>].

-dialyzer({nowarn_function, roots/0}).

roots() ->
    case fields(bridges_v2) of
        [] ->
            [
                {bridges_v2,
                    ?HOCON(hoconsc:map(name, typerefl:map()), #{importance => ?IMPORTANCE_LOW})}
            ];
        _ ->
            [{bridges_v2, ?HOCON(?R_REF(bridges_v2), #{importance => ?IMPORTANCE_LOW})}]
    end.

fields(bridges_v2) ->
    [] ++ enterprise_fields_actions().

desc(bridges_v2) ->
    ?DESC("desc_bridges_v2");
desc(_) ->
    undefined.
