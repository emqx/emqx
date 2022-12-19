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

-module(emqx_connector_jwt).

-include_lib("emqx_connector/include/emqx_connector_tables.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

%% API
-export([
    lookup_jwt/1,
    lookup_jwt/2
]).

-type jwt() :: binary().

-spec lookup_jwt(resource_id()) -> {ok, jwt()} | {error, not_found}.
lookup_jwt(ResourceId) ->
    ?MODULE:lookup_jwt(?JWT_TABLE, ResourceId).

-spec lookup_jwt(ets:table(), resource_id()) -> {ok, jwt()} | {error, not_found}.
lookup_jwt(TId, ResourceId) ->
    try
        case ets:lookup(TId, {ResourceId, jwt}) of
            [{{ResourceId, jwt}, JWT}] ->
                {ok, JWT};
            [] ->
                {error, not_found}
        end
    catch
        error:badarg ->
            {error, not_found}
    end.
