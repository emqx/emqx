%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mtns).

-include("emqx_cm.hrl").

-export([
    cid/1,
    cid/2,
    get_mtns/1,
    get_clientid/1,
    parse_cid/1
]).

-spec cid(emqx_types:clientinfo()) -> emqx_types:cid().
cid(ClientInfo = #{clientid := ClientId}) ->
    {get_mtns(ClientInfo), ClientId}.

-spec cid(emqx_types:mtns(), emqx_types:clientid()) -> emqx_types:cid().
cid(Mtns, ClientId) ->
    {Mtns, ClientId}.

-spec get_mtns(emqx_types:clientinfo()) -> emqx_types:mtns().
get_mtns(ClientInfo) ->
    emqx_utils_maps:deep_get([client_attr, mtns], ClientInfo, undefined).

-spec get_clientid(emqx_types:cid()) -> emqx_types:clientid().
get_clientid({_, Id}) -> Id.

-spec parse_cid(emqx_types:clientid() | emqx_types:cid()) ->
    {emqx_types:mtns(), emqx_types:clientid()}.
parse_cid(ClientId) when is_binary(ClientId) ->
    {?NO_MTNS, ClientId};
parse_cid({Mtns, ClientId}) ->
    {Mtns, ClientId}.
