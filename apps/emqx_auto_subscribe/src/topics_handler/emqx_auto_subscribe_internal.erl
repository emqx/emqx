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
-module(emqx_auto_subscribe_internal).

-export([init/1]).

-export([handle/3]).

-spec init(Config :: map()) -> HandlerOptions :: term().
init(#{topics := Topics}) ->
    emqx_auto_subscribe_placeholder:generate(Topics).

-spec handle(ClientInfo :: map(), ConnInfo :: map(), HandlerOptions :: term()) ->
    TopicTables :: list().
handle(ClientInfo, ConnInfo, PlaceHolders) ->
    emqx_auto_subscribe_placeholder:to_topic_table(PlaceHolders, ClientInfo, ConnInfo).
