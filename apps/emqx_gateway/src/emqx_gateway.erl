%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_gateway).

-include("include/emqx_gateway.hrl").

%% APIs
-export([ registered_gateway/0
        , create/5
        , remove/2
        , update/2
        , start/2
        , stop/2
        , list/0
        ]).

-spec registered_gateway() ->
    [{gateway_id(), emqx_gateway_registry:descriptor()}].
registered_gateway() ->
    emqx_gateway_registry:list().

%%--------------------------------------------------------------------
%% Gateway Instace APIs

-spec list() -> [{gateway_id(), [instance()]}].
list() ->
    emqx_gateway_sup:list_gateway_insta().

-spec create(instance_id(), gateway_id(), binary(), binary(), map())
    -> {ok, pid()}
     | {error, any()}.
create(InstaId, GwId, Name, Descr, RawConf) ->
    Insta = #instance{
               id = InstaId,
               gwid = GwId,
               name = Name,
               descr = Descr,
               rawconf = RawConf
              },
    emqx_gateway_sup:create_gateway_insta(Insta).

-spec remove(instance_id(), gateway_id()) -> ok | {error, any()}.
remove(InstaId, GwId) ->
    emqx_gateway_sup:remove_gateway_insta(InstaId, GwId).

-spec update(instance(), gateway_id()) -> ok | {error, any()}.
update(NewInsta, GwId) ->
    emqx_gateway_sup:update_gateway_insta(NewInsta, GwId).

-spec start(instance_id(), gateway_id()) -> ok | {error, any()}.
start(InstaId, GwId) ->
    emqx_gateway_sup:start_gateway_insta(InstaId, GwId).

-spec stop(instance_id(), gateway_id()) -> ok | {error, any()}.
stop(InstaId, GwId) ->
    emqx_gateway_sup:stop_gateway_insta(InstaId, GwId).
