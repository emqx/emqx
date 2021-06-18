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
-export([ types/0
        , create/5
        , remove/2
        , update/2
        , start/2
        , stop/2
        , list/0
        ]).

types() ->
    emqx_gateway_registry:types().

%%--------------------------------------------------------------------
%% Gateway Instace APIs

%% FIXME: Map is better ???
-spec list() -> [instance()].
list() ->
    %% TODO:
    [].

%% XXX: InstaId 是不是可以自己生成(保证全集群唯一)

-spec create(atom(), atom(), binary(), binary(), map())
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

-spec remove(atom(), atom()) -> ok | {error, any()}.
remove(InstaId, GwId) ->
    emqx_gateway_sup:remove_gateway_insta(InstaId, GwId).

-spec update(instance(), atom()) -> ok | {error, any()}.
update(NewInsta, GwId) ->
    emqx_gateway_sup:update_gateway_insta(NewInsta, GwId).

-spec start(atom(), atom()) -> ok | {error, any()}.
start(InstaId, GwId) ->
    emqx_gateway_sup:start_gateway_insta(InstaId, GwId).

-spec stop(atom(), atom()) -> ok | {error, any()}.
stop(InstaId, GwId) ->
    emqx_gateway_sup:stop_gateway_insta(InstaId, GwId).
