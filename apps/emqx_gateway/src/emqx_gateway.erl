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
        , create/4
        , remove/1
        , lookup/1
        , update/1
        , start/1
        , stop/1
        , list/0
        ]).

-spec registered_gateway() ->
    [{gateway_type(), emqx_gateway_registry:descriptor()}].
registered_gateway() ->
    emqx_gateway_registry:list().

%%--------------------------------------------------------------------
%% Gateway Instace APIs

-spec list() -> [instance()].
list() ->
    lists:append(lists:map(
      fun({_, Insta}) -> Insta end,
      emqx_gateway_sup:list_gateway_insta()
     )).

-spec create(gateway_type(), binary(), binary(), map())
    -> {ok, pid()}
     | {error, any()}.
create(Type, Name, Descr, RawConf) ->
    Insta = #{ id => clacu_insta_id(Type, Name)
             , type => Type
             , name => Name
             , descr => Descr
             , rawconf => RawConf
             },
    emqx_gateway_sup:create_gateway_insta(Insta).

-spec remove(instance_id()) -> ok | {error, any()}.
remove(InstaId) ->
    emqx_gateway_sup:remove_gateway_insta(InstaId).

%% TODO:
-spec lookup(instance_id()) -> instance() | undefined.
lookup(InstaId) ->
    emqx_gateway_sup:lookup_gateway_insta(InstaId).

-spec update(instance()) -> ok | {error, any()}.
update(NewInsta) ->
    emqx_gateway_sup:update_gateway_insta(NewInsta).

-spec start(instance_id()) -> ok | {error, any()}.
start(InstaId) ->
    emqx_gateway_sup:start_gateway_insta(InstaId).

-spec stop(instance_id()) -> ok | {error, any()}.
stop(InstaId) ->
    emqx_gateway_sup:stop_gateway_insta(InstaId).

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

clacu_insta_id(Type, Name) when is_binary(Name) ->
    list_to_atom(lists:concat([Type, "#", binary_to_list(Name)])).
