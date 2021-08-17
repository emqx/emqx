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
        , load/2
        , unload/1
        , lookup/1
        , update/1
        , start/1
        , stop/1
        , list/0
        ]).

-spec registered_gateway() ->
    [{gateway_name(), emqx_gateway_registry:descriptor()}].
registered_gateway() ->
    emqx_gateway_registry:list().

%%--------------------------------------------------------------------
%% Gateway Instace APIs

-spec list() -> [gateway()].
list() ->
    emqx_gateway_sup:list_gateway_insta().

-spec load(gateway_name(), map())
    -> {ok, pid()}
     | {error, any()}.
load(Name, RawConf) ->
    Gateway = #{ name => Name
               , descr => undefined
               , rawconf => RawConf
               },
    emqx_gateway_sup:load_gateway(Gateway).

-spec unload(gateway_name()) -> ok | {error, any()}.
unload(Name) ->
    emqx_gateway_sup:unload_gateway(Name).

-spec lookup(gateway_name()) -> gateway() | undefined.
lookup(Name) ->
    emqx_gateway_sup:lookup_gateway(Name).

-spec update(gateway()) -> ok | {error, any()}.
update(NewGateway) ->
    emqx_gateway_sup:update_gateway(NewGateway).

-spec start(gateway_name()) -> ok | {error, any()}.
start(Name) ->
    emqx_gateway_sup:start_gateway_insta(Name).

-spec stop(gateway_name()) -> ok | {error, any()}.
stop(Name) ->
    emqx_gateway_sup:stop_gateway_insta(Name).

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------
