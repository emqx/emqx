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

-module(emqx_gateway).

-include("include/emqx_gateway.hrl").

%% Gateway APIs
-export([
    registered_gateway/0,
    load/2,
    unload/1,
    lookup/1,
    update/2,
    start/1,
    stop/1,
    list/0
]).

%% APIs For `emqx_telemetry'
-export([get_basic_usage_info/0]).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec registered_gateway() ->
    [{gateway_name(), emqx_gateway_registry:descriptor()}].
registered_gateway() ->
    emqx_gateway_registry:list().

%%--------------------------------------------------------------------
%% Gateway APIs

%% @doc List the load gateways
-spec list() -> [gateway()].
list() ->
    emqx_gateway_sup:list_gateway_insta().

-spec load(gateway_name(), emqx_config:config()) ->
    {ok, pid()}
    | {error, any()}.
load(Name, Config) ->
    Gateway = #{
        name => Name,
        descr => undefined,
        config => Config
    },
    emqx_gateway_sup:load_gateway(Gateway).

-spec unload(gateway_name()) -> ok | {error, not_found}.
unload(Name) ->
    emqx_gateway_sup:unload_gateway(Name).

-spec lookup(gateway_name()) -> gateway() | undefined.
lookup(Name) ->
    emqx_gateway_sup:lookup_gateway(Name).

-spec update(gateway_name(), emqx_config:config()) -> ok | {error, any()}.
%% @doc This function only supports full configuration updates
%%
%% Note: If the `enable` option is missing, it will be set to true by default
update(Name, Config) ->
    emqx_gateway_sup:update_gateway(Name, Config).

-spec start(gateway_name()) -> ok | {error, any()}.
start(Name) ->
    emqx_gateway_sup:start_gateway_insta(Name).

-spec stop(gateway_name()) -> ok | {error, any()}.
stop(Name) ->
    emqx_gateway_sup:stop_gateway_insta(Name).

%% @doc Expose basic info for `emqx_telemetry'.
-spec get_basic_usage_info() ->
    #{
        authn => Authn,
        num_clients => non_neg_integer(),
        listeners => [
            #{
                type => atom(),
                authn => Authn
            }
        ]
    }
when
    Authn :: binary().
get_basic_usage_info() ->
    lists:foldl(
        fun(GatewayInfo, Acc) ->
            Config = maps:get(config, GatewayInfo, #{}),
            GatewayType = maps:get(name, GatewayInfo),
            GatewayAuthn = get_authn_type(Config),
            Listeners = get_listeners(Config),
            TabName = emqx_gateway_cm:tabname(chan, GatewayType),
            NumClients = ets:info(TabName, size),
            Acc#{
                GatewayType => #{
                    authn => GatewayAuthn,
                    listeners => Listeners,
                    num_clients => NumClients
                }
            }
        end,
        #{},
        list()
    ).

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

get_authn_type(#{authentication := Authn = #{mechanism := Mechanism, backend := Backend}}) when
    is_atom(Mechanism), is_atom(Backend)
->
    emqx_authentication_config:authenticator_id(Authn);
get_authn_type(_) ->
    <<"undefined">>.

get_listeners(#{listeners := Listeners0}) when is_map(Listeners0) ->
    Listeners = [
        {ListenerType, Config}
     || {ListenerType, Listeners1} <- maps:to_list(Listeners0),
        {_Name, Config} <- maps:to_list(Listeners1)
    ],
    lists:map(
        fun({ListenerType, ListenerConfig}) ->
            ListenerAuthn = get_authn_type(ListenerConfig),
            #{type => ListenerType, authn => ListenerAuthn}
        end,
        Listeners
    );
get_listeners(_) ->
    [].
