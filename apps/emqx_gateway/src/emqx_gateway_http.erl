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

%% @doc Gateway Interface Module for HTTP-APIs
-module(emqx_gateway_http).

-include("include/emqx_gateway.hrl").

%% Mgmt APIs - gateway
-export([ gateways/1
        ]).

%% Mgmt APIs - clients
-export([ client_lookup/2
        , client_kickout/2
        , client_subscribe/4
        , client_unsubscribe/3
        , client_subscriptions/2
        ]).

%% Utils for http, swagger, etc.
-export([ return_http_error/2
        ]).

-type gateway_summary() ::
        #{ name := binary()
         , status := running | stopped | unloaded
         , started_at => binary()
         , max_connection => integer()
         , current_connect => integer()
         , listeners => []
         }.

%%--------------------------------------------------------------------
%% Mgmt APIs - gateway
%%--------------------------------------------------------------------

-spec gateways(Status :: all | running | stopped | unloaded)
    -> [gateway_summary()].
gateways(Status) ->
    Gateways = lists:map(fun({GwName, _}) ->
        case emqx_gateway:lookup(GwName) of
            undefined -> #{name => GwName, status => unloaded};
            GwInfo = #{config := Config} ->
                GwInfo0 = emqx_gateway_utils:unix_ts_to_rfc3339(
                            [created_at, started_at, stopped_at],
                            GwInfo),
                GwInfo1 = maps:with([name,
                                     status,
                                     created_at,
                                     started_at,
                                     stopped_at], GwInfo0),
                GwInfo1#{listeners => get_listeners_status(GwName, Config)}

        end
    end, emqx_gateway_registry:list()),
    case Status of
        all -> Gateways;
        _ ->
            [Gw || Gw = #{status := S} <- Gateways, S == Status]
    end.

%% @private
get_listeners_status(GwName, Config) ->
    Listeners = emqx_gateway_utils:normalize_config(Config),
    lists:map(fun({Type, LisName, ListenOn, _, _}) ->
        Name0 = listener_name(GwName, Type, LisName),
        Name = {Name0, ListenOn},
        case catch esockd:listener(Name) of
            _Pid when is_pid(_Pid) ->
                #{Name0 => <<"activing">>};
            _ ->
                #{Name0 => <<"inactived">>}

        end
    end, Listeners).

%% @private
listener_name(GwName, Type, LisName) ->
    list_to_atom(lists:concat([GwName, ":", Type, ":", LisName])).

%%--------------------------------------------------------------------
%% Mgmt APIs - clients
%%--------------------------------------------------------------------

-spec client_lookup(gateway_name(), emqx_type:clientid())
    -> {ok, {emqx_types:infos(), emqx_types:stats()}}
     | {error, any()}.
client_lookup(_GwName, _ClientId) ->
    %% FIXME: The Gap between `ClientInfo in HTTP-API` and
    %% ClientInfo defination
    todo.

-spec client_kickout(gateway_name(), emqx_type:clientid())
    -> {error, any()}
     | ok.
client_kickout(GwName, ClientId) ->
    emqx_gateway_cm:kick_session(GwName, ClientId).

-spec client_subscriptions(gateway_name(), emqx_type:clientid())
    -> {error, any()}
     | {ok, list()}.     %% FIXME: #{<<"t/1">> =>
                         %%           #{nl => 0,qos => 0,rap => 0,rh => 0,
                         %%             sub_props => #{}}
client_subscriptions(_GwName, _ClientId) ->
    todo.

-spec client_subscribe(gateway_name(), emqx_type:clientid(),
                       emqx_type:topic(), emqx_type:qos())
    -> {error, any()}
     | ok.
client_subscribe(_GwName, _ClientId, _Topic, _QoS) ->
    todo.

-spec client_unsubscribe(gateway_name(),
                         emqx_type:clientid(), emqx_type:topic())
    -> {error, any()}
     | ok.
client_unsubscribe(_GwName, _ClientId, _Topic) ->
    todo.

%%--------------------------------------------------------------------
%% Utils
%%--------------------------------------------------------------------

-spec return_http_error(integer(), binary()) -> binary().
return_http_error(Code, Msg) ->
    emqx_json:encode(
      #{code => codestr(Code),
        reason => emqx_gateway_utils:stringfy(Msg)
       }).

codestr(404) -> 'RESOURCE_NOT_FOUND';
codestr(401) -> 'NOT_SUPPORTED_NOW';
codestr(500) -> 'UNKNOW_ERROR'.
