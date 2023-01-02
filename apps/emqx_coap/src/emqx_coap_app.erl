%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_coap_app).

-behaviour(application).

-emqx_plugin(protocol).

-include("emqx_coap.hrl").

-export([ start/2
        , stop/1
        ]).

start(_Type, _Args) ->
    {ok, Sup} = emqx_coap_sup:start_link(),
    coap_server_registry:add_handler([<<"mqtt">>], emqx_coap_resource, undefined),
    coap_server_registry:add_handler([<<"ps">>], emqx_coap_pubsub_resource, undefined),
    _ = emqx_coap_pubsub_topics:start_link(),
    emqx_coap_server:start(application:get_all_env(?APP)),
    {ok,Sup}.

stop(_State) ->
    coap_server_registry:remove_handler([<<"mqtt">>], emqx_coap_resource, undefined),
    coap_server_registry:remove_handler([<<"ps">>], emqx_coap_pubsub_resource, undefined),
    emqx_coap_server:stop(application:get_all_env(?APP)).
