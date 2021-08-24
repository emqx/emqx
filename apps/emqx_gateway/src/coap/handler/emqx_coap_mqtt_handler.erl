%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_coap_mqtt_handler).

-include_lib("emqx_gateway/src/coap/include/emqx_coap.hrl").

-export([handle_request/5]).
-import(emqx_coap_message, [response/2, response/3]).

handle_request([<<"connection">>], #coap_message{method = Method} = Msg, _Cfg, _Ctx, _CInfo) ->
    handle_method(Method, Msg);

handle_request(_, Msg, _, _, _) ->
    ?REPLY({error, bad_request}, Msg).

handle_method(put, Msg) ->
    ?REPLY({ok, changed}, Msg);

handle_method(post, _) ->
    #{connection => open};

handle_method(delete, _) ->
    #{connection => close};

handle_method(_, Msg) ->
    ?REPLY({error, method_not_allowed}, Msg).
