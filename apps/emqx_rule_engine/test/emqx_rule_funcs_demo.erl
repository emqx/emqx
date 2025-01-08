%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_rule_funcs_demo).

-export([
    is_my_topic/1,
    duplicate_payload/2
]).

%% check if the topic is of 5 levels.
is_my_topic(Topic) ->
    emqx_topic:levels(Topic) =:= 5.

%% duplicate the payload, but only supports 2 or 3 copies.
duplicate_payload(Payload, 2) ->
    [Payload, Payload];
duplicate_payload(Payload, 3) ->
    [Payload, Payload, Payload].
