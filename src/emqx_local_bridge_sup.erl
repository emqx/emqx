%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_local_bridge_sup).

-include("emqx.hrl").

-export([start_link/3]).

-spec(start_link(node(), emqx_topic:topic(), [emqx_local_bridge:option()])
      -> {ok, pid()} | {error, term()}).
start_link(Node, Topic, Options) ->
    MFA = {emqx_local_bridge, start_link, [Node, Topic, Options]},
    emqx_pool_sup:start_link({bridge, Node, Topic}, random, MFA).

