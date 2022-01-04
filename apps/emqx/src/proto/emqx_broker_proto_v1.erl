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

-module(emqx_broker_proto_v1).

-introduced_in("5.0.0").

-export([ forward/3
        , forward_async/3
        ]).

-include("bpapi.hrl").
-include("emqx.hrl").

-spec forward(node(), emqx_types:topic(), emqx_types:delivery()) -> emqx_types:deliver_result().
forward(Node, Topic, Delivery = #delivery{}) when is_binary(Topic) ->
    emqx_rpc:call(Topic, Node, emqx_broker, dispatch, [Topic, Delivery]).

-spec forward_async(node(), emqx_types:topic(), emqx_types:delivery()) -> ok.
forward_async(Node, Topic, Delivery = #delivery{}) when is_binary(Topic) ->
    emqx_rpc:cast(Topic, Node, emqx_broker, dispatch, [Topic, Delivery]).
