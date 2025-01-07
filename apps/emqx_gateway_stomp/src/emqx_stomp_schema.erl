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

-module(emqx_stomp_schema).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").

%% config schema provides
-export([namespace/0, fields/1, desc/1]).

namespace() -> "gateway".

fields(stomp) ->
    [
        {frame, sc(ref(stomp_frame))},
        {mountpoint, emqx_gateway_schema:mountpoint()},
        {listeners, sc(ref(emqx_gateway_schema, tcp_listeners), #{desc => ?DESC(tcp_listeners)})}
    ] ++ emqx_gateway_schema:gateway_common_options();
fields(stomp_frame) ->
    [
        {max_headers,
            sc(
                non_neg_integer(),
                #{
                    default => 10,
                    desc => ?DESC(stomp_frame_max_headers)
                }
            )},
        {max_headers_length,
            sc(
                non_neg_integer(),
                #{
                    default => 1024,
                    desc => ?DESC(stomp_frame_max_headers_length)
                }
            )},
        {max_body_length,
            sc(
                integer(),
                #{
                    default => 65536,
                    desc => ?DESC(stomp_frame_max_body_length)
                }
            )}
    ].

desc(stomp) ->
    "The STOMP protocol gateway provides EMQX with the ability to access STOMP\n"
    "(Simple (or Streaming) Text Orientated Messaging Protocol) protocol.";
desc(stomp_frame) ->
    "Size limits for the STOMP frames.";
desc(_) ->
    undefined.

%%--------------------------------------------------------------------
%% internal functions

sc(Type) ->
    sc(Type, #{}).

sc(Type, Meta) ->
    hoconsc:mk(Type, Meta).

ref(StructName) ->
    ref(?MODULE, StructName).

ref(Mod, Field) ->
    hoconsc:ref(Mod, Field).
