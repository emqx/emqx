%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_ds_beamsplitter_proto_v1).

-behavior(emqx_bpapi).
-include_lib("emqx_utils/include/bpapi.hrl").

%% API:
-export([dispatch/2]).

%% behavior callbacks:
-export([introduced_in/0]).

%%================================================================================
%% API functions
%%================================================================================

-spec dispatch(node(), emqx_ds_beamformer:beam()) -> true.
dispatch(Node, Beam) ->
    emqx_rpc:cast(Node, emqx_ds_beamformer, do_dispatch, [Beam]).

%%================================================================================
%% behavior callbacks
%%================================================================================

introduced_in() ->
    "5.8.0".
