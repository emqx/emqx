%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_plugins_proto_v6).
-behaviour(emqx_bpapi).
-include_lib("emqx/include/bpapi.hrl").
-export([introduced_in/0, supports_install_lock/1, acquire_install_lock/2, release_install_lock/2]).

introduced_in() -> "6.0.4".

%% @doc Whether `Node' takes the cluster wide installation lock, so that it may
%% be the target of an operation which installs a package.
%%
%% The version `Node' announces in `bpapi.versions' is not enough on its own: a
%% core node which joins a running cluster after it booted keeps no announcement
%% for itself (`emqx_bpapi_replicant_checker' re-announces replicants only), so
%% its peers report `undefined' for a node which may well be upgraded.  Ask the
%% node itself: a node which has no serializer fails here, and the caller turns
%% that into `false'.
-spec supports_install_lock(node()) -> boolean().
supports_install_lock(Node) ->
    erpc:call(Node, emqx_plugins_install_serializer, supports_install_lock, [], 5_000).

-spec acquire_install_lock(node(), pid()) -> {ok, reference()} | {error, term()}.
acquire_install_lock(Node, Holder) ->
    %% Keep the caller waiting for its token until the acquisition completes.
    erpc:call(Node, emqx_plugins_install_serializer, acquire_lock, [Holder], infinity).

-spec release_install_lock(node(), reference()) -> ok.
release_install_lock(Node, Lock) ->
    erpc:call(Node, emqx_plugins_install_serializer, release_lock, [Lock], 25_000).
