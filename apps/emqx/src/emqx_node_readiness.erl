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

%% Node readiness flag.
%%
%% Connection processes check this flag at init and refuse to serve
%% until the node has finished booting, so no client can connect before
%% authentication, authorization and plugin hooks are installed.
%%
%% The flag defaults to `true': only the managed boot sequence
%% (`emqx_machine_boot:ensure_apps_started/0') clears it and sets it
%% back once all applications (including plugins) are started.
%% Contexts that do not boot through `emqx_machine' (test suites) are
%% therefore always ready.
-module(emqx_node_readiness).

-export([is_ready/0, mark_ready/0, mark_not_ready/0]).

-define(KEY, {?MODULE, ready}).

-spec is_ready() -> boolean().
is_ready() ->
    persistent_term:get(?KEY, true).

-spec mark_ready() -> ok.
mark_ready() ->
    persistent_term:put(?KEY, true).

-spec mark_not_ready() -> ok.
mark_not_ready() ->
    persistent_term:put(?KEY, false).
