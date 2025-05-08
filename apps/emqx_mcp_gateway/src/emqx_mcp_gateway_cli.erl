%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mcp_gateway_cli).

%% This is an example on how to extend `emqx ctl` with your own commands.

-export([cmd/1]).

cmd(["arg1", "arg2"]) ->
    emqx_ctl:print("ok");
cmd(_) ->
    emqx_ctl:usage([{"cmd arg1 arg2", "cmd demo"}]).
