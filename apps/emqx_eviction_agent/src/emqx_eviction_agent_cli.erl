%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_eviction_agent_cli).

%% APIs
-export([ load/0
        , unload/0
        , cli/1
        ]).

load() ->
    emqx_ctl:register_command(eviction, {?MODULE, cli}, []).

unload() ->
    emqx_ctl:unregister_command(eviction).

cli(["status"]) ->
    case emqx_eviction_agent:status() of
        disabled ->
            emqx_ctl:print("Eviction status: disabled~n");
        {enabled, _Stats} ->
            emqx_ctl:print("Eviction status: enabled~n")
    end;

cli(_) ->
    emqx_ctl:usage(
      [{"eviction status",
        "Get current node eviction status"}]).
