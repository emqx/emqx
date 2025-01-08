%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_observer_cli).

-export([
    enable/0,
    disable/0
]).

-export([cmd/1]).

%%--------------------------------------------------------------------
%% enable/disable
%%--------------------------------------------------------------------
enable() ->
    emqx_ctl:register_command(observer, {?MODULE, cmd}, []).

disable() ->
    emqx_ctl:unregister_command(observer).

cmd(["status"]) ->
    observer_cli:start();
cmd(["bin_leak"]) ->
    lists:foreach(
        fun(Row) -> emqx_ctl:print("~p~n", [Row]) end,
        recon:bin_leak(100)
    );
cmd(["load", Mod]) ->
    case nodes() of
        [] ->
            emqx_ctl:print("No other nodes in the cluster~n");
        Nodes ->
            case emqx_utils:safe_to_existing_atom(Mod) of
                {ok, Module} ->
                    case code:get_object_code(Module) of
                        error ->
                            emqx_ctl:print("Module(~s)'s object code not found~n", [Mod]);
                        _ ->
                            Res = recon:remote_load(Nodes, Module),
                            emqx_ctl:print("Loaded ~p module on ~p: ~p~n", [Module, Nodes, Res])
                    end;
                {error, Reason} ->
                    emqx_ctl:print("Module(~s) not found: ~p~n", [Mod, Reason])
            end
    end;
cmd(_) ->
    emqx_ctl:usage([
        {"observer status", "Start observer in the current console"},
        {"observer bin_leak",
            "Force all processes to perform garbage collection "
            "and prints the top-100 processes that freed the "
            "biggest amount of binaries, potentially highlighting leaks."},
        {"observer load Mod", "Enhanced module synchronization across all cluster nodes"}
    ]).
