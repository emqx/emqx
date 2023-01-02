%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_recon_cli).

-export([ cmd/1
        , load/0
        , unload/0
        ]).

load() ->
    emqx_ctl:register_command(recon, {?MODULE, cmd}, []).

cmd(["memory"]) ->
    Print = fun(Key, Keyword) ->
              emqx_ctl:print("~-20s: ~w~n", [concat(Key, Keyword), recon_alloc:memory(Key, Keyword)])
            end,
    [Print(Key, Keyword) || Key <- [usage, used, allocated, unused], Keyword <- [current, max]];

cmd(["allocated"]) ->
    Print = fun(Keyword, Key, Val) -> emqx_ctl:print("~-20s: ~w~n", [concat(Key, Keyword), Val]) end,
    Alloc = fun(Keyword) -> recon_alloc:memory(allocated_types, Keyword) end,
    [Print(Keyword, Key, Val) || Keyword <- [current, max], {Key, Val} <- Alloc(Keyword)];

cmd(["bin_leak"]) ->
    [emqx_ctl:print("~p~n", [Row]) || Row <- recon:bin_leak(100)];

cmd(["node_stats"]) ->
    recon:node_stats_print(10, 1000);

cmd(["remote_load", Mod]) ->
    emqx_ctl:print("~p~n", [remote_load(list_to_atom(Mod))]);

cmd(["proc_count", Attr, N]) ->
    emqx_ctl:print("~p~n", [recon:proc_count(list_to_atom(Attr), list_to_integer(N))]);

cmd(_) ->
    emqx_ctl:usage([{"recon memory",           "recon_alloc:memory/2"},
                    {"recon allocated",        "recon_alloc:memory(allocated_types, current|max)"},
                    {"recon bin_leak",         "recon:bin_leak(100)"},
                    {"recon node_stats",       "recon:node_stats(10, 1000)"},
                    {"recon remote_load Mod",  "recon:remote_load(Mod)"},
                    {"recon proc_count Attr N","recon:proc_count(Attr, N)"}]).

unload() ->
    emqx_ctl:unregister_command(recon).

concat(Key, Keyword) ->
    lists:concat([atom_to_list(Key), "/", atom_to_list(Keyword)]).

remote_load(Module) -> remote_load(nodes(), Module).

%% recon:remote_load/1 has a bug, when nodes() returns [], it is
%% taken by recon as a node name.
%% before OTP 23, the call returns a 'badrpc' tuple
%% after OTP 23, it crashes with 'badarg' error
remote_load([], _Module) -> ok;
remote_load(Nodes, Module) -> recon:remote_load(Nodes, Module).

