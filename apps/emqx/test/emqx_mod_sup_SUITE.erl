%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mod_sup_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_ct:all(?MODULE).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_start(_) ->
    {ok, _} = emqx_mod_sup:start_link(),
    ?assertEqual([], supervisor:which_children(emqx_mod_sup)).

t_start_child(_) ->
    %% Set the emqx_mod_sup child with emqx_hooks for test
    Mod = emqx_hooks,
    Spec = #{id => Mod,
             start => {Mod, start_link, []},
             restart => permanent,
             shutdown => 5000,
             type => worker,
             modules => [Mod]},

    {ok, _} = emqx_mod_sup:start_link(),
    {ok, _} = emqx_mod_sup:start_child(Mod, worker),
    {error, {already_started, _}} = emqx_mod_sup:start_child(Spec),

    ok = emqx_mod_sup:stop_child(Mod),
    {error, not_found} = emqx_mod_sup:stop_child(Mod),
    ok.

