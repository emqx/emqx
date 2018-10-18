%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_misc_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-define(SOCKOPTS, [binary,
                   {packet,    raw},
                   {reuseaddr, true},
                   {backlog,   512},
                   {nodelay,   true}]).

all() -> [t_merge_opts, t_init_proc_mng_policy].

t_merge_opts(_) ->
    Opts = emqx_misc:merge_opts(?SOCKOPTS, [raw,
                                            binary,
                                            {backlog, 1024},
                                            {nodelay, false},
                                            {max_clients, 1024},
                                            {acceptors, 16}]),
    ?assertEqual(1024, proplists:get_value(backlog, Opts)),
    ?assertEqual(1024, proplists:get_value(max_clients, Opts)),
    [binary, raw,
     {acceptors, 16},
     {backlog, 1024},
     {max_clients, 1024},
     {nodelay, false},
     {packet, raw},
     {reuseaddr, true}] = lists:sort(Opts).

t_init_proc_mng_policy(_) ->
    application:set_env(emqx, zones, [{policy, [{force_shutdown_policy, #{max_heap_size => 1}}]}]),
    {ok, _} = emqx_zone:start_link(),
    ok = emqx_misc:init_proc_mng_policy(policy),
    ok = emqx_misc:init_proc_mng_policy(undefined),
    emqx_zone:stop().
