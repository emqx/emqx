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

-module(emqx_node_helpers).

-include_lib("eunit/include/eunit.hrl").

-define(SLAVE_START_APPS, [emqx]).

-export([start_slave/1,
         start_slave/2,
         stop_slave/1]).

start_slave(Name) ->
    start_slave(Name, #{}).

start_slave(Name, Opts) ->
    {ok, Node} = ct_slave:start(list_to_atom(atom_to_list(Name) ++ "@" ++ host()),
                                [{kill_if_fail, true},
                                 {monitor_master, true},
                                 {init_timeout, 10000},
                                 {startup_timeout, 10000},
                                 {erl_flags, ebin_path()}]),

    pong = net_adm:ping(Node),
    setup_node(Node, Opts),
    Node.

stop_slave(Node) ->
    rpc:call(Node, ekka, leave, []),
    ct_slave:stop(Node).

host() ->
    [_, Host] = string:tokens(atom_to_list(node()), "@"), Host.

ebin_path() ->
    string:join(["-pa" | lists:filter(fun is_lib/1, code:get_path())], " ").

is_lib(Path) ->
    string:prefix(Path, code:lib_dir()) =:= nomatch.

setup_node(Node, #{} = Opts) ->
    Listeners = maps:get(listeners, Opts, []),
    StartApps = maps:get(start_apps, Opts, ?SLAVE_START_APPS),
    DefaultEnvHandler =
        fun(emqx) ->
                application:set_env(
                  emqx,
                  listeners,
                  Listeners),
                application:set_env(gen_rpc, port_discovery, stateless),
                ok;
           (_) ->
                ok
        end,
    EnvHandler = maps:get(env_handler, Opts, DefaultEnvHandler),

    [ok = rpc:call(Node, application, load, [App]) || App <- [gen_rpc, emqx]],
    ok = rpc:call(Node, emqx_ct_helpers, start_apps, [StartApps, EnvHandler]),

    rpc:call(Node, ekka, join, [node()]),

    %% Sanity check. Assert that `gen_rpc' is set up correctly:
    ?assertEqual( Node
                , gen_rpc:call(Node, erlang, node, [])
                ),
    ?assertEqual( node()
                , gen_rpc:call(Node, gen_rpc, call, [node(), erlang, node, []])
                ),
    ok.
