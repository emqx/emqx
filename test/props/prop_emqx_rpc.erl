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

-module(prop_emqx_rpc).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(NODENAME, 'test@127.0.0.1').

-define(ALL(Vars, Types, Exprs),
        ?SETUP(fun() ->
            State = do_setup(),
            fun() -> do_teardown(State) end
         end, ?FORALL(Vars, Types, Exprs))).

%%--------------------------------------------------------------------
%% Properties
%%--------------------------------------------------------------------

prop_node() ->
    ?ALL(Node0, nodename(),
         begin
             Node = punch(Node0),
             ?assert(emqx_rpc:cast(Node, erlang, system_time, [])),
             case emqx_rpc:call(Node, erlang, system_time, []) of
                 {badrpc, _Reason} -> true;
                 Delivery when is_integer(Delivery) -> true;
                 _Other -> false
             end
         end).

prop_node_with_key() ->
    ?ALL({Node0, Key}, nodename_with_key(),
         begin
             Node = punch(Node0),
             ?assert(emqx_rpc:cast(Key, Node, erlang, system_time, [])),
             case emqx_rpc:call(Key, Node, erlang, system_time, []) of
                 {badrpc, _Reason} -> true;
                 Delivery when is_integer(Delivery) -> true;
                 _Other -> false
             end
         end).

prop_nodes() ->
    ?ALL(Nodes0, nodesname(),
         begin
             Nodes = punch(Nodes0),
             case emqx_rpc:multicall(Nodes, erlang, system_time, []) of
                 {badrpc, _Reason} -> true;
                 {RealResults, RealBadNodes}
                   when is_list(RealResults);
                        is_list(RealBadNodes) ->
                     true;
                 _Other -> false
             end
         end).

prop_nodes_with_key() ->
    ?ALL({Nodes0, Key}, nodesname_with_key(),
         begin
             Nodes = punch(Nodes0),
             case emqx_rpc:multicall(Key, Nodes, erlang, system_time, []) of
                 {badrpc, _Reason} -> true;
                 {RealResults, RealBadNodes}
                   when is_list(RealResults);
                        is_list(RealBadNodes) ->
                     true;
                 _Other -> false
             end
         end).

%%--------------------------------------------------------------------
%%  Helper
%%--------------------------------------------------------------------

do_setup() ->
    ensure_distributed_nodename(),
    ok = logger:set_primary_config(#{level => warning}),
    {ok, _Apps} = application:ensure_all_started(gen_rpc),
    ok = application:set_env(gen_rpc, call_receive_timeout, 100),
    ok = meck:new(gen_rpc, [passthrough, no_history]),
    ok = meck:expect(gen_rpc, multicall,
                     fun(Nodes, Mod, Fun, Args) ->
                             gen_rpc:multicall(Nodes, Mod, Fun, Args, 100)
                     end).

do_teardown(_) ->
    ok = net_kernel:stop(),
    ok = application:stop(gen_rpc),
    ok = meck:unload(gen_rpc),
    %% wait for tcp close
    timer:sleep(1500).

ensure_distributed_nodename() ->
    case net_kernel:start([?NODENAME]) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            net_kernel:stop(),
            net_kernel:start([?NODENAME]);
        {error, {{shutdown, {_, _, {'EXIT', nodistribution}}}, _}} ->
            %% start epmd first
            spawn_link(fun() -> os:cmd("epmd") end),
            timer:sleep(100),
            net_kernel:start([?NODENAME])
    end.

%%--------------------------------------------------------------------
%% Generator
%%--------------------------------------------------------------------


nodename() ->
    ?LET({NodePrefix, HostName},
         {node_prefix(), hostname()},
         begin
             Node = NodePrefix ++ "@" ++ HostName,
             list_to_atom(Node)
         end).

nodename_with_key() ->
    ?LET({NodePrefix, HostName, Key},
         {node_prefix(), hostname(), choose(0, 10)},
         begin
             Node = NodePrefix ++ "@" ++ HostName,
             {list_to_atom(Node), Key}
         end).

nodesname() ->
    oneof([list(nodename()), [node()]]).

nodesname_with_key() ->
    oneof([{list(nodename()), choose(0, 10)}, {[node()], 1}]).

node_prefix() ->
    oneof(["emqxct", text_like()]).

text_like() ->
    ?SUCHTHAT(Text, list(range($a, $z)), (length(Text) =< 100 andalso length(Text) > 0)).

hostname() ->
    oneof(["127.0.0.1", "localhost"]).

%%--------------------------------------------------------------------
%% Utils
%%--------------------------------------------------------------------

%% After running the props, the `node()` () is only able to return an
%% incorrect node name - `nonode@nohost`, But we want a distributed nodename
%% So, just translate the `nonode@nohost` to ?NODENAME
punch(Nodes) when is_list(Nodes) ->
    lists:map(fun punch/1, Nodes);
punch('nonode@nohost') ->
    node();  %% Equal to ?NODENAME
punch(GoodBoy) ->
    GoodBoy.
