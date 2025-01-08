%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_rule_engine_api_cluster_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("snabbkaffe/include/test_macros.hrl").

-define(APPSPECS, [
    emqx,
    emqx_conf,
    emqx_management,
    {emqx_rule_engine, "rule_engine { rules {} }"}
]).

-define(APPSPEC_DASHBOARD,
    {emqx_dashboard, "dashboard.listeners.http { enable = true, bind = 18083 }"}
).

-define(SIMPLE_RULE(NAME_SUFFIX), #{
    <<"description">> => <<"A simple rule">>,
    <<"enable">> => true,
    <<"actions">> => [#{<<"function">> => <<"console">>}],
    <<"sql">> => <<"SELECT * from \"t/1\"">>,
    <<"name">> => <<"test_rule", NAME_SUFFIX/binary>>
}).

%%------------------------------------------------------------------------------
%% Setup
%%------------------------------------------------------------------------------

all() ->
    [{group, cluster}].

groups() ->
    AllTCs = emqx_common_test_helpers:all(?MODULE),
    [{cluster, [], AllTCs}].

suite() ->
    [{timetrap, {seconds, 120}}].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(cluster = Name, Config) ->
    Nodes = [NodePrimary | _] = mk_cluster(Config),
    init_api([{group, Name}, {cluster_nodes, Nodes}, {node, NodePrimary} | Config]).

init_api(Config) ->
    APINode = ?config(node, Config),
    {ok, App} = erpc:call(APINode, emqx_common_test_http, create_default_app, []),
    [{api, App} | Config].

mk_cluster(Config) ->
    mk_cluster(Config, #{}).

mk_cluster(Config, Opts) ->
    Node1Apps = ?APPSPECS ++ [?APPSPEC_DASHBOARD],
    Node2Apps = ?APPSPECS ++ [],
    emqx_cth_cluster:start(
        [
            {emqx_rule_engine_api_cluster_SUITE_1, Opts#{role => core, apps => Node1Apps}},
            {emqx_rule_engine_api_cluster_SUITE_2, Opts#{role => core, apps => Node2Apps}}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ).

end_per_group(Group, Config) when Group =:= cluster ->
    ok = emqx_cth_cluster:stop(?config(cluster_nodes, Config));
end_per_group(_, Config) ->
    emqx_cth_suite:stop(?config(group_apps, Config)),
    ok.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_double_delete_on_diff_node(Config) ->
    [Node1, Node2] = ?config(cluster_nodes, Config),

    CreateFun = fun() ->
        {201, Rule} = create_rule(Node2, ?SIMPLE_RULE(<<"test_rule1">>)),
        RuleId = maps:get(id, Rule),

        Parent = self(),

        erlang:spawn(fun() ->
            R = delete_rule(Node1, RuleId),
            Parent ! {delete_result, Node1, R}
        end),

        erlang:spawn(fun() ->
            R = delete_rule(Node2, RuleId),
            Parent ! {delete_result, Node2, R}
        end),

        receive
            {delete_result, Node1, R1} ->
                receive
                    {delete_result, Node2, R2} ->
                        assert_return_204_or_404(R1),
                        assert_return_204_or_404(R2),
                        ?assertEqual(true, lists:member({204}, [R1, R2]))
                after 5000 ->
                    error({wait_timeout, Node2})
                end
        after 5000 ->
            error({wait_timeout, Node1})
        end
    end,

    lists:foreach(fun(_) -> CreateFun() end, lists:seq(1, 10)),

    TxId1 = cluster_conf_tx_id(Node1),
    TxId2 = cluster_conf_tx_id(Node2),
    %% confirm all config updates are applied
    ?assertEqual(TxId1, TxId2).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

create_rule(Node, Params) when is_map(Params) ->
    rpc:call(Node, emqx_rule_engine_api, '/rules', [post, #{body => Params}]).

delete_rule(Node, RuleId) when is_binary(RuleId) ->
    rpc:call(
        Node,
        emqx_rule_engine_api,
        '/rules/:id',
        [delete, #{bindings => #{id => RuleId}}]
    ).

cluster_conf_tx_id(Node) ->
    rpc:call(Node, emqx_cluster_rpc, get_node_tnx_id, [Node]).

assert_return_204_or_404({204}) -> ok;
assert_return_204_or_404({404, _}) -> ok;
assert_return_204_or_404(R) -> error({unexpected_result, R}).
