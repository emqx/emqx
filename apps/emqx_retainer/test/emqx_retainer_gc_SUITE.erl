%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_retainer_gc_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/asserts.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

all() ->
    [
        {group, mnesia_without_indices},
        {group, mnesia_with_indices}
    ].

groups() ->
    TCs = emqx_common_test_helpers:all(?MODULE),
    [
        {mnesia_without_indices, [], TCs},
        {mnesia_with_indices, [], TCs}
    ].

init_per_group(mnesia_without_indices, Config) ->
    ExtraConf = "retainer.backend.index_specs = []",
    init_cluster(ExtraConf, Config);
init_per_group(mnesia_with_indices, Config) ->
    init_cluster("", Config).

init_cluster(ExtraConf, Config) ->
    Conf =
        "retainer {"
        "\n enable = true"
        "\n msg_clear_interval = 0s"
        "\n msg_expiry_interval = 3s"
        "\n backend {"
        "\n   type = built_in_database"
        "\n   storage_type = disc"
        "\n }"
        "\n }",
    NodeSpec = #{
        role => core,
        apps => [emqx, emqx_conf, {emqx_retainer, [Conf, ExtraConf]}]
    },
    Nodes = emqx_cth_cluster:start(
        [{emqx_retainer_gc1, NodeSpec}, {emqx_retainer_gc2, NodeSpec}],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{cluster, Nodes} | Config].

end_per_group(_Group, Config) ->
    emqx_cth_cluster:stop(?config(cluster, Config)).

%%

t_exclusive_gc(Config) ->
    [N1 | _] = ?config(cluster, Config),
    ?check_trace(
        begin
            Interval = 1000,
            ok = enable_clear_expired(Interval, Config),
            ok = timer:sleep(round(Interval * 1.5)),
            ok = disable_clear_expired(Config)
        end,
        fun(Trace) ->
            %% Only one node should have ran GC:
            ?assertMatch(
                [#{?snk_meta := #{node := N1}}],
                ?of_kind(emqx_retainer_cleared_expired, Trace)
            )
        end
    ).

t_limited_gc_runtime(Config) ->
    [N1 | _] = ?config(cluster, Config),
    ?check_trace(
        begin
            ok = store_retained(_NMessages = 250, Config),
            ok = timer:sleep(1000),
            {ok, {ok, _}} = ?wait_async_action(
                enable_clear_expired(_Interval = 1000, Config),
                #{?snk_kind := emqx_retainer_cleared_expired, complete := true}
            ),
            ok = disable_clear_expired(Config)
        end,
        fun(Trace) ->
            %% Only one node should have ran GC:
            ?assertMatch(
                [
                    #{complete := false, n_cleared := 100, ?snk_meta := #{node := N1}},
                    #{complete := false, n_cleared := 100, ?snk_meta := #{node := N1}},
                    #{complete := true, n_cleared := 50, ?snk_meta := #{node := N1}}
                ],
                ?of_kind(emqx_retainer_cleared_expired, Trace)
            )
        end
    ).

store_retained(NMessages, Config) ->
    [N1 | _] = ?config(cluster, Config),
    ?ON(
        N1,
        lists:foreach(
            fun(N) ->
                Num = integer_to_binary(N),
                Msg = emqx_message:make(
                    ?MODULE,
                    0,
                    <<"retained/", Num/binary>>,
                    <<"payload">>,
                    #{retain => true},
                    #{properties => #{'Message-Expiry-Interval' => 1}}
                ),
                emqx:publish(Msg)
            end,
            lists:seq(1, NMessages)
        )
    ).

enable_clear_expired(Interval, Config) ->
    [N1 | _] = ?config(cluster, Config),
    {ok, _} = ?ON(N1, emqx_retainer:update_config(#{<<"msg_clear_interval">> => Interval})),
    ok.

disable_clear_expired(Config) ->
    [N1 | _] = ?config(cluster, Config),
    {ok, _} = ?ON(N1, emqx_retainer:update_config(#{<<"msg_clear_interval">> => 0})),
    ok.
