%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
        "\n msg_clear_limit = 100"
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
            %% Since limit is 100, we should observe 3 GC events for 250 messages:
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

%% Verifies that, even if a retained message has a long expiry time, if we set a maximum
%% expiry time override, the latter wins.
t_expiry_override(Config) ->
    NumMsgs = 10,
    ?check_trace(
        #{timetrap => 10_000},
        begin
            %% Longer than override
            ok = store_retained(NumMsgs, [{expiry_interval_s, 1000}, {start_n, 1} | Config]),
            %% Shorter than override
            ok = store_retained(NumMsgs, [{expiry_interval_s, 1}, {start_n, NumMsgs + 1} | Config]),
            %% Would never expire.
            ok = store_retained(NumMsgs, [
                {expiry_interval_s, 0}, {start_n, 2 * NumMsgs + 1} | Config
            ]),

            ok = set_message_expiry_override(<<"1500ms">>, Config),
            %% Initially, we don't override messages with infinity expiry time.
            ok = set_allow_never_expire(true, Config),
            {ok, {ok, _}} = ?wait_async_action(
                enable_clear_expired(_Interval = 2_000, Config),
                #{?snk_kind := emqx_retainer_cleared_expired, n_cleared := N} when
                    N > 0
            ),
            %% Now we override messages with infinity expiry time.
            {ok, {ok, _}} = ?wait_async_action(
                begin
                    ok = set_allow_never_expire(false, Config),
                    enable_clear_expired(_Interval2 = 500, Config)
                end,
                #{?snk_kind := emqx_retainer_cleared_expired}
            ),

            ok = set_allow_never_expire(true, Config),
            ok = disable_clear_expired(Config),
            ok = unset_message_expiry_override(Config),
            ok
        end,
        fun(Trace) ->
            SubTrace =
                lists:filter(
                    fun
                        (#{?snk_kind := emqx_retainer_cleared_expired, n_cleared := N}) when
                            N > 0
                        ->
                            true;
                        (_) ->
                            false
                    end,
                    Trace
                ),
            %% `NumMsgs * 2' because we have one batch that was already going to expire
            %% without the override, and one batch that is overridden.
            ExpectedCleared1 = NumMsgs * 2,
            %% Second cleared batch contains the messages that would never expire, after
            %% we set `allow_never_expire = false' so they are affected by GC.
            ExpectedCleared2 = NumMsgs,
            ?assertMatch(
                [
                    #{complete := true, n_cleared := ExpectedCleared1},
                    #{complete := true, n_cleared := ExpectedCleared2}
                ],
                SubTrace
            ),
            ok
        end
    ),
    ok.

store_retained(NMessages, Config) ->
    [N1 | _] = ?config(cluster, Config),
    StartN = proplists:get_value(start_n, Config, 1),
    ExpiryInterval = proplists:get_value(expiry_interval_s, Config, 1),
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
                    #{properties => #{'Message-Expiry-Interval' => ExpiryInterval}}
                ),
                emqx:publish(Msg)
            end,
            lists:seq(StartN, StartN + NMessages - 1)
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

set_message_expiry_override(Override, Config) ->
    [N1 | _] = ?config(cluster, Config),
    {ok, _} = ?ON(
        N1, emqx_retainer:update_config(#{<<"msg_expiry_interval_override">> => Override})
    ),
    ok.

unset_message_expiry_override(Config) ->
    [N1 | _] = ?config(cluster, Config),
    {ok, _} = ?ON(
        N1, emqx_retainer:update_config(#{<<"msg_expiry_interval_override">> => <<"disabled">>})
    ),
    ok.

set_allow_never_expire(Bool, Config) ->
    [N1 | _] = ?config(cluster, Config),
    {ok, _} = ?ON(
        N1, emqx_retainer:update_config(#{<<"allow_never_expire">> => Bool})
    ),
    ok.
