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

-module(emqx_retainer_cli_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_retainer.hrl").

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [emqx, emqx_conf, emqx_retainer_SUITE:app_spec(enabled)],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)).

t_reindex_status(_Config) ->
    ok = emqx_retainer_cli:retainer(["reindex", "status"]).

t_info(_Config) ->
    ok = emqx_retainer_cli:retainer(["info"]).

t_topics(_Config) ->
    ok = emqx_retainer_cli:retainer(["topics"]).

t_topics_with_len(_Config) ->
    ok = emqx_retainer_cli:retainer(["topics", "100", "200"]).

t_clean(_Config) ->
    ok = emqx_retainer_cli:retainer(["clean"]).

t_topic(_Config) ->
    ok = emqx_retainer_cli:retainer(["clean", "foo/bar"]).

t_reindex(_Config) ->
    {ok, C} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(C),

    ok = emqx_retainer:clean(),

    ?check_trace(
        ?wait_async_action(
            lists:foreach(
                fun(N) ->
                    emqtt:publish(
                        C,
                        erlang:iolist_to_binary([
                            <<"retained/">>,
                            io_lib:format("~5..0w", [N])
                        ]),
                        <<"this is a retained message">>,
                        [{qos, 0}, {retain, true}]
                    )
                end,
                lists:seq(1, 1000)
            ),
            #{?snk_kind := message_retained, topic := <<"retained/01000">>},
            1000
        ),
        []
    ),

    emqx_config:put([retainer, backend, index_specs], [[4, 5]]),
    ok = emqx_retainer_cli:retainer(["reindex", "start"]),

    ?assertEqual(1000, mnesia:table_info(?TAB_INDEX, size)).
