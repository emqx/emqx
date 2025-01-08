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
-module(emqx_machine_replicant_health_probe).

%% @doc This process performs periodic checks to assess the health of a replicant node.
%% It's not started on core nodes.
%% This process must start before we call `emqx_machine_boot:post_boot' because
%% applications might hang while waiting for tables.

-feature(maybe_expr, enable).

%% API
-export([
    start_link/0
]).

%% `gen_server' API
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-include_lib("emqx/include/logger.hrl").

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(COUNT_CORE_INTERVAL, 30_000).
-define(INITIAL_COUNT_CORE_INTERVAL, 10_000).

%% calls/casts/infos
-record(check_core_node_count, {}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

start_link() ->
    gen_server:start_link(?MODULE, [], []).

%%------------------------------------------------------------------------------
%% `gen_server' API
%%------------------------------------------------------------------------------

init(_Opts) ->
    case mria_rlog:role() of
        core ->
            ignore;
        replicant ->
            State = #{},
            _ = start_core_node_count_timeout(?INITIAL_COUNT_CORE_INTERVAL),
            {ok, State}
    end.

handle_call(_Call, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(#check_core_node_count{}, State) ->
    _ = handle_check_core_node_count(),
    _ = start_core_node_count_timeout(),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

%% Counts how many known core nodes are running under the same version as the current
%% node.  If none, prints a warning hinting the user how to proceed with rolling upgrade.
handle_check_core_node_count() ->
    CoreInfos = get_core_custom_infos(),
    case do_check_core_node_count(CoreInfos) of
        ok ->
            ok;
        {error, #{known_cores := KnownCores}} ->
            Hint = iolist_to_binary([
                <<"There are no core EMQX nodes running the same version as this node.">>,
                <<" If you are performing a rolling upgrade, please keep different core node">>,
                <<" versions (both old and new) around until all replicant nodes are migrated.">>
            ]),
            ?SLOG(warning, #{
                msg => "no_available_core_node_at_same_version",
                hint => Hint,
                known_cores => KnownCores
            }),
            ok
    end.

do_check_core_node_count(CoreInfos) ->
    CoresAtSameVersion =
        lists:filtermap(
            fun({N, MaybeInfo}) ->
                maybe
                    {ok, OtherVsn} ?= MaybeInfo,
                    true ?= emqx_machine:mria_lb_custom_info_check(OtherVsn),
                    {true, N}
                else
                    _ -> false
                end
            end,
            CoreInfos
        ),
    case CoresAtSameVersion of
        [_ | _] ->
            ok;
        [] ->
            CoreInfos1 =
                lists:foldl(
                    fun({N, MaybeInfo}, Acc) ->
                        case MaybeInfo of
                            {ok, Vsn} ->
                                Acc#{N => #{version => Vsn}};
                            Error ->
                                Acc#{N => #{probe_failure => Error}}
                        end
                    end,
                    #{},
                    CoreInfos
                ),
            {error, #{known_cores => CoreInfos1}}
    end.

get_core_custom_infos() ->
    Timeout = 15_000,
    KnownCores = mria_rlog:core_nodes(),
    lists:zip(
        KnownCores,
        erpc:multicall(KnownCores, emqx_machine, mria_lb_custom_info, [], Timeout)
    ).

start_core_node_count_timeout() ->
    start_core_node_count_timeout(?COUNT_CORE_INTERVAL).

start_core_node_count_timeout(Timeout) ->
    _ = erlang:send_after(Timeout, self(), #check_core_node_count{}),
    ok.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

check_core_node_count_test_() ->
    _ = application:load(emqx),
    CurrentVsn = emqx_machine:mria_lb_custom_info(),
    %% assert
    true = is_list(CurrentVsn),
    [
        {"no known cores",
            ?_assertEqual({error, #{known_cores => #{}}}, do_check_core_node_count([]))},
        {"1 core at same version",
            ?_assertEqual(ok, do_check_core_node_count([{c1, {ok, CurrentVsn}}]))},
        {"1 core at same version, another at different version (1)",
            ?_assertEqual(
                ok,
                do_check_core_node_count([{c1, {ok, CurrentVsn}}, {c2, {ok, "other version"}}])
            )},
        {"1 core at same version, another at different version (2)",
            ?_assertEqual(
                ok,
                do_check_core_node_count([{c2, {ok, "other version"}}, {c1, {ok, CurrentVsn}}])
            )},
        {"only cores at different versions (1)",
            ?_assertEqual(
                {error, #{known_cores => #{c1 => #{version => "v1"}}}},
                do_check_core_node_count([{c1, {ok, "v1"}}])
            )},
        {"only cores at different versions (2)",
            ?_assertEqual(
                {error, #{known_cores => #{c1 => #{version => "v1"}, c2 => #{version => "v2"}}}},
                do_check_core_node_count([{c2, {ok, "v2"}}, {c1, {ok, "v1"}}])
            )},
        {"undefined vsn",
            ?_assertEqual(
                {error, #{known_cores => #{c1 => #{version => undefined}}}},
                do_check_core_node_count([{c1, {ok, undefined}}])
            )},
        {"erpc error (1)",
            ?_assertEqual(
                {error, #{known_cores => #{c1 => #{probe_failure => {error, {erpc, timeout}}}}}},
                do_check_core_node_count([{c1, {error, {erpc, timeout}}}])
            )},
        {"erpc error (2)",
            ?_assertMatch(
                {error, #{
                    known_cores := #{c1 := #{probe_failure := {error, {exception, undef, _}}}}
                }},
                do_check_core_node_count([
                    {c1, {error, {exception, undef, [{emqx_machine, mria_lb_custom_info, []}]}}}
                ])
            )}
    ].
-endif.
