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

-module(prop_emqx_route_index).

-compile(export_all).

-include_lib("stdlib/include/assert.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx/include/emqx.hrl").

-define(TOPIC, <<"t/#">>).

-if(false).
-define(TRACE(FMT, ARGS),
    io:format("~s [~p] ~p/~p:~p " FMT "~n", [
        calendar:system_time_to_rfc3339(erlang:system_time(millisecond), [{unit, millisecond}]),
        self(),
        ?FUNCTION_NAME,
        ?FUNCTION_ARITY,
        ?LINE
        | ARGS
    ])
).
-else.
-define(TRACE(FMT, ARGS), []).
-endif.

-import(emqx_proper_types, [scaled/2]).

prop_concurrent_route_consistency() ->
    % Verify that the index built from an inconsistent view of the route table
    % (due to concurrent updates) eventually converges with the route table,
    % after concurrent updates stream ends.
    %
    % Essentially we want to verify that what happens in `emqx_route_indexer` does
    % not break consistency in the presence of concurrent table updates. Particularly:
    % 1. Subscribe to table updates.
    % 2. Run fold over route table to build index.
    % 3. Apply accumulated table updates to the index.
    ?FORALL(
        Stream,
        scaled(10, stream_t()),
        verify_consistency(run_stream(Stream))
    ).

verify_consistency(Ctx = #{route_tab := RouteTab, index_tab := IndexTab}) ->
    ?assertEqual(
        lists:sort(ets:tab2list(RouteTab)),
        lists:sort(emqx_route_index:all(IndexTab))
    ),
    _ = cleanup(Ctx),
    true.

run_stream(Stream) ->
    % Stream usually looks like this:
    % ```
    % [{insert,#route{topic = <<"t/#">>,dest = 1}},
    %  {insert,#route{topic = <<"t/#">>,dest = 5}},
    %  {insert,#route{topic = <<"t/#">>,dest = 8}},
    %  {indexer,start},
    %  {insert,#route{topic = <<"t/#">>,dest = 1}},
    %  {insert,#route{topic = <<"t/#">>,dest = 4}},
    %  {insert,#route{topic = <<"t/#">>,dest = 9}},
    %  {fold,1},
    %  {delete,#route{topic = <<"t/#">>,dest = 2}},
    %  {fold,finish},
    %  {delete,#route{topic = <<"t/#">>,dest = 4}},
    %  {delete,#route{topic = <<"t/#">>,dest = 1}},
    %  eos]
    % ```
    ok = mria:start(),
    RouteTab = mk_route_tab(),
    ok = emqx_route_index:init(?MODULE),
    run_stream(Stream, #{route_tab => RouteTab, index_tab => ?MODULE}).

run_stream(Stream, CtxIn) ->
    lists:foldl(fun run_command/2, CtxIn, Stream).

run_command({insert, Route} = Update, Ctx = #{route_tab := Tab}) ->
    % 1. Insert route in the route table.
    % 2. Send update to the indexer process, if it's already online.
    ?TRACE("-- ~0p", [Update]),
    true = ets:insert(Tab, Route),
    _ = send_update(Update, Ctx),
    Ctx;
run_command({delete, Route} = Update, Ctx = #{route_tab := Tab}) ->
    % 1. Delete route from the route table.
    % 2. Send update to the indexer process, if it's already online.
    ?TRACE("-- ~0p", [Update]),
    true = ets:delete_object(Tab, Route),
    _ = send_update(Update, Ctx),
    Ctx;
run_command({indexer, start}, Ctx = #{route_tab := Tab, index_tab := Idx}) ->
    % 1. Start the indexer process.
    %    It will wait for a `{fold, ...}` message before starting `ets:foldl` over the
    %    route table, but will still accumulate updates in the mailbox, to process them
    %    after the fold ends.
    Pid = erlang:spawn_link(fun() -> run_indexer(Tab, Idx) end),
    Ctx#{indexer => Pid};
run_command({fold, _} = Cmd, Ctx = #{indexer := Pid}) ->
    % 1. Send `{fold, Next}` command to the indexer process.
    %    First such message will start `ets:foldl` over the route table. `Next` may be
    %    either number, which tells indexer how many table entries to fold before
    %    suspending, or atom `finish`, which tell indexer to run fold to completion.
    % 2. Sleep for a bit, to force a kind of concurrency.
    _ = Pid ! Cmd,
    _ = timer:sleep(1),
    Ctx;
run_command(eos, Ctx = #{indexer := Pid}) ->
    % 1. Notify indexer that we're done
    % 2. Wait until it empties the queue and exits.
    _ = Pid ! {eos, self()},
    _ = ?assertReceive(done),
    Ctx#{indexer := done}.

send_update(Update, #{indexer := Pid}) ->
    Pid ! Update;
send_update(_Update, #{}) ->
    ok.

run_indexer(Tab, Idx) ->
    _ = run_fold(Tab, Idx),
    run_updater(Idx).

run_fold(Tab, Idx) ->
    ?TRACE("-- fold wait", []),
    receive
        {fold, Next} ->
            ets:foldl(fun(Route, N) -> run_fold_iter(N, Route, Idx) end, Next, Tab)
    end.

run_fold_iter(0, Route, Idx) ->
    ?TRACE("-- fold wait", []),
    receive
        {fold, Next} -> run_fold_iter(Next, Route, Idx)
    end;
run_fold_iter(Next, Route, Idx) ->
    ?TRACE("-- ~p | Route = ~0p", [Next, Route]),
    true = emqx_route_index:insert(Route, Idx, unsafe),
    case Next of
        finish -> finish;
        N -> N - 1
    end.

run_updater(Idx) ->
    ?TRACE("-- start", []),
    receive
        {insert, Route} ->
            ?TRACE("-- {insert, ~0p}", [Route]),
            true = emqx_route_index:insert(Route, Idx, unsafe),
            run_updater(Idx);
        {delete, Route} ->
            ?TRACE("-- {delete, ~0p}", [Route]),
            true = emqx_route_index:delete(Route, Idx, unsafe),
            run_updater(Idx);
        {eos, Caller} ->
            ?TRACE("-- eos", []),
            Caller ! done
    end.

stream_t() ->
    ?LET(
        {Updates, Fold, Iters},
        {non_empty(list(update_t())), fold_t(), list(fold_iter_t())},
        sanitize_stream(
            [{Op, Arg} || {_, Op, Arg} <- lists:sort(lists:flatten(Updates ++ Fold ++ Iters))],
            #{}
        )
    ).

sanitize_stream([Command | Rest], St) ->
    case sanitize_command(Command, St) of
        StNext = #{} ->
            [Command | sanitize_stream(Rest, StNext)];
        false ->
            sanitize_stream(Rest, St)
    end;
sanitize_stream([], _) ->
    [eos].

sanitize_command({insert, Route}, St) ->
    St#{Route => inserted};
sanitize_command({delete, Route}, St) when map_get(Route, St) == inserted ->
    St#{Route => deleted};
sanitize_command({delete, _Route}, _St) ->
    false;
sanitize_command({indexer, start}, St) ->
    St#{indexer => fold};
sanitize_command({fold, finish}, St) ->
    St#{indexer => update};
sanitize_command({fold, _N}, St = #{indexer := fold}) ->
    St;
sanitize_command({fold, _N}, _St) ->
    false.

update_t() ->
    ?LET(
        {I, D, Route},
        {order_t(), delay_t(), route_t()},
        oneof([
            [{I, insert, Route}],
            [{I, insert, Route}, {I + D, delete, Route}]
        ])
    ).

fold_t() ->
    ?LET(
        {I, D},
        {order_t(), delay_t()},
        [{I, indexer, start}, {I + D, fold, finish}]
    ).

fold_iter_t() ->
    ?LET(I, order_t(), [{I, fold, range(1, 3)}]).

order_t() ->
    pos_integer().

delay_t() ->
    pos_integer().

route_t() ->
    ?LET(Dest, dest_t(), #route{topic = ?TOPIC, dest = Dest}).

dest_t() ->
    oneof(lists:seq(1, 9)).

%%

mk_route_tab() ->
    % TODO: should be identical to `emqx_router` backing table.
    ets:new(emqx_route, [
        bag,
        {keypos, #route.topic},
        {read_concurrency, true},
        {write_concurrency, true}
    ]).

cleanup(#{route_tab := RouteTab, index_tab := IndexTab}) ->
    _ = ets:delete(RouteTab),
    _ = mnesia:delete_table(IndexTab),
    ok.
