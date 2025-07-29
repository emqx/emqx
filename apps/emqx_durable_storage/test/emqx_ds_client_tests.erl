%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_client_tests).

-compile(nowarn_export_all).
-compile(export_all).
-compile(noinline).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-include("emqx_ds.hrl").
-include("../src/emqx_ds_client_internals.hrl").
-include("emqx_ds_client_tests.hrl").

-define(fake_shards, [<<"0">>, <<"12">>]).
-define(test_sub_ids, [id1, id2]).
%% -define(fake_shards, [<<"0">>]).
%% -define(test_sub_ids, [id1]).

%%================================================================================
%% Basic tests
%%================================================================================

%% Fake reference that can be created deterministically:
-define(sub_handle(SUBID, REF), {h, SUBID, REF}).
-define(sub_ref(SUBID, HANDLE), {s, SUBID, HANDLE}).

filter_effects_test() ->
    IsEven = fun(E) -> (E rem 2) =:= 0 end,
    GS = #cs{plan = [1, 2, 3, 4], retry = [5, 6, 7, 8]},
    ?assertMatch(
        #cs{plan = [2, 4], retry = [6, 8]},
        emqx_ds_client:filter_effects(IsEven, GS)
    ).

%%================================================================================
%% Host state
%%================================================================================

%% Test CBM state:
-record(test_host_state, {
    %% Current generations:
    generations = #{} :: #{{emqx_ds_client:sub_id(), emqx_ds:shard()} => emqx_ds:generation()},
    %% Saved replay positions:
    iterators = #{} :: #{
        {emqx_ds_client:sub_id(), emqx_ds:stream()} => emqx_ds:iteraator() | end_of_stream
    }
}).

get_current_generation(SubId, Shard, #test_host_state{generations = Gens}) ->
    maps:get({SubId, Shard}, Gens, 0).

on_advance_generation(
    SubId, Shard, NextGen, HS = #test_host_state{generations = Gens0}
) ->
    OldCurrent = get_current_generation(SubId, Shard, HS),
    ?tp(info, test_host_advance_generation, #{
        subid => SubId, shard => Shard, old => OldCurrent, new => NextGen
    }),
    ?assert(
        NextGen > OldCurrent,
        {"New generation should be greater than the old one", NextGen, '>', OldCurrent}
    ),
    Gens = Gens0#{{SubId, Shard} => NextGen},
    HS#test_host_state{
        generations = Gens
    }.

on_new_iterator(
    SubId, _Slab, Stream, It, HS = #test_host_state{iterators = Its}
) ->
    ?tp(test_host_new_iterator, #{subid => SubId, stream => Stream, it => It}),
    ?assertNot(maps:is_key({SubId, Stream}, Its), "Client should not re-create iterators"),
    {subscribe, host_set_iter(SubId, Stream, It, HS)}.

host_get_iter(SubId, Stream, #test_host_state{iterators = Its}) ->
    maps:get({SubId, Stream}, Its, undefined).

host_set_iter(SubId, Stream, It, HS = #test_host_state{iterators = Its}) ->
    HS#test_host_state{
        iterators = Its#{{SubId, Stream} => It}
    }.

on_unrecoverable_error(SubId, _Slab, Stream, Reason, HS = #test_host_state{}) ->
    ?tp(test_host_unrecoverable, #{subid => SubId, stream => Stream, reason => Reason}),
    HS.

on_subscription_down(SubId, _Slab, Stream, HS) ->
    ?tp(test_host_sub_down, #{subid => SubId, stream => Stream}),
    HS.

get_iterator(SubId, _Slab, Stream, #test_host_state{iterators = Its}) ->
    case Its of
        #{{SubId, Stream} := It} ->
            {subscribe, It};
        #{} ->
            undefined
    end.

%%================================================================================
%% Proper test
%%================================================================================

-type error_type() :: ?err_get_streams | ?err_make_iterator | ?err_subscribe.

%% Model state (updated by proper command generator):
-record(model_state, {
    %% Counter for creating unique IDs:
    counter = 0 :: integer(),
    current_generation = #{} :: #{emqx_ds:shard() => emqx_ds:generation()},
    streams = [] :: [#fake_stream{}],
    %% Mask of injected recoverable errors for the shards:
    err_rec = #{} :: #{{emqx_ds:shard(), error_type()} => true},
    subs = #{} :: #{emqx_ds_client:sub_id() => {_DB, _Topic, _Opts}},
    exists = false :: boolean(),
    runtime = {undefined, #test_host_state{}}
}).

current_gen(Shard, #model_state{current_generation = CG}) ->
    maps:get(Shard, CG, 0).

%%------------------------------------------------------------------------------
%% Proper generators
%%------------------------------------------------------------------------------

-define(call_wrapper(MS, FUN, ARGS), {call, ?MODULE, wrapper, [MS, FUN, ARGS]}).

gen_add_generation(MS) ->
    ?LET(
        {Shard, Delta},
        {oneof(?fake_shards), range(1, 4)},
        ?call_wrapper(MS, add_generation, [Shard, current_gen(Shard, MS) + Delta])
    ).

gen_del_generation(MS) ->
    ?LET(
        Shard,
        oneof(?fake_shards),
        ?LET(
            {Generation, Graceful},
            {range(-2, current_gen(Shard, MS)), boolean()},
            ?call_wrapper(MS, del_generation, [Shard, Generation, Graceful])
        )
    ).

gen_add_stream(MS = #model_state{counter = Ctr}) ->
    ?LET(
        Shard,
        oneof(?fake_shards),
        begin
            Stream = #fake_stream{shard = Shard, gen = current_gen(Shard, MS), id = Ctr},
            ?call_wrapper(MS, add_stream, [Stream])
        end
    ).

gen_new(MS) ->
    exactly(?call_wrapper(MS, new_client, [])).

gen_destroy(MS = #model_state{}) ->
    exactly(?call_wrapper(MS, client_destroy, [])).

gen_subscribe(MS) ->
    ?LET(
        Id,
        oneof(?test_sub_ids),
        ?call_wrapper(MS, client_subscribe, [Id, ?DB, [<<"test_topic">>]])
    ).

gen_unsubscribe(MS) ->
    ?LET(
        Id,
        oneof(?test_sub_ids),
        ?call_wrapper(MS, client_unsubscribe, [Id])
    ).

gen_error_type() ->
    oneof([
        ?err_get_streams,
        ?err_make_iterator,
        ?err_subscribe,
        ?err_publish
    ]).

gen_inject_error(MS) ->
    ?LET(
        {Shard, Mask},
        {oneof(?fake_shards), gen_error_type()},
        ?call_wrapper(MS, inject_error, [Shard, Mask])
    ).

gen_fix_error(MS) ->
    ?LET(
        {Shard, Mask},
        oneof(maps:keys(MS#model_state.err_rec)),
        ?call_wrapper(MS, fix_error, [Shard, Mask])
    ).

gen_fix_all_errors(MS) ->
    exactly(?call_wrapper(MS, fix_all_errors, [])).

gen_ds_sub_recoverable_error(MS) ->
    ?LET(
        Reason,
        oneof(['DOWN', ?err_rec(simulated)]),
        ?call_wrapper(MS, ds_sub_recoverable_error, [Reason])
    ).

gen_ds_sub_payloads(MS) ->
    ?LET(
        {BatchSize, SeqNoError, GracefulTerminateGoneStreams},
        {
            range(0, 5),
            frequency([
                {5, 0},
                {1, range(-3, 3)}
            ]),
            boolean()
        },
        ?call_wrapper(MS, ds_publish_payloads, [BatchSize, SeqNoError, GracefulTerminateGoneStreams])
    ).

%%------------------------------------------------------------------------------
%% Proper statem callbacks
%%------------------------------------------------------------------------------

initial_state() ->
    #model_state{}.

command(MS = #model_state{exists = false}) ->
    gen_new(MS);
command(MS = #model_state{err_rec = Errors, subs = Subs}) ->
    frequency(
        [{3, gen_fix_all_errors(MS)} || maps:size(Errors) > 0] ++
            [{3, gen_fix_error(MS)} || maps:size(Errors) > 0] ++
            [{2, gen_ds_sub_recoverable_error(MS)} || maps:size(Subs) > 0] ++
            [{2, gen_ds_sub_payloads(MS)} || maps:size(Subs) > 0] ++
            [
                {3, gen_inject_error(MS)},
                {1, gen_destroy(MS)},
                {3, gen_subscribe(MS)},
                {2, gen_unsubscribe(MS)},
                {5, gen_add_generation(MS)},
                {1, gen_del_generation(MS)},
                {5, gen_add_stream(MS)}
            ]
    ).

next_state(MS0, RuntimeState, ?call_wrapper(_, Fun, Args)) ->
    MS = next_state_(MS0, Fun, Args),
    MS#model_state{runtime = RuntimeState}.

next_state_(ModelState, new_client, _) ->
    ModelState#model_state{
        exists = true
    };
next_state_(ModelState, client_destroy, _) ->
    ModelState#model_state{
        exists = false,
        subs = #{}
    };
next_state_(MS = #model_state{subs = Subs}, client_subscribe, [SubId, DB, Topic]) ->
    MS#model_state{
        subs = Subs#{SubId => {DB, Topic}}
    };
next_state_(MS = #model_state{subs = Subs}, client_unsubscribe, [SubId]) ->
    MS#model_state{
        subs = maps:remove(SubId, Subs)
    };
next_state_(MS = #model_state{current_generation = CG}, add_generation, [Shard, Generation]) ->
    MS#model_state{
        current_generation = CG#{Shard => Generation}
    };
next_state_(MS = #model_state{streams = Streams0}, del_generation, [Shard, Generation, _Graceful]) ->
    Streams = lists:filter(
        fun(#fake_stream{shard = S, gen = G}) ->
            S =/= Shard orelse G > Generation
        end,
        Streams0
    ),
    MS#model_state{streams = Streams};
next_state_(MS = #model_state{streams = Streams, counter = Ctr}, add_stream, [Stream]) ->
    MS#model_state{
        counter = Ctr + 1,
        streams = [Stream | Streams]
    };
next_state_(MS = #model_state{err_rec = Errors}, inject_error, [Shard, ErrorType]) ->
    MS#model_state{
        err_rec = Errors#{{Shard, ErrorType} => true}
    };
next_state_(MS, ds_sub_recoverable_error, _) ->
    MS;
next_state_(MS, ds_publish_payloads, _) ->
    MS;
next_state_(MS = #model_state{err_rec = Errors}, fix_error, [Shard, ErrorType]) ->
    MS#model_state{
        err_rec = maps:remove({Shard, ErrorType}, Errors)
    };
next_state_(MS = #model_state{}, fix_all_errors, _) ->
    MS#model_state{
        err_rec = #{}
    }.

precondition(#model_state{subs = Subs}, ?call_wrapper(_, client_subscribe, [SubId | _])) ->
    not maps:is_key(SubId, Subs);
precondition(#model_state{subs = Subs}, ?call_wrapper(_, client_unsubscribe, [SubId | _])) ->
    maps:is_key(SubId, Subs);
precondition(_, _) ->
    true.

postcondition(PrevState, Call, Result) ->
    CurrentState = next_state(PrevState, Result, Call),
    prop_ownership(CurrentState),
    prop_active_subscriptions(CurrentState),
    prop_no_pending_when_healthy(CurrentState),
    prop_host_seen_all_streams(CurrentState),
    prop_host_has_iterators_for_all_model_streams(CurrentState),
    true.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

proper_test_() ->
    {setup,
        fun() ->
            emqx_ds_fake:load()
        end,
        fun(Fake) ->
            emqx_ds_fake:unload(Fake)
        end,
        fun(_) ->
            {timeout, 120, [fun run_proper/0]}
        end}.

run_proper() ->
    ProperOpts = [
        {numtests, 200},
        {max_size, 100},
        {on_output, fun(Fmt, Args) -> io:format(user, Fmt, Args) end},
        {max_shrinks, 1000}
    ],
    ?assert(
        proper:quickcheck(
            ?forall_trace(
                Cmds,
                proper_statem:more_commands(
                    2,
                    proper_statem:commands(?MODULE)
                ),
                try
                    emqx_ds_fake:new(?fake_shards),
                    {_History, _State, Result} = proper_statem:run_commands(?MODULE, Cmds),
                    ?assertMatch(ok, Result),
                    aggregate(command_names(Cmds), true)
                after
                    emqx_ds_fake:destroy()
                end,
                []
            ),
            ProperOpts
        )
    ).

format_cmds(Cmds) ->
    lists:map(
        fun({set, _, ?call_wrapper(_, Fun, Args)}) ->
            io_lib:format("   ~p ~p~n", [Fun, Args])
        end,
        Cmds
    ).

format_state(MS = #model_state{runtime = {CS, HS}}) ->
    ?record_to_map(model_state, MS#model_state{
        runtime = #{
            client => emqx_ds_client:inspect(CS),
            host => ?record_to_map(test_host_state, HS),
            world => emqx_ds_fake:inspect()
        }
    }).

%%------------------------------------------------------------------------------
%% Properties
%%------------------------------------------------------------------------------

%% This function verifies 1:1 relation between watches and
%% subscriptions owned by the client and those that exist in the fake
%% world. That is, there aren't any dangling or leaked subscriptions.
prop_ownership(#model_state{runtime = {CS, _}}) ->
    Subs = emqx_ds_fake:ls_subs(),
    Watches = emqx_ds_fake:ls_watches(),
    case CS of
        undefined ->
            %% Client doesn't exist:
            snabbkaffe_diff:assert_lists_eq(
                [],
                Watches,
                #{comment => "Leaked watches"}
            ),
            snabbkaffe_diff:assert_lists_eq(
                [],
                Subs,
                #{comment => "Leaked DS subscriptions"}
            );
        #cs{new_streams_watches = OwnedWatches, ds_subs = OwnedSubs} ->
            %% Client exists:
            Comment = #{cs => emqx_ds_client:inspect(CS), ws => emqx_ds_fake:inspect()},
            snabbkaffe_diff:assert_lists_eq(
                lists:sort(maps:keys(OwnedWatches)),
                lists:sort(Watches),
                #{comment => Comment}
            ),
            snabbkaffe_diff:assert_lists_eq(
                lists:sort(maps:keys(OwnedSubs)),
                lists:sort(Subs),
                #{comment => Comment}
            )
    end,
    true.

%% This function verifies result of `emqx_ds_client:dispatch' function.
-spec prop_dispatch_result(_Message, emqx_ds_client:t(), _DispatchResult) -> ok.
prop_dispatch_result(
    #emqx_ds_client_retry{ref = Ref}, CS = #cs{ref = CRef, retry_tref = TRef}, Result
) ->
    case Result of
        {_CS, _HS} when is_reference(TRef), Ref =:= CRef ->
            %% Retry timer is active, client should update its state.
            ok;
        ignore ->
            %% Otherwise it should ignore the retry timers:
            ok;
        _ ->
            error({unexpected_retry, #{result => Result, ref => Ref, state => CS}})
    end;
prop_dispatch_result(#new_stream_event{subref = W}, CS, Result) ->
    case maps:is_key(W, CS#cs.new_streams_watches) of
        true ->
            ?assertMatch(
                {_CS, _HS},
                Result,
                """
                Client should update its state when it receives a new stream
                notification for an existing watch
                """
            );
        false ->
            ?assertMatch(
                ignore,
                Result,
                "Client should ignore unknown stream notifications"
            )
    end;
prop_dispatch_result({'DOWN', MRef, _, _, _}, CS, Result) ->
    case maps:is_key(MRef, CS#cs.ds_subs) of
        true ->
            ?assertMatch(
                {_CS, _HS},
                Result,
                "Client should update its state when a DS subscription dies"
            );
        false ->
            ?assertMatch(
                ignore,
                Result,
                "Client should ignore stray DOWN messages"
            )
    end;
prop_dispatch_result(Msg = #ds_sub_reply{ref = Ref}, CS, Result) ->
    IsSub = maps:is_key(Ref, CS#cs.ds_subs),
    case Result of
        {_CS, _HS} when IsSub ->
            ok;
        {data, _SubId, _Stream, #ds_sub_reply{}} when IsSub ->
            ok;
        ignore when not IsSub ->
            ok;
        _ ->
            error(
                {"Invalid response from dispatch_message function", #{
                    msg => Msg,
                    is_owned => IsSub,
                    state => emqx_ds_client:inspect(CS),
                    result => Result
                }}
            )
    end;
prop_dispatch_result(Message, Client, Result) ->
    error(
        {"Invalid response from dispatch_message function", #{
            msg => Message,
            state => emqx_ds_client:inspect(Client),
            result => Result
        }}
    ).

prop_host_seen_all_streams(#model_state{runtime = {undefined, _}}) ->
    %% Client doesn't exist, nothing to verify.
    true;
prop_host_seen_all_streams(#model_state{
    err_rec = ErrRec, runtime = {CS, HS}, streams = Streams
}) ->
    case maps:size(ErrRec) of
        0 ->
            %% When system is healthy state, the following should hold:
            %%
            %% 1. All streams for the existing subscriptions for the current
            %% generation should be active. Iterators should exist in the test
            %% host state.
            %%
            %% 2. All streams for the past generations should be fully
            %% replayed.
            %%
            %% 3. There should not be any record of streams for the
            %% future generations in the host state.
            #cs{subs = Subs} = CS,
            #test_host_state{iterators = HostIters} = HS,
            %% Run check for each subscription:
            maps:foreach(
                fun(SubId, _Sub) ->
                    %% Cache current generations for the subscription:
                    CurrentGens = maps:from_list(
                        [
                            {Shard, get_current_generation(SubId, Shard, HS)}
                         || Shard <- ?fake_shards
                        ]
                    ),
                    lists:foreach(
                        fun(Stream = #fake_stream{shard = Shard, gen = Gen}) ->
                            case maps:get(Shard, CurrentGens) of
                                Current when Gen > Current ->
                                    ?assertNot(
                                        maps:is_key({SubId, Stream}, HostIters),
                                        "Stream from a future generation, iterator should not exist"
                                    );
                                Current when Gen =:= Current ->
                                    ?assertMatch(
                                        #{{SubId, Stream} := _},
                                        HostIters,
                                        #{
                                            msg =>
                                                "Current generation, there should be an iterator or `end_of_stream'",
                                            sub_id => SubId,
                                            stream => Stream,
                                            cs => emqx_ds_client:inspect(CS),
                                            gen => Gen
                                        }
                                    );
                                Current when Gen < Current ->
                                    %% Note: test host doesn't clean up replayed streams.
                                    ?assertMatch(
                                        #{{SubId, Stream} := end_of_stream},
                                        HostIters,
                                        #{
                                            msg =>
                                                "Past generation, stream should be fully replayed",
                                            sub_id => SubId,
                                            stream => Stream,
                                            cs => emqx_ds_client:inspect(CS),
                                            gen => Gen
                                        }
                                    )
                            end
                        end,
                        Streams
                    )
                end,
                Subs
            ),
            true;
        _ ->
            %% There are injected errors. System is in unknown state.
            %% Skip verification until it recovers.
            true
    end.

%% When the system is healthy, all streams for the current generation
%% should be either fully replayed or active.
prop_no_pending_when_healthy(#model_state{err_rec = Errors, runtime = {CS, _}}) ->
    Healthy = maps:size(Errors) =:= 0,
    case CS of
        #cs{} when Healthy ->
            %% Client exists and the system is healthy:
            maps:foreach(
                fun({SubId, Shard}, #stream_cache{pending_iterator = Pending}) ->
                    ?assertMatch(
                        [],
                        Pending,
                        #{
                            msg => "All iterators should be present when the system is healthy",
                            subid => SubId,
                            shard => Shard
                        }
                    )
                end,
                CS#cs.streams
            );
        _ ->
            ok
    end.

%% There is 1:1 correspondence between active streams and DS subscriptions:
prop_active_subscriptions(#model_state{runtime = {undefined, _HS}}) ->
    ok;
prop_active_subscriptions(#model_state{runtime = {CS, _HS}}) ->
    #cs{ds_subs = DSSubs, streams = Streams} = CS,
    %% Collect all active streams with subscriptions:
    ActiveStreams = maps:fold(
        fun(_, #stream_cache{active = Active}, Acc) ->
            maps:fold(
                fun
                    (_, {_It, undefined}, Acc1) ->
                        Acc1;
                    (_, {_It, SubRef}, Acc1) ->
                        [SubRef | Acc1]
                end,
                Acc,
                Active
            )
        end,
        [],
        Streams
    ),
    %% Compare the result with the DS subs:
    snabbkaffe_diff:assert_lists_eq(
        lists:sort(ActiveStreams),
        lists:sort(maps:keys(DSSubs)),
        #{comment => emqx_ds_client:inspect(CS)}
    ).

%% Verify that when the system is healthy, the host is aware of all
%% streams existing in the model.
prop_host_has_iterators_for_all_model_streams(
    MS = #model_state{err_rec = Errors, runtime = {CS, HS}, streams = ModelStreams, subs = Subs}
) ->
    IsSystemHealthy = maps:size(Errors) =:= 0,
    IsSystemHealthy andalso
        maps:foreach(
            fun(SubId, _) ->
                HostStreams = maps:fold(
                    fun({SID, Stream}, _, Acc) ->
                        case SID of
                            SubId ->
                                [Stream | Acc];
                            _ ->
                                Acc
                        end
                    end,
                    [],
                    HS#test_host_state.iterators
                ),
                Comment = #{
                    sub_id => SubId,
                    msg => "Host should not miss any streams that exist in the model",
                    ms => format_state(MS)
                },
                %% Note: extra streams are allowed, since the host doesn't delete streams from deleted generations:
                snabbkaffe_diff:assert_lists_eq(
                    [],
                    ModelStreams -- HostStreams,
                    #{comment => Comment}
                )
            end,
            Subs
        ),
    true.

%%------------------------------------------------------------------------------
%% Commands
%%------------------------------------------------------------------------------

add_stream(#model_state{runtime = Runtime}, Stream) ->
    emqx_ds_fake:add_stream(Stream),
    Runtime.

add_generation(#model_state{runtime = Runtime}, Shard, Generation) ->
    emqx_ds_fake:add_generation(Shard, Generation),
    Runtime.

del_generation(#model_state{runtime = Runtime}, Shard, Generation, Graceful) ->
    emqx_ds_fake:del_generation(Shard, Generation, Graceful),
    Runtime.

inject_error(#model_state{runtime = Runtime}, Shard, ErrType) ->
    emqx_ds_fake:seterr(Shard, ErrType),
    Runtime.

fix_error(#model_state{runtime = Runtime}, Shard, ErrType) ->
    emqx_ds_fake:fixerr(Shard, ErrType),
    Runtime.

fix_all_errors(#model_state{runtime = Runtime}) ->
    emqx_ds_fake:fixall(),
    Runtime.

ds_publish_payloads(
    #model_state{runtime = Runtime}, BatchSize, SeqNoError, GracefulTerminateGoneStreams
) ->
    emqx_ds_fake:publish_payloads(BatchSize, SeqNoError, GracefulTerminateGoneStreams),
    Runtime.

ds_sub_recoverable_error(#model_state{runtime = Runtime}, Reason) ->
    emqx_ds_fake:ds_sub_recoverable_error(Reason),
    Runtime.

new_client(#model_state{runtime = {_, HS}}) ->
    CS = emqx_ds_client:new(?MODULE, #{retry_interval => timer:seconds(1000)}),
    {CS, HS}.

client_destroy(#model_state{runtime = {CS, HS0}}) ->
    HS = emqx_ds_client:destroy(CS, HS0),
    {undefined, HS}.

client_subscribe(#model_state{runtime = {CS0, HS0}}, Id, DB, Topic) ->
    {ok, CS, HS} = emqx_ds_client:subscribe(CS0, #{id => Id, db => DB, topic => Topic}, HS0),
    {CS, HS}.

client_unsubscribe(#model_state{runtime = {CS0, HS0}}, SubId) ->
    {ok, CS, HS} = emqx_ds_client:unsubscribe(CS0, SubId, HS0),
    {CS, HS}.

%%------------------------------------------------------------------------------
%% Helper functions
%%------------------------------------------------------------------------------

wrapper(MS0, Fun, Args) ->
    %% Due to PropEr design, here MS0 is the model state _before_
    %% applying the effect. This is fairly inconvenient. Advance the
    %% model state to the current accoding to the symbolic execution
    %% rule:
    MS = next_state_(MS0, Fun, Args),
    ?tp("test_" ++ atom_to_list(Fun), #{args => Args}),
    %% Apply the function to the state and also verify the return
    %% value of the operation, it should be a valid runtime state
    %% triple:
    RS = {CS, #test_host_state{}} = apply(?MODULE, Fun, [MS | Args]),
    %% Emulate firing of the retry timer if the client exists:
    case CS of
        undefined ->
            RS;
        #cs{ref = Ref} ->
            %% Emulate retry timer firing:
            self() ! #emqx_ds_client_retry{ref = Ref},
            dispatch_messages(RS)
    end.

dispatch_messages({CS0, HS0}) ->
    receive
        Message ->
            ?tp(test_client_message, #{
                message => Message,
                cs => emqx_ds_client:inspect(CS0),
                hs => ?record_to_map(test_host_state, HS0)
            }),
            Result = emqx_ds_client:dispatch_message(Message, CS0, HS0),
            prop_dispatch_result(Message, CS0, Result),
            case Result of
                ignore ->
                    dispatch_messages({CS0, HS0});
                {data, SubId, Stream, Reply} ->
                    case Reply of
                        #ds_sub_reply{ref = Ref, payload = {ok, end_of_stream}} ->
                            HS1 = host_set_iter(SubId, Stream, end_of_stream, HS0),
                            dispatch_messages(emqx_ds_client:complete_stream(CS0, Ref, HS1));
                        #ds_sub_reply{} ->
                            dispatch_messages({CS0, HS0})
                    end;
                {CS, HS} ->
                    dispatch_messages({CS, HS})
            end
    after 0 -> {CS0, HS0}
    end.
