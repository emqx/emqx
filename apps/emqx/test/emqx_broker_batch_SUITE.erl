%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_broker_batch_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_router.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    [{group, v2}, {group, v3}].

groups() ->
    Cases = [t_subscribe_batch, t_unsubscribe_order, t_other_call_order],
    [{v2, [], Cases ++ [t_syncer]}, {v3, [], Cases}].

init_per_group(Schema, Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx, #{
                override_env => [{boot_modules, [broker]}],
                config => #{
                    <<"node">> => #{<<"broker_pool_size">> => 1},
                    <<"broker">> => #{
                        <<"routing">> => #{
                            <<"storage_schema">> => atom_to_binary(Schema),
                            <<"batch_sync">> => #{<<"enable_on">> => <<"none">>}
                        }
                    }
                }
            }}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    ?assertEqual(Schema, emqx_router:get_schema_vsn()),
    [{apps, Apps} | Config].

end_per_group(_Group, Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_Case, Config) ->
    [Worker] = emqx_broker_sup:get_broker_pool_workers(),
    meck:new(emqx_router, [passthrough, no_link]),
    [{worker, Worker} | Config].

end_per_testcase(_Case, Config) ->
    Worker = ?config(worker, Config),
    Worker ! continue_batch,
    _ = catch sys:resume(Worker),
    _ = sys:get_state(Worker),
    meck:unload(emqx_router),
    lists:foreach(
        fun
            (<<"batch/", _/binary>> = Topic) ->
                ets:delete(?SUBSCRIBER, Topic),
                ok = emqx_router:do_delete_route(Topic);
            (_) ->
                ok
        end,
        emqx_router:topics()
    ),
    emqx_config:put([broker, routing, batch_sync, enable_on], none).

-doc "Queued subscriptions share a route activity and receive replies after all routes exist.".
t_subscribe_batch(Config) ->
    Worker = ?config(worker, Config),
    Existing = <<"batch/existing">>,
    Plain = <<"batch/plain">>,
    Wildcard = <<"batch/+/wild">>,
    ok = gen_server:call(Worker, {subscribe, Existing, self(), 0}),
    meck:reset(emqx_router),
    TestPid = self(),
    meck:expect(emqx_router, do_add_routes, fun(Topics) ->
        TestPid ! {route_batch, self(), Topics},
        receive
            continue_batch -> meck:passthrough([Topics])
        after 5000 ->
            error(batch_not_released)
        end
    end),
    Requests = with_suspended(Worker, fun() ->
        [
            request(Worker, Plain, self()),
            request(Worker, Wildcard, self()),
            request(Worker, Plain, Worker),
            request(Worker, Existing, self())
        ]
    end),
    receive
        {route_batch, Worker, Topics} ->
            ?assertEqual(lists:sort([Plain, Wildcard]), lists:sort(Topics))
    after 5000 ->
        ct:fail(no_route_batch)
    end,
    lists:foreach(
        fun(Request) -> ?assertEqual(timeout, gen_server:wait_response(Request, 0)) end,
        Requests
    ),
    ?assertEqual([], emqx_broker:subscribers(Plain)),
    Worker ! continue_batch,
    await_replies(Requests),
    ?assertEqual(lists:sort([self(), Worker]), lists:sort(emqx_broker:subscribers(Plain))),
    ?assertEqual([self()], emqx_broker:subscribers(Wildcard)),
    ?assertEqual([self()], emqx_broker:subscribers(Existing)),
    ?assertEqual(
        [#route{topic = Wildcard, dest = node()}],
        emqx_router:match_routes(<<"batch/x/wild">>)
    ),
    ?assert(emqx_router:has_route(Plain, node())),
    ?assertEqual(1, meck:num_calls(emqx_router, do_add_routes, '_')).

-doc "An unsubscribe queued before a resubscribe must not remove the new subscription.".
t_unsubscribe_order(Config) ->
    Worker = ?config(worker, Config),
    Topic = <<"batch/resubscribe">>,
    ok = gen_server:call(Worker, {subscribe, Topic, self(), 0}),
    Requests = with_suspended(Worker, fun() ->
        First = request(Worker, <<"batch/first">>, self()),
        gen_server:cast(Worker, {unsubscribed, Topic, self(), 0}),
        Second = request(Worker, Topic, self()),
        [First, Second]
    end),
    await_replies(Requests),
    _ = sys:get_state(Worker),
    ?assertEqual([self()], emqx_broker:subscribers(Topic)),
    ?assert(emqx_router:has_route(Topic, node())).

-doc "A non-batched call keeps its own reply and separates subscription batches.".
t_other_call_order(Config) ->
    Worker = ?config(worker, Config),
    [First, Other, Second] = with_suspended(Worker, fun() ->
        [
            request(Worker, <<"batch/before-call">>, self()),
            gen_server:send_request(Worker, unexpected_request),
            request(Worker, <<"batch/after-call">>, self())
        ]
    end),
    ?assertEqual({reply, ok}, gen_server:receive_response(First, 5000)),
    ?assertEqual({reply, ignored}, gen_server:receive_response(Other, 5000)),
    ?assertEqual({reply, ok}, gen_server:receive_response(Second, 5000)),
    ?assertEqual(2, meck:num_calls(emqx_router, do_add_routes, '_')).

-doc "Schema v2 keeps router-syncer acknowledgements when batch_sync is enabled.".
t_syncer(Config) ->
    Worker = ?config(worker, Config),
    ok = emqx_config:put([broker, routing, batch_sync, enable_on], all),
    Topics = [<<"batch/syncer/1">>, <<"batch/syncer/2">>],
    Requests = with_suspended(Worker, fun() ->
        [request(Worker, Topic, self()) || Topic <- Topics]
    end),
    lists:foreach(
        fun(Request) ->
            {reply, Ref} = gen_server:receive_response(Request, 5000),
            ?assert(is_reference(Ref)),
            ?assertEqual(ok, emqx_router_syncer:wait(Ref))
        end,
        Requests
    ),
    [?assert(emqx_router:has_route(Topic, node())) || Topic <- Topics],
    ?assertEqual(0, meck:num_calls(emqx_router, do_add_routes, '_')).

request(Worker, Topic, SubPid) ->
    gen_server:send_request(Worker, {subscribe, Topic, SubPid, 0}).

with_suspended(Worker, Fun) ->
    ok = sys:suspend(Worker),
    try
        Fun()
    after
        ok = sys:resume(Worker)
    end.

await_replies(Requests) ->
    lists:foreach(
        fun(Request) -> ?assertEqual({reply, ok}, gen_server:receive_response(Request, 5000)) end,
        Requests
    ).
