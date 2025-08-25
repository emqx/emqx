%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_consumer_dispatchq_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(SLAB, {<<"0">>, 1}).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            {emqx_mq, emqx_mq_test_utils:cth_config()}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

%% Test fetching non-redispatched messages
t_fetch_initial(_Config) ->
    Q0 = emqx_mq_consumer_dispatchq:new(),
    empty = emqx_mq_consumer_dispatchq:fetch(Q0),
    Q1 = emqx_mq_consumer_dispatchq:add(Q0, [{?SLAB, 2}, {?SLAB, 1}]),
    ct:sleep(1),
    Q2 = emqx_mq_consumer_dispatchq:add(Q1, [{?SLAB, 3}]),
    {ok, [{?SLAB, 1}, {?SLAB, 2}, {?SLAB, 3}], Q3} = emqx_mq_consumer_dispatchq:fetch(Q2),
    empty = emqx_mq_consumer_dispatchq:fetch(Q3),
    Q4 = emqx_mq_consumer_dispatchq:add(Q3, [{?SLAB, 4}]),
    {ok, [{?SLAB, 4}], Q5} = emqx_mq_consumer_dispatchq:fetch(Q4),
    empty = emqx_mq_consumer_dispatchq:fetch(Q5).

%% Test fetching redispatched messages
t_fetch_redispatch(_Config) ->
    Q0 = emqx_mq_consumer_dispatchq:new(),
    Q1 = emqx_mq_consumer_dispatchq:add(Q0, [{?SLAB, 1}]),
    Q2 = emqx_mq_consumer_dispatchq:add_redispatch(Q1, {?SLAB, 2}, 100),
    Q3 = emqx_mq_consumer_dispatchq:add(Q2, [{?SLAB, 3}]),
    Q4 = emqx_mq_consumer_dispatchq:add_redispatch(Q3, {?SLAB, 4}, 50),
    {ok, [{?SLAB, 1}, {?SLAB, 3}], Q5} = emqx_mq_consumer_dispatchq:fetch(Q4),
    {delay, Delay0} = emqx_mq_consumer_dispatchq:fetch(Q5),
    ct:sleep(Delay0),
    {ok, [{?SLAB, 4}], Q6} = emqx_mq_consumer_dispatchq:fetch(Q5),
    {delay, Delay1} = emqx_mq_consumer_dispatchq:fetch(Q6),
    ct:sleep(Delay1),
    {ok, [{?SLAB, 2}], Q7} = emqx_mq_consumer_dispatchq:fetch(Q6),
    empty = emqx_mq_consumer_dispatchq:fetch(Q7).
