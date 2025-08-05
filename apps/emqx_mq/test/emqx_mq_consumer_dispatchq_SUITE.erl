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
            emqx_mq
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
    {[], Q1} = emqx_mq_consumer_dispatchq:fetch(Q0),
    Q2 = emqx_mq_consumer_dispatchq:add_initial(Q1, {?SLAB, 3}),
    Q3 = emqx_mq_consumer_dispatchq:add_initial(Q2, {?SLAB, 2}),
    Q4 = emqx_mq_consumer_dispatchq:add_initial(Q3, {?SLAB, 1}),
    {[{?SLAB, 1}, {?SLAB, 2}, {?SLAB, 3}], Q5} = emqx_mq_consumer_dispatchq:fetch(Q4),
    {[], Q6} = emqx_mq_consumer_dispatchq:fetch(Q5),
    Q7 = emqx_mq_consumer_dispatchq:add_initial(Q6, {?SLAB, 4}),
    {[{?SLAB, 4}], Q8} = emqx_mq_consumer_dispatchq:fetch(Q7),
    {[], _Q9} = emqx_mq_consumer_dispatchq:fetch(Q8).

%% Test fetching redispatched messages
t_fetch_redispatch(_Config) ->
    Q0 = emqx_mq_consumer_dispatchq:new(),
    Q1 = emqx_mq_consumer_dispatchq:add_initial(Q0, {?SLAB, 1}),
    Q2 = emqx_mq_consumer_dispatchq:add_redispatch(Q1, {?SLAB, 2}, 100),
    Q3 = emqx_mq_consumer_dispatchq:add_initial(Q2, {?SLAB, 3}),
    Q4 = emqx_mq_consumer_dispatchq:add_redispatch(Q3, {?SLAB, 4}, 50),
    {[{?SLAB, 1}, {?SLAB, 3}], Delay0, Q5} = emqx_mq_consumer_dispatchq:fetch(Q4),
    ct:sleep(Delay0),
    {[{?SLAB, 4}], Delay1, Q6} = emqx_mq_consumer_dispatchq:fetch(Q5),
    ct:sleep(Delay1),
    {[{?SLAB, 2}], _Q7} = emqx_mq_consumer_dispatchq:fetch(Q6).
