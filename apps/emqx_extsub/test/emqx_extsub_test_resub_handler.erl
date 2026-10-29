%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_extsub_test_resub_handler).

-moduledoc """
ExtSub handler for subscribe result tests.
The `result` subscription option selects the `handle_subscribe/4` result.
The state counts `{ok, State}` results for each topic filter.
Callbacks report to the calling process, which is the test process in registry tests.
""".

-behaviour(emqx_extsub_handler).

-export([
    handle_subscribe/4,
    handle_unsubscribe/4,
    handle_terminate/1,
    handle_delivered/4,
    handle_info/3
]).

handle_subscribe(_SubscribeType, #{subopts := #{result := ok}}, State0, TopicFilter) ->
    State = maps:update_with(TopicFilter, fun(N) -> N + 1 end, 1, emqx_maybe:define(State0, #{})),
    self() ! {subscribed, TopicFilter, State},
    {ok, State};
handle_subscribe(_SubscribeType, #{subopts := #{result := Result}}, _State, _TopicFilter) ->
    Result;
handle_subscribe(_SubscribeType, _SubscribeCtx, _State, _TopicFilter) ->
    ignore.

handle_unsubscribe(_UnsubscribeType, _UnsubscribeCtx, State, TopicFilter) ->
    maps:remove(TopicFilter, State).

handle_terminate(State) ->
    self() ! {terminated, State},
    ok.

handle_delivered(State, _AckCtx, _Msg, _Ack) ->
    {ok, State}.

handle_info(State, _InfoCtx, _Info) ->
    {ok, State}.
