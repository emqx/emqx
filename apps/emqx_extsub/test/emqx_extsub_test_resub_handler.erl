%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_extsub_test_resub_handler).

-moduledoc """
ExtSub handler for subscribe and destroy result tests.
The `result` subscription option selects the `handle_subscribe/4` result.
The state counts `{ok, State}` results for each topic filter.
Info messages and the terms in delivered message payloads are commands, see `handle_command/2`.
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

%% `{destroy, Topics}' and `destroy_all' mark the state and return the matching destroy result.
handle_delivered(State, _AckCtx, Msg, _Ack) ->
    handle_command(State, binary_to_term(emqx_message:payload(Msg))).

handle_info(State, _InfoCtx, Info) ->
    handle_command(State, Info).

handle_command(State, {destroy, TopicFilters}) ->
    {destroy, State#{marked => true}, TopicFilters};
handle_command(State, destroy_all) ->
    {destroy, State#{marked => true}};
handle_command(State, report) ->
    self() ! {state, State},
    {ok, State};
handle_command(State, _Command) ->
    {ok, State}.
