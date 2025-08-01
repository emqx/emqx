%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_durable_test_timer).

%% API:
-export([init/0, apply_after/3, dead_hand/3, cancel/1]).

%% behavior callbacks:
-export([durable_timer_type/0, timer_introduced_in/0, handle_durable_timeout/2]).

-include("../src/internals.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

%%================================================================================
%% API functions
%%================================================================================

init() ->
    emqx_durable_timer:register_type(?MODULE).

apply_after(Key, Val, Delay) ->
    emqx_durable_timer:apply_after(durable_timer_type(), Key, Val, Delay).

dead_hand(Key, Val, Delay) ->
    emqx_durable_timer:dead_hand(durable_timer_type(), Key, Val, Delay).

cancel(Key) ->
    emqx_durable_timer:cancel(durable_timer_type(), Key).

%%================================================================================
%% behavior callbacks
%%================================================================================

durable_timer_type() -> 16#fffffffe.

timer_introduced_in() -> "6.0.0".

handle_durable_timeout(Key, Value) ->
    ?tp(info, ?tp_test_fire, #{key => Key, val => Value, type => durable_timer_type()}).

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================
