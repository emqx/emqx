%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_persistent_session_ds_gc_timer).

-behaviour(emqx_durable_timer).

%% API:
-export([init/0]).
-export([on_connect/2, on_disconnect/2, delete/1]).

%% behavior callbacks:
-export([durable_timer_type/0, handle_durable_timeout/2, timer_introduced_in/0]).

%% internal exports:
-export([]).

-export_type([]).

-include_lib("snabbkaffe/include/trace.hrl").
-include("../emqx_tracepoints.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

%%================================================================================
%% API functions
%%================================================================================

-spec init() -> ok.
init() ->
    emqx_durable_timer:register_type(?MODULE).

-spec on_connect(
    emqx_types:clientid(),
    non_neg_integer()
) ->
    ok | emqx_ds:error(_).
on_connect(ClientId, ExpiryIntervalMS) ->
    emqx_durable_timer:dead_hand(
        durable_timer_type(), ClientId, <<>>, ExpiryIntervalMS
    ).

-spec on_disconnect(
    emqx_types:clientid(),
    non_neg_integer()
) -> ok.
on_disconnect(ClientId, ExpiryIntervalMS) ->
    emqx_durable_timer:apply_after(
        durable_timer_type(), ClientId, <<>>, ExpiryIntervalMS
    ).

-spec delete(emqx_types:clientid()) -> ok.
delete(ClientId) ->
    emqx_durable_timer:cancel(durable_timer_type(), ClientId).

%%================================================================================
%% behavior callbacks
%%================================================================================

durable_timer_type() -> 16#DEAD5E55.

timer_introduced_in() -> "6.0.0".

handle_durable_timeout(SessionId, ChannelCookie) ->
    ?tp(debug, ?sessds_expired, #{id => SessionId, cookie => ChannelCookie}),
    emqx_persistent_session_ds:destroy_session(SessionId).

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================
