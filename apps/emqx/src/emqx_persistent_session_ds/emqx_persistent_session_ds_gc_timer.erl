%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_persistent_session_ds_gc_timer).

-behaviour(emqx_durable_timer).

%% API:
-export([init/0]).
-export([on_connect/3, on_disconnect/3, delete/1]).

%% behavior callbacks:
-export([durable_timer_type/0, handle_durable_timeout/2, timer_introduced_in/0]).

%% internal exports:
-export([]).

-export_type([]).

-include_lib("snabbkaffe/include/trace.hrl").
-include("../emqx_tracepoints.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").

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
    emqx_persistent_session_ds_state:guard(),
    non_neg_integer()
) ->
    ok | emqx_ds:error(_).
on_connect(ClientId, Guard, ExpiryIntervalMS) when
    is_binary(ClientId), is_binary(Guard), is_integer(ExpiryIntervalMS)
->
    emqx_durable_timer:dead_hand(
        durable_timer_type(), ClientId, Guard, ExpiryIntervalMS
    ).

-spec on_disconnect(
    emqx_types:clientid(),
    emqx_persistent_session_ds_state:guard(),
    non_neg_integer()
) -> ok.
on_disconnect(ClientId, Guard, ExpiryIntervalMS) when
    is_binary(ClientId), is_binary(Guard), is_integer(ExpiryIntervalMS)
->
    warn_timeout(
        emqx_durable_timer:apply_after(
            durable_timer_type(), ClientId, Guard, ExpiryIntervalMS
        )
    ).

-spec delete(emqx_types:clientid()) -> ok.
delete(ClientId) ->
    emqx_durable_timer:cancel(durable_timer_type(), ClientId).

%%================================================================================
%% behavior callbacks
%%================================================================================

durable_timer_type() -> 16#DEAD5E55.

timer_introduced_in() -> "6.0.0".

handle_durable_timeout(SessionId, MaybeGuard) ->
    ?tp(debug, ?sessds_expired, #{id => SessionId, guard => MaybeGuard}),
    Guard =
        case MaybeGuard of
            <<>> ->
                %% Handle timers created on versions older than 6.1.x:
                %% those didn't check the guard and kept this value
                %% empty. This branch should not be triggered by the
                %% new code.
                '_';
            _ ->
                MaybeGuard
        end,
    emqx_persistent_session_ds:destroy_session(SessionId, Guard).

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

warn_timeout(ok) ->
    ok;
warn_timeout(?err_unrec(commit_timeout)) ->
    ?tp(warning, "sessds_gc_timer_commit_timeout", #{}),
    ok;
warn_timeout(Err) ->
    Err.
