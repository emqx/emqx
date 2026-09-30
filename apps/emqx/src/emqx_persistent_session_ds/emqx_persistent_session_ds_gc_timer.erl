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
on_connect(ClientId, Cookie, ExpiryIntervalMS) ->
    %% TODO: don't mask errors and do the whole thing async-ly, so the
    %% session can install dead hand without blocking and with retry.
    case emqx_durable_timer:dead_hand(durable_timer_type(), ClientId, Cookie, ExpiryIntervalMS) of
        ok ->
            ok;
        Err ->
            ?tp(warning, sessds_failed_to_set_up_gc_timer, #{
                clientid => ClientId,
                reason => Err
            }),
            ok
    end.

-spec on_disconnect(
    emqx_types:clientid(),
    emqx_persistent_session_ds_state:guard(),
    non_neg_integer()
) -> ok | emqx_ds:error(_).
on_disconnect(ClientId, Cookie, ExpiryIntervalMS) ->
    warn_timeout(
        emqx_durable_timer:apply_after(
            durable_timer_type(), ClientId, Cookie, ExpiryIntervalMS
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

handle_durable_timeout(SessionId, Cookie) ->
    ?tp(debug, ?sessds_expired, #{id => SessionId, cookie => Cookie}),
    case Cookie of
        <<>> ->
            %% Legacy case: in the original version of the code (ca.
            %% 6.0.0) the timer didn't hold the guard and would
            %% destroy sessions indiscriminately. Emulate this
            %% behavior for backward compatibility. New code must not
            %% use this path.
            emqx_persistent_session_ds_state:delete(SessionId, '_');
        _ ->
            case emqx_persistent_session_ds_state:delete(SessionId, Cookie) of
                ok ->
                    ok;
                ?err_rec(Reason) ->
                    emqx_durable_timer:retry(Reason);
                ?err_unrec(_) ->
                    %% Session conflicts are ok
                    ok
            end
    end.

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
