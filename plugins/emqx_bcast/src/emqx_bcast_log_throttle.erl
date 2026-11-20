%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_log_throttle).

%% Node-local throttling for plugin log messages. This deliberately does not
%% depend on the core's `log.throttling.msgs' list, so a plugin-only upgrade
%% keeps bounded logging on older EMQX versions. At most one log per message
%% type is emitted in each configured log-throttling window.

-export([init/0, allow/1]).

-define(KEY(Msg), {?MODULE, Msg}).
-define(LAST_LOG, 1).
-define(SUPPRESSED, 2).
-define(DEFAULT_WINDOW_MS, 60_000).

%% @doc (Re)initialize the per-node throttle state. Called from application
%% start and directly by tests that do not start the full plugin application.
init() ->
    WindowMs = configured_window_ms(),
    lists:foreach(
        fun(Msg) ->
            persistent_term:put(?KEY(Msg), atomics:new(3, [{signed, false}]))
        end,
        [
            bcast_batch_pub_device_quota_exceeded,
            bcast_batch_pub_global_quota_exceeded,
            bcast_batch_pub_intake_busy
        ]
    ),
    persistent_term:put({?MODULE, window_ms}, WindowMs),
    persistent_term:put(
        {?MODULE, monotonic_origin},
        erlang:monotonic_time(millisecond)
    ),
    ok.

%% @doc Return `{true, SuppressedSinceLastLog}' for the first message in a
%% window, otherwise `false'. The caller must check `logger:allow/2' first so
%% disabled log levels pay neither the map construction nor this call.
-spec allow(atom()) -> {true, non_neg_integer()} | false.
allow(Msg) ->
    case persistent_term:get(?KEY(Msg), undefined) of
        undefined ->
            %% The application starts this module before any API request can
            %% reach the plugin. Keeping a missing state visible is safer than
            %% silently dropping the first diagnostic during an unusual boot
            %% order.
            {true, 0};
        Ref ->
            allow(Ref, now_ms(), window_ms())
    end.

allow(Ref, Now, WindowMs) ->
    Last = atomics:get(Ref, ?LAST_LOG),
    case Last =:= 0 orelse Now - Last >= WindowMs of
        true ->
            case atomics:compare_exchange(Ref, ?LAST_LOG, Last, Now) of
                ok ->
                    {true, atomics:exchange(Ref, ?SUPPRESSED, 0)};
                _Actual ->
                    suppress(Ref)
            end;
        false ->
            suppress(Ref)
    end.

suppress(Ref) ->
    _ = atomics:add(Ref, ?SUPPRESSED, 1),
    false.

window_ms() ->
    persistent_term:get({?MODULE, window_ms}, ?DEFAULT_WINDOW_MS).

now_ms() ->
    Now = erlang:monotonic_time(millisecond),
    Origin = persistent_term:get(
        {?MODULE, monotonic_origin},
        erlang:monotonic_time(millisecond)
    ),
    max(1, Now - Origin + 1).

configured_window_ms() ->
    try emqx:get_config([log, throttling, time_window], 60) of
        Seconds when is_integer(Seconds), Seconds > 0 ->
            Seconds * 1000;
        _ ->
            ?DEFAULT_WINDOW_MS
    catch
        _:_ ->
            ?DEFAULT_WINDOW_MS
    end.
