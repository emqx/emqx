%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_sync_request_cli).

-export([
    load/0,
    unload/0,
    cmd/1
]).

load() ->
    emqx_ctl:register_command(sync_request, {?MODULE, cmd}, []).

unload() ->
    emqx_ctl:unregister_command(sync_request).

cmd(["status"]) ->
    print_status(emqx_sync_request:status());
cmd(_) ->
    emqx_ctl:usage("sync_request status", "Show node-local Sync Request status.").

print_status(Status) ->
    Output = io_lib:format(
        "Counters since plugin start:~n"
        "sync_request.requests.total: ~w~n"
        "sync_request.requests.succeeded: ~w~n"
        "sync_request.requests.failed: ~w~n"
        "sync_request.requests.bad_request: ~w~n"
        "sync_request.requests.no_subscribers: ~w~n"
        "sync_request.requests.conflict: ~w~n"
        "sync_request.requests.too_many_requests: ~w~n"
        "sync_request.requests.dispatch_failed: ~w~n"
        "sync_request.requests.timeout: ~w~n"
        "sync_request.requests.internal_error: ~w~n"
        "~n"
        "Current gauges:~n"
        "sync_request.inflight_requests: ~w~n"
        "sync_request.pending_responses: ~w~n",
        [
            maps:get(requests_total, Status),
            maps:get(requests_succeeded, Status),
            maps:get(requests_failed, Status),
            maps:get(requests_bad_request, Status),
            maps:get(requests_no_subscribers, Status),
            maps:get(requests_conflict, Status),
            maps:get(requests_too_many_requests, Status),
            maps:get(requests_dispatch_failed, Status),
            maps:get(requests_timeout, Status),
            maps:get(requests_internal_error, Status),
            maps:get(inflight_requests, Status),
            maps:get(pending_responses, Status)
        ]
    ),
    emqx_ctl:print("~s", [Output]).
