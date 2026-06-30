%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cth_log_capture).

-moduledoc """
Test helper to capture structured log reports emitted while running a function.

Installs a temporary `logger` handler that forwards report-style log events at or
above a given level to the calling process, runs the provided function, and
returns the captured reports in emission order.
""".

%% API
-export([capture/1, capture/2]).

%% logger handler callback
-export([log/2]).

-define(HANDLER_ID, ?MODULE).

-doc "Same as `capture(warning, Fun)`.".
-spec capture(fun(() -> term())) -> [map()].
capture(Fun) ->
    capture(warning, Fun).

-doc """
Run `Fun' while capturing log reports at or above `Level', returning the captured
reports (the `Data' maps passed to `?SLOG'/`?TRACE') in emission order.
""".
-spec capture(logger:level(), fun(() -> term())) -> [map()].
capture(Level, Fun) ->
    ok = logger:add_handler(?HANDLER_ID, ?MODULE, #{
        level => Level,
        config => #{test_pid => self()},
        filter_default => log,
        filters => []
    }),
    PrevLevel = emqx_logger:get_primary_log_level(),
    ok = emqx_logger:set_primary_log_level(Level),
    try
        _ = Fun(),
        collect([])
    after
        ok = emqx_logger:set_primary_log_level(PrevLevel),
        ok = logger:remove_handler(?HANDLER_ID)
    end.

%% @private logger handler callback.
log(#{msg := {report, Report}}, #{config := #{test_pid := Pid}}) when is_map(Report) ->
    Pid ! {?MODULE, Report},
    ok;
log(_Event, _Config) ->
    ok.

collect(Acc) ->
    receive
        {?MODULE, Report} -> collect([Report | Acc])
    after 200 -> lists:reverse(Acc)
    end.
