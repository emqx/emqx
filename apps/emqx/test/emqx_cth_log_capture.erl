%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

%% Test helper to capture structured log reports emitted while running a function.
%%
%% Installs a temporary `logger' handler that forwards report-style log events at
%% or above a given level to the calling process, runs the provided function, and
%% returns the captured reports in emission order.

-module(emqx_cth_log_capture).

%% API
-export([capture/1, capture/2]).

%% logger handler callback
-export([log/2]).

-define(HANDLER_ID, ?MODULE).

%% Same as capture(warning, Fun).
-spec capture(fun(() -> term())) -> [map()].
capture(Fun) ->
    capture(warning, Fun).

%% Run Fun while capturing log reports at or above Level, returning the captured
%% reports (the Data maps passed to ?SLOG/?TRACE) in emission order.
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
