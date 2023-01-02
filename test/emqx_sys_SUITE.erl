%%--------------------------------------------------------------------
%% Copyright (c) 2019-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_sys_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    application:load(emqx),
    ok = application:set_env(emqx, broker_sys_interval, 1),
    ok = application:set_env(emqx, broker_sys_heartbeat, 1),
    ok = emqx_logger:set_log_level(emergency),
    Config.

end_per_suite(_Config) ->
    application:unload(emqx),
    ok = emqx_logger:set_log_level(error),
    ok.

% t_version(_) ->
%     error('TODO').

% t_sysdescr(_) ->
%     error('TODO').

t_uptime(_) ->
    ?assert(is_list(emqx_sys:uptime())),
    ?assertEqual(<<"1 seconds">>, iolist_to_binary(emqx_sys:uptime(1))),
    ?assertEqual(<<"1 minutes, 0 seconds">>, iolist_to_binary(emqx_sys:uptime(60))),
    ?assertEqual(<<"1 hours, 0 minutes, 0 seconds">>, iolist_to_binary(emqx_sys:uptime(3600))),
    ?assertEqual(<<"1 hours, 1 minutes, 1 seconds">>, iolist_to_binary(emqx_sys:uptime(3661))),
    ?assertEqual(<<"1 days, 0 hours, 0 minutes, 0 seconds">>, iolist_to_binary(emqx_sys:uptime(86400))),
    lists:map(fun({D, H, M, S}) ->
        Expect = <<
            (integer_to_binary(D))/binary, " days, ",
            (integer_to_binary(H))/binary, " hours, ",
            (integer_to_binary(M))/binary, " minutes, ",
            (integer_to_binary(S))/binary, " seconds"
        >>,
        Actual = iolist_to_binary(emqx_sys:uptime(D * 86400 + H * 3600 + M * 60 + S)),
        ?assertEqual(Expect, Actual)
              end,
        [{1, 2, 3, 4}, {10, 20, 30, 40}, {2222, 3, 56, 59}, {59, 23, 59, 59}]).

% t_datetime(_) ->
%     error('TODO').

% t_sys_interval(_) ->
%     error('TODO').

% t_sys_heatbeat_interval(_) ->
%     error('TODO').

% t_info(_) ->
%     error('TODO').
