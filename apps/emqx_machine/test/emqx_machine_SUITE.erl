%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_machine_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    %% CASE-SIDE-EFFICT:
    %%
    %% Running-Seq:
    %%   emqx_authz_api_mnesia_SUITE.erl
    %%   emqx_gateway_api_SUITE.erl
    %%   emqx_machine_SUITE.erl
    %%
    %% Reason:
    %%   the `emqx_machine_boot:ensure_apps_started()` will crashed
    %%   on starting `emqx_authz` with dirty confs, which caused the file
    %%   `.._build/test/lib/emqx_conf/etc/acl.conf` could not be found
    %%
    %% Workaround:
    %%   Unload emqx_authz to avoid reboot this application
    %%
    application:unload(emqx_authz),

    emqx_common_test_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:stop_apps([]).

t_shutdown_reboot(_Config) ->
    emqx_machine_boot:stop_apps(),
    false = emqx:is_running(node()),
    emqx_machine_boot:ensure_apps_started(),
    true = emqx:is_running(node()),
    ok = emqx_machine_boot:stop_apps(),
    false = emqx:is_running(node()).
